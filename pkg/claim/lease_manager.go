// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	stderrors "errors"
	"sort"
	"strings"
	"sync"
	stdtime "time"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

const (
	// LeaseDuration is how long a claim stays valid without a renew.
	LeaseDuration = 300 * libtime.Second
	// TickInterval is how often the lease loop ticks.
	TickInterval = 10 * libtime.Second
	// RenewInterval is how often a held claim is renewed.
	RenewInterval = 60 * libtime.Second
	// FenceMargin is how long before the lease end the holder stops its Job.
	FenceMargin = 60 * libtime.Second
	// FenceLead is how much earlier than fenceAt a tick fences, so a 10 s tick
	// lands at or before fenceAt.
	FenceLead = 10 * libtime.Second
	// TakeoverGrace is how long after the lease end a takeover is allowed.
	TakeoverGrace = 30 * libtime.Second
	// JoblessGrace is how long a held claim's Job must be continuously absent
	// before the lease loop releases the claim.
	JoblessGrace = 300 * libtime.Second
	// JoblessCheckInterval is how often the lease loop asks JobControl whether a
	// held claim's Job is still running.
	JoblessCheckInterval = 60 * libtime.Second
)

// ErrLeaseNotExpired is returned by Takeover when the listed claim is still
// inside its lease or its lease_until cannot be parsed as RFC3339.
var ErrLeaseNotExpired = stderrors.New("lease not expired")

// HeldClaim is the manager's view of a claim it holds.
type HeldClaim struct {
	Key        string
	TaskID     string
	Sha        string
	LeaseUntil libtime.DateTime
}

// TakeoverResult is returned by Takeover. PreviousTaskID is the task id of the
// expired claim that was replaced, or "" when the key was absent and a create
// was used instead.
type TakeoverResult struct {
	Key            string
	TaskID         string
	Sha            string
	LeaseUntil     libtime.DateTime
	PreviousTaskID string
}

// LeaseManager owns this executor's claims for one cluster name.
type LeaseManager interface {
	Claim(ctx context.Context, key string, taskID string) (*HeldClaim, error)
	// ClaimPair creates both refs of a claim. The primary is written first; the
	// secondary is written only when it is non-empty. The claim is held only
	// when every write succeeded, or when a failed primary write was read back
	// as this manager's own. A secondary conflict releases the primary and
	// returns ErrLeaseConflict.
	ClaimPair(
		ctx context.Context,
		primary string,
		secondary string,
		taskID string,
	) (*HeldClaim, error)
	Release(ctx context.Context, key string) error
	// ReleaseAfterSuccess releases the claim like Release, but not before
	// SuccessGracePeriod has elapsed since completedAt. Until then the claim
	// stays held and keeps being renewed, so a peer cluster's Claim gets a 409
	// instead of a task another cluster has already run.
	ReleaseAfterSuccess(ctx context.Context, key string, completedAt libtime.DateTime) error
	Takeover(ctx context.Context, key string, taskID string) (*TakeoverResult, error)
	// TakeoverPair is Takeover over both refs of a claim. An empty secondary
	// behaves exactly like Takeover.
	TakeoverPair(
		ctx context.Context,
		primary string,
		secondary string,
		taskID string,
	) (*TakeoverResult, error)
	Tick(ctx context.Context) error
	Run(ctx context.Context) error
	Recover(ctx context.Context) error
	// HeldClaims returns a snapshot sorted by Key. Claims in pending Job
	// deletion or pending release are included: they are still held until the
	// pending action completes or the fence drops them.
	HeldClaims() []HeldClaim
	// AuditMismatch compares the claims this manager holds with the given set of
	// running Job task ids. It adds 1 to executor_claim_mismatch_total for every
	// running Job with no held claim and for every held claim with no running
	// Job, and logs which case each was. A task present in both is not counted.
	AuditMismatch(ctx context.Context, runningTaskIDs map[string]bool)
}

// NewLeaseManager constructs a LeaseManager. An empty cluster is an error.
// waiter is used only by Run; tests pass libtime.NewWaiterDuration() or the
// counterfeiter fake.
func NewLeaseManager(
	cluster string,
	refClient ClaimRefClient,
	jobControl JobControl,
	currentDateTime libtime.CurrentDateTimeGetter,
	waiter libtime.WaiterDuration,
) (LeaseManager, error) {
	if cluster == "" {
		return nil, stderrors.New("lease manager cluster must not be empty")
	}
	return &leaseManager{
		cluster:         cluster,
		refClient:       refClient,
		jobControl:      jobControl,
		currentDateTime: currentDateTime,
		waiter:          waiter,
		held:            map[string]*heldClaim{},
	}, nil
}

// heldClaim is the manager's internal state for one held claim.
type heldClaim struct {
	key           string
	taskID        string
	sha           string
	leaseUntil    libtime.DateTime
	leaseUntilStr string
	// secondKey and secondSha are the secondary ref of a dual claim. They are
	// empty when the derivation produced one key. The pair is one claim: a
	// release, fence or lost path acts on both refs together.
	secondKey           string
	secondSha           string
	lastRenewAt         libtime.DateTime
	renewNextTick       bool
	pendingJobDeletion  bool
	pendingDeletionPath string
	releasePending      bool
	needsRunningCheck   bool
	// holdUntil is set by ReleaseAfterSuccess: the claim stays held, and keeps
	// being renewed, until this instant, so a peer cannot take the task over in
	// the window between a Job succeeding and its result reaching the vault.
	holdUntil libtime.DateTime
	// joblessSince is the instant of the first consecutive check that saw the
	// Job not running; the zero value means "not currently jobless".
	joblessSince libtime.DateTime
	// lastJoblessCheckAt is the instant of the last IsJobRunning call made by the
	// jobless pass; the zero value means "never checked".
	lastJoblessCheckAt libtime.DateTime
}

type leaseManager struct {
	mu              sync.Mutex
	cluster         string
	refClient       ClaimRefClient
	jobControl      JobControl
	currentDateTime libtime.CurrentDateTimeGetter
	waiter          libtime.WaiterDuration
	held            map[string]*heldClaim
}

// countClaim increments executor_claim_total{result}.
func countClaim(result string) {
	metrics.ClaimTotal.WithLabelValues(result).Inc()
}

// countClaimRemoval increments executor_claim_total{result} and logs the one
// release line for a claim this manager stops holding. Every path that counts
// released, already_released, fenced or claim_lost calls it. A fence whose Job
// deletion fails is counted by countClaimFencePending instead;
// retryPendingDeletions logs its release line later without counting again.
func countClaimRemoval(key string, taskID string, result string, sha string) {
	metrics.ClaimTotal.WithLabelValues(result).Inc()
	if result == "claim_lost" {
		glog.Warningf(
			"event=claim_released key=%s task=%s result=%s sha=%s",
			key,
			taskID,
			result,
			sha,
		)
		return
	}
	glog.Infof(
		"event=claim_released key=%s task=%s result=%s sha=%s",
		key,
		taskID,
		result,
		sha,
	)
}

// countClaimFencePending counts a fence whose Job deletion failed. The claim
// stays held in pending Job deletion, so it logs the fence-pending line
// instead of a release line. retryPendingDeletions logs the release line,
// without counting again, once a later tick deletes the Job and the ref.
func countClaimFencePending(key string, taskID string) {
	countClaim("fenced")
	glog.Warningf("event=claim_fence_pending key=%s task=%s", key, taskID)
}

// fenceAt is the instant a holder must have stopped its Job.
func fenceAt(leaseUntil libtime.DateTime) libtime.DateTime {
	return leaseUntil.Add(-FenceMargin)
}

// fenceDue reports whether the fence must fire at now.
func fenceDue(now, leaseUntil libtime.DateTime) bool {
	return now.After(fenceAt(leaseUntil).Add(-FenceLead))
}

// renewDue reports whether a renew is due at now.
func renewDue(now, lastRenewAt libtime.DateTime) bool {
	return now.Sub(lastRenewAt) >= RenewInterval
}

// readBack lists the exact key and returns the entry whose ref equals
// "refs/"+key. A sibling under refs/<key>/x never matches.
func (m *leaseManager) readBack(ctx context.Context, key string) (*ClaimRef, error) {
	refs, err := m.refClient.List(ctx, key)
	if err != nil {
		return nil, err
	}
	want := "refs/" + key
	for _, ref := range refs {
		if ref.Ref == want {
			found := ref
			return &found, nil
		}
	}
	return nil, nil
}

// ownTriple reports whether ref carries exactly the write this manager made.
func (m *leaseManager) ownTriple(ref *ClaimRef, taskID, leaseUntilStr string) bool {
	return ref != nil &&
		ref.Cluster == m.cluster &&
		ref.TaskID == taskID &&
		ref.LeaseUntil == leaseUntilStr
}

// heldKeys returns the held keys sorted, so every tick phase is deterministic.
func (m *leaseManager) heldKeys() []string {
	keys := make([]string, 0, len(m.held))
	for key := range m.held {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// requestDeadline is now+RequestTimeout, capped at the earliest fenceAt of the
// held claims so no fence is delayed by a hanging request.
func (m *leaseManager) requestDeadline(now libtime.DateTime) libtime.DateTime {
	deadline := now.Add(RequestTimeout)
	var earliest libtime.DateTime
	found := false
	for _, held := range m.held {
		if held.pendingJobDeletion {
			continue
		}
		at := fenceAt(held.leaseUntil)
		if !found || at.Before(earliest) {
			earliest = at
			found = true
		}
	}
	if found && earliest.Before(deadline) {
		return earliest
	}
	return deadline
}

// requestContext derives the deadline-capped context used by every ref call
// made inside Tick.
func (m *leaseManager) requestContext(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	return context.WithDeadline(ctx, m.requestDeadline(m.currentDateTime.Now()).Time())
}

// bestEffortDelete deletes the ref and ignores the outcome.
func (m *leaseManager) bestEffortDelete(ctx context.Context, key, sha string) {
	reqCtx, cancel := m.requestContext(ctx)
	defer cancel()
	_ = m.refClient.Delete(reqCtx, key, sha)
}

// Claim creates the claim at key for taskID.
func (m *leaseManager) Claim(
	ctx context.Context,
	key string,
	taskID string,
) (*HeldClaim, error) {
	return m.ClaimPair(ctx, key, "", taskID)
}

// ClaimPair creates the claim at primary and, when secondary is non-empty, the
// claim at secondary too.
func (m *leaseManager) ClaimPair(
	ctx context.Context,
	primary string,
	secondary string,
	taskID string,
) (*HeldClaim, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.claimPairLocked(ctx, primary, secondary, taskID)
}

// findHeld returns the held claim that owns key, whether key is its primary or
// its secondary. The caller must hold m.mu.
func (m *leaseManager) findHeld(key string) (*heldClaim, bool) {
	if held, ok := m.held[key]; ok {
		return held, true
	}
	for _, held := range m.held {
		if held.secondKey == key {
			return held, true
		}
	}
	return nil, false
}

// claimPairLocked is ClaimPair without taking the lock.
func (m *leaseManager) claimPairLocked(
	ctx context.Context,
	primary string,
	secondary string,
	taskID string,
) (*HeldClaim, error) {
	if held, ok := m.findHeld(primary); ok {
		glog.V(3).Infof("claim pair: key %s already held by task %s", primary, held.taskID)
		countClaim("conflict")
		return nil, ErrLeaseConflict
	}
	if secondary != "" {
		if _, ok := m.findHeld(secondary); ok {
			countClaim("conflict")
			return nil, ErrLeaseConflict
		}
	}
	now := m.currentDateTime.Now()
	leaseUntil := now.Add(LeaseDuration)
	leaseUntilStr := formatLeaseUntil(leaseUntil)
	primarySha, recovered, err := m.createPrimary(ctx, primary, taskID, leaseUntil, leaseUntilStr)
	if err != nil {
		return nil, err
	}
	if secondary == "" {
		held := m.storeClaim(
			primary,
			"",
			taskID,
			primarySha,
			"",
			leaseUntil,
			leaseUntilStr,
			now,
		)
		countClaimCreated(recovered)
		return held, nil
	}
	secondarySha, err := m.refClient.Put(ctx, secondary, "", m.cluster, taskID, leaseUntil)
	if err == nil {
		held := m.storeClaim(
			primary,
			secondary,
			taskID,
			primarySha,
			secondarySha,
			leaseUntil,
			leaseUntilStr,
			now,
		)
		logDualAcquired(primary, secondary, taskID)
		countClaimCreated(recovered)
		return held, nil
	}
	if !errors.Is(err, ErrLeaseConflict) {
		_ = m.refClient.Delete(ctx, primary, primarySha)
		countClaim("error")
		return nil, err
	}
	if held, ok := m.displaceSecondary(
		ctx,
		primary,
		secondary,
		taskID,
		primarySha,
		leaseUntil,
		leaseUntilStr,
		now,
	); ok {
		return held, nil
	}
	_ = m.refClient.Delete(ctx, primary, primarySha)
	logDualConflict(secondary, taskID)
	countClaim("conflict")
	return nil, err
}

// createPrimary writes the primary ref and reports whether the write was
// recovered by reading it back. It counts the conflict or error itself.
func (m *leaseManager) createPrimary(
	ctx context.Context,
	key string,
	taskID string,
	leaseUntil libtime.DateTime,
	leaseUntilStr string,
) (string, bool, error) {
	sha, err := m.refClient.Put(ctx, key, "", m.cluster, taskID, leaseUntil)
	if err == nil {
		return sha, false, nil
	}
	if errors.Is(err, ErrLeaseConflict) {
		countClaim("conflict")
		return "", false, err
	}
	ref, readErr := m.readBack(ctx, key)
	if readErr != nil {
		countClaim("error")
		return "", false, errors.Wrapf(
			ctx,
			readErr,
			"claim %s: read back after create failed (%v)",
			key,
			err,
		)
	}
	if m.ownTriple(ref, taskID, leaseUntilStr) {
		return ref.Sha, true, nil
	}
	countClaim("error")
	return "", false, err
}

// displaceSecondary replaces a secondary ref left behind by a crashed or
// claim-lost predecessor once its lease has expired. It reports whether the
// pair is now held and counts taken_over on success.
func (m *leaseManager) displaceSecondary(
	ctx context.Context,
	primary string,
	secondary string,
	taskID string,
	primarySha string,
	leaseUntil libtime.DateTime,
	leaseUntilStr string,
	now libtime.DateTime,
) (*HeldClaim, bool) {
	listed, readErr := m.readBack(ctx, secondary)
	if readErr != nil || listed == nil {
		return nil, false
	}
	parsed, parseErr := stdtime.Parse(stdtime.RFC3339, listed.LeaseUntil)
	if parseErr != nil {
		return nil, false
	}
	if now.Before(libtime.DateTime(parsed).Add(TakeoverGrace)) {
		return nil, false
	}
	sha, err := m.refClient.Put(ctx, secondary, listed.Sha, m.cluster, taskID, leaseUntil)
	if err != nil {
		return nil, false
	}
	held := m.storeClaim(
		primary,
		secondary,
		taskID,
		primarySha,
		sha,
		leaseUntil,
		leaseUntilStr,
		now,
	)
	logDualAcquired(primary, secondary, taskID)
	countClaim("taken_over")
	return held, true
}

// countClaimCreated counts a fresh claim or one recovered from a read-back.
func countClaimCreated(recovered bool) {
	if recovered {
		countClaim("recovered")
		return
	}
	countClaim("claimed")
}

// storeClaim records a held claim and returns its public view.
func (m *leaseManager) storeClaim(
	primaryKey string,
	secondaryKey string,
	taskID string,
	primarySha string,
	secondarySha string,
	leaseUntil libtime.DateTime,
	leaseUntilStr string,
	now libtime.DateTime,
) *HeldClaim {
	m.held[primaryKey] = &heldClaim{
		key:           primaryKey,
		taskID:        taskID,
		sha:           primarySha,
		secondKey:     secondaryKey,
		secondSha:     secondarySha,
		leaseUntil:    leaseUntil,
		leaseUntilStr: leaseUntilStr,
		lastRenewAt:   now,
	}
	return &HeldClaim{
		Key:        primaryKey,
		TaskID:     taskID,
		Sha:        primarySha,
		LeaseUntil: leaseUntil,
	}
}

// logDualAcquired records one successful dual acquire.
func logDualAcquired(primary, secondary, taskID string) {
	glog.Infof("event=claim_dual_acquired key=%s second=%s task=%s", primary, secondary, taskID)
}

// logDualConflict records one secondary conflict that released the primary.
func logDualConflict(secondary, taskID string) {
	glog.Warningf("event=claim_dual_conflict key=%s task=%s", secondary, taskID)
}

// Release deletes the held claim. A 409 means already released and is nil.
// A claim inside its success hold is kept until the hold expires.
func (m *leaseManager) Release(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	held, ok := m.findHeld(key)
	if !ok {
		return nil
	}
	// A success hold outranks a plain release. The terminal phase reaches this
	// cluster's vault view before a peer's does, so deleting here would end the
	// hold exactly while the peer still reads the task as eligible (observed
	// 2026-10-23: task 70688157 re-ran on sun 149 s after succeeding on nuke
	// prod). The hold's own expiry in releaseExpiredHolds releases it.
	if m.currentDateTime.Now().Time().Before(held.holdUntil.Time()) {
		glog.Infof(
			"event=release_kept_success_grace key=%s task_id=%s hold_until=%s",
			key,
			held.taskID,
			held.holdUntil.Time().UTC().Format(stdtime.RFC3339),
		)
		return nil
	}
	return m.releaseLocked(ctx, held)
}

// ReleaseAfterSuccess is Release, but it refuses to let go until
// SuccessGracePeriod has elapsed since completedAt.
//
// The gap it covers: a succeeded Job means the agent published its result, but
// the vault learns about it on the controller's own cycle. In that gap a peer
// cluster still reads the task as eligible and has no record that it just ran —
// the spawn path's success record is per-pod memory, so a peer that starts cold
// has never heard of it. The claim is the only artefact both clusters share, so
// holding it is what makes the peer's Claim fail (409) instead of running the
// task a second time.
//
// The hold is bounded on both sides. It never outlives the grace window: once
// holdUntil passes, the next Tick releases it through releaseExpiredHolds. And
// it cannot outlive the lease — a held claim keeps being renewed, so a renewal
// failure fences it exactly as for any other held claim.
func (m *leaseManager) ReleaseAfterSuccess(
	ctx context.Context,
	key string,
	completedAt libtime.DateTime,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	held, ok := m.findHeld(key)
	if !ok {
		return nil
	}
	now := m.currentDateTime.Now().Time()
	holdUntil := completedAt.Time().Add(SuccessGracePeriod)
	if holdUntil.After(now.Add(SuccessGracePeriod)) {
		// A completion stamped in the future — clock skew between the API server
		// and this pod — must not stretch the hold. Cap it at one window from now.
		holdUntil = now.Add(SuccessGracePeriod)
	}
	if now.Before(holdUntil) {
		// Extend, never shorten: an older completion must not pull a newer hold in.
		if holdUntil.After(held.holdUntil.Time()) {
			held.holdUntil = libtime.DateTime(holdUntil)
		}
		glog.Infof(
			"event=release_deferred_success_grace key=%s task_id=%s hold_until=%s",
			key,
			held.taskID,
			held.holdUntil.Time().UTC().Format(stdtime.RFC3339),
		)
		return nil
	}
	// A stale success — an older Job of the same task re-reported after a newer
	// run already took a hold — must not release the claim that hold protects.
	if now.Before(held.holdUntil.Time()) {
		glog.Infof(
			"event=release_kept_success_grace key=%s task_id=%s hold_until=%s",
			key,
			held.taskID,
			held.holdUntil.Time().UTC().Format(stdtime.RFC3339),
		)
		return nil
	}
	return m.releaseLocked(ctx, held)
}

// deleteRef deletes one ref and reports "released", or "already_released" when
// the store answered 409. Any other error is returned unchanged.
func (m *leaseManager) deleteRef(ctx context.Context, key, sha string) (string, error) {
	err := m.refClient.Delete(ctx, key, sha)
	if err == nil {
		return "released", nil
	}
	if errors.Is(err, ErrLeaseConflict) {
		return "already_released", nil
	}
	return "", err
}

// deleteRefWithDeadline deletes one ref under the tick's deadline-capped
// context and reports the same labels as deleteRef.
func (m *leaseManager) deleteRefWithDeadline(
	ctx context.Context,
	key, sha string,
) (string, error) {
	reqCtx, cancel := m.requestContext(ctx)
	defer cancel()
	return m.deleteRef(reqCtx, key, sha)
}

// pairRemovalResult combines the two ref outcomes into one removal label: only
// a pair whose refs were both already gone counts as already_released.
func pairRemovalResult(primary, secondary string) string {
	if primary == "already_released" && secondary == "already_released" {
		return "already_released"
	}
	return "released"
}

// releaseLocked deletes both refs of a held claim as one unit. The caller
// holds m.mu. A non-409 failure on either ref leaves the whole claim
// pending-release so the retry pass finishes it; the primary is never counted
// as released while the secondary is still there.
func (m *leaseManager) releaseLocked(ctx context.Context, held *heldClaim) error {
	running, err := m.jobControl.IsJobRunning(ctx, held.taskID)
	if err != nil {
		countClaim("error")
		return errors.Wrapf(ctx, err, "check job running before release")
	}
	if running {
		// Something upstream says this task is over, but this cluster's Job for
		// it has not finished. Dropping the claim here would stop renewing it and
		// stop fencing that Job — and because the fence only iterates the held
		// set, the Job would then run on unclaimed while another cluster could
		// legitimately take the task over. Keep the claim: the loop keeps it
		// alive, and the Job's own terminal event releases it.
		glog.Infof(
			"event=release_deferred_job_running key=%s task_id=%s",
			held.key,
			held.taskID,
		)
		return nil
	}
	primaryResult, err := m.deleteRef(ctx, held.key, held.sha)
	if err != nil {
		held.releasePending = true
		countClaim("error")
		return err
	}
	if held.secondKey == "" {
		delete(m.held, held.key)
		countClaimRemoval(held.key, held.taskID, primaryResult, held.sha)
		return nil
	}
	secondaryResult, err := m.deleteRef(ctx, held.secondKey, held.secondSha)
	if err != nil {
		held.releasePending = true
		countClaim("error")
		return err
	}
	delete(m.held, held.key)
	countClaimRemoval(
		held.key,
		held.taskID,
		pairRemovalResult(primaryResult, secondaryResult),
		held.sha,
	)
	return nil
}

// Takeover replaces an expired claim with one for taskID, using one CAS write.
func (m *leaseManager) Takeover(
	ctx context.Context,
	key string,
	taskID string,
) (*TakeoverResult, error) {
	return m.TakeoverPair(ctx, key, "", taskID)
}

// TakeoverPair replaces an expired claim pair with one for taskID. An empty
// secondary takes the single-key path unchanged.
func (m *leaseManager) TakeoverPair(
	ctx context.Context,
	primary string,
	secondary string,
	taskID string,
) (*TakeoverResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.findHeld(primary); ok {
		countClaim("conflict")
		return nil, ErrLeaseConflict
	}
	if secondary != "" {
		if _, ok := m.findHeld(secondary); ok {
			countClaim("conflict")
			return nil, ErrLeaseConflict
		}
	}
	primaryRef, err := m.readBack(ctx, primary)
	if err != nil {
		countClaim("error")
		return nil, err
	}
	if primaryRef == nil {
		held, claimErr := m.claimPairLocked(ctx, primary, secondary, taskID)
		if claimErr != nil {
			return nil, claimErr
		}
		return &TakeoverResult{
			Key:            held.Key,
			TaskID:         held.TaskID,
			Sha:            held.Sha,
			LeaseUntil:     held.LeaseUntil,
			PreviousTaskID: "",
		}, nil
	}
	if secondary == "" {
		return m.takeoverExpired(ctx, primary, taskID, primaryRef)
	}
	return m.takeoverPairExpired(ctx, primary, secondary, taskID, primaryRef)
}

// takeoverPairExpired replaces both refs of an expired pair.
func (m *leaseManager) takeoverPairExpired(
	ctx context.Context,
	primary string,
	secondary string,
	taskID string,
	primaryRef *ClaimRef,
) (*TakeoverResult, error) {
	now := m.currentDateTime.Now()
	secondaryRef, err := m.takeoverSecondaryRef(ctx, secondary, now)
	if err != nil {
		return nil, err
	}
	if err := m.checkPrimaryTakeoverAllowed(now, primaryRef); err != nil {
		return nil, err
	}
	newLease := now.Add(LeaseDuration)
	newLeaseStr := formatLeaseUntil(newLease)
	primarySha, err := m.takeoverPrimary(ctx, primary, taskID, primaryRef, newLease, newLeaseStr)
	if err != nil {
		return nil, err
	}
	secondarySha, err := m.takeoverSecondary(
		ctx,
		primary,
		secondary,
		taskID,
		secondaryRef,
		primarySha,
		newLease,
	)
	if err != nil {
		return nil, err
	}
	m.storeClaim(
		primary,
		secondary,
		taskID,
		primarySha,
		secondarySha,
		newLease,
		newLeaseStr,
		now,
	)
	logDualAcquired(primary, secondary, taskID)
	countClaim("taken_over")
	return &TakeoverResult{
		Key:            primary,
		TaskID:         taskID,
		Sha:            primarySha,
		LeaseUntil:     newLease,
		PreviousTaskID: primaryRef.TaskID,
	}, nil
}

// takeoverSecondaryRef reads the secondary ref and refuses a takeover while
// its lease is still live. A missing ref, or one whose lease cannot be parsed,
// does not block.
func (m *leaseManager) takeoverSecondaryRef(
	ctx context.Context,
	secondary string,
	now libtime.DateTime,
) (*ClaimRef, error) {
	ref, err := m.readBack(ctx, secondary)
	if err != nil {
		countClaim("error")
		return nil, err
	}
	if ref == nil {
		return nil, nil
	}
	parsed, parseErr := stdtime.Parse(stdtime.RFC3339, ref.LeaseUntil)
	if parseErr == nil && now.Before(libtime.DateTime(parsed).Add(TakeoverGrace)) {
		countClaim("takeover_too_early")
		return nil, ErrLeaseNotExpired
	}
	return ref, nil
}

// checkPrimaryTakeoverAllowed refuses a takeover while the primary lease is
// live or its lease_until cannot be parsed.
func (m *leaseManager) checkPrimaryTakeoverAllowed(
	now libtime.DateTime,
	ref *ClaimRef,
) error {
	parsed, parseErr := stdtime.Parse(stdtime.RFC3339, ref.LeaseUntil)
	if parseErr != nil {
		countClaim("takeover_too_early")
		glog.Warningf(
			"claim takeover: unparseable lease_until ref=%s value=%q",
			ref.Ref,
			ref.LeaseUntil,
		)
		return ErrLeaseNotExpired
	}
	if now.Before(libtime.DateTime(parsed).Add(TakeoverGrace)) {
		countClaim("takeover_too_early")
		return ErrLeaseNotExpired
	}
	return nil
}

// takeoverPrimary replaces the primary ref with one CAS write and returns the
// new sha.
func (m *leaseManager) takeoverPrimary(
	ctx context.Context,
	primary string,
	taskID string,
	primaryRef *ClaimRef,
	newLease libtime.DateTime,
	newLeaseStr string,
) (string, error) {
	sha, err := m.refClient.Put(ctx, primary, primaryRef.Sha, m.cluster, taskID, newLease)
	if err == nil {
		return sha, nil
	}
	if errors.Is(err, ErrLeaseConflict) {
		countClaim("conflict")
		return "", err
	}
	listed, readErr := m.readBack(ctx, primary)
	if readErr != nil || !m.ownTriple(listed, taskID, newLeaseStr) {
		countClaim("error")
		return "", err
	}
	return listed.Sha, nil
}

// takeoverSecondary replaces the secondary ref. A failed write releases the
// primary again so the pair is never half-held.
func (m *leaseManager) takeoverSecondary(
	ctx context.Context,
	primary string,
	secondary string,
	taskID string,
	secondaryRef *ClaimRef,
	primarySha string,
	newLease libtime.DateTime,
) (string, error) {
	expectedSha := ""
	if secondaryRef != nil {
		expectedSha = secondaryRef.Sha
	}
	sha, err := m.refClient.Put(ctx, secondary, expectedSha, m.cluster, taskID, newLease)
	if err == nil {
		return sha, nil
	}
	_ = m.refClient.Delete(ctx, primary, primarySha)
	if errors.Is(err, ErrLeaseConflict) {
		countClaim("conflict")
	} else {
		countClaim("error")
	}
	return "", err
}

// takeoverExpired replaces the expired claim in ref.
func (m *leaseManager) takeoverExpired(
	ctx context.Context,
	key string,
	taskID string,
	ref *ClaimRef,
) (*TakeoverResult, error) {
	parsed, parseErr := stdtime.Parse(stdtime.RFC3339, ref.LeaseUntil)
	if parseErr != nil {
		countClaim("takeover_too_early")
		glog.Warningf(
			"claim takeover: unparseable lease_until ref=%s value=%q",
			ref.Ref,
			ref.LeaseUntil,
		)
		return nil, ErrLeaseNotExpired
	}
	leaseUntil := libtime.DateTime(parsed)
	now := m.currentDateTime.Now()
	if now.Before(leaseUntil.Add(TakeoverGrace)) {
		countClaim("takeover_too_early")
		return nil, ErrLeaseNotExpired
	}
	newLease := now.Add(LeaseDuration)
	newLeaseStr := formatLeaseUntil(newLease)
	sha, err := m.refClient.Put(ctx, key, ref.Sha, m.cluster, taskID, newLease)
	if err == nil {
		m.storeClaim(key, "", taskID, sha, "", newLease, newLeaseStr, now)
		countClaim("taken_over")
		return &TakeoverResult{
			Key:            key,
			TaskID:         taskID,
			Sha:            sha,
			LeaseUntil:     newLease,
			PreviousTaskID: ref.TaskID,
		}, nil
	}
	if errors.Is(err, ErrLeaseConflict) {
		countClaim("conflict")
		return nil, err
	}
	listed, readErr := m.readBack(ctx, key)
	if readErr != nil || !m.ownTriple(listed, taskID, newLeaseStr) {
		countClaim("error")
		return nil, err
	}
	m.storeClaim(key, "", taskID, listed.Sha, "", newLease, newLeaseStr, now)
	countClaim("recovered")
	return &TakeoverResult{
		Key:            key,
		TaskID:         taskID,
		Sha:            listed.Sha,
		LeaseUntil:     newLease,
		PreviousTaskID: ref.TaskID,
	}, nil
}

// HeldClaims returns a copy of the held claims sorted by key.
func (m *leaseManager) HeldClaims() []HeldClaim {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]HeldClaim, 0, len(m.held))
	for _, held := range m.held {
		result = append(result, HeldClaim{
			Key:        held.key,
			TaskID:     held.taskID,
			Sha:        held.sha,
			LeaseUntil: held.leaseUntil,
		})
		if held.secondKey != "" {
			result = append(result, HeldClaim{
				Key:        held.secondKey,
				TaskID:     held.taskID,
				Sha:        held.secondSha,
				LeaseUntil: held.leaseUntil,
			})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result
}

// Recover re-adopts, releases or fences the claims this cluster held before a
// restart. It never sends a create PUT and never touches another cluster.
func (m *leaseManager) Recover(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	refs, err := m.refClient.List(ctx, "claims/")
	if err != nil {
		countClaim("error")
		return err
	}
	for _, ref := range refs {
		// Streak refs are not claims: skip them by key before any cluster
		// comparison, so an executor whose cluster is the streak sentinel still
		// never adopts, renews, fences or releases one.
		if strings.HasPrefix(strings.TrimPrefix(ref.Ref, "refs/"), RedriveStreakPrefix) {
			continue
		}
		if ref.Cluster != m.cluster {
			continue
		}
		m.recoverOne(ctx, ref)
	}
	return nil
}

// recoverOne applies the restart rule to one own-cluster claim.
func (m *leaseManager) recoverOne(ctx context.Context, ref ClaimRef) {
	now := m.currentDateTime.Now()
	key := strings.TrimPrefix(ref.Ref, "refs/")
	leaseUntil := m.recoverLeaseUntil(now, ref)
	if fenceDue(now, leaseUntil) {
		m.recoverFence(ctx, key, ref, leaseUntil)
		return
	}
	m.recoverAdopt(ctx, key, ref, leaseUntil, now)
}

// recoverLeaseUntil parses the listed lease. An unparseable value is treated
// as already past, so the entry is fenced rather than adopted.
func (m *leaseManager) recoverLeaseUntil(
	now libtime.DateTime,
	ref ClaimRef,
) libtime.DateTime {
	parsed, err := stdtime.Parse(stdtime.RFC3339, ref.LeaseUntil)
	if err != nil {
		countClaim("error")
		glog.Warningf(
			"claim recovery: unparseable lease_until ref=%s value=%q",
			ref.Ref,
			ref.LeaseUntil,
		)
		return now.Add(-LeaseDuration)
	}
	return libtime.DateTime(parsed)
}

// recoverFence stops the Job of a claim too close to its lease end and drops
// the ref. A failed Job deletion leaves the claim in pending deletion.
func (m *leaseManager) recoverFence(
	ctx context.Context,
	key string,
	ref ClaimRef,
	leaseUntil libtime.DateTime,
) {
	running, runErr := m.jobControl.IsJobRunning(ctx, ref.TaskID)
	if runErr != nil {
		countClaim("error")
	}
	if running || runErr != nil {
		if err := m.jobControl.DeleteJob(ctx, ref.TaskID); err != nil {
			countClaim("error")
			countClaimFencePending(key, ref.TaskID)
			m.held[key] = &heldClaim{
				key:                 key,
				taskID:              ref.TaskID,
				sha:                 ref.Sha,
				leaseUntil:          leaseUntil,
				leaseUntilStr:       ref.LeaseUntil,
				pendingJobDeletion:  true,
				pendingDeletionPath: "fence",
			}
			return
		}
	}
	m.bestEffortDelete(ctx, key, ref.Sha)
	countClaimRemoval(key, ref.TaskID, "fenced", ref.Sha)
}

// recoverAdopt holds, releases or re-checks the claim of a Job that may still
// be running.
func (m *leaseManager) recoverAdopt(
	ctx context.Context,
	key string,
	ref ClaimRef,
	leaseUntil libtime.DateTime,
	now libtime.DateTime,
) {
	running, runErr := m.jobControl.IsJobRunning(ctx, ref.TaskID)
	if runErr != nil {
		countClaim("error")
		m.held[key] = &heldClaim{
			key:               key,
			taskID:            ref.TaskID,
			sha:               ref.Sha,
			leaseUntil:        leaseUntil,
			leaseUntilStr:     ref.LeaseUntil,
			lastRenewAt:       now,
			renewNextTick:     true,
			needsRunningCheck: true,
		}
		return
	}
	if running {
		m.held[key] = &heldClaim{
			key:           key,
			taskID:        ref.TaskID,
			sha:           ref.Sha,
			leaseUntil:    leaseUntil,
			leaseUntilStr: ref.LeaseUntil,
			lastRenewAt:   now,
			renewNextTick: true,
		}
		countClaim("adopted")
		return
	}
	err := m.refClient.Delete(ctx, key, ref.Sha)
	switch {
	case err == nil:
		countClaimRemoval(key, ref.TaskID, "released", ref.Sha)
	case errors.Is(err, ErrLeaseConflict):
		countClaimRemoval(key, ref.TaskID, "already_released", ref.Sha)
	default:
		countClaim("error")
	}
}
