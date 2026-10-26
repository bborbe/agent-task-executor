// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	stderrors "errors"
	"sort"
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

// leaseSeconds is the TTL every acquire and renew asks for, in the whole
// seconds the typed API speaks. Deriving it from LeaseDuration keeps one
// constant as the source of the lease length instead of two that can drift.
func leaseSeconds() int {
	return int(LeaseDuration.Duration() / stdtime.Second)
}

// claimantTaskID is the task id a claim write carries.
//
// The two halves are not symmetric and the server enforces only one: a
// repository or per-PR claim REQUIRES the claiming run's task id (400
// missing_task_id without it), while a task claim must carry none because the
// server derives the task id from the address it was given. Returning the wrong
// side here is refused locally by claimantTaskIDError rather than surfacing as a
// 400 from the server.
func claimantTaskID(target ClaimTarget, taskID string) string {
	if target.TaskID != "" {
		return ""
	}
	return taskID
}

// HeldClaim is the manager's view of a claim it holds.
type HeldClaim struct {
	Key        string
	TaskID     string
	LeaseToken string
	LeaseUntil libtime.DateTime
}

// TakeoverResult is returned by Takeover. PreviousTaskID is the task id of the
// expired claim that was replaced, or "" when the key was absent and a create
// was used instead.
type TakeoverResult struct {
	Key            string
	TaskID         string
	LeaseToken     string
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

// NewLeaseManager constructs a LeaseManager. An empty cluster is an error, and
// so is a nil client: the typed claim client is the only way this manager
// reaches a claim, so a nil one is a construction failure rather than a
// degraded mode that silently holds nothing.
//
// waiter is used only by Run; tests pass libtime.NewWaiterDuration() or the
// counterfeiter fake.
func NewLeaseManager(
	cluster string,
	claimClient ClaimClient,
	jobControl JobControl,
	currentDateTime libtime.CurrentDateTimeGetter,
	waiter libtime.WaiterDuration,
) (LeaseManager, error) {
	if cluster == "" {
		return nil, stderrors.New("lease manager cluster must not be empty")
	}
	if claimClient == nil {
		return nil, stderrors.New("lease manager claim client must not be nil")
	}
	return &leaseManager{
		cluster:         cluster,
		claimClient:     claimClient,
		jobControl:      jobControl,
		currentDateTime: currentDateTime,
		waiter:          waiter,
		held:            map[string]*heldClaim{},
	}, nil
}

// heldClaim is the manager's internal state for one held claim.
type heldClaim struct {
	key string
	// target is what the claim is held ON, recovered from the key so every
	// renew and release can address it without a caller supplying the target
	// again.
	target     ClaimTarget
	taskID     string
	leaseToken string
	leaseUntil libtime.DateTime
	// secondKey and secondTarget are the secondary ref of a dual claim. They are
	// empty when the derivation produced one key. The pair is one claim: a
	// release, fence or lost path acts on both refs together.
	secondKey           string
	secondTarget        ClaimTarget
	secondToken         string
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
	claimClient     ClaimClient
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
func countClaimRemoval(key string, taskID string, result string, leaseToken string) {
	metrics.ClaimTotal.WithLabelValues(result).Inc()
	if result == "claim_lost" {
		glog.Warningf(
			"event=claim_released key=%s task=%s result=%s token=%s",
			key,
			taskID,
			result,
			leaseToken,
		)
		return
	}
	glog.Infof(
		"event=claim_released key=%s task=%s result=%s token=%s",
		key,
		taskID,
		result,
		leaseToken,
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

// ReadBackTimeout bounds the recovery read that follows a claim write which
// failed without a lost-lease answer. It is a fresh window, deliberately not a
// share of the caller's: the caller's request context has already expired in
// exactly the case this read exists to recover, so deriving from it would kill
// the recovery at the moment it is needed.
//
// It is deliberately shorter than RequestTimeout, which the client applies to
// the same call. The read runs uninterruptibly (WithoutCancel) while the caller
// holds the lease manager's lock, so this window also bounds how long a
// shutdown or a fence can be held up by a store that has stopped answering.
const ReadBackTimeout = 10 * libtime.Second

// recoveryContext returns the bounded context used by the claim recovery path:
// the read that proves a failed write landed, and the compensating release
// that unwinds a half-written claim pair. Both run after the caller's context
// may already be dead, so both must survive its cancellation.
func recoveryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), ReadBackTimeout.Duration())
}

// readBackRecovery is readBack for the create and takeover recovery paths. It
// drops the caller's deadline and cancellation with context.WithoutCancel and
// applies its own bound, so a write that failed after the caller's context
// expired can still be read back and recovered. Context values are preserved.
//
// Without this, a caller whose context expired mid-acquire — the reconcile pass
// bounds the whole pass, and the client caps the write at RequestTimeout — left
// the read dead on the already-expired context. An acquire that had in fact
// landed was then recorded as an error, leaving a lease nobody owned until it
// expired, and the next admission re-ran the task.
func (m *leaseManager) readBackRecovery(
	ctx context.Context,
	target ClaimTarget,
) (*Claim, error) {
	reqCtx, cancel := recoveryContext(ctx)
	defer cancel()
	return m.readBack(reqCtx, target)
}

// releaseRecovery is the compensating release that unwinds a half-written claim
// pair. It ignores the outcome, like the other best-effort releases, but runs on
// recoveryContext: a recovered primary is unwound in exactly the case the
// caller's context is already dead, so a caller-context release would be a no-op
// and would leak the claim it was recovering.
func (m *leaseManager) releaseRecovery(ctx context.Context, target ClaimTarget, token string) {
	reqCtx, cancel := recoveryContext(ctx)
	defer cancel()
	_ = m.claimClient.Release(reqCtx, target, token)
}

// readBack returns the claim on target, or nil when it is free. ErrClaimFree is
// the ordinary idle state and is translated rather than propagated, so every
// caller compares against nil instead of importing the client's sentinel.
func (m *leaseManager) readBack(ctx context.Context, target ClaimTarget) (*Claim, error) {
	claim, err := m.claimClient.Read(ctx, target)
	if err != nil {
		if errors.Is(err, ErrClaimFree) {
			return nil, nil
		}
		return nil, err
	}
	return claim, nil
}

// heldByUs reports whether the listed claim is this manager's own. It replaces
// the git-rest ownTriple check: the typed API returns a token and a holder
// rather than a SHA and a cluster, and "ours" is the holder comparison.
func (m *leaseManager) heldByUs(claim *Claim) bool {
	return claim != nil && claim.Holder == m.cluster
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

// requestContext derives the deadline-capped context used by every claim call
// made inside Tick.
func (m *leaseManager) requestContext(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	return context.WithDeadline(ctx, m.requestDeadline(m.currentDateTime.Now()).Time())
}

// bestEffortRelease releases the claim's refs and ignores the outcome.
func (m *leaseManager) bestEffortRelease(ctx context.Context, held *heldClaim) {
	reqCtx, cancel := m.requestContext(ctx)
	defer cancel()
	_ = m.claimClient.Release(reqCtx, held.target, held.leaseToken)
	if held.secondKey != "" {
		_ = m.claimClient.Release(reqCtx, held.secondTarget, held.secondToken)
	}
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
	primaryTarget, ok := TargetFromKey(primary)
	if !ok {
		countClaim("error")
		return nil, errors.Errorf(ctx, "claim key %q is not a claim address", primary)
	}
	var secondaryTarget ClaimTarget
	if secondary != "" {
		secondaryTarget, ok = TargetFromKey(secondary)
		if !ok {
			countClaim("error")
			return nil, errors.Errorf(ctx, "claim key %q is not a claim address", secondary)
		}
	}
	if held, found := m.findHeld(primary); found {
		glog.V(3).Infof("claim pair: key %s already held by task %s", primary, held.taskID)
		countClaim("conflict")
		return nil, ErrLeaseConflict
	}
	if secondary != "" {
		if _, found := m.findHeld(secondary); found {
			countClaim("conflict")
			return nil, ErrLeaseConflict
		}
	}
	now := m.currentDateTime.Now()
	primaryClaim, recovered, err := m.createPrimary(ctx, primaryTarget, taskID)
	if err != nil {
		return nil, err
	}
	if secondary == "" {
		held := m.storeClaim(
			primary,
			primaryTarget,
			"",
			ClaimTarget{},
			taskID,
			primaryClaim,
			nil,
			now,
		)
		countClaimCreated(recovered)
		return held, nil
	}
	secondaryClaim, err := m.claimClient.Acquire(
		ctx,
		secondaryTarget,
		m.cluster,
		claimantTaskID(secondaryTarget, taskID),
		leaseSeconds(),
	)
	if err == nil {
		held := m.storeClaim(
			primary,
			primaryTarget,
			secondary,
			secondaryTarget,
			taskID,
			primaryClaim,
			secondaryClaim,
			now,
		)
		logDualAcquired(primary, secondary, taskID)
		countClaimCreated(recovered)
		return held, nil
	}
	m.releaseRecovery(ctx, primaryTarget, primaryClaim.LeaseToken)
	if errors.Is(err, ErrClaimHeld) {
		logDualConflict(secondary, taskID)
		countClaim("conflict")
		return nil, ErrLeaseConflict
	}
	countClaim("error")
	return nil, err
}

// createPrimary acquires the primary claim and reports whether it was recovered
// by reading it back. It counts the conflict or error itself.
func (m *leaseManager) createPrimary(
	ctx context.Context,
	target ClaimTarget,
	taskID string,
) (*Claim, bool, error) {
	// A create must never displace an existing claim, not even an expired one.
	// The typed API's acquire takes over an expired lease by itself, which is
	// right for the takeover path and wrong here: taking a claim over is a
	// decision the caller makes after TakeoverGrace, and letting a plain create
	// do it silently would let a holder that is one tick late lose its claim.
	existing, readErr := m.readBack(ctx, target)
	if readErr != nil {
		countClaim("error")
		return nil, false, readErr
	}
	if existing != nil {
		countClaim("conflict")
		return nil, false, ErrLeaseConflict
	}
	claim, err := m.claimClient.Acquire(
		ctx,
		target,
		m.cluster,
		claimantTaskID(target, taskID),
		leaseSeconds(),
	)
	if err == nil {
		return claim, false, nil
	}
	if errors.Is(err, ErrClaimHeld) {
		countClaim("conflict")
		return nil, false, ErrLeaseConflict
	}
	listed, readErr := m.readBackRecovery(ctx, target)
	if readErr != nil {
		countClaim("error")
		return nil, false, errors.Wrapf(
			ctx,
			readErr,
			"claim %s: read back after acquire failed (%v)",
			target.describe(),
			err,
		)
	}
	if m.heldByUs(listed) {
		return listed, true, nil
	}
	countClaim("error")
	return nil, false, err
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
	primaryTarget ClaimTarget,
	secondaryKey string,
	secondaryTarget ClaimTarget,
	taskID string,
	primaryClaim *Claim,
	secondaryClaim *Claim,
	now libtime.DateTime,
) *HeldClaim {
	held := &heldClaim{
		key:          primaryKey,
		target:       primaryTarget,
		taskID:       taskID,
		leaseToken:   primaryClaim.LeaseToken,
		leaseUntil:   primaryClaim.LeaseUntil,
		secondKey:    secondaryKey,
		secondTarget: secondaryTarget,
		lastRenewAt:  now,
	}
	if secondaryClaim != nil {
		held.secondToken = secondaryClaim.LeaseToken
	}
	m.held[primaryKey] = held
	return &HeldClaim{
		Key:        primaryKey,
		TaskID:     taskID,
		LeaseToken: primaryClaim.LeaseToken,
		LeaseUntil: primaryClaim.LeaseUntil,
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

// Release deletes the held claim. A lost lease means already released and is
// nil. A claim inside its success hold is kept until the hold expires.
func (m *leaseManager) Release(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	held, ok := m.findHeld(key)
	if !ok {
		return nil
	}
	// A success hold outranks a plain release. The terminal phase reaches this
	// cluster's vault view before a peer's does, so releasing here would end the
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

// releaseRef releases one ref and reports "released", or "already_released" when
// the token is no longer current. Any other error is returned unchanged.
func (m *leaseManager) releaseRef(
	ctx context.Context,
	target ClaimTarget,
	token string,
) (string, error) {
	err := m.claimClient.Release(ctx, target, token)
	if err == nil {
		return "released", nil
	}
	if errors.Is(err, ErrLeaseLost) {
		return "already_released", nil
	}
	return "", err
}

// releaseRefWithDeadline releases one ref under the tick's deadline-capped
// context and reports the same labels as releaseRef.
func (m *leaseManager) releaseRefWithDeadline(
	ctx context.Context,
	target ClaimTarget,
	token string,
) (string, error) {
	reqCtx, cancel := m.requestContext(ctx)
	defer cancel()
	return m.releaseRef(reqCtx, target, token)
}

// pairRemovalResult combines the two ref outcomes into one removal label: only
// a pair whose refs were both already gone counts as already_released.
func pairRemovalResult(primary, secondary string) string {
	if primary == "already_released" && secondary == "already_released" {
		return "already_released"
	}
	return "released"
}

// releaseLocked releases both refs of a held claim as one unit. The caller
// holds m.mu. A non-lost-lease failure on either ref leaves the whole claim
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
	primaryResult, err := m.releaseRef(ctx, held.target, held.leaseToken)
	if err != nil {
		held.releasePending = true
		countClaim("error")
		return err
	}
	if held.secondKey == "" {
		delete(m.held, held.key)
		countClaimRemoval(held.key, held.taskID, primaryResult, held.leaseToken)
		return nil
	}
	secondaryResult, err := m.releaseRef(ctx, held.secondTarget, held.secondToken)
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
		held.leaseToken,
	)
	return nil
}

// Takeover replaces an expired claim with one for taskID.
func (m *leaseManager) Takeover(
	ctx context.Context,
	key string,
	taskID string,
) (*TakeoverResult, error) {
	return m.TakeoverPair(ctx, key, "", taskID)
}

// TakeoverPair replaces an expired claim pair with one for taskID. An empty
// secondary takes the single-key path unchanged.
//
// The typed API has no takeover verb: an acquire already takes over an expired
// lease, and refuses a live one with 409. So this method's whole job is the
// grace window the server does not apply — it reads the claim first, refuses
// while the lease is still inside TakeoverGrace, and only then acquires. The
// read is also what supplies PreviousTaskID, which the acquire response cannot
// carry because it describes the claim that now exists.
func (m *leaseManager) TakeoverPair(
	ctx context.Context,
	primary string,
	secondary string,
	taskID string,
) (*TakeoverResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	primaryTarget, secondaryTarget, err := takeoverTargets(primary, secondary)
	if err != nil {
		countClaim("error")
		return nil, err
	}
	if _, found := m.findHeld(primary); found {
		countClaim("conflict")
		return nil, ErrLeaseConflict
	}
	if secondary != "" {
		if _, found := m.findHeld(secondary); found {
			countClaim("conflict")
			return nil, ErrLeaseConflict
		}
	}
	now := m.currentDateTime.Now()
	listed, err := m.readBack(ctx, primaryTarget)
	if err != nil {
		countClaim("error")
		return nil, err
	}

	if listed == nil {
		held, claimErr := m.claimPairLocked(ctx, primary, secondary, taskID)
		if claimErr != nil {
			return nil, claimErr
		}
		return &TakeoverResult{
			Key:            held.Key,
			TaskID:         held.TaskID,
			LeaseToken:     held.LeaseToken,
			LeaseUntil:     held.LeaseUntil,
			PreviousTaskID: "",
		}, nil
	}
	if err := takeoverRefusal(now, listed, primary); err != nil {
		return nil, err
	}
	primaryClaim, recoveredPrimary, err := m.acquireForTakeover(ctx, primaryTarget, taskID)
	if err != nil {
		return nil, err
	}
	if secondary == "" {
		m.storeClaim(
			primary, primaryTarget, "", ClaimTarget{}, taskID, primaryClaim, nil, now,
		)
		if !recoveredPrimary {
			countClaim("taken_over")
		}
		return &TakeoverResult{
			Key:            primary,
			TaskID:         taskID,
			LeaseToken:     primaryClaim.LeaseToken,
			LeaseUntil:     primaryClaim.LeaseUntil,
			PreviousTaskID: listed.TaskID,
		}, nil
	}
	return m.finishTakeoverPair(
		ctx, primary, secondary, primaryTarget, secondaryTarget, taskID,
		primaryClaim, listed, recoveredPrimary, now,
	)
}

// finishTakeoverPair acquires the secondary and stores the pair. The pair is
// never half-held: a failed secondary releases the primary again, so a peer can
// take the task cleanly instead of finding a claim whose partner nobody holds.
func (m *leaseManager) finishTakeoverPair(
	ctx context.Context,
	primary string,
	secondary string,
	primaryTarget ClaimTarget,
	secondaryTarget ClaimTarget,
	taskID string,
	primaryClaim *Claim,
	listed *Claim,
	recoveredPrimary bool,
	now libtime.DateTime,
) (*TakeoverResult, error) {
	secondaryClaim, err := m.claimClient.Acquire(
		ctx,
		secondaryTarget,
		m.cluster,
		claimantTaskID(secondaryTarget, taskID),
		leaseSeconds(),
	)
	if err != nil {
		m.releaseRecovery(ctx, primaryTarget, primaryClaim.LeaseToken)
		if errors.Is(err, ErrClaimHeld) {
			countClaim("conflict")
			return nil, ErrLeaseConflict
		}
		countClaim("error")
		return nil, err
	}
	m.storeClaim(
		primary, primaryTarget, secondary, secondaryTarget, taskID,
		primaryClaim, secondaryClaim, now,
	)
	logDualAcquired(primary, secondary, taskID)
	if !recoveredPrimary {
		countClaim("taken_over")
	}
	return &TakeoverResult{
		Key:            primary,
		TaskID:         taskID,
		LeaseToken:     primaryClaim.LeaseToken,
		LeaseUntil:     primaryClaim.LeaseUntil,
		PreviousTaskID: listed.TaskID,
	}, nil
}

// takeoverAllowed reports whether a lease has been over long enough to take
// over. The server takes over the instant a lease expires; this grace window is
// the client's, and it exists so a holder that is one tick late is not
// displaced while it is still renewing.
func takeoverAllowed(now, leaseUntil libtime.DateTime) bool {
	return !now.Before(leaseUntil.Add(TakeoverGrace))
}

// takeoverTargets parses the two keys of a takeover into the addresses they
// name. A key that is not a claim address is an error rather than a silent
// single-claim takeover of the wrong object.
func takeoverTargets(primary, secondary string) (ClaimTarget, ClaimTarget, error) {
	primaryTarget, ok := TargetFromKey(primary)
	if !ok {
		return ClaimTarget{}, ClaimTarget{},
			errors.Errorf(context.Background(), "claim key %q is not a claim address", primary)
	}
	var secondaryTarget ClaimTarget
	if secondary != "" {
		secondaryTarget, ok = TargetFromKey(secondary)
		if !ok {
			return ClaimTarget{}, ClaimTarget{},
				errors.Errorf(
					context.Background(),
					"claim key %q is not a claim address",
					secondary,
				)
		}
	}
	return primaryTarget, secondaryTarget, nil
}

// takeoverRefusal is the client-side half of a takeover: the server takes over
// an expired lease the instant it expires and refuses a live one, so the grace
// window and the unreadable-lease refusal are decided here.
func takeoverRefusal(now libtime.DateTime, listed *Claim, primary string) error {
	if listed.LeaseUnreadable {
		// A lease nobody can read is refused, not taken over: taking it over
		// would displace a holder whose remaining time is unknown, and the
		// caller's own vocabulary for "refused" is this error.
		countClaim("takeover_too_early")
		glog.Warningf("claim takeover: unparseable lease_until key=%s", primary)
		return ErrLeaseNotExpired
	}
	if !takeoverAllowed(now, listed.LeaseUntil) {
		countClaim("takeover_too_early")
		return ErrLeaseNotExpired
	}
	return nil
}

// acquireForTakeover acquires the primary claim and reports whether a failed
// write was recovered by reading it back. A write that failed without a
// conflict may still have landed — the response is what was lost, not the
// write — and reading it back is what keeps a taken-over claim from being
// reported as an error and re-run.
func (m *leaseManager) acquireForTakeover(
	ctx context.Context,
	target ClaimTarget,
	taskID string,
) (*Claim, bool, error) {
	claim, err := m.claimClient.Acquire(
		ctx,
		target,
		m.cluster,
		claimantTaskID(target, taskID),
		leaseSeconds(),
	)
	if err == nil {
		return claim, false, nil
	}
	if errors.Is(err, ErrClaimHeld) {
		countClaim("conflict")
		return nil, false, ErrLeaseConflict
	}
	recovered, readErr := m.readBackRecovery(ctx, target)
	if readErr != nil || !m.heldByUs(recovered) {
		countClaim("error")
		return nil, false, err
	}
	countClaim("recovered")
	return recovered, true, nil
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
			LeaseToken: held.leaseToken,
			LeaseUntil: held.leaseUntil,
		})
		if held.secondKey != "" {
			result = append(result, HeldClaim{
				Key:        held.secondKey,
				TaskID:     held.taskID,
				LeaseToken: held.secondToken,
				LeaseUntil: held.leaseUntil,
			})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result
}

// Recover re-adopts, releases or fences the claims this cluster held before a
// restart.
//
// It lists rather than reading key by key, and it never acquires: a claim it
// adopts is adopted exactly as listed, with the lease token the listing
// returned. That token is the whole reason this works — the typed API can only
// renew or release a claim it holds a token for, and a restarted process has
// none of its own. Acquiring instead would be refused, because the server
// answers 409 to a live claim regardless of who asks.
func (m *leaseManager) Recover(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	claims, err := m.claimClient.List(ctx, m.cluster)
	if err != nil {
		countClaim("error")
		return err
	}
	byTask := map[string][]Claim{}
	for _, listed := range claims {
		// The listing is already scoped to this holder and already excludes
		// anything that is not an addressable claim, so there is no streak-prefix
		// skip and no cluster filter here: the server does both, and repeating
		// them would only hide a claim the server does return.
		byTask[listed.TaskID] = append(byTask[listed.TaskID], listed)
	}
	taskIDs := make([]string, 0, len(byTask))
	for taskID := range byTask {
		taskIDs = append(taskIDs, taskID)
	}
	sort.Strings(taskIDs)
	for _, taskID := range taskIDs {
		m.recoverTaskGroup(ctx, byTask[taskID])
	}
	return nil
}

// recoverTaskGroup splits one task's listed claims into groups. A repository
// claim and a per-PR claim form one pair; any other combination is processed as
// separate single claims in sorted key order.
func (m *leaseManager) recoverTaskGroup(ctx context.Context, claims []Claim) {
	if len(claims) == 2 {
		var repoClaim, prClaim *Claim
		for i := range claims {
			switch claims[i].Scope {
			case "repo":
				repoClaim = &claims[i]
			case "pr":
				prClaim = &claims[i]
			}
		}
		if repoClaim != nil && prClaim != nil {
			m.recoverGroup(ctx, *repoClaim, prClaim)
			return
		}
	}
	sorted := make([]Claim, len(claims))
	copy(sorted, claims)
	sort.Slice(sorted, func(i, j int) bool {
		return claimKeyOf(sorted[i]) < claimKeyOf(sorted[j])
	})
	for i := range sorted {
		m.recoverGroup(ctx, sorted[i], nil)
	}
}

// recoverGroup applies the restart rule to one claim, which is either a single
// listed claim or the repository/per-PR pair.
func (m *leaseManager) recoverGroup(ctx context.Context, primary Claim, secondary *Claim) {
	now := m.currentDateTime.Now()
	leaseUntil := recoveredLeaseUntil(now, primary)
	if secondary != nil {
		if secondaryLease := recoveredLeaseUntil(now, *secondary); secondaryLease.Before(
			leaseUntil,
		) {
			leaseUntil = secondaryLease
		}
	}
	if fenceDue(now, leaseUntil) {
		m.recoverFenceGroup(ctx, primary, secondary, leaseUntil)
		return
	}
	m.recoverAdoptGroup(ctx, primary, secondary, leaseUntil, now)
}

// recoveredLeaseUntil is the lease a recovered claim is judged by. A lease that
// cannot be read is treated as already past, so the claim is fenced rather than
// adopted: adopting it would mean renewing a lease whose length nobody knows,
// and dropping it would leave a claim nothing can ever release.
func recoveredLeaseUntil(now libtime.DateTime, listed Claim) libtime.DateTime {
	if listed.LeaseUnreadable {
		countClaim("error")
		glog.Warningf(
			"claim recovery: unparseable lease_until key=%s task=%s",
			claimKeyOf(listed),
			listed.TaskID,
		)
		return now.Add(-LeaseDuration)
	}
	return listed.LeaseUntil
}

// claimKeyOf rebuilds the claim key from a listed claim. It is the inverse of
// the server's key parse and mirrors claimScopeFromKey: scope and id are exactly
// the two components the key was built from.
func claimKeyOf(claim Claim) string {
	return "claims/" + claim.Scope + "/" + claim.ID
}

// recoverFenceGroup stops the Job of a claim too close to its lease end and
// drops both refs. A failed Job deletion leaves the whole claim in pending
// deletion.
func (m *leaseManager) recoverFenceGroup(
	ctx context.Context,
	primary Claim,
	secondary *Claim,
	leaseUntil libtime.DateTime,
) {
	key := claimKeyOf(primary)
	running, runErr := m.jobControl.IsJobRunning(ctx, primary.TaskID)
	if runErr != nil {
		countClaim("error")
	}
	if running || runErr != nil {
		if err := m.jobControl.DeleteJob(ctx, primary.TaskID); err != nil {
			countClaim("error")
			countClaimFencePending(key, primary.TaskID)
			held := &heldClaim{
				key:                 key,
				target:              targetFor(primary),
				taskID:              primary.TaskID,
				leaseToken:          primary.LeaseToken,
				leaseUntil:          leaseUntil,
				pendingJobDeletion:  true,
				pendingDeletionPath: "fence",
			}
			if secondary != nil {
				held.secondKey = claimKeyOf(*secondary)
				held.secondTarget = targetFor(*secondary)
				held.secondToken = secondary.LeaseToken
			}
			m.held[key] = held
			return
		}
	}
	m.bestEffortRelease(ctx, &heldClaim{
		key:          key,
		target:       targetFor(primary),
		taskID:       primary.TaskID,
		leaseToken:   primary.LeaseToken,
		secondKey:    secondKeyOf(secondary),
		secondTarget: secondTargetOf(secondary),
		secondToken:  secondTokenOf(secondary),
	})
	countClaimRemoval(key, primary.TaskID, "fenced", primary.LeaseToken)
}

// targetFor rebuilds the typed address of a listed claim. A claim the listing
// returned is by construction one of the three shapes, so an unparseable key
// here is a programming error rather than a runtime condition — it falls back
// to the zero target, which every call path then refuses locally.
func targetFor(claim Claim) ClaimTarget {
	target, ok := TargetFromKey(claimKeyOf(claim))
	if !ok {
		return ClaimTarget{}
	}
	return target
}

func secondKeyOf(claim *Claim) string {
	if claim == nil {
		return ""
	}
	return claimKeyOf(*claim)
}

func secondTargetOf(claim *Claim) ClaimTarget {
	if claim == nil {
		return ClaimTarget{}
	}
	return targetFor(*claim)
}

func secondTokenOf(claim *Claim) string {
	if claim == nil {
		return ""
	}
	return claim.LeaseToken
}

// recoverAdoptGroup holds, releases or re-checks the claim of a Job that may
// still be running. A single-ref claim is adopted exactly as listed; a pair
// recovered from an earlier release is adopted as a pair.
func (m *leaseManager) recoverAdoptGroup(
	ctx context.Context,
	primary Claim,
	secondary *Claim,
	leaseUntil libtime.DateTime,
	now libtime.DateTime,
) {
	running, runErr := m.jobControl.IsJobRunning(ctx, primary.TaskID)
	if runErr != nil {
		countClaim("error")
		if secondary == nil {
			m.repairOrAdoptSingle(primary, leaseUntil, now, true)
			return
		}
		m.storeRecoveredPair(primary, *secondary, leaseUntil, now, true)
		return
	}
	if running {
		if secondary == nil {
			m.repairOrAdoptSingle(primary, leaseUntil, now, false)
			return
		}
		m.storeRecoveredPair(primary, *secondary, leaseUntil, now, false)
		countClaim("adopted")
		return
	}
	key := claimKeyOf(primary)
	primaryResult, err := m.releaseRef(ctx, targetFor(primary), primary.LeaseToken)
	if err != nil {
		countClaim("error")
		return
	}
	result := primaryResult
	if secondary != nil {
		secondaryResult, secondaryErr := m.releaseRef(
			ctx,
			targetFor(*secondary),
			secondary.LeaseToken,
		)
		if secondaryErr != nil {
			countClaim("error")
			return
		}
		result = pairRemovalResult(primaryResult, secondaryResult)
	}
	countClaimRemoval(key, primary.TaskID, result, primary.LeaseToken)
}

// storeRecoveredPair records both refs of an adopted claim.
func (m *leaseManager) storeRecoveredPair(
	primary Claim,
	secondary Claim,
	leaseUntil libtime.DateTime,
	now libtime.DateTime,
	needsRunningCheck bool,
) {
	m.held[claimKeyOf(primary)] = &heldClaim{
		key:               claimKeyOf(primary),
		target:            targetFor(primary),
		taskID:            primary.TaskID,
		leaseToken:        primary.LeaseToken,
		leaseUntil:        leaseUntil,
		secondKey:         claimKeyOf(secondary),
		secondTarget:      targetFor(secondary),
		secondToken:       secondary.LeaseToken,
		lastRenewAt:       now,
		renewNextTick:     true,
		needsRunningCheck: needsRunningCheck,
	}
}

// repairOrAdoptSingle adopts a single listed claim into the held set. Every
// claim is a single ref, so the claim is adopted exactly as listed: no partner
// key is derived or written. A per-PR claim is therefore never paired with its
// per-repository key, which is what used to make every review of one repository
// serialise behind a shared claim.
func (m *leaseManager) repairOrAdoptSingle(
	primary Claim,
	leaseUntil libtime.DateTime,
	now libtime.DateTime,
	needsRunningCheck bool,
) {
	key := claimKeyOf(primary)
	m.held[key] = &heldClaim{
		key:               key,
		target:            targetFor(primary),
		taskID:            primary.TaskID,
		leaseToken:        primary.LeaseToken,
		leaseUntil:        leaseUntil,
		lastRenewAt:       now,
		renewNextTick:     true,
		needsRunningCheck: needsRunningCheck,
	}
	if !needsRunningCheck {
		countClaim("adopted")
	}
}
