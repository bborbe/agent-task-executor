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
	Release(ctx context.Context, key string) error
	Takeover(ctx context.Context, key string, taskID string) (*TakeoverResult, error)
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
	key                 string
	taskID              string
	sha                 string
	leaseUntil          libtime.DateTime
	leaseUntilStr       string
	lastRenewAt         libtime.DateTime
	renewNextTick       bool
	pendingJobDeletion  bool
	pendingDeletionPath string
	releasePending      bool
	needsRunningCheck   bool
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.claimLocked(ctx, key, taskID)
}

// claimLocked is Claim without taking the lock.
func (m *leaseManager) claimLocked(
	ctx context.Context,
	key string,
	taskID string,
) (*HeldClaim, error) {
	if _, ok := m.held[key]; ok {
		countClaim("conflict")
		return nil, ErrLeaseConflict
	}
	now := m.currentDateTime.Now()
	leaseUntil := now.Add(LeaseDuration)
	leaseUntilStr := formatLeaseUntil(leaseUntil)
	sha, err := m.refClient.Put(ctx, key, "", m.cluster, taskID, leaseUntil)
	if err == nil {
		countClaim("claimed")
		return m.storeClaim(key, taskID, sha, leaseUntil, leaseUntilStr, now), nil
	}
	if errors.Is(err, ErrLeaseConflict) {
		countClaim("conflict")
		return nil, err
	}
	ref, readErr := m.readBack(ctx, key)
	if readErr != nil {
		countClaim("error")
		return nil, errors.Wrapf(
			ctx,
			readErr,
			"claim %s: read back after create failed (%v)",
			key,
			err,
		)
	}
	if m.ownTriple(ref, taskID, leaseUntilStr) {
		held := m.storeClaim(key, taskID, ref.Sha, leaseUntil, leaseUntilStr, now)
		countClaim("recovered")
		return held, nil
	}
	countClaim("error")
	return nil, err
}

// storeClaim records a held claim and returns its public view.
func (m *leaseManager) storeClaim(
	key string,
	taskID string,
	sha string,
	leaseUntil libtime.DateTime,
	leaseUntilStr string,
	now libtime.DateTime,
) *HeldClaim {
	m.held[key] = &heldClaim{
		key:           key,
		taskID:        taskID,
		sha:           sha,
		leaseUntil:    leaseUntil,
		leaseUntilStr: leaseUntilStr,
		lastRenewAt:   now,
	}
	return &HeldClaim{Key: key, TaskID: taskID, Sha: sha, LeaseUntil: leaseUntil}
}

// Release deletes the held claim. A 409 means already released and is nil.
func (m *leaseManager) Release(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	held, ok := m.held[key]
	if !ok {
		return nil
	}
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
			key,
			held.taskID,
		)
		return nil
	}
	err = m.refClient.Delete(ctx, key, held.sha)
	if err == nil {
		delete(m.held, key)
		countClaim("released")
		return nil
	}
	if errors.Is(err, ErrLeaseConflict) {
		delete(m.held, key)
		countClaim("already_released")
		return nil
	}
	held.releasePending = true
	countClaim("error")
	return err
}

// Takeover replaces an expired claim with one for taskID, using one CAS write.
func (m *leaseManager) Takeover(
	ctx context.Context,
	key string,
	taskID string,
) (*TakeoverResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.held[key]; ok {
		countClaim("conflict")
		return nil, ErrLeaseConflict
	}
	ref, err := m.readBack(ctx, key)
	if err != nil {
		countClaim("error")
		return nil, err
	}
	if ref == nil {
		held, claimErr := m.claimLocked(ctx, key, taskID)
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
	return m.takeoverExpired(ctx, key, taskID, ref)
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
		m.storeClaim(key, taskID, sha, newLease, newLeaseStr, now)
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
	m.storeClaim(key, taskID, listed.Sha, newLease, newLeaseStr, now)
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
			countClaim("fenced")
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
	countClaim("fenced")
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
		countClaim("released")
	case errors.Is(err, ErrLeaseConflict):
		countClaim("already_released")
	default:
		countClaim("error")
	}
}
