// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	stdtime "time"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
)

// Run ticks, then waits TickInterval through the injected waiter, until the
// context is done. It starts no goroutine and never calls Recover.
func (m *leaseManager) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := m.Tick(ctx); err != nil {
			glog.Warningf("claim lease tick failed: %v", err)
		}
		err := m.waiter.Wait(ctx, TickInterval)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

// Tick runs one lease-manager pass. It returns an error only when the context
// is done; per-claim failures are counted and handled internally.
func (m *leaseManager) Tick(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retryPendingDeletions(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	m.evaluateFence(ctx, m.currentDateTime.Now())
	if err := ctx.Err(); err != nil {
		return err
	}
	m.retryPendingReleases(ctx)
	m.releaseExpiredHolds(ctx)
	m.releaseJoblessClaims(ctx)
	m.recheckRunning(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	m.renewDueClaims(ctx)
	return ctx.Err()
}

// retryPendingDeletions retries the Job deletion of claims that were already
// pending at tick start. A claim that becomes pending later waits one tick.
func (m *leaseManager) retryPendingDeletions(ctx context.Context) {
	for _, key := range m.heldKeys() {
		held, ok := m.held[key]
		if !ok || !held.pendingJobDeletion {
			continue
		}
		if err := m.jobControl.DeleteJob(ctx, held.taskID); err != nil {
			countClaim("error")
			continue
		}
		if held.pendingDeletionPath == "fence" {
			m.bestEffortDelete(ctx, key, held.sha)
		}
		delete(m.held, key)
	}
}

// evaluateFence fences every held claim whose fence point has arrived. It is
// called before any renew request and again after every request.
func (m *leaseManager) evaluateFence(ctx context.Context, now libtime.DateTime) {
	for _, key := range m.heldKeys() {
		held, ok := m.held[key]
		if !ok || held.pendingJobDeletion {
			continue
		}
		if !fenceDue(now, held.leaseUntil) {
			continue
		}
		m.fenceClaim(ctx, key, held)
	}
}

// fenceClaim stops the Job and drops the claim without renewing it.
func (m *leaseManager) fenceClaim(ctx context.Context, key string, held *heldClaim) {
	if held.releasePending {
		m.bestEffortDelete(ctx, key, held.sha)
		delete(m.held, key)
		countClaim("fenced")
		return
	}
	if err := m.jobControl.DeleteJob(ctx, held.taskID); err != nil {
		countClaim("error")
		countClaim("fenced")
		held.pendingJobDeletion = true
		held.pendingDeletionPath = "fence"
		return
	}
	m.bestEffortDelete(ctx, key, held.sha)
	delete(m.held, key)
	countClaim("fenced")
}

// retryPendingReleases retries the ref DELETE of claims whose release failed.
func (m *leaseManager) retryPendingReleases(ctx context.Context) {
	for _, key := range m.heldKeys() {
		held, ok := m.held[key]
		if !ok || !held.releasePending {
			continue
		}
		reqCtx, cancel := m.requestContext(ctx)
		err := m.refClient.Delete(reqCtx, key, held.sha)
		cancel()
		switch {
		case err == nil:
			delete(m.held, key)
			countClaim("released")
		case errors.Is(err, ErrLeaseConflict):
			delete(m.held, key)
			countClaim("already_released")
		default:
			countClaim("error")
		}
	}
}

// releaseExpiredHolds releases claims whose success-grace hold has run out.
//
// ReleaseAfterSuccess sets that hold when a Job succeeds, covering the window in
// which the vault has not yet absorbed the result. Nothing else would ever end
// it — no further event arrives for a task that just finished — so this pass is
// what turns a bounded hold into an actual release. A failed release leaves
// releasePending set and is retried by retryPendingReleases on the next tick.
func (m *leaseManager) releaseExpiredHolds(ctx context.Context) {
	now := m.currentDateTime.Now().Time()
	for _, key := range m.heldKeys() {
		held, ok := m.held[key]
		if !ok || held.holdUntil.Time().IsZero() {
			continue
		}
		if now.Before(held.holdUntil.Time()) {
			continue
		}
		// holdUntil is deliberately NOT cleared here. Clearing it first would leave
		// a claim with no hold and no releasePending when the release fails — a
		// failed IsJobRunning read is exactly that — and the claim would then be
		// renewed forever with nothing left to release it. Leaving it expired means
		// the next tick tries again, which is the same convergence retryPendingReleases
		// uses.
		reqCtx, cancel := m.requestContext(ctx)
		_ = m.releaseLocked(reqCtx, key, held)
		cancel()
	}
}

// releaseJoblessClaims releases held claims whose Job has been continuously
// absent for at least JoblessGrace. Claims owned by another release path keep
// their own handling.
func (m *leaseManager) releaseJoblessClaims(ctx context.Context) {
	now := m.currentDateTime.Now()
	for _, key := range m.heldKeys() {
		held, ok := m.held[key]
		if !ok || !joblessEligible(held) {
			continue
		}
		if !held.lastJoblessCheckAt.Time().IsZero() &&
			now.Sub(held.lastJoblessCheckAt) < JoblessCheckInterval {
			continue
		}
		held.lastJoblessCheckAt = now
		checkCtx, cancel := m.requestContext(ctx)
		running, err := m.jobControl.IsJobRunning(checkCtx, held.taskID)
		cancel()
		if err != nil {
			countClaim("error")
			continue
		}
		if running {
			held.joblessSince = libtime.DateTime(stdtime.Time{})
			continue
		}
		if held.joblessSince.Time().IsZero() {
			held.joblessSince = now
		}
		if now.Sub(held.joblessSince) < JoblessGrace {
			continue
		}
		m.releaseJoblessClaim(ctx, key, held)
	}
}

// joblessEligible reports whether the jobless release owns this claim. The
// success hold, a pending release, a pending Job deletion and an unanswered
// adopted running check each keep their own path.
func joblessEligible(held *heldClaim) bool {
	return !held.pendingJobDeletion &&
		!held.releasePending &&
		!held.needsRunningCheck &&
		held.holdUntil.Time().IsZero()
}

// releaseJoblessClaim releases one claim whose Job is gone, logging and
// counting the decision once. A failed DELETE keeps the claim with a pending
// release, which retryPendingReleases handles without counting again.
func (m *leaseManager) releaseJoblessClaim(
	ctx context.Context,
	key string,
	held *heldClaim,
) {
	countClaim("jobless_released")
	glog.Warningf(
		"event=claim_jobless_released key=%s task=%s jobless_since=%s",
		key,
		held.taskID,
		held.joblessSince.Time().UTC().Format(stdtime.RFC3339),
	)
	m.releaseAfterRunningCheck(ctx, key, held)
}

// recheckRunning re-asks the Job running check of claims adopted with an
// unknown Job state. The fence already ran, so a near-expiry claim is gone.
func (m *leaseManager) recheckRunning(ctx context.Context) {
	for _, key := range m.heldKeys() {
		held, ok := m.held[key]
		if !ok || !held.needsRunningCheck {
			continue
		}
		running, err := m.jobControl.IsJobRunning(ctx, held.taskID)
		if err != nil {
			countClaim("error")
			continue
		}
		if running {
			held.needsRunningCheck = false
			countClaim("adopted")
			continue
		}
		m.releaseAfterRunningCheck(ctx, key, held)
	}
}

// releaseAfterRunningCheck releases a claim whose Job is gone.
func (m *leaseManager) releaseAfterRunningCheck(
	ctx context.Context,
	key string,
	held *heldClaim,
) {
	reqCtx, cancel := m.requestContext(ctx)
	err := m.refClient.Delete(reqCtx, key, held.sha)
	cancel()
	switch {
	case err == nil:
		delete(m.held, key)
		countClaim("released")
	case errors.Is(err, ErrLeaseConflict):
		delete(m.held, key)
		countClaim("already_released")
	default:
		countClaim("error")
		held.releasePending = true
	}
}

// renewDueClaims renews every due claim, one after another. The clock is
// re-read before each claim and the fence is re-evaluated after each request.
func (m *leaseManager) renewDueClaims(ctx context.Context) {
	for _, key := range m.heldKeys() {
		held, ok := m.held[key]
		if !ok || held.pendingJobDeletion || held.releasePending {
			continue
		}
		now := m.currentDateTime.Now()
		if !held.renewNextTick && !renewDue(now, held.lastRenewAt) {
			continue
		}
		m.renewClaim(ctx, key, held, now)
		m.evaluateFence(ctx, m.currentDateTime.Now())
	}
}

// renewClaim sends one renew PUT and applies the outcome.
func (m *leaseManager) renewClaim(
	ctx context.Context,
	key string,
	held *heldClaim,
	now libtime.DateTime,
) {
	newLease := now.Add(LeaseDuration)
	newLeaseStr := formatLeaseUntil(newLease)
	reqCtx, cancel := m.requestContext(ctx)
	sha, err := m.refClient.Put(reqCtx, key, held.sha, m.cluster, held.taskID, newLease)
	cancel()
	held.lastRenewAt = now
	held.renewNextTick = false
	switch {
	case err == nil:
		held.sha = sha
		held.leaseUntil = newLease
		held.leaseUntilStr = newLeaseStr
		countClaim("renewed")
	case errors.Is(err, ErrLeaseConflict):
		m.claimLost(ctx, key, held)
	default:
		m.renewReadBack(ctx, key, held, newLease, newLeaseStr)
	}
}

// claimLost deletes the Job and drops the claim without contacting git-rest.
func (m *leaseManager) claimLost(ctx context.Context, key string, held *heldClaim) {
	countClaim("claim_lost")
	glog.Warningf(
		"claim lost key=%s task_id=%s sha=%s",
		key,
		held.taskID,
		held.sha,
	)
	if err := m.jobControl.DeleteJob(ctx, held.taskID); err != nil {
		countClaim("error")
		held.pendingJobDeletion = true
		held.pendingDeletionPath = "lost"
		return
	}
	delete(m.held, key)
}

// renewReadBack decides a failed renew from the listed ref, after re-running
// the fence. A claim fenced in between is not read back.
func (m *leaseManager) renewReadBack(
	ctx context.Context,
	key string,
	held *heldClaim,
	newLease libtime.DateTime,
	newLeaseStr string,
) {
	m.evaluateFence(ctx, m.currentDateTime.Now())
	if _, ok := m.held[key]; !ok {
		return
	}
	reqCtx, cancel := m.requestContext(ctx)
	ref, err := m.readBack(reqCtx, key)
	cancel()
	switch {
	case err != nil:
		countClaim("renew_failed")
	case m.ownTriple(ref, held.taskID, newLeaseStr):
		held.sha = ref.Sha
		held.leaseUntil = newLease
		held.leaseUntilStr = newLeaseStr
		countClaim("recovered")
	case ref != nil && ref.Sha == held.sha:
		countClaim("renew_failed")
	default:
		m.claimLost(ctx, key, held)
	}
}
