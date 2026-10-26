// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	"sort"
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
			m.bestEffortRelease(ctx, held)
			glog.Infof(
				"event=claim_released key=%s task=%s result=fenced token=%s",
				held.key,
				held.taskID,
				held.leaseToken,
			)
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
		m.bestEffortRelease(ctx, held)
		delete(m.held, key)
		countClaimRemoval(held.key, held.taskID, "fenced", held.leaseToken)
		return
	}
	if err := m.jobControl.DeleteJob(ctx, held.taskID); err != nil {
		countClaim("error")
		countClaimFencePending(key, held.taskID)
		held.pendingJobDeletion = true
		held.pendingDeletionPath = "fence"
		return
	}
	m.bestEffortRelease(ctx, held)
	delete(m.held, key)
	countClaimRemoval(held.key, held.taskID, "fenced", held.leaseToken)
}

// retryPendingReleases retries the ref DELETE of claims whose release failed.
// Both refs of a pair are retried in one pass; a 409 on either means that ref
// is already gone, and the pair is dropped only when both are gone.
func (m *leaseManager) retryPendingReleases(ctx context.Context) {
	for _, key := range m.heldKeys() {
		held, ok := m.held[key]
		if !ok || !held.releasePending {
			continue
		}
		primaryResult, err := m.releaseRefWithDeadline(ctx, held.target, held.leaseToken)
		if err != nil {
			countClaim("error")
			continue
		}
		if held.secondKey == "" {
			delete(m.held, key)
			countClaimRemoval(held.key, held.taskID, primaryResult, held.leaseToken)
			continue
		}
		secondaryResult, err := m.releaseRefWithDeadline(
			ctx,
			held.secondTarget,
			held.secondToken,
		)
		if err != nil {
			countClaim("error")
			continue
		}
		delete(m.held, key)
		countClaimRemoval(
			held.key,
			held.taskID,
			pairRemovalResult(primaryResult, secondaryResult),
			held.leaseToken,
		)
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
		_ = m.releaseLocked(reqCtx, held)
		cancel()
	}
}

// joblessPassBudget stops one jobless pass from starting further Job checks
// once this much time has passed since the pass began; a check already
// started may still run up to RequestTimeout.
const joblessPassBudget = 10 * libtime.Second

// releaseJoblessClaims releases held claims whose Job has been continuously
// absent for at least JoblessGrace. Claims owned by another release path keep
// their own handling.
func (m *leaseManager) releaseJoblessClaims(ctx context.Context) {
	now := m.currentDateTime.Now()
	for _, key := range m.joblessDueKeys(now) {
		if ctx.Err() != nil {
			return
		}
		if m.currentDateTime.Now().Sub(now) >= joblessPassBudget {
			return
		}
		held, ok := m.held[key]
		if !ok {
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

// joblessDueKeys returns the keys of held claims the jobless pass owns and
// whose check is due at now. Never-checked claims come first, then the oldest
// lastJoblessCheckAt; ties keep heldKeys order, which is sorted by key.
func (m *leaseManager) joblessDueKeys(now libtime.DateTime) []string {
	keys := make([]string, 0, len(m.held))
	for _, key := range m.heldKeys() {
		held := m.held[key]
		if !joblessEligible(held) {
			continue
		}
		if !held.lastJoblessCheckAt.Time().IsZero() &&
			now.Sub(held.lastJoblessCheckAt) < JoblessCheckInterval {
			continue
		}
		keys = append(keys, key)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return m.held[keys[i]].lastJoblessCheckAt.Time().Before(
			m.held[keys[j]].lastJoblessCheckAt.Time(),
		)
	})
	return keys
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

// releaseAfterRunningCheck releases a claim whose Job is gone. Both refs of a
// pair are deleted; a non-409 failure on either keeps the whole pair pending
// release.
func (m *leaseManager) releaseAfterRunningCheck(
	ctx context.Context,
	key string,
	held *heldClaim,
) {
	primaryResult, err := m.releaseRefWithDeadline(ctx, held.target, held.leaseToken)
	if err != nil {
		countClaim("error")
		held.releasePending = true
		return
	}
	if held.secondKey == "" {
		delete(m.held, key)
		countClaimRemoval(held.key, held.taskID, primaryResult, held.leaseToken)
		return
	}
	secondaryResult, err := m.releaseRefWithDeadline(
		ctx,
		held.secondTarget,
		held.secondToken,
	)
	if err != nil {
		countClaim("error")
		held.releasePending = true
		return
	}
	delete(m.held, key)
	countClaimRemoval(
		held.key,
		held.taskID,
		pairRemovalResult(primaryResult, secondaryResult),
		held.leaseToken,
	)
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

// renewClaim renews both refs of a held claim with one deadline and counts one
// outcome for the claim. A failed primary renew leaves the pair's effective
// expiry at its previous value even when the secondary write succeeds, so the
// fence never reads a lease the primary does not have.
func (m *leaseManager) renewClaim(
	ctx context.Context,
	key string,
	held *heldClaim,
	now libtime.DateTime,
) {
	held.lastRenewAt = now
	held.renewNextTick = false
	result := "renewed"

	reqCtx, cancel := m.requestContext(ctx)
	renewed, err := m.claimClient.Renew(
		reqCtx,
		held.target,
		m.cluster,
		held.leaseToken,
		claimantTaskID(held.target, held.taskID),
		leaseSeconds(),
	)
	cancel()
	switch {
	case err == nil:
		held.leaseToken = renewed.LeaseToken
		held.leaseUntil = renewed.LeaseUntil
	case errors.Is(err, ErrLeaseLost):
		m.claimLost(ctx, key, held)
		return
	default:
		var ok bool
		result, ok = m.renewReadBack(ctx, held)
		if !ok {
			return
		}
	}

	if held.secondKey == "" {
		countClaim(result)
		return
	}

	reqCtx, cancel = m.requestContext(ctx)
	secondRenewed, err := m.claimClient.Renew(
		reqCtx,
		held.secondTarget,
		m.cluster,
		held.secondToken,
		claimantTaskID(held.secondTarget, held.taskID),
		leaseSeconds(),
	)
	cancel()
	switch {
	case err == nil:
		held.secondToken = secondRenewed.LeaseToken
		countClaim(result)
	case errors.Is(err, ErrLeaseLost):
		m.claimLost(ctx, key, held)
	default:
		held.renewNextTick = true
		countClaim("renew_failed")
	}
}

// claimLost deletes the Job and drops the claim without contacting the API: a
// lost lease is already gone, so releasing it would only be refused.
func (m *leaseManager) claimLost(ctx context.Context, key string, held *heldClaim) {
	countClaimRemoval(key, held.taskID, "claim_lost", held.leaseToken)
	if err := m.jobControl.DeleteJob(ctx, held.taskID); err != nil {
		countClaim("error")
		held.pendingJobDeletion = true
		held.pendingDeletionPath = "lost"
		return
	}
	delete(m.held, key)
}

// renewReadBack decides a failed primary renew from a fresh read, after
// re-running the fence. It returns the result label the caller must count and
// whether the claim is still held. On a recovered renew it adopts the token and
// lease the read returned; it never counts.
func (m *leaseManager) renewReadBack(
	ctx context.Context,
	held *heldClaim,
) (string, bool) {
	m.evaluateFence(ctx, m.currentDateTime.Now())
	if _, ok := m.held[held.key]; !ok {
		return "", false
	}
	reqCtx, cancel := m.requestContext(ctx)
	listed, err := m.readBack(reqCtx, held.target)
	cancel()
	switch {
	case err != nil:
		return "renew_failed", true
	case m.heldByUs(listed) && listed.LeaseToken == held.leaseToken:
		// The token is unchanged, so the renew did not land: this is one failed
		// interval, not a recovery. Adopting the listed lease here would report a
		// renewal that never happened.
		return "renew_failed", true
	case m.heldByUs(listed):
		held.leaseToken = listed.LeaseToken
		held.leaseUntil = listed.LeaseUntil
		return "recovered", true
	default:
		m.claimLost(ctx, held.key, held)
		return "", false
	}
}
