// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"time"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// ClaimLifecycleEventHandler is the TaskEventHandler plus the claim lifecycle
// seam main and the Job watcher use. Every claim method is a no-op in claim
// mode off.
type ClaimLifecycleEventHandler interface {
	TaskEventHandler
	pkg.ClaimReleaser
	// ClaimLeaseManager returns this executor's lease manager, or nil in claim
	// mode off, where no manager is constructed.
	ClaimLeaseManager(ctx context.Context) (claim.LeaseManager, error)
}

// ReleaseTaskClaim releases every claim this executor holds for taskID. It is
// the single release handle every end point calls. A claim the manager no
// longer holds is a no-op, so two end points firing for one claim release once.
// A failed delete is logged and left to the lease loop's retryPendingReleases;
// this function never retries and never returns an error.
func (h *taskEventHandler) ReleaseTaskClaim(ctx context.Context, taskID lib.TaskIdentifier) {
	if h.claimMode == pkg.ClaimModeOff {
		return
	}
	// The pending check, the HeldClaims snapshot and the deletes all run under
	// the per-task claim lock the spawn path takes around mark+claim, so a spawn
	// cannot slip its fresh claim in between the check and the delete.
	unlockClaim := h.lockClaimRelease(taskID)
	defer unlockClaim()
	h.releaseTaskClaimLocked(ctx, taskID)
}

// ReleaseTaskClaimAfterSuccess is ReleaseTaskClaim for a Job that SUCCEEDED: the
// claim is not deleted yet, it is held for claim.SuccessGracePeriod from
// completedAt and released by the lease loop once that window passes.
//
// The window exists because a succeeded Job is not yet a finished task. The
// agent published its result, but the vault learns about it on the controller's
// own cycle, and until it does a peer cluster reads the task as still eligible —
// with no record anywhere that it just ran, since that record is per-pod memory.
// The claim is the one artefact both clusters share, so keeping it is what makes
// the peer's Claim fail instead of running the task twice. Measured 2026-10-23:
// task 1788bf3c completed on nuke prod at 05:55:00 and ran again on sun at
// 05:55:30, both successful.
//
// A failed Job must use ReleaseTaskClaim instead: a retry belongs to this
// cluster, and holding its claim would only delay it.
func (h *taskEventHandler) ReleaseTaskClaimAfterSuccess(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	phase string,
	completedAt time.Time,
) {
	// Recorded first, and in every claim-taking mode. The hold below covers the peer
	// window; the record covers the restart — a fresh pod has never heard of the
	// success, so without it the suppression is lost on every deploy. The two are
	// written through one end point rather than a second parallel path.
	h.recordTaskSuccess(ctx, taskID, phase, completedAt)
	if h.claimMode == pkg.ClaimModeOff {
		return
	}
	unlockClaim := h.lockClaimRelease(taskID)
	defer unlockClaim()
	h.releaseTaskClaimLockedWith(
		ctx,
		taskID,
		func(manager claim.LeaseManager, key string) error {
			return manager.ReleaseAfterSuccess(ctx, key, libtime.DateTime(completedAt))
		},
	)
}

// releaseTaskClaimLocked is ReleaseTaskClaim's body for a caller that already
// holds this task's claim lock. The failed-spawn path needs it so that clearing
// the in-flight marker and releasing are atomic against a concurrent spawn.
func (h *taskEventHandler) releaseTaskClaimLocked(
	ctx context.Context,
	taskID lib.TaskIdentifier,
) {
	h.releaseTaskClaimLockedWith(
		ctx,
		taskID,
		func(manager claim.LeaseManager, key string) error {
			return manager.Release(ctx, key)
		},
	)
}

// releaseTaskClaimLockedWith runs one release per claim this executor holds for
// the task, using the caller's release strategy.
func (h *taskEventHandler) releaseTaskClaimLockedWith(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	release func(manager claim.LeaseManager, key string) error,
) {
	// A spawn in flight for this task holds the claim for the replacement Job:
	// the store still names the previous Job, so a terminal event arriving here
	// is about work that has already been superseded. Skipping is the safe
	// direction — the spawn either records its Job and keeps the claim, or fails
	// and releases it explicitly.
	if h.spawnPendingFor(taskID) {
		glog.V(2).Infof(
			"event=claim_release_skipped task=%s reason=spawn_pending",
			taskID,
		)
		return
	}
	manager, err := h.leaseManager(ctx)
	if err != nil {
		glog.Warningf(
			"event=claim_release_failed task=%s stage=lease_manager err=%v",
			taskID,
			err,
		)
		return
	}
	for _, held := range manager.HeldClaims() {
		if held.TaskID != string(taskID) {
			continue
		}
		if releaseErr := release(manager, held.Key); releaseErr != nil {
			glog.Warningf(
				"event=claim_release_failed task=%s key=%s err=%v",
				taskID,
				held.Key,
				releaseErr,
			)
		}
	}
}

// ClaimLeaseManager returns this executor's lease manager, or nil in claim mode
// off, where no manager is constructed.
func (h *taskEventHandler) ClaimLeaseManager(
	ctx context.Context,
) (claim.LeaseManager, error) {
	if h.claimMode == pkg.ClaimModeOff {
		return nil, nil
	}
	return h.leaseManager(ctx)
}
