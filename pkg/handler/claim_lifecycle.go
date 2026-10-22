// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	lib "github.com/bborbe/agent"
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

// releaseTaskClaimLocked is ReleaseTaskClaim's body for a caller that already
// holds this task's claim lock. The failed-spawn path needs it so that clearing
// the in-flight marker and releasing are atomic against a concurrent spawn.
func (h *taskEventHandler) releaseTaskClaimLocked(
	ctx context.Context,
	taskID lib.TaskIdentifier,
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
		if releaseErr := manager.Release(ctx, held.Key); releaseErr != nil {
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
