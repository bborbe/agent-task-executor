// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	lib "github.com/bborbe/agent"
)

// finalSpawnRefusals runs the two gates that stand immediately before the Job is
// created, and reports whether either refuses the spawn.
//
// claimStillValid is the claim-epoch check: a claimed replay must not spawn under
// an eviction that landed after its snapshot, and the lock order is assignee
// spawn lock → deferredMu.
//
// The vault-index gate is the authority on terminality, and this is the one spawn
// path that reaches it directly. The deferred seed, the deferred evaluation and
// gatherCandidate each check the index before they build a candidate, so a task
// they suppress never gets this far — but a task event for a completion another
// cluster recorded does, and the view may not have replayed that completion yet.
// Without it the foreign-Job guard is the only thing standing in the way, and it
// lets the spawn through as soon as the other cluster releases its claim:
// measured 2026-10-30, nuke prod recorded success for 406feb87 at 11:51:26Z,
// released the claim at 11:51:42Z, and sun created a Job for it at 11:52:15Z
// with no suppression line anywhere.
//
// It sits last because it is the only gate that costs a network read: every
// cheaper refusal above returns before reaching it. Inside a pass the read comes
// from the pass cache, so the extra request is one per pass, not one per spawn.
func (h *taskEventHandler) finalSpawnRefusals(
	ctx context.Context,
	task lib.Task,
	claim *deferredClaim,
) bool {
	if !h.claimStillValid(claim) {
		h.recordClaimInvalidated(task)
		return true
	}
	suppressions := &indexTerminalSuppression{}
	if !h.indexGateSuppresses(ctx, task, suppressions) {
		return false
	}
	suppressions.report()
	return true
}
