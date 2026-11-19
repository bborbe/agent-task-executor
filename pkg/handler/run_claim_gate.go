// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// DefaultRunClaimTTL is how long a run_claim frontmatter value is treated as
// live. It matches the dev run budget (runBudgetSeconds: 7200), so one claim
// covers the whole window a single trading run may occupy.
const DefaultRunClaimTTL = 2 * time.Hour

// suppressForActiveRun answers the two frontmatter liveness gates in order:
// the existing current_job gate first, then the run_claim gate. It exists
// because spawnIfNeededClaimed is at gocognit's limit of 20; folding both
// gates behind one call keeps the funnel's complexity unchanged.
func (h *taskEventHandler) suppressForActiveRun(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
	claim *deferredClaim,
) (bool, error) {
	suppress, err := h.suppressForActiveCurrentJob(ctx, task, config, claim)
	if err != nil || suppress {
		return suppress, err
	}
	return h.suppressForLiveRunClaim(task), nil
}

// suppressForLiveRunClaim answers the run_claim gate. A task whose frontmatter
// carries a run_claim younger than DefaultRunClaimTTL is suppressed: the
// trading run has already started and the executor must not start a second
// Job for the same idea. A missing, stale or unparseable claim lets the spawn
// proceed unchanged. The gate is read-only; it writes no frontmatter, parks
// nothing and publishes nothing, so the reconcile floor re-drives the task
// once the claim expires.
func (h *taskEventHandler) suppressForLiveRunClaim(task lib.Task) bool {
	raw, ok := task.Frontmatter.String("run_claim")
	if !ok || raw == "" {
		return false
	}
	claimedAt, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return false
	}
	age := h.currentDateTime.Now().Time().Sub(claimedAt)
	if age >= DefaultRunClaimTTL {
		return false
	}
	glog.Infof(
		"event=spawn_suppressed_run_claim task=%s run_claim=%s age=%.0fs",
		task.TaskIdentifier,
		claimedAt.UTC().Format(time.RFC3339),
		age.Seconds(),
	)
	metrics.TaskEventsTotal.WithLabelValues("skipped_run_claim").Inc()
	return true
}
