// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// checkActiveCurrentJob verifies whether spawn must be suppressed due to current_job state.
// Returns (true, nil) when the spawn must be suppressed (job still active or inside grace window).
// Returns (false, nil) when spawn may proceed. Returns (false, err) on unexpected errors.
func (h *taskEventHandler) checkActiveCurrentJob(
	ctx context.Context,
	task lib.Task,
	currentJob string,
	config *pkg.AgentConfiguration,
	claim *deferredClaim,
) (bool, error) {
	active, err := h.jobSpawner.IsJobActive(ctx, task.TaskIdentifier)
	if err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return false, errors.Wrapf(
			ctx,
			err,
			"check current_job active for task %s",
			task.TaskIdentifier,
		)
	}
	if active {
		glog.V(3).Infof(
			"skip task %s: current_job %s still active (from frontmatter)",
			task.TaskIdentifier, currentJob,
		)
		metrics.TaskEventsTotal.WithLabelValues("skipped_active_job").Inc()
		return true, nil
	}
	// Grace window: suppress respawn while the agent's terminal-phase write propagates.
	// Treat missing or unparseable job_started_at as elapsed (preserves legacy-task behavior).
	jobStartedAt, parseErr := task.Frontmatter.JobStartedAt()
	if parseErr != nil {
		glog.Warningf(
			"task %s: failed to parse job_started_at: %v; treating grace period as elapsed",
			task.TaskIdentifier, parseErr,
		)
	}
	if parseErr == nil && !jobStartedAt.IsZero() {
		elapsed := h.currentDateTime.Now().Time().Sub(jobStartedAt)
		if elapsed < defaultRespawnGracePeriod {
			glog.Infof(
				"event=respawn_grace_window task=%s current_job=%s elapsed=%.0fs",
				task.TaskIdentifier, currentJob, elapsed.Seconds(),
			)
			metrics.TaskEventsTotal.WithLabelValues("respawn_grace_window").Inc()
			h.storeGraceWindowDeferral(
				task,
				config,
				jobStartedAt.Add(defaultRespawnGracePeriod),
				claim,
			)
			return true, nil
		}
	}
	// No Job for this task is active here, which is not the same as the Job
	// being over: current_job lives in the shared vault frontmatter, so it can
	// name a Job another cluster created, and this informer only sees its own
	// cluster's Jobs. Without this guard the frontmatter's Job reads as finished
	// and authorises a second cluster to run a task that is still running.
	provablyFinished, err := h.jobProvablyFinished(ctx, task, currentJob)
	if err != nil {
		return false, errors.Wrapf(ctx, err, "check whether current_job is finished")
	}
	if !provablyFinished {
		// V(1): a task in the foreign state repeats this line on every dispatch
		// pass and every deferred replay, like redriveStreakCapped. The
		// skipped_foreign_job counter is the alerting signal.
		glog.V(1).Infof(
			"event=respawn_foreign_job task=%s current_job=%s: this executor spawned a different Job for the task; suppressing spawn",
			task.TaskIdentifier,
			currentJob,
		)
		metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_job").Inc()
		h.storeGraceWindowDeferral(
			task,
			config,
			h.currentDateTime.Now().Time().Add(deferredRespawnInterval),
			claim,
		)
		return true, nil
	}

	glog.V(2).Infof(
		"task %s: current_job %s no longer active, proceeding to spawn",
		task.TaskIdentifier, currentJob,
	)
	return false, nil
}

// jobProvablyFinished reports whether the Job named in current_job has certainly
// ended, given that no Job for this task is active in this cluster.
//
// current_job lives in the shared vault frontmatter, so it can name a Job that
// another cluster created, and this informer only sees its own cluster's Jobs —
// "not active here" is not by itself evidence that the Job is over. The one
// local fact that settles it is the store: its entry is written when this
// executor creates a Job and removed when that Job's terminal event is
// processed, which is also what clears current_job. A store entry naming a
// different Job therefore proves the frontmatter's Job belongs to someone else.
//
// With no store entry (a restart, or a spawn this executor never made) there is
// nothing local to distinguish a foreign Job from a finished one, so the
// previous behaviour stands: the grace window is the only bound, and a respawn
// after it proceeds. Bounding that case by the Job's deadline instead was tried
// and reverted — it withholds a legitimate respawn for up to 30 minutes, which
// the deferred-respawn contract does not allow.
//
// A store entry can also LEAK, which the mismatch rule alone would turn into
// permanent suppression. Nothing clears an entry except the job informer
// processing that Job's terminal event, and the informer registers no
// DeleteFunc: a Job garbage-collected before its terminal update was observed
// leaves an entry no one ever removes, and the task is then suppressed on every
// pass, forever. So the mismatch is only treated as proof while the stored Job
// still exists here — if the cluster says it is gone, the entry is stale, it is
// dropped, and the spawn proceeds.
func (h *taskEventHandler) jobProvablyFinished(
	ctx context.Context,
	task lib.Task,
	currentJob string,
) (bool, error) {
	stored := h.storedCurrentJob(task)
	if stored == "" || stored == currentJob {
		return true, nil
	}
	exists, err := h.jobSpawner.JobExists(ctx, stored)
	if err != nil {
		// A failed read is not evidence of absence: keep suppressing, and let the
		// next pass retry rather than risk spawning alongside a live foreign Job.
		return false, err
	}
	if exists {
		return false, nil
	}
	glog.Infof(
		"event=cleared_stale_store_entry task=%s job=%s: the Job this executor recorded no longer exists in this cluster",
		task.TaskIdentifier,
		stored,
	)
	h.taskStore.Delete(task.TaskIdentifier)
	return true, nil
}
