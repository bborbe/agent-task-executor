// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	lib "github.com/bborbe/agent"
	libk8s "github.com/bborbe/k8s"
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
	namespace, err := h.jobNamespaceForTask(ctx, config, task.TaskIdentifier)
	if err != nil {
		// Not counted here: the caller counts every error it propagates, and an
		// increment on both sides would count this failure twice.
		return false, err
	}
	active, err := h.jobSpawner.IsJobActive(ctx, namespace, task.TaskIdentifier)
	if err != nil {
		// The caller counts the error, not this branch — see deferIfAtConcurrencyCap.
		return false, wrapJobLookupError(
			ctx,
			err,
			assigneeOf(config),
			namespace,
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
	if h.withinRespawnGraceWindow(task, config, currentJob, claim) {
		return true, nil
	}
	// No Job for this task is active here, which is not the same as the Job
	// being over: current_job lives in the shared vault frontmatter, so it can
	// name a Job another cluster created, and this informer only sees its own
	// cluster's Jobs. Without this guard the frontmatter's Job reads as finished
	// and authorises a second cluster to run a task that is still running.
	provablyFinished, err := h.currentJobFinished(
		ctx,
		task,
		currentJob,
		namespace,
		assigneeOf(config),
	)
	if err != nil {
		return false, err
	}
	if !provablyFinished {
		// V(1): a task in the foreign state repeats this line on every dispatch
		// pass and every deferred replay, like redriveStreakCapped. The
		// skipped_foreign_job counter is the alerting signal.
		glog.V(1).Infof(
			"event=respawn_foreign_job task=%s current_job=%s: the Job is not provably over here — another cluster's Job, or a live claim on the task; suppressing spawn",
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

// storedCurrentJob returns the store entry's current_job, or "" when the store
// holds no entry for the task.
func (h *taskEventHandler) storedCurrentJob(task lib.Task) string {
	stored, ok := h.taskStore.Load(task.TaskIdentifier)
	if !ok {
		return ""
	}
	return stored.Frontmatter.CurrentJob()
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
// With no store entry (a restart, or a spawn this executor never made) nothing
// LOCAL distinguishes a foreign Job from a finished one — which is why the
// store alone cannot settle that case. Bounding it by the Job's deadline was
// tried and reverted: it withholds a legitimate respawn for up to 30 minutes,
// which the deferred-respawn contract does not allow.
//
// What does settle it is the claim. A claim is written to the shared ref store
// before a Job is created and released when that Job ends (held for the success
// grace after a success), so a live claim proves some cluster is still running
// the task while its absence proves none is — including the cluster whose Job
// the frontmatter names. That is the same artefact the exactly-once work relies
// on, read instead of written. Claim mode off leaves no such record, so there
// the previous behaviour stands unchanged, and this function says so rather than
// reading the absence of a claim it never had as proof.
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
	namespace libk8s.Namespace,
) (bool, error) {
	stored := h.storedCurrentJob(task)
	if stored != "" && stored == currentJob {
		return true, nil
	}
	if stored == "" {
		return h.currentJobFinishedWithoutStoreEntry(ctx, task)
	}
	exists, err := h.jobSpawner.JobExists(ctx, namespace, stored)
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
	// Compare-and-delete: the JobExists call above is I/O, and a spawn can store a
	// new entry for a new Job while it is in flight. Deleting unconditionally would
	// drop that entry and leave its Job unclaimed and invisible to the release path.
	h.taskStore.DeleteIfCurrentJob(task.TaskIdentifier, stored)
	return true, nil
}

// currentJobFinishedWithoutStoreEntry decides the case the store cannot: no
// entry names this task, so the Job in current_job is either this executor's
// own from before a restart, or one another cluster created. Nothing local
// separates them; the claim does.
func (h *taskEventHandler) currentJobFinishedWithoutStoreEntry(
	ctx context.Context,
	task lib.Task,
) (bool, error) {
	probe, err := h.claimProbe(ctx, task)
	if err != nil {
		// A failed read is not evidence of absence — suppress and retry on the
		// next pass, the same direction the failed Job lookup takes. Not fatal:
		// a vault-api blip must not restart the executor for every assignee. The
		// probe already counted and logged it, with the stage that failed.
		return false, nil
	}
	switch probe {
	case claimProbeHeld:
		glog.V(2).Infof(
			"event=foreign_job_claimed_elsewhere task=%s: a live claim shows the task is still running somewhere this cluster cannot see",
			task.TaskIdentifier,
		)
		return false, nil
	case claimProbeFree:
		return true, nil
	default:
		// Claim mode off keeps no shared record, so the ambiguity cannot be
		// resolved here. Stated rather than hidden: the respawn proceeds on
		// local evidence alone, which is the pre-existing behaviour.
		glog.V(2).Infof(
			"event=foreign_job_unprobeable task=%s mode=%s: no claim is kept to read; respawn proceeds on local evidence",
			task.TaskIdentifier,
			h.claimMode,
		)
		return true, nil
	}
}
