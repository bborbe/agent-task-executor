// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"
	batchv1 "k8s.io/api/batch/v1"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// parkIfPhaseDidNotMove publishes phase human_review when a succeeded Job's task
// is still at the phase that Job was spawned for.
//
// It never fails the success path: a publish error is logged and the record is
// left to the next pass, because the succeeded-Job handling itself is correct
// regardless of whether the park landed.
//
// It takes the task from the fresh-read view rather than the caller's store
// entry, so it also covers a task the store no longer holds — after an executor
// restart, or once the entry is evicted. The store's phase is the spawn phase by
// construction and must not be published from.
func (w *jobWatcher) parkIfPhaseDidNotMove(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	job *batchv1.Job,
) {
	viewTask, phase, ok := w.immobilePhase(taskID, job)
	if !ok {
		return
	}
	if err := w.publisher.PublishParkSucceeded(ctx, viewTask); err != nil {
		glog.Errorf(
			"publish park for task %s (job %s, phase %s): %v",
			taskID,
			job.Name,
			phase,
			err,
		)
		return
	}
	glog.Infof(
		"event=parked_succeeded task=%s job=%s phase=%s",
		taskID,
		job.Name,
		phase,
	)
}

// immobilePhase reports the view's task and phase when the phase provably did
// not move, and false when that cannot be proven.
//
// The discriminator is the phase the Job was SPAWNED for, recorded on the Job as
// TaskPhaseAnnotationKey — not "is the phase a drive phase". A Job is spawned for
// one phase and the agent may advance the task inside that same Job and still
// exit 0: an agent that finishes `planning` writes `phase: execution` and expects
// the controller to drive the next phase. Parking on "still a drive phase" would
// rewrite human_review over that advance and stall every multi-phase agent at its
// first phase. recent_success.go uses the same comparison for the same reason.
//
// The other conditions exist because a phase that has not moved YET is not the
// same as one that did not move:
//
//   - the view must be ready — an unready view holds a partial replay;
//   - the view must hold an entry for the id;
//   - the Job must carry its spawn phase — absent, nothing can be compared;
//   - the task must not have been re-spawned under a newer Job. The informer
//     re-delivers a terminal Job every ~5 min, so without this fence a
//     re-delivered job-1 success parks a task that job-2 is running right now.
//     Every other terminal path on this watcher is fenced the same way;
//   - the success grace must have elapsed. A succeeded Job means the agent
//     published its result, but the vault learns about it on the controller's
//     own cycle, so for up to SuccessGracePeriod the file still reads the phase
//     the Job ran for. Parking at the moment of completion acts on maximum
//     ambiguity — it cannot see a write that is in flight, only one already
//     applied. Nothing re-drives inside the grace, so waiting costs nothing and
//     makes the comparison meaningful;
//   - the task must still be re-drive-eligible: status in_progress and an
//     assignee. A needs_input result clears the assignee and leaves the phase
//     alone by design, so the record is already out of the loop and parked in
//     the operator inbox; rewriting its phase would contradict the agent.
func (w *jobWatcher) immobilePhase(
	taskID lib.TaskIdentifier,
	job *batchv1.Job,
) (lib.Task, domain.TaskPhase, bool) {
	if w.taskView == nil {
		return lib.Task{}, "", false
	}
	if !w.taskView.Ready() {
		glog.Infof("event=park_skipped task=%s reason=view_not_ready", taskID)
		return lib.Task{}, "", false
	}
	spawnPhase := job.Annotations[TaskPhaseAnnotationKey]
	if spawnPhase == "" {
		glog.Infof("event=park_skipped task=%s reason=no_spawn_phase", taskID)
		return lib.Task{}, "", false
	}
	viewTask, ok := w.taskView.Get(taskID)
	if !ok {
		glog.V(2).Infof("event=park_skipped task=%s reason=no_view_entry", taskID)
		return lib.Task{}, "", false
	}
	if currentJob := viewTask.Frontmatter.CurrentJob(); currentJob != "" && currentJob != job.Name {
		glog.V(2).Infof(
			"event=park_skipped task=%s reason=superseded_by_job current_job=%s",
			taskID,
			currentJob,
		)
		return lib.Task{}, "", false
	}
	if job.Status.CompletionTime == nil {
		glog.V(2).Infof("event=park_skipped task=%s reason=no_completion_time", taskID)
		return lib.Task{}, "", false
	}
	elapsed := w.currentDateTime.Now().Time().Sub(job.Status.CompletionTime.Time)
	if elapsed < 0 {
		// CompletionTime is stamped by the API server, "now" by this pod; a
		// small skew must not read as a Job that finished in the future.
		elapsed = 0
	}
	if elapsed < claim.SuccessGracePeriod {
		glog.V(2).Infof(
			"event=park_skipped task=%s reason=within_success_grace elapsed=%.0fs",
			taskID,
			elapsed.Seconds(),
		)
		return lib.Task{}, "", false
	}
	phase := viewTask.Frontmatter.Phase()
	if phase == nil {
		glog.Infof("event=park_skipped task=%s reason=no_phase", taskID)
		return lib.Task{}, "", false
	}
	if string(*phase) != spawnPhase {
		// The agent advanced the task inside this Job. Not a park.
		return lib.Task{}, "", false
	}
	if status := viewTask.Frontmatter.Status(); status != domain.TaskStatusInProgress {
		glog.V(2).Infof(
			"event=park_skipped task=%s reason=not_re_drive_eligible status=%s",
			taskID,
			status,
		)
		return lib.Task{}, "", false
	}
	if viewTask.Frontmatter.Assignee() == "" {
		glog.V(2).Infof("event=park_skipped task=%s reason=no_assignee", taskID)
		return lib.Task{}, "", false
	}
	return viewTask, *phase, true
}
