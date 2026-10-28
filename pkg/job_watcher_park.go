// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"
	batchv1 "k8s.io/api/batch/v1"
)

// drivePhases are the phases the executor drives. A succeeded Job whose task is
// still in one of these did not finish the work — it parked, and must leave the
// loop rather than be re-driven forever.
//
// TaskPhaseInProgress is the legacy alias for execution; it is still a known
// phase string, so a task written by an older vault-cli must not slip past the
// guard.
var drivePhases = map[domain.TaskPhase]struct{}{
	domain.TaskPhasePlanning:   {},
	domain.TaskPhaseExecution:  {},
	domain.TaskPhaseInProgress: {},
	domain.TaskPhaseAIReview:   {},
}

// parkIfPhaseDidNotMove publishes phase human_review when a succeeded Job's task
// is provably still in a drive phase.
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
	viewTask, phase, ok := w.immobileDrivePhase(taskID, job)
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

// immobileDrivePhase reports the view's task and its current phase when the
// phase is provably a drive phase that did not move, and false when that cannot
// be proven.
//
// The proof has four parts, and every one of them is a way this guard could
// otherwise write human_review over a task the agent had actually finished:
//
//   - the view must be ready — an unready view holds a partial replay, so an
//     absent or stale entry proves nothing;
//   - the view must hold an entry for the id at all;
//   - the phase must be a drive phase;
//   - the entry must not be newer than the job's completion — a task event that
//     landed after the job finished is the agent (or anything else) writing a
//     change, so the phase may well have moved and the view has not caught up.
//
// A zero Modified is treated as unprovable rather than as ancient: it means the
// producer did not stamp the field, and guessing would reintroduce exactly the
// overwrite this guard exists to prevent. The skip is logged under its own
// event name so a zero-stamp population is visible in the dev measurement
// instead of silently disabling the park.
func (w *jobWatcher) immobileDrivePhase(
	taskID lib.TaskIdentifier,
	job *batchv1.Job,
) (lib.Task, domain.TaskPhase, bool) {
	if w.taskView == nil {
		return lib.Task{}, "", false
	}
	if !w.taskView.Ready() {
		glog.V(2).Infof("event=park_skipped task=%s reason=view_not_ready", taskID)
		return lib.Task{}, "", false
	}
	viewTask, ok := w.taskView.Get(taskID)
	if !ok {
		glog.V(2).Infof("event=park_skipped task=%s reason=no_view_entry", taskID)
		return lib.Task{}, "", false
	}
	phase := viewTask.Frontmatter.Phase()
	if phase == nil {
		glog.V(2).Infof("event=park_skipped task=%s reason=no_phase", taskID)
		return lib.Task{}, "", false
	}
	if _, driven := drivePhases[*phase]; !driven {
		return lib.Task{}, "", false
	}
	if job.Status.CompletionTime == nil {
		glog.V(2).Infof("event=park_skipped task=%s reason=no_completion_time", taskID)
		return lib.Task{}, "", false
	}
	modified := time.Time(viewTask.Modified)
	if modified.IsZero() {
		glog.V(2).Infof("event=park_skipped task=%s reason=unstamped_modified", taskID)
		return lib.Task{}, "", false
	}
	if modified.After(job.Status.CompletionTime.Time) {
		glog.V(2).Infof(
			"event=park_skipped task=%s reason=task_updated_after_job phase=%s",
			taskID,
			*phase,
		)
		return lib.Task{}, "", false
	}
	return viewTask, *phase, true
}
