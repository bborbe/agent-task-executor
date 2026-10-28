// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"strings"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// recentlySucceeded reports whether a Job for the task, spawned for the phase
// the task is still at, has already succeeded and this pass must not spawn it
// again.
//
// It answers for two windows, not one. A recurring task is suppressed until the
// cadence it declares has elapsed, because its phase does not advance between
// occurrences and anything shorter would stop it for good. Every other task is
// suppressed only inside the write-back grace; past that window
// parkSucceededWithoutPhaseMove takes over and publishes human_review, so the
// record leaves the loop and reaches a human instead of being suppressed in
// silence forever.
//
// The success record is the authority, and it is shared: a pod that replaced the
// one which ran the Job reads the same record, which is what makes either
// outcome survive a restart.
//
// Scoped by phase, so a task the agent advanced (success at planning, file now
// at execution) is spawned for its next phase at once. A failed Job records
// nothing, so it is still re-driven.
func (h *taskEventHandler) recentlySucceeded(ctx context.Context, task lib.Task) bool {
	phase := taskPhase(task)
	succeeded, ok := h.lastSuccess(ctx, task, phase)
	if !ok {
		return false
	}
	elapsed := h.currentDateTime.Now().Time().Sub(succeeded.CompletedAt)
	if elapsed < 0 {
		// completionTime is stamped by the API server, "now" by this pod; a
		// small skew must not read as a success from the future. Count it as
		// just completed, which keeps the guard engaged.
		elapsed = 0
	}
	if period, recurring := recurringPeriod(task.Frontmatter); recurring {
		if elapsed >= period {
			return false
		}
	} else if elapsed >= defaultRespawnGracePeriod {
		// Past the write-back window a non-recurring task is not suppressed here:
		// the caller parks it, which suppresses the spawn just as firmly and
		// leaves a record a human can see.
		return false
	}
	glog.Infof(
		"event=skip_recent_success task=%s phase=%s elapsed=%.0fs",
		task.TaskIdentifier, succeeded.Phase, elapsed.Seconds(),
	)
	metrics.TaskEventsTotal.WithLabelValues("skipped_recent_success").Inc()
	return true
}

// lastSuccess returns the task's latest successful Job at phase.
//
// The pod's own memory is asked first: it is written the moment the Job watcher
// sees the success, so on the pod that ran the Job it is both cheaper and more
// current than a store read. Only a miss — the fresh-store case, a pod that
// started after the Job completed — reaches the shared record, and a hit there is
// written back into memory so every later attempt on this pod is free.
//
// The write-back is what keeps the two halves on one retention horizon: the
// reconcile prune bounds the map, and the shared record's lease bounds the store,
// and both use successRecordRetention, so a record does not reappear from the
// store after the map has forgotten it.
func (h *taskEventHandler) lastSuccess(
	ctx context.Context,
	task lib.Task,
	phase string,
) (pkg.SucceededJob, bool) {
	if succeeded, ok := h.taskStore.LastSucceeded(task.TaskIdentifier); ok &&
		succeeded.Phase == phase {
		return succeeded, true
	}
	completedAt, ok := h.sharedSuccess(ctx, task.TaskIdentifier, phase)
	if !ok {
		return pkg.SucceededJob{}, false
	}
	h.taskStore.MarkSucceeded(task.TaskIdentifier, phase, completedAt.Time())
	return pkg.SucceededJob{Phase: phase, CompletedAt: completedAt.Time()}, true
}

// recurringCadences maps the declared `recurring:` token to the period after
// which a recurring task may run again at an unchanged phase. The token is the
// operator-facing vocabulary task files already carry; the durations are the
// fixed reading of each token, not tunables.
var recurringCadences = map[string]time.Duration{
	"daily":     24 * time.Hour,
	"weekdays":  24 * time.Hour,
	"weekly":    7 * 24 * time.Hour,
	"monthly":   30 * 24 * time.Hour,
	"quarterly": 90 * 24 * time.Hour,
}

// successRecordRetention is how long a success record is kept before the
// reconcile pass prunes it. It must cover the longest declared cadence, not the
// re-run window: the suppression is driven by the record, and a recurring task
// is released only when its cadence elapses, so pruning at taskRerunWindow (1 h)
// would forget the record long before any cadence could fire and make the
// release unreachable. Derived from recurringCadences so the two cannot drift.
var successRecordRetention = func() time.Duration {
	longest := taskRerunWindow
	for _, period := range recurringCadences {
		if period > longest {
			longest = period
		}
	}
	return longest
}()

// recurringPeriod returns the cadence the task declares in its `recurring:`
// frontmatter key, and whether the task is recurring at all.
//
// An absent or empty token means the task is not recurring. An unrecognised
// token is treated the same way and logged, never defaulted to a period:
// inventing a cadence for a token we do not understand would suppress that task
// forever, which is the failure this release exists to prevent.
func recurringPeriod(frontmatter lib.TaskFrontmatter) (time.Duration, bool) {
	token, ok := frontmatter.String("recurring")
	if !ok {
		return 0, false
	}
	trimmed := strings.TrimSpace(token)
	if trimmed == "" || trimmed == "<nil>" {
		return 0, false
	}
	period, known := recurringCadences[strings.ToLower(trimmed)]
	if !known {
		glog.Warningf(
			"event=unknown_recurring_token token=%q treating as not recurring",
			token,
		)
		return 0, false
	}
	return period, true
}

// recordTaskRerun counts a spawn whose task already succeeded at the phase it
// is being spawned for, inside taskRerunWindow. That is what a re-run is: the
// same unit of work running twice.
//
// Scoped by phase for the same reason recentlySucceeded is. A task that
// succeeds at planning and is spawned again for execution has not re-run — it
// advanced — and counting that would fire the re-run alert on every ordinary
// task. Only a second spawn for the phase that just succeeded is a re-run.
//
// The series exists at 0 from the first spawn, not only after the first re-run:
// WithLabelValues creates the child metric on miss. That is what lets the
// absent-tier alert tell "no re-run yet" from "the metric stopped being
// exported" — the second being the failure that alert exists to catch.
func (h *taskEventHandler) recordTaskRerun(task lib.Task) {
	taskType := string(task.Frontmatter.TaskType())
	counter := metrics.TaskRerunsTotal.WithLabelValues(taskType)
	succeeded, ok := h.taskStore.LastSucceeded(task.TaskIdentifier)
	if !ok || succeeded.Phase != taskPhase(task) {
		return
	}
	elapsed := h.currentDateTime.Now().Time().Sub(succeeded.CompletedAt)
	if elapsed < 0 || elapsed >= taskRerunWindow {
		return
	}
	counter.Inc()
	glog.Infof(
		"event=task_rerun task=%s task_type=%s phase=%s elapsed=%.0fs",
		task.TaskIdentifier, taskType, succeeded.Phase, elapsed.Seconds(),
	)
}

// taskPhase returns the task's phase as a string, or "" when absent.
func taskPhase(task lib.Task) string {
	if p := task.Frontmatter.Phase(); p != nil {
		return string(*p)
	}
	return ""
}
