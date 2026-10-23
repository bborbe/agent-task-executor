// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	lib "github.com/bborbe/agent"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// recentlySucceeded reports whether a Job for the task, spawned for the phase
// the task is still at, succeeded within the respawn grace window. Such a
// task's result is still on its way to the vault: the file keeps reading the
// phase the Job ran for until the controller's write-back syncs, and the spawn
// notification carrying current_job may not have landed either, so
// checkActiveCurrentJob's grace never engages. Without this the reconcile floor
// re-spawned a finished task every tick until the write-back arrived (measured
// nuke dev 2026-10-21: task a055d019 spawned 3x, ~60 s apart, each Job done in
// ~20 s).
//
// Scoped by phase so a task the agent advanced (success at planning, file now
// at execution) is spawned for its next phase at once. A failed Job records
// nothing, so it is still re-driven; a task still eligible after the window is
// spawned as before.
func (h *taskEventHandler) recentlySucceeded(task lib.Task) bool {
	succeeded, ok := h.taskStore.LastSucceeded(task.TaskIdentifier)
	if !ok || succeeded.Phase != taskPhase(task) {
		return false
	}
	elapsed := h.currentDateTime.Now().Time().Sub(succeeded.CompletedAt)
	if elapsed < 0 {
		// completionTime is stamped by the API server, "now" by this pod; a
		// small skew must not read as a success from the future. Count it as
		// just completed, which keeps the guard engaged.
		elapsed = 0
	}
	if elapsed >= defaultRespawnGracePeriod {
		return false
	}
	glog.Infof(
		"event=skip_recent_success task=%s phase=%s elapsed=%.0fs",
		task.TaskIdentifier, succeeded.Phase, elapsed.Seconds(),
	)
	metrics.TaskEventsTotal.WithLabelValues("skipped_recent_success").Inc()
	return true
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
