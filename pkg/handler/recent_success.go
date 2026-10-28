// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"strings"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// recentlySucceeded reports whether a Job for the task, spawned for the phase
// the task is still at, has already succeeded and must not be spawned again
// until that phase changes.
//
// This is a suppression, not a throttle. It began as a 300 s window covering the
// gap before the controller's write-back syncs (measured nuke dev 2026-10-21:
// task a055d019 spawned 3x, ~60 s apart, each Job done in ~20 s), but a window
// that lapses re-spawns the same unit of work for as long as the phase stays
// put — and for a task whose phase never advances that is forever, bounded only
// by maxConsecutiveRedrives. The success record is the authority: a task that
// succeeded at the phase it is still at has nothing left to do there.
//
// Scoped by phase, so a task the agent advanced (success at planning, file now
// at execution) is spawned for its next phase at once. A failed Job records
// nothing, so it is still re-driven.
//
// The one release besides a phase change is a recurring task type: a task
// declaring `recurring: daily|weekdays|weekly|monthly|quarterly` runs again once
// that period has elapsed, because its phase does not advance between
// occurrences and a permanent suppression would stop it for good. A task with no
// recognised token is not recurring, and the suppression holds until its phase
// changes.
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
	if recurringReleased(task.Frontmatter, elapsed) {
		return false
	}
	glog.Infof(
		"event=skip_recent_success task=%s phase=%s elapsed=%.0fs",
		task.TaskIdentifier, succeeded.Phase, elapsed.Seconds(),
	)
	metrics.TaskEventsTotal.WithLabelValues("skipped_recent_success").Inc()
	return true
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

// recurringReleased reports whether a task that recorded success at its current
// phase has served the cadence it declares and may run again.
//
// An absent or empty token means the task is not recurring. An unrecognised
// token is treated the same way and logged, never defaulted to a period:
// inventing a cadence for a token we do not understand would suppress that task
// forever, which is the failure this release exists to prevent.
func recurringReleased(frontmatter lib.TaskFrontmatter, elapsed time.Duration) bool {
	token, ok := frontmatter.String("recurring")
	if !ok {
		return false
	}
	trimmed := strings.TrimSpace(token)
	if trimmed == "" || trimmed == "<nil>" {
		return false
	}
	period, known := recurringCadences[strings.ToLower(trimmed)]
	if !known {
		glog.Warningf(
			"event=unknown_recurring_token token=%q elapsed=%.0fs treating as not recurring",
			token, elapsed.Seconds(),
		)
		return false
	}
	return elapsed >= period
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
