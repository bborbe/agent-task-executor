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
	if next, recurring := recurringNextRun(task.Frontmatter, succeeded.CompletedAt); recurring {
		if !h.currentDateTime.Now().Time().Before(next) {
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
// Both sources are consulted and the later completion wins. Memory alone is not
// enough: a pod caches the completion it saw, so a peer cluster that recorded a
// newer success at the same phase is invisible to it. On a recurring task that is
// a duplicate, not a stale read — the peer's newer success is what should hold the
// cadence, and evaluating on the cached older one makes the cadence look elapsed
// and spawns a second Job for work already done. Comparing the two is what makes
// the shared record the authority it was added to be.
//
// The read is cheap where it matters: inside a reconcile pass the shared side is
// the pass snapshot (one list for every candidate), and on the event paths it is
// one Lookup per event, cached back into memory on a hit so the next event on this
// pod is free.
//
// A failed shared read falls back to memory rather than to "no record": a store
// that cannot be read is not evidence the task has not succeeded, and treating it
// as one would strand the task behind an unreachable store.
func (h *taskEventHandler) lastSuccess(
	ctx context.Context,
	task lib.Task,
	phase string,
) (pkg.SucceededJob, bool) {
	memory, hasMemory := h.taskStore.LastSucceeded(task.TaskIdentifier)
	if hasMemory && memory.Phase != phase {
		hasMemory = false
	}
	shared, hasShared := h.sharedSuccess(ctx, task.TaskIdentifier, phase)
	switch {
	case hasMemory && hasShared:
		if shared.Time().After(memory.CompletedAt) {
			h.taskStore.MarkSucceeded(task.TaskIdentifier, phase, shared.Time())
			return pkg.SucceededJob{Phase: phase, CompletedAt: shared.Time()}, true
		}
		return memory, true
	case hasShared:
		h.taskStore.MarkSucceeded(task.TaskIdentifier, phase, shared.Time())
		return pkg.SucceededJob{Phase: phase, CompletedAt: shared.Time()}, true
	case hasMemory:
		return memory, true
	}
	return pkg.SucceededJob{}, false
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
	token, ok := recurringToken(frontmatter)
	if !ok {
		return 0, false
	}
	return recurringCadences[token], true
}

// recurringToken returns the task's declared cadence token, normalised, and
// whether it declares a recognised one at all.
func recurringToken(frontmatter lib.TaskFrontmatter) (string, bool) {
	token, ok := frontmatter.String("recurring")
	if !ok {
		return "", false
	}
	trimmed := strings.ToLower(strings.TrimSpace(token))
	if trimmed == "" || trimmed == "<nil>" {
		return "", false
	}
	if _, known := recurringCadences[trimmed]; !known {
		glog.Warningf(
			"event=unknown_recurring_token token=%q treating as not recurring",
			token,
		)
		return "", false
	}
	return trimmed, true
}

// recurringNextRun returns the earliest time a task that recorded success at
// completedAt may run again, and whether the task is recurring at all.
//
// `weekdays` is the one token whose name is a schedule rather than a duration: a
// flat 24 h would release a Friday completion on Saturday, and nothing downstream
// re-imposes the weekday constraint — this is the only schedule control on the
// suppression path. So the nominal day is advanced over the weekend. The other
// tokens keep their fixed durations; `monthly` and `quarterly` drift a day or two
// from calendar months, which is the approximation the declared token already
// carries rather than the wrong day class.
func recurringNextRun(
	frontmatter lib.TaskFrontmatter,
	completedAt time.Time,
) (time.Time, bool) {
	token, ok := recurringToken(frontmatter)
	if !ok {
		return time.Time{}, false
	}
	next := completedAt.Add(recurringCadences[token])
	if token == "weekdays" {
		for next.Weekday() == time.Saturday || next.Weekday() == time.Sunday {
			next = next.AddDate(0, 0, 1)
		}
	}
	return next, true
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
