// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// maxRedrivesWithoutAdvance bounds how many spawns a task may accumulate at one
// phase:ref scope on a non-done result before the park proceeds anyway.
//
// It exists because the park is the only thing that stops a task re-spawning
// forever at an unchanged phase, and the trigger cap that would otherwise bound
// it engages only for a task with a ref or an explicit max_triggers. A
// human-authored task carrying neither is uncapped by design, so a run that ends
// in_progress without advancing — a step runner saving progress and exiting —
// would re-drive every grace period indefinitely. Three is deliberately more
// permissive than the single run the park used to allow: a transient failure now
// gets retried, and only a task that cannot get past the phase is escalated.
const maxRedrivesWithoutAdvance = 3

// redriveBudgetSpent reports whether a task has used up its re-drive budget at
// the scope it is still sitting at, so a non-done result should park instead of
// re-driving again.
//
// trigger_count is the counter rather than a new one: it is charged once per
// spawn (recordTriggerSpawn) and scoped to phase:ref, resetting to 1 whenever
// that scope changes (triggerBudgetWrite). It therefore reads as "spawns at this
// scope since the task last advanced". That scoping only holds because
// trigger_scope and trigger_count are CONTROLLER-owned — the controller's result
// writer keeps the on-disk value over an agent's spawn-time snapshot
// (bborbe/agent-task-controller, controllerOwnedFields, v0.22.3) — so an agent
// pod cannot reset the counter it is being measured by. Without that guard the
// count would be rewritten by every delivery and the cap would never fire.
//
// The scope comparison is load-bearing, not defensive: the scope is only
// rewritten on the NEXT spawn, so between a phase change and that spawn the count
// still belongs to the old scope. A count-only check would cap a task that has
// just advanced, stranding exactly the task the reset exists to release — the
// same hazard redriveStreakCapped documents for its own counter.
func redriveBudgetSpent(task lib.Task) bool {
	if task.Frontmatter.TriggerCount() < maxRedrivesWithoutAdvance {
		return false
	}
	storedScope, hasScope := task.Frontmatter.String("trigger_scope")
	if !hasScope || storedScope != triggerScope(task.Frontmatter) {
		return false
	}
	return true
}

// suppressOrPark reports whether this pass must not spawn, and is the single
// entry point the reconcile uses so both decisions share one clock: inside the
// write-back grace recentlySucceeded suppresses, and past it a task still at the
// phase its last Job succeeded for is parked.
func (h *taskEventHandler) suppressOrPark(ctx context.Context, task lib.Task) bool {
	if h.recentlySucceeded(ctx, task) {
		return true
	}
	if _, recurring := recurringPeriod(task.Frontmatter); recurring {
		// A recurring task is released at its cadence and is never parked. Its phase
		// does not advance between occurrences, so a park would send a human to the
		// same record on every run — the outcome the cadence release exists to avoid.
		return false
	}
	return h.parkSucceededWithoutPhaseMove(ctx, task)
}

// parkSucceededWithoutPhaseMove publishes phase human_review for a task whose
// last Job succeeded at the phase the task is still at, and reports whether it
// parked.
//
// It runs in the shared spawn funnel, immediately after recentlySucceeded — the
// funnel every spawn path goes through (the reconcile, the Kafka consumer and
// the deferred-respawn loop), not the reconcile alone. That placement is the
// whole point: a succeeded Job whose phase never moved will never be advanced by
// anything else, so without a park the reconcile finds the task eligible on
// every pass and re-spawns it forever.
//
// "Succeeded Job" is not the same as "the agent finished cleanly", and this is
// where that distinction is made. The park reads last_result_status, which the
// agent's delivery stamps and the spawn notification clears:
//
//   - done — the run finished and did not advance the phase. Park.
//   - failed — the run reported a transient failure. Re-drive, up to
//     maxRedrivesWithoutAdvance spawns at this scope, then park anyway.
//   - in_progress — a step runner saving progress and exiting. Re-drive, same
//     bound. Parking it would stop every multi-run task in the fleet at its
//     first non-advancing run, which is a far larger blast radius than this
//     defect and not reversible without another release.
//   - needs_input — the agent asked for a human. Re-drive is harmless because
//     the deliverer has already cleared the assignee, so the spawn funnel skips
//     the task; it surfaces in the operator inbox either way.
//   - absent or unrecognised — unknown: an older agent, or a run that died
//     before delivering. Re-drive, because parking is the irreversible direction.
//
// The cap that bounds the re-drive loop engages only for a task with a ref or an
// explicit max_triggers; an uncapped task relies on maxRedrivesWithoutAdvance
// alone.
//
// Why here and not on the succeeded-Job path in the job watcher: the decision has
// to wait out the write-back, because a succeeded Job means the agent published
// its result but the vault learns about it on the controller's own cycle. For up
// to defaultRespawnGracePeriod the task file still reads the phase the Job ran
// for, so parking at completion cannot tell "the agent parked" from "the agent
// advanced and the write has not landed". The grace is exactly the window
// recentlySucceeded suppresses re-drive in — so a park on the job watcher would
// become possible at the same instant the re-drive resumes, on a slower clock (a
// 5-minute informer resync against a 60-second reconcile), and would lose the
// race almost every time. Evaluating both decisions in one place makes them
// mutually exclusive by construction: this pass either suppresses (inside the
// grace), parks, or spawns.
//
// The phase comparison is what keeps a multi-phase agent alive. A Job is spawned
// for one phase and the agent may advance the task inside it and still exit 0 —
// an agent that finishes planning writes phase: execution and expects the next
// phase to be driven. So the test is "the task is still at the phase the Job was
// spawned for", not "the task is still in a drive phase"; the latter would park
// every task that legitimately advanced. recentlySucceeded compares the same two
// values for the same reason.
//
// task is the reconcile's own view of the task — the fresh one. Reading the
// TaskStore entry here instead would compare the spawn-time snapshot, whose phase
// always equals the recorded phase, and park everything.
func (h *taskEventHandler) parkSucceededWithoutPhaseMove(
	ctx context.Context,
	task lib.Task,
) bool {
	// A handler built without a view cannot park: there is no fresh phase to
	// compare against, and parking on the caller's snapshot would rewrite
	// human_review over a task that has since advanced. Every deployed handler has
	// one; this guard keeps the funnel safe for the ones a test builds bare.
	if h.view == nil {
		return false
	}
	// Re-read from the view rather than trusting the caller's task. Two of the
	// three callers of this funnel pass a snapshot taken earlier: the Kafka
	// consumer hands over the decoded event, and the deferred-respawn loop hands
	// over the entry stored when the task was deferred. Either can be minutes
	// stale on a replayed or backlogged event, and parking on a stale phase would
	// rewrite human_review over a task that has since advanced.
	current, ok := h.view.Get(task.TaskIdentifier)
	if !ok {
		return false
	}
	// A park answers "a Job ran here and did not move the phase". The success
	// record that drives it is shared, so it can exist for a task this cluster
	// never ran — another cluster's success, or one whose Job this cluster never
	// recorded. Parking on the record alone writes human_review onto a task with
	// no run behind it, and the assignee clear the human_review chokepoint
	// applies then makes it unspawnable. Judged on the fresh view, not the
	// caller's snapshot. Measured on bborbe/github-releaser-agent#44 @ 28a4415b:
	// no current_job, no job_started_at, `## Parked / Reason: parked_succeeded`.
	if !taskShowsPriorRun(current) {
		return false
	}
	// The success comes from the shared record, not the pod's own memory: the pod
	// that ran the Job may be gone, and a restart must not turn a record that was
	// due to be parked back into an eligible one. lastSuccess is phase-scoped, so
	// it answers exactly the question this park asks — did this task succeed at the
	// phase it is still at — and writes a shared hit back into memory, so the next
	// pass on this pod is free.
	succeeded, ok := h.lastSuccess(ctx, current, taskPhase(current))
	if !ok {
		return false
	}
	// A succeeded Job is not by itself evidence of a deliberate park. An agent
	// that reports failure still exits 0 — publishing a failed result returns nil —
	// so a transient failure leaves a succeeded Job at an unchanged phase, and
	// nothing in the Job distinguishes it from an agent that parked on purpose.
	// The park therefore reads the result status the agent's delivery stamps
	// (last_result_status, written by bborbe/agent delivery.content-generator) and
	// parks only a done result.
	//
	// failed, needs_input, in_progress, an unrecognised value and an ABSENT field
	// all re-drive instead. Absent is the one most likely to be got wrong: an older
	// agent, a record written before the field existed, or any path that omits it
	// must not be read as "done". Parking is the irreversible direction — it writes
	// human_review, which is outside the trigger phases, so nothing re-drives the
	// task and only a human can undo it. Observed 2026-11-09 on bborbe/trading#480:
	// the reviewer agent hit `GH rate limit nearly exhausted (0/12500 remaining)`,
	// reported failed, exited 0, and was parked here — where it stayed for good.
	//
	// Re-driving is bounded by maxRedrivesWithoutAdvance below, and that check runs
	// after the success record and the grace gates so it fires only for a pass that
	// would otherwise have parked.
	if resultStatus, _ := current.Frontmatter.String("last_result_status"); resultStatus != string(lib.AgentStatusDone) {
		if !redriveBudgetSpent(current) {
			return false
		}
		glog.V(2).Infof(
			"event=park_redrive_cap task=%s phase=%s last_result_status=%q trigger_count=%d",
			task.TaskIdentifier,
			taskPhase(current),
			resultStatus,
			current.Frontmatter.TriggerCount(),
		)
		metrics.TaskEventsTotal.WithLabelValues("park_redrive_cap").Inc()
	}
	now := h.currentDateTime.Now().Time()
	elapsed := now.Sub(succeeded.CompletedAt)
	if elapsed < 0 {
		// CompletedAt is stamped by the API server, "now" by this pod; a small
		// skew must not read as a Job that finished in the future.
		elapsed = 0
	}
	if elapsed < defaultRespawnGracePeriod {
		// Inside the write-back window: recentlySucceeded already suppressed
		// this pass, and the phase may still be about to move.
		return false
	}
	// A park already published for this success is never published again. The
	// window decides only what this pass does, because the task reading this
	// phase has exactly two meanings and the window is what separates them:
	// inside it the controller has not applied the park yet, so suppress; past it
	// the park landed and an operator re-armed the task — the documented way out
	// of human_review — so let it spawn.
	if publishedAt, ok := h.taskStore.ParkPublishedAt(
		task.TaskIdentifier,
		succeeded.CompletedAt,
	); ok {
		if now.Sub(publishedAt) < defaultRespawnGracePeriod {
			glog.V(2).Infof(
				"event=park_awaiting_write_back task=%s elapsed=%.0fs",
				task.TaskIdentifier,
				now.Sub(publishedAt).Seconds(),
			)
			return true
		}
		return false
	}
	if err := h.resultPublisher.PublishParkSucceeded(ctx, current); err != nil {
		// Suppress this pass rather than fall through to the spawn: spawning is
		// the loop the park exists to end, so a park that could not be written
		// must not re-run the work. The next pass re-evaluates and retries.
		glog.Errorf(
			"publish park for task %s (phase %s): %v",
			task.TaskIdentifier,
			succeeded.Phase,
			err,
		)
		return true
	}
	glog.Infof(
		"event=parked_succeeded task=%s phase=%s elapsed=%.0fs",
		task.TaskIdentifier,
		succeeded.Phase,
		elapsed.Seconds(),
	)
	metrics.TaskEventsTotal.WithLabelValues("parked_succeeded").Inc()
	// Record the publish, keyed by the success it acted on and carrying when it
	// went out. The time is load-bearing: until the controller applies the write
	// the task still reads this phase, and only the window distinguishes that lag
	// from an operator re-arming the task to the same phase.
	h.taskStore.MarkParkPublished(task.TaskIdentifier, succeeded.CompletedAt, now)
	return true
}
