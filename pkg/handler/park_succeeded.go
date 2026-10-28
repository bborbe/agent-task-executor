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

// suppressOrPark reports whether this pass must not spawn, and is the single
// entry point the reconcile uses so both decisions share one clock: inside the
// write-back grace recentlySucceeded suppresses, and past it a task still at the
// phase its last Job succeeded for is parked.
func (h *taskEventHandler) suppressOrPark(ctx context.Context, task lib.Task) bool {
	if h.recentlySucceeded(task) {
		return true
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
// whole point: a succeeded Job whose phase never moved is a deliberate human
// park — the agent exited 0 without advancing the task, so nothing will ever
// move it, and without this the reconcile finds it eligible on every pass and
// re-spawns it forever. v0.46.3 removed the cap charge that used to bound that
// loop, so nothing else stops it.
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
	succeeded, ok := h.taskStore.LastSucceeded(task.TaskIdentifier)
	if !ok {
		return false
	}
	if succeeded.Phase != taskPhase(task) {
		return false
	}
	elapsed := h.currentDateTime.Now().Time().Sub(succeeded.CompletedAt)
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
	if h.taskStore.ParkConsumed(task.TaskIdentifier, succeeded.CompletedAt) {
		// Already parked for this success. The task matching it now means an
		// operator moved its phase back to the drive phase — the documented way
		// out of human_review — so let it spawn.
		return false
	}
	if err := h.resultPublisher.PublishParkSucceeded(ctx, task); err != nil {
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
	// Consume the success, keyed by its completion time. Deleting the record
	// would not work: the Job informer re-delivers the terminal Job every
	// resync for its whole TTL and re-stamps the identical record, so the
	// deletion is undone within minutes and the re-armed task is parked again.
	h.taskStore.MarkParkConsumed(task.TaskIdentifier, succeeded.CompletedAt)
	return true
}
