// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"testing"
	"time"

	lib "github.com/bborbe/agent"
)

// parkFixture builds a handler plus a recording publisher, and returns the
// handler's own idea of "now" so every spec sets CompletedAt relative to it
// rather than to a fixed wall-clock instant.
func parkFixture(
	t *testing.T,
	phase string,
) (*taskEventHandler, *parkRecordingPublisher, lib.Task, time.Time) {
	t.Helper()
	publisher := &parkRecordingPublisher{}
	h, _ := newDecisionHandler(t, 0, nil, publisher)
	now := h.currentDateTime.Now().Time()
	task := lib.Task{
		TaskIdentifier: "task-park",
		Frontmatter: lib.TaskFrontmatter{
			"phase":    phase,
			"status":   "in_progress",
			"assignee": "claude",
		},
	}
	return h, publisher, task, now
}

// longAgo is safely past the write-back grace.
var longAgo = 2 * defaultRespawnGracePeriod

func TestParkSucceededWithoutPhaseMoveParksWhenThePhaseDidNotMove(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo))

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("parkSucceededWithoutPhaseMove = false; want true")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1", got)
	}
	if published := publisher.parkedSucceeded[0]; published.TaskIdentifier != task.TaskIdentifier {
		t.Fatalf("published task = %q; want %q", published.TaskIdentifier, task.TaskIdentifier)
	}
}

// The regression this guard exists to avoid: a Job is spawned for one phase and
// the agent may advance the task inside it and still exit 0. Parking there would
// stall every multi-phase agent at its first phase.
func TestParkSucceededWithoutPhaseMoveDoesNotParkATaskTheAgentAdvanced(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "planning", now.Add(-longAgo))

	if h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("parkSucceededWithoutPhaseMove = true; want false for an advanced phase")
	}
	if got := len(publisher.parkedSucceeded); got != 0 {
		t.Fatalf("park count = %d; want 0", got)
	}
}

func TestParkSucceededWithoutPhaseMoveDoesNotParkInsideTheGrace(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	// Half the grace ago: the vault write-back may still be in flight.
	h.taskStore.MarkSucceeded(
		task.TaskIdentifier,
		"execution",
		now.Add(-defaultRespawnGracePeriod/2),
	)

	if h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("parkSucceededWithoutPhaseMove = true; want false inside the grace")
	}
	if got := len(publisher.parkedSucceeded); got != 0 {
		t.Fatalf("park count = %d; want 0", got)
	}
}

// The exact `>=` edge is not pinnable here: the fixture's clock is the real one,
// so the second read of Now() inside the guard is strictly later than the first
// and "exactly at the grace" cannot be constructed. The inside-grace spec above
// and this one bracket the transition, which is what the behaviour depends on.
func TestParkSucceededWithoutPhaseMoveParksOncePastTheGrace(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-defaultRespawnGracePeriod))

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("parkSucceededWithoutPhaseMove = false; want true past the grace")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1", got)
	}
}

func TestParkSucceededWithoutPhaseMoveDoesNotParkWithoutASuccessRecord(t *testing.T) {
	h, publisher, task, _ := parkFixture(t, "execution")

	if h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("parkSucceededWithoutPhaseMove = true; want false with no success record")
	}
	if got := len(publisher.parkedSucceeded); got != 0 {
		t.Fatalf("park count = %d; want 0", got)
	}
}

func TestParkSucceededWithoutPhaseMoveDoesNotParkATaskWithoutAPhase(t *testing.T) {
	h, publisher, _, now := parkFixture(t, "execution")
	task := lib.Task{
		TaskIdentifier: "task-park",
		Frontmatter:    lib.TaskFrontmatter{"status": "in_progress"},
	}
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo))

	if h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("parkSucceededWithoutPhaseMove = true; want false for a task with no phase")
	}
	if got := len(publisher.parkedSucceeded); got != 0 {
		t.Fatalf("park count = %d; want 0", got)
	}
}

func TestParkSucceededWithoutPhaseMoveSuppressesThePassWhenThePublishFails(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo))
	publisher.failParkSucceeded = true

	// True = this pass must not spawn. Spawning is the loop the park exists to
	// end, so a park that could not be written must not re-run the work.
	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal(
			"parkSucceededWithoutPhaseMove = false; want true (suppress) when the publish fails",
		)
	}
	// Nothing is consumed, so the next pass re-evaluates and retries.
	if h.taskStore.ParkConsumed(task.TaskIdentifier, now.Add(-longAgo)) {
		t.Fatal("a failed publish must not consume the success")
	}
}

// The recovery path for a park is an operator moving the phase back to the drive
// phase. That task still matches the same success record, so the park must
// consume the success by identity — and stay consumed even when the Job informer
// re-stamps the identical record on its next resync, which it does every few
// minutes for the Job's whole TTL.
func TestParkSucceededWithoutPhaseMoveConsumesTheSuccessSoAReArmIsNotReParked(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	completedAt := now.Add(-longAgo)
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", completedAt)

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("first pass did not park")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1", got)
	}

	// The informer re-delivers the terminal Job and MarkSucceeded re-stamps the
	// same record. A deletion-based fix would be undone here.
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", completedAt)

	// The operator re-arms by putting the task back at the drive phase. The
	// next pass must let it spawn, not park it again.
	if h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("re-armed task was parked again; the consumed success did not survive the re-mark")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1 after a re-arm", got)
	}
}

// A genuinely later success is a different unit of work and may park again.
func TestParkSucceededWithoutPhaseMoveParksAgainForALaterSuccess(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo))

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("first pass did not park")
	}
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo/2))

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("a later success was not parked")
	}
	if got := len(publisher.parkedSucceeded); got != 2 {
		t.Fatalf("park count = %d; want 2", got)
	}
}
