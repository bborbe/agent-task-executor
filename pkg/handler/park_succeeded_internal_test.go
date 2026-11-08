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

// parkFixture builds a handler whose view holds the task, a recording publisher,
// and the handler's own idea of "now" so every spec sets CompletedAt relative to
// it rather than to a fixed wall-clock instant.
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
	// The park reads the phase from the view, never from the caller's snapshot.
	h.view = &fakeTaskView{tasks: map[lib.TaskIdentifier]lib.Task{task.TaskIdentifier: task}}
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

// The caller's task is not trustworthy: the Kafka consumer passes the decoded
// event and the deferred loop passes a snapshot taken when the task was
// deferred, so a replayed or backlogged pass can carry a phase the task has long
// left. The park must read the view instead.
func TestParkSucceededWithoutPhaseMoveIgnoresAStaleCallerSnapshot(t *testing.T) {
	h, publisher, _, now := parkFixture(t, "execution")
	stale := lib.Task{
		TaskIdentifier: "task-park",
		Frontmatter:    lib.TaskFrontmatter{"phase": "planning"},
	}
	h.taskStore.MarkSucceeded(stale.TaskIdentifier, "planning", now.Add(-longAgo))

	if h.parkSucceededWithoutPhaseMove(context.Background(), stale) {
		t.Fatal("parked on a stale caller snapshot; the view says the task advanced")
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
// so the second read of Now() inside the guard is strictly later than the first.
// The inside-grace spec above and this one bracket the transition.
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

func TestParkSucceededWithoutPhaseMoveDoesNotParkWithoutAViewEntry(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	h.view = &fakeTaskView{}
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo))

	if h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("parked with no view entry; an unreadable phase proves nothing")
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
	if _, ok := h.taskStore.ParkPublishedAt(task.TaskIdentifier, now.Add(-longAgo)); ok {
		t.Fatal("a failed publish must not be recorded as a park")
	}
}

// The park is published, not applied: until the controller writes it the view
// still reads the succeeded phase. Falling through to a spawn in that window
// would re-run the very work the park exists to stop.
func TestParkSucceededWithoutPhaseMoveSuppressesUntilTheParkIsObservable(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	completedAt := now.Add(-longAgo)
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", completedAt)

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("first pass did not park")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1", got)
	}

	// The view has not caught up yet; a second pass must suppress, not spawn.
	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("a pass inside the park's write-back window did not suppress")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1 (no second publish)", got)
	}
}

// Past the write-back window the task still reading this phase means an operator
// re-armed it — the documented way out of human_review — so it must spawn.
// A park overdue past the write-back window is re-published, never released.
// The old contract released it here on the theory that a re-arm must be the only
// way a task still reads the succeeded phase this late — but a landed park moves
// the phase and exits the phase-scoped lookup above, so this branch is
// unreachable for a re-arm and reachable only for a write-back that never landed.
func TestParkSucceededWithoutPhaseMoveRepublishesAnOverduePark(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	completedAt := now.Add(-longAgo)
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", completedAt)
	// A park published long ago, whose write-back window is over.
	h.taskStore.MarkParkPublished(task.TaskIdentifier, completedAt, now.Add(-longAgo))

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("overdue park was released; the task would be spawned a second time")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("re-publish count = %d; want 1", got)
	}
}

// A park whose write-back has not landed must not release the task.
//
// Reaching the park branch at all proves the write has not landed: the success
// lookup above is phase-scoped, so a park the controller had applied would have
// moved the task to human_review and returned before this point. Releasing on a
// timer therefore spawns the work a second time — measured on nukedev 2026-11-08,
// one task re-spawned every ~12 minutes for as long as it stayed at one phase.
func TestParkSucceededWithoutPhaseMoveDoesNotReleaseAParkThatNeverLanded(t *testing.T) {
	h, _, task, now := parkFixture(t, "execution")
	completedAt := now.Add(-longAgo)
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", completedAt)
	// The park went out long ago, but the task still reads the phase the Job ran
	// for — the controller never applied it.
	h.taskStore.MarkParkPublished(task.TaskIdentifier, completedAt, now.Add(-longAgo))

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("an unlanded park was released; the task would be spawned a second time")
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
