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

func TestParkSucceededWithoutPhaseMoveParksExactlyAtTheGraceBoundary(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-defaultRespawnGracePeriod))

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("parkSucceededWithoutPhaseMove = false; want true at the grace boundary")
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

func TestParkSucceededWithoutPhaseMoveReportsFalseWhenThePublishFails(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo))
	publisher.failParkSucceeded = true

	if h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("parkSucceededWithoutPhaseMove = true; want false when the publish fails")
	}
}

// The recovery path for a park is an operator moving the phase back to the drive
// phase. That task matches the same success record the park consumed, so the
// record must not outlive the park — otherwise the re-arm is silently undone on
// the next pass and the documented way out of the terminal state does nothing
// until the record is pruned an hour later.
func TestParkSucceededWithoutPhaseMoveConsumesTheRecordSoAReArmIsNotReParked(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo))

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("first pass did not park")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1", got)
	}

	// The operator re-arms by putting the task back at the drive phase. The
	// next pass must let it spawn, not park it again.
	if h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("re-armed task was parked again; the success record outlived its park")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1 after a re-arm", got)
	}
}
