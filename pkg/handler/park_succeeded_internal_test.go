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
			// A park answers "a Job ran here and did not move the phase", so the
			// fixture carries the Job evidence every park spec is about.
			"current_job": "agent-task-park",
			// The park also requires a done result: a Job that exits 0 while
			// reporting failure is not a deliberate park. The default fixture is a
			// successful run; the status matrix is exercised separately.
			"last_result_status": string(lib.AgentStatusDone),
		},
	}
	// The park reads the phase from the view, never from the caller's snapshot.
	h.view = &fakeTaskView{tasks: map[lib.TaskIdentifier]lib.Task{task.TaskIdentifier: task}}
	return h, publisher, task, now
}

// longAgo is safely past the write-back grace.
var longAgo = 2 * defaultRespawnGracePeriod

// TestParkSucceededWithoutPhaseMoveDoesNotParkATaskThatNeverRan pins the rule
// that a park answers "a Job ran here and did not move the phase".
//
// A success record alone is not that answer: the record is shared, so a success
// written for this task by a cluster that could not spawn it — or one left by a
// run whose Job this cluster never recorded — leaves a task with no
// current_job and no job_started_at. Parking it writes human_review onto a task
// that never ran, and the assignee clear that follows makes it unspawnable.
// Measured on bborbe/github-releaser-agent#44 @ 28a4415b: created 23:46:28Z,
// parked 00:07:58Z with `## Parked / Reason: parked_succeeded`, no current_job
// and no job_started_at.
func TestParkSucceededWithoutPhaseMoveDoesNotParkATaskThatNeverRan(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	delete(task.Frontmatter, "current_job")
	h.view = &fakeTaskView{tasks: map[lib.TaskIdentifier]lib.Task{task.TaskIdentifier: task}}
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo))

	if h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("parkSucceededWithoutPhaseMove = true; want false for a task that never ran here")
	}
	if got := len(publisher.parkedSucceeded); got != 0 {
		t.Fatalf("park count = %d; want 0 for a task that never ran", got)
	}
}

// parkCase is one row of the park's result-status matrix.
type parkCase struct {
	name         string
	status       string
	present      bool
	triggerCount int
	staleScope   bool
	want         bool
}

// seeded builds a handler whose view holds the task this row describes, with the
// run evidence the park requires already recorded.
func (tc parkCase) seeded(t *testing.T) (*taskEventHandler, *parkRecordingPublisher, lib.Task) {
	t.Helper()
	h, publisher, task, now := parkFixture(t, "execution")
	seedLastResultStatus(task, tc.status, tc.present)
	seedTriggerBudget(task, tc.triggerCount, tc.staleScope)
	h.view = &fakeTaskView{tasks: map[lib.TaskIdentifier]lib.Task{task.TaskIdentifier: task}}
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo))
	return h, publisher, task
}

func seedLastResultStatus(task lib.Task, status string, present bool) {
	if present {
		task.Frontmatter["last_result_status"] = status
		return
	}
	delete(task.Frontmatter, "last_result_status")
}

// seedTriggerBudget gives the task a spent or unspent spawn budget at the scope
// it is sitting at. The fixture's phase is "execution" and it carries no ref, so
// the computed scope is "execution:"; a stale row carries the scope the task had
// before it advanced, which the cap must not honour.
func seedTriggerBudget(task lib.Task, count int, stale bool) {
	task.Frontmatter["trigger_count"] = count
	if count == 0 {
		return
	}
	task.Frontmatter["trigger_scope"] = "execution:"
	if stale {
		task.Frontmatter["trigger_scope"] = "planning:"
	}
}

// TestParkSucceededWithoutPhaseMoveParksOnlyOnADoneResult pins the rule that a
// succeeded Job is not by itself a deliberate park.
//
// An agent that reports failure still exits 0 — publishing a failed result
// returns nil — so a transient failure leaves a succeeded Job at an unchanged
// phase and nothing in the Job separates it from an agent that parked on
// purpose. The park therefore reads last_result_status, which the agent's
// delivery stamps, and only a done result may park.
//
// The absent row is the one that matters most. Parking is the irreversible
// direction: human_review is outside the trigger phases, so nothing re-drives
// the task and only a human can undo it. An older agent, a record written before
// the field existed, or any write path that omits it must therefore fail toward
// RE-DRIVING, never toward parking — reading an absent field as "done" would
// rebuild the original false premise in a new place.
//
// Observed 2026-11-09 on bborbe/trading#480: the reviewer agent hit
// `GH rate limit nearly exhausted (0/12500 remaining)`, reported failed, exited
// 0, and was parked here — where nothing dispatched it again.
func TestParkSucceededWithoutPhaseMoveParksOnlyOnADoneResult(t *testing.T) {
	cases := []parkCase{
		{name: "done result parks", status: string(lib.AgentStatusDone), present: true, want: true},
		{
			name:    "failed result does not park",
			status:  string(lib.AgentStatusFailed),
			present: true,
			want:    false,
		},
		{
			name:    "needs_input result does not park",
			status:  string(lib.AgentStatusNeedsInput),
			present: true,
			want:    false,
		},
		{
			name:    "in_progress result does not park",
			status:  string(lib.AgentStatusInProgress),
			present: true,
			want:    false,
		},
		{name: "absent status does not park", present: false, want: false},
		{
			name:    "unrecognised status does not park",
			status:  "something-else",
			present: true,
			want:    false,
		},
		// The re-drive cap: a task that cannot get past the phase is escalated
		// once its spawn budget at this scope is spent, so the non-done statuses
		// above cannot loop forever on an uncapped task.
		{
			name:         "failed result parks once the re-drive budget is spent",
			status:       string(lib.AgentStatusFailed),
			present:      true,
			triggerCount: maxRedrivesWithoutAdvance,
			want:         true,
		},
		{
			name:         "absent status parks once the re-drive budget is spent",
			present:      false,
			triggerCount: maxRedrivesWithoutAdvance,
			want:         true,
		},
		{
			name:         "failed result still re-drives one spawn below the cap",
			status:       string(lib.AgentStatusFailed),
			present:      true,
			triggerCount: maxRedrivesWithoutAdvance - 1,
			want:         false,
		},
		// The count only applies to the scope the task is still at: a task that
		// has just advanced carries the previous scope's count until its next
		// spawn rewrites it, and capping there would strand it.
		{
			name:         "failed result re-drives when the count belongs to a stale scope",
			status:       string(lib.AgentStatusFailed),
			present:      true,
			triggerCount: maxRedrivesWithoutAdvance,
			staleScope:   true,
			want:         false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, publisher, task := tc.seeded(t)
			got := h.parkSucceededWithoutPhaseMove(context.Background(), task)
			if got != tc.want {
				t.Fatalf("parkSucceededWithoutPhaseMove = %t; want %t", got, tc.want)
			}
			wantPublish := 0
			if tc.want {
				wantPublish = 1
			}
			if published := len(publisher.parkedSucceeded); published != wantPublish {
				t.Fatalf("park count = %d; want %d", published, wantPublish)
			}
		})
	}
}

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
func TestParkSucceededWithoutPhaseMoveLetsAReArmSpawnOnceTheWindowHasPassed(t *testing.T) {
	h, publisher, task, now := parkFixture(t, "execution")
	completedAt := now.Add(-longAgo)
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", completedAt)
	// A park published long ago, whose write-back window is over.
	h.taskStore.MarkParkPublished(task.TaskIdentifier, completedAt, now.Add(-longAgo))

	if h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("re-armed task was suppressed; the park's window had passed")
	}
	if got := len(publisher.parkedSucceeded); got != 0 {
		t.Fatalf("park count = %d; want 0", got)
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
