// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	stderrors "errors"
	"strings"
	"sync"
	"testing"
	"time"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// fakeParkStore is a hand-written claim.ParkStore with independent read and
// write failure knobs. It is hand-written rather than taken from mocks because
// that package imports pkg/handler, which an internal test cannot import.
type fakeParkStore struct {
	mu       sync.Mutex
	records  map[string]map[int64]time.Time
	readErr  error
	writeErr error
}

func newFakeParkStore() *fakeParkStore {
	return &fakeParkStore{records: map[string]map[int64]time.Time{}}
}

// seed records a published park without going through the failure knobs, so a
// spec can model a marker written by an earlier pod.
func (s *fakeParkStore) seed(taskID string, completedAt, publishedAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byCompletion := s.records[taskID]
	if byCompletion == nil {
		byCompletion = map[int64]time.Time{}
		s.records[taskID] = byCompletion
	}
	byCompletion[completedAt.Unix()] = publishedAt
}

// Record mirrors the real store's one-ref-per-task bound: a later completion
// supersedes and deletes a strictly older one.
func (s *fakeParkStore) Record(
	_ context.Context,
	taskID string,
	_ string,
	completedAt libtime.DateTime,
	publishedAt libtime.DateTime,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeErr != nil {
		return s.writeErr
	}
	byCompletion := s.records[taskID]
	if byCompletion == nil {
		byCompletion = map[int64]time.Time{}
		s.records[taskID] = byCompletion
	}
	for completion := range byCompletion {
		if completion < completedAt.Time().Unix() {
			delete(byCompletion, completion)
		}
	}
	byCompletion[completedAt.Time().Unix()] = publishedAt.Time()
	return nil
}

func (s *fakeParkStore) PublishedAt(
	_ context.Context,
	taskID string,
	completedAt libtime.DateTime,
) (libtime.DateTime, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readErr != nil {
		return libtime.DateTime{}, false, s.readErr
	}
	byCompletion := s.records[taskID]
	if byCompletion == nil {
		return libtime.DateTime{}, false, nil
	}
	at, ok := byCompletion[completedAt.Time().Unix()]
	if !ok {
		return libtime.DateTime{}, false, nil
	}
	return libtime.DateTime(at), true, nil
}

// LatestPublishedAt returns the newest publish time recorded for the task,
// regardless of completion, mirroring the real store's task-keyed read.
func (s *fakeParkStore) LatestPublishedAt(
	_ context.Context,
	taskID string,
) (libtime.DateTime, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readErr != nil {
		return libtime.DateTime{}, false, s.readErr
	}
	var latest time.Time
	found := false
	for _, publishedAt := range s.records[taskID] {
		if !found || publishedAt.After(latest) {
			latest = publishedAt
			found = true
		}
	}
	if !found {
		return libtime.DateTime{}, false, nil
	}
	return libtime.DateTime(latest), true, nil
}

// newParkMarkerHandler builds a real handler in claim mode off — so the shared
// success store is absent and lastSuccess reads memory — with an injected park
// store. The clock is fixed, so every completed/published time is relative to a
// known instant.
func newParkMarkerHandler(
	t *testing.T,
	store claim.ParkStore,
) (*taskEventHandler, *parkRecordingPublisher, lib.Task, time.Time) {
	t.Helper()
	base := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := libtime.NewCurrentDateTime()
	clock.SetNow(libtime.DateTime(base))
	publisher := &parkRecordingPublisher{}
	task := lib.Task{
		TaskIdentifier: "task-park-marker",
		Frontmatter: lib.TaskFrontmatter{
			"phase":    "execution",
			"status":   "in_progress",
			"assignee": "claude",
			// A park answers "a Job ran here and did not move the phase".
			"current_job": "agent-task-park",
			// Only a done result is finished work and may park.
			"last_result_status": string(lib.AgentStatusDone),
		},
	}
	handler := NewTaskEventHandler(
		&stubJobSpawner{},
		"dev",
		"prod",
		"self",
		true,
		true,
		nil,
		publisher,
		pkg.NewTaskStore(),
		clock,
		&fakeTaskView{tasks: map[lib.TaskIdentifier]lib.Task{task.TaskIdentifier: task}},
		"v",
		DefaultReconcilePassTimeout,
		DefaultReconcileInterval,
		pkg.ClaimModeOff,
		nil,
		WithParkStore(store),
	)
	h, ok := handler.(*taskEventHandler)
	if !ok {
		t.Fatalf("expected *taskEventHandler, got %T", handler)
	}
	return h, publisher, task, base
}

// TestParkMarkerRecordedParkSuppressesRepublish is AC 1 and AC 8: a marker for
// the same success suppresses the second publish, so the task file is not
// rewritten and the pass does not spawn.
func TestParkMarkerRecordedParkSuppressesRepublish(t *testing.T) {
	store := newFakeParkStore()
	h, publisher, task, now := newParkMarkerHandler(t, store)
	completedAt := now.Add(-longAgo)
	// A park published past the write-back grace, with the handler's memory empty.
	store.seed(string(task.TaskIdentifier), completedAt, now.Add(-longAgo))

	var parked bool
	out := captureGlogStderr(t, func() {
		parked = h.parkSucceededWithoutPhaseMove(context.Background(), task)
	})

	if parked {
		t.Fatal("parkSucceededWithoutPhaseMove = true; want false for an already-parked success")
	}
	if got := len(publisher.parkedSucceeded); got != 0 {
		t.Fatalf("park count = %d; want 0 for an already-parked success", got)
	}
	if got := strings.Count(out, "event=parked_succeeded"); got != 0 {
		t.Fatalf("parked_succeeded lines = %d; want 0 (no frontmatter write)", got)
	}
}

// TestParkMarkerNoRecordParksOnce is AC 3, the negative control: with no marker,
// a done success past the grace parks exactly once.
func TestParkMarkerNoRecordParksOnce(t *testing.T) {
	store := newFakeParkStore()
	h, publisher, task, now := newParkMarkerHandler(t, store)
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo))

	var parked bool
	out := captureGlogStderr(t, func() {
		parked = h.parkSucceededWithoutPhaseMove(context.Background(), task)
	})

	if !parked {
		t.Fatal("parkSucceededWithoutPhaseMove = false; want true for an unrecorded success")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1", got)
	}
	if got := strings.Count(out, "event=parked_succeeded"); got != 1 {
		t.Fatalf("parked_succeeded lines = %d; want 1", got)
	}
}

// TestParkMarkerLaterCompletionParksAgain is AC 2 at the handler level: a later
// completion advances the marker and gets its own park.
func TestParkMarkerLaterCompletionParksAgain(t *testing.T) {
	store := newFakeParkStore()
	h, publisher, task, now := newParkMarkerHandler(t, store)
	first := now.Add(-longAgo)
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", first)

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("first pass did not park")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1", got)
	}
	if _, ok, err := store.PublishedAt(
		context.Background(), string(task.TaskIdentifier), libtime.DateTime(first),
	); err != nil || !ok {
		t.Fatalf("marker for the first completion = (ok=%t, err=%v); want recorded", ok, err)
	}

	second := now.Add(-longAgo / 2)
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", second)

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("a later completion was not parked")
	}
	if got := len(publisher.parkedSucceeded); got != 2 {
		t.Fatalf("park count = %d; want 2", got)
	}
	if _, ok, err := store.PublishedAt(
		context.Background(), string(task.TaskIdentifier), libtime.DateTime(second),
	); err != nil || !ok {
		t.Fatalf("marker for the second completion = (ok=%t, err=%v); want recorded", ok, err)
	}
	if _, ok, err := store.PublishedAt(
		context.Background(), string(task.TaskIdentifier), libtime.DateTime(first),
	); err != nil || ok {
		t.Fatalf("marker for the first completion = (ok=%t, err=%v); want superseded", ok, err)
	}
}

// TestParkMarkerReadFailureFallsBackToMemory is AC 5: an unreadable store does
// not suppress the first park, and the pod's own memory still suppresses the
// second.
func TestParkMarkerReadFailureFallsBackToMemory(t *testing.T) {
	store := newFakeParkStore()
	store.readErr = stderrors.New("park store down")
	h, publisher, task, now := newParkMarkerHandler(t, store)
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo))

	var first bool
	out := captureGlogStderr(t, func() {
		first = h.parkSucceededWithoutPhaseMove(context.Background(), task)
	})
	if !first {
		t.Fatal("first pass did not park despite an unreadable store")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1", got)
	}
	if !strings.Contains(out, "event=park_marker_read_failed") {
		t.Fatalf("stderr = %q; want a park_marker_read_failed line", out)
	}

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("second pass did not suppress from memory")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1 (no second publish)", got)
	}
}

// TestParkMarkerWriteFailureKeepsThePark is the failure mode: a failed marker
// write never fails the park and never re-enables the write-back path.
func TestParkMarkerWriteFailureKeepsThePark(t *testing.T) {
	store := newFakeParkStore()
	store.writeErr = stderrors.New("park store down")
	h, publisher, task, now := newParkMarkerHandler(t, store)
	h.taskStore.MarkSucceeded(task.TaskIdentifier, "execution", now.Add(-longAgo))

	var first bool
	out := captureGlogStderr(t, func() {
		first = h.parkSucceededWithoutPhaseMove(context.Background(), task)
	})
	if !first {
		t.Fatal("parkSucceededWithoutPhaseMove = false; want true even when the marker write fails")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1", got)
	}
	if !strings.Contains(out, "event=park_marker_write_failed") {
		t.Fatalf("stderr = %q; want a park_marker_write_failed line", out)
	}

	if !h.parkSucceededWithoutPhaseMove(context.Background(), task) {
		t.Fatal("second pass did not suppress from memory after a failed marker write")
	}
	if got := len(publisher.parkedSucceeded); got != 1 {
		t.Fatalf("park count = %d; want 1 (no second publish)", got)
	}
}
