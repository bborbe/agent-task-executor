// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"testing"
	"time"

	lib "github.com/bborbe/agent"
)

// The admission rule is exercised end to end by the reconcile and deferred
// integration specs; these pin the comparator itself so the ordering key cannot
// drift without a failing unit test.

func TestAdmissionPriorityNeverAttemptedSortsFirst(t *testing.T) {
	attempted := lib.Task{Frontmatter: lib.TaskFrontmatter{
		"job_started_at": "2026-10-21T08:00:00Z",
	}}
	fresh := lib.Task{Frontmatter: lib.TaskFrontmatter{}}
	if !admissionPriority(fresh).Before(admissionPriority(attempted)) {
		t.Fatalf(
			"a never-attempted task must sort before an attempted one: fresh=%v attempted=%v",
			admissionPriority(fresh),
			admissionPriority(attempted),
		)
	}
}

func TestAdmissionPriorityPrefersEarlierQueueEntry(t *testing.T) {
	// The two tasks are deliberately inverted against job_started_at: queued_at
	// (the moment the FIRST wait began) is what orders attempted tasks, so the
	// one that has been waiting longer wins even though it ran more recently.
	earlier := lib.Task{Frontmatter: lib.TaskFrontmatter{
		"job_started_at": "2026-10-21T08:00:00Z",
		"queued_at":      "2026-10-21T07:00:00Z",
	}}
	later := lib.Task{Frontmatter: lib.TaskFrontmatter{
		"job_started_at": "2026-10-21T07:30:00Z",
		"queued_at":      "2026-10-21T07:45:00Z",
	}}
	if !admissionPriority(earlier).Before(admissionPriority(later)) {
		t.Fatalf(
			"queued_at must order attempted tasks: earlier=%v later=%v",
			admissionPriority(earlier),
			admissionPriority(later),
		)
	}
}

func TestAdmissionPriorityFallsBackToJobStartedAt(t *testing.T) {
	task := lib.Task{Frontmatter: lib.TaskFrontmatter{
		"job_started_at": "2026-10-21T08:00:00Z",
	}}
	want, err := time.Parse(time.RFC3339, "2026-10-21T08:00:00Z")
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if got := admissionPriority(task); !got.Equal(want) {
		t.Fatalf("admissionPriority = %v, want %v", got, want)
	}
}

func TestSortDeferredByAdmissionPriority(t *testing.T) {
	entries := []deferredEntry{
		{task: lib.Task{
			TaskIdentifier: "attempted",
			Frontmatter:    lib.TaskFrontmatter{"job_started_at": "2026-10-21T08:00:00Z"},
		}},
		{task: lib.Task{
			TaskIdentifier: "fresh",
			Frontmatter:    lib.TaskFrontmatter{},
		}},
		{task: lib.Task{
			TaskIdentifier: "older",
			Frontmatter:    lib.TaskFrontmatter{"job_started_at": "2026-10-21T06:00:00Z"},
		}},
	}
	sortDeferredByAdmissionPriority(entries)
	want := []lib.TaskIdentifier{"fresh", "older", "attempted"}
	for i, id := range want {
		if entries[i].task.TaskIdentifier != id {
			t.Fatalf("position %d = %s, want %s", i, entries[i].task.TaskIdentifier, id)
		}
	}
}
