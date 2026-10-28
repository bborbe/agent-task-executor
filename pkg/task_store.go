// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"sync"
	"time"

	lib "github.com/bborbe/agent"
)

// TaskPhaseAnnotationKey records on a spawned Job the task phase it was spawned
// for, so the job watcher can tell which phase a succeeded Job completed. An
// annotation rather than a label: nothing selects on it.
const TaskPhaseAnnotationKey = "agent.benjamin-borbe.de/task-phase"

// TaskStore is a thread-safe map from TaskIdentifier to Task.
// It is populated when the executor spawns a Job and consumed by the job
// informer when publishing synthetic failure results.
//
// It also remembers when each task's latest Job succeeded. That record outlives
// the task entry (which the watcher evicts on success) so the spawn path can
// tell "a result is in flight to the vault" from "the agent never ran".
type TaskStore struct {
	mu        sync.RWMutex
	tasks     map[lib.TaskIdentifier]lib.Task
	succeeded map[lib.TaskIdentifier]SucceededJob
	// parked records which success each task has already been parked for,
	// keyed by the completion time that identifies it. A consumed set rather
	// than a deletion: the Job informer re-delivers a terminal Job every
	// resync for the Job's whole TTL and MarkSucceeded re-stamps the identical
	// record each time, so deleting the record would be undone within minutes.
	// Keyed by completion time because that is the only part of the record a
	// re-mark cannot change — a genuinely new success carries a later one.
	parked map[lib.TaskIdentifier]time.Time
}

// SucceededJob is the latest successful Job of a task: the phase it was spawned
// for (empty for a Job spawned before the phase annotation existed) and when it
// completed.
type SucceededJob struct {
	Phase       string
	CompletedAt time.Time
}

// NewTaskStore creates an empty TaskStore.
func NewTaskStore() *TaskStore {
	return &TaskStore{
		tasks:     make(map[lib.TaskIdentifier]lib.Task),
		succeeded: make(map[lib.TaskIdentifier]SucceededJob),
		parked:    make(map[lib.TaskIdentifier]time.Time),
	}
}

// MarkSucceeded records that a Job for the task, spawned for the given phase,
// completed successfully at the given time. Only a later completion replaces
// the record, so the informer's periodic re-delivery of an old terminal Job
// cannot move it.
func (s *TaskStore) MarkSucceeded(id lib.TaskIdentifier, phase string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.succeeded[id]; ok && !at.After(prev.CompletedAt) {
		return
	}
	s.succeeded[id] = SucceededJob{Phase: phase, CompletedAt: at}
}

// MarkParkConsumed records that the task has been parked for the success that
// completed at completedAt, so that success cannot park it a second time.
//
// This is what makes the recovery path work. An operator un-parks a task by
// moving its phase back to the drive phase, and that task still matches the same
// success record — the informer re-stamps it on every resync and MarkSucceeded
// keeps the earlier completion, so the record never goes away on its own.
// Consuming by completion time lets the re-armed task spawn while leaving a
// genuinely later success free to park again.
func (s *TaskStore) MarkParkConsumed(id lib.TaskIdentifier, completedAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.parked[id] = completedAt
}

// ParkConsumed reports whether the task has already been parked for the success
// that completed at completedAt.
func (s *TaskStore) ParkConsumed(id lib.TaskIdentifier, completedAt time.Time) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	parkedAt, ok := s.parked[id]
	return ok && parkedAt.Equal(completedAt)
}

// LastSucceeded returns the task's latest successful Job, if one is recorded.
func (s *TaskStore) LastSucceeded(id lib.TaskIdentifier) (SucceededJob, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.succeeded[id]
	return job, ok
}

// PruneSucceededBefore drops success records that completed before the cutoff,
// bounding the map to the tasks whose result may still be in flight.
func (s *TaskStore) PruneSucceededBefore(cutoff time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, job := range s.succeeded {
		if job.CompletedAt.Before(cutoff) {
			delete(s.succeeded, id)
		}
	}
}

// Store saves the task for the given identifier (called on job spawn).
func (s *TaskStore) Store(id lib.TaskIdentifier, task lib.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[id] = task
}

// Load retrieves the task for the given identifier.
func (s *TaskStore) Load(id lib.TaskIdentifier) (lib.Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	return t, ok
}

// Delete removes the task for the given identifier (called on job termination).
func (s *TaskStore) Delete(id lib.TaskIdentifier) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tasks, id)
}

// DeleteIfCurrentJob removes the entry only while it still names jobName, and
// reports whether it did.
//
// A caller that decides an entry is stale has read it, done I/O (a cluster
// lookup), and only then deletes. In that window a spawn can store a NEW entry
// for a new Job; an unconditional delete would drop it, leaving that Job
// unclaimed and invisible to the release path — the duplicate this whole
// mechanism exists to prevent. Comparing the Job name closes that window.
func (s *TaskStore) DeleteIfCurrentJob(id lib.TaskIdentifier, jobName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[id]
	if !ok || task.Frontmatter.CurrentJob() != jobName {
		return false
	}
	delete(s.tasks, id)
	return true
}

// Snapshot returns a shallow copy of the current task map for read-only iteration.
// Safe to call concurrently with Store/Delete; the returned map is owned by the caller.
func (s *TaskStore) Snapshot() map[lib.TaskIdentifier]lib.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[lib.TaskIdentifier]lib.Task, len(s.tasks))
	for k, v := range s.tasks {
		out[k] = v
	}
	return out
}
