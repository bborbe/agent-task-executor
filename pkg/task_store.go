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

// ClearSucceeded drops the task's success record.
//
// Called once a record has been consumed by a park. Without it the record
// outlives its meaning: an operator who re-arms a parked task by moving its
// phase back to the phase that succeeded would match the same stale record and
// be re-parked on the very next pass, so the documented recovery path would do
// nothing until PruneSucceededBefore dropped the record an hour later.
func (s *TaskStore) ClearSucceeded(id lib.TaskIdentifier) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.succeeded, id)
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
