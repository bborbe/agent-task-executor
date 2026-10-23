// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"testing"
	"time"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/prometheus/client_golang/prometheus/testutil"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// inactiveSpawner reports no active Job for any task, which is the state this
// guard exists for: a Job that runs somewhere this cluster cannot see.
type inactiveSpawner struct{}

func (inactiveSpawner) SpawnJob(
	context.Context, lib.Task, pkg.AgentConfiguration,
) (string, error) {
	return "", nil
}

func (inactiveSpawner) IsJobActive(
	context.Context, lib.TaskIdentifier,
) (bool, error) {
	return false, nil
}

func (inactiveSpawner) CountActiveJobs(context.Context, string) (int, error) {
	return 0, nil
}

func (inactiveSpawner) ListActiveJobs(
	context.Context, string,
) (map[lib.TaskIdentifier]bool, error) {
	return map[lib.TaskIdentifier]bool{}, nil
}

// JobExists reports the stored Job as still present — the foreign case: the Job
// runs somewhere this cluster cannot see, so the guard must keep suppressing.
func (inactiveSpawner) JobExists(context.Context, string) (bool, error) {
	return true, nil
}

// currentJobTask is a task whose frontmatter names jobName, started startedAgo
// before now.
func currentJobTask(jobName string, startedAgo time.Duration) lib.Task {
	startedAt := time.Now().Add(-startedAgo).UTC().Format(time.RFC3339)
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier("tid-current-job-1"),
		Frontmatter: lib.TaskFrontmatter{
			"current_job":    jobName,
			"job_started_at": startedAt,
		},
	}
}

// jobConfig is the config the guard is asked to decide under.
func jobConfig() *pkg.AgentConfiguration {
	return &pkg.AgentConfiguration{Assignee: "pr-reviewer-agent"}
}

// newCurrentJobHandler builds a handler whose store and spawner are empty.
func newCurrentJobHandler() *taskEventHandler {
	return &taskEventHandler{
		jobSpawner:       inactiveSpawner{},
		taskStore:        pkg.NewTaskStore(),
		currentDateTime:  libtime.NewCurrentDateTime(),
		deferredRespawns: map[lib.TaskIdentifier]deferredEntry{},
	}
}

// TestJobProvablyFinishedRejectsAnotherClustersJob covers the incident this
// guard exists for: the store still names the Job this executor created, while
// the frontmatter names a different one — so that Job belongs to another
// cluster, and its absence here says nothing about whether it is running.
func TestJobProvablyFinishedRejectsAnotherClustersJob(t *testing.T) {
	h := newCurrentJobHandler()
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)
	h.taskStore.Store(task.TaskIdentifier, lib.Task{
		TaskIdentifier: task.TaskIdentifier,
		Frontmatter: lib.TaskFrontmatter{
			"current_job": "pr-reviewer-agent-94c1801c-20261022202004",
		},
	})

	finished, err := h.jobProvablyFinished(
		context.Background(), task, "pr-reviewer-agent-94c1801c-20261022205503",
	)
	if err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil", err)
	}
	if finished {
		t.Fatal("provably finished = true for a Job this executor did not spawn; want false")
	}
}

// TestJobProvablyFinishedAcceptsItsOwnJob covers the ordinary respawn: the store
// names the same Job and it is no longer active, so it is over.
func TestJobProvablyFinishedAcceptsItsOwnJob(t *testing.T) {
	h := newCurrentJobHandler()
	jobName := "pr-reviewer-agent-94c1801c-20261022202004"
	task := currentJobTask(jobName, 7*time.Minute)
	h.taskStore.Store(task.TaskIdentifier, task)

	finished, err := h.jobProvablyFinished(context.Background(), task, jobName)
	if err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil", err)
	}
	if !finished {
		t.Fatal("provably finished = false for a Job this executor spawned and saw end; want true")
	}
}

// TestJobProvablyFinishedWithoutStoreEntryKeepsPriorBehaviour pins the boundary
// of the guard: with no store entry (a restart, or a spawn this executor never
// made) nothing local distinguishes a foreign Job from a finished one, so the
// respawn proceeds as it did before. Bounding this case by the Job deadline was
// tried and reverted — it withholds a legitimate respawn for up to 30 minutes,
// which the deferred-respawn contract does not allow.
func TestJobProvablyFinishedWithoutStoreEntryKeepsPriorBehaviour(t *testing.T) {
	h := newCurrentJobHandler()
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)

	finished, err := h.jobProvablyFinished(
		context.Background(), task, "pr-reviewer-agent-94c1801c-20261022205503",
	)
	if err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil", err)
	}
	if !finished {
		t.Fatal("provably finished = false with an empty store; want true (prior behaviour)")
	}
}

// goneSpawner reports every named Job as absent from this cluster: the Job
// finished and was garbage-collected before the informer observed a terminal
// update, which is the leak the store can never clear by itself.
type goneSpawner struct{ inactiveSpawner }

func (goneSpawner) JobExists(context.Context, string) (bool, error) {
	return false, nil
}

// TestJobProvablyFinishedClearsALeakedStoreEntry is the convergence guard. The
// store entry is only cleared by the job informer processing a terminal event,
// and the informer registers no DeleteFunc, so a Job GC'd before that event was
// seen leaves an entry nothing removes. Without the self-heal the task is
// suppressed on every pass, forever — silent, and indistinguishable from an idle
// agent. With it, the cluster's own answer (the Job is gone) drops the entry and
// the task respawns.
func TestJobProvablyFinishedClearsALeakedStoreEntry(t *testing.T) {
	h := newCurrentJobHandler()
	h.jobSpawner = goneSpawner{}
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)
	h.taskStore.Store(task.TaskIdentifier, lib.Task{
		TaskIdentifier: task.TaskIdentifier,
		Frontmatter: lib.TaskFrontmatter{
			"current_job": "pr-reviewer-agent-94c1801c-20261022202004",
		},
	})

	finished, err := h.jobProvablyFinished(
		context.Background(), task, "pr-reviewer-agent-94c1801c-20261022205503",
	)
	if err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil", err)
	}
	if !finished {
		t.Fatal(
			"provably finished = false for a leaked store entry; want true so the task respawns",
		)
	}
	if _, ok := h.taskStore.Load(task.TaskIdentifier); ok {
		t.Fatal("store entry still present after the stale-Job clear; want it dropped")
	}
}

// TestCheckActiveCurrentJobSuppressesAForeignJob is the end-to-end assertion: a
// dispatch pass whose current_job names a Job this executor never created spawns
// nothing and re-defers instead of dropping the task.
func TestCheckActiveCurrentJobSuppressesAForeignJob(t *testing.T) {
	ctx := context.Background()
	h := newCurrentJobHandler()
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)
	h.taskStore.Store(task.TaskIdentifier, lib.Task{
		TaskIdentifier: task.TaskIdentifier,
		Frontmatter: lib.TaskFrontmatter{
			"current_job": "pr-reviewer-agent-94c1801c-20261022202004",
		},
	})
	before := testutil.ToFloat64(
		metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_job"),
	)

	suppress, err := h.checkActiveCurrentJob(
		ctx,
		task,
		"pr-reviewer-agent-94c1801c-20261022205503",
		jobConfig(),
		nil,
	)

	if err != nil {
		t.Fatalf("checkActiveCurrentJob: %v", err)
	}
	if !suppress {
		t.Fatal("suppress = false for a foreign Job; want true — this is the duplicate")
	}
	after := testutil.ToFloat64(
		metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_job"),
	)
	if delta := after - before; delta != 1 {
		t.Errorf("skipped_foreign_job delta = %v; want 1", delta)
	}
	entry, ok := h.deferredRespawns[task.TaskIdentifier]
	if !ok {
		t.Fatal("no deferred entry stored; the task would not be re-checked")
	}
	if entry.retryAfter.IsZero() {
		t.Error("deferred entry retryAfter = zero; want a future re-check")
	}
}
