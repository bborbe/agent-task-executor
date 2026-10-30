// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"testing"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// indexTerminalSeedVault is the vault the seeded task belongs to. testTaskVault
// lives in package handler_test and is not visible here.
const indexTerminalSeedVault = "openclaw"

// indexTerminalSeedResolver returns one Config for every lookup. It is local so
// this file stays self-contained; the task_type-fallback path also calls
// ResolveByTaskType, which the embedded nil interface would otherwise panic on.
type indexTerminalSeedResolver struct {
	config pkg.AgentConfiguration
}

func (r indexTerminalSeedResolver) Resolve(
	context.Context, string,
) (pkg.AgentConfiguration, error) {
	return r.config, nil
}

func (r indexTerminalSeedResolver) ResolveByTaskType(
	context.Context, string, string,
) (pkg.AgentConfiguration, error) {
	return r.config, nil
}

// TestIndexTerminalSeedGate proves the startup seed refuses a task whose view
// status is already terminal, so it is never parked and never re-spawned. The
// map assertion is load-bearing: a status check placed inside
// spawnIfNeededClaimed would also yield zero spawns but would leave the task
// parked and would gate the Kafka path.
func TestIndexTerminalSeedGate(t *testing.T) {
	ctx := context.Background()
	spawner := &stubJobSpawner{}
	store := pkg.NewTaskStore()

	handler := NewTaskEventHandler(
		spawner,
		"dev",
		"prod",
		"self",
		true,
		true,
		indexTerminalSeedResolver{
			config: pkg.AgentConfiguration{
				Assignee: "claude",
				Trigger: &agentv1.Trigger{
					Statuses: domain.TaskStatuses{
						domain.TaskStatusInProgress,
						domain.TaskStatusCompleted,
						domain.TaskStatusAborted,
					},
					Phases: domain.TaskPhases{domain.TaskPhaseExecution},
				},
			},
		},
		stubResultPublisher{},
		store,
		libtime.NewCurrentDateTime(),
		notReadyView{},
		indexTerminalSeedVault,
		DefaultReconcilePassTimeout,
		DefaultReconcileInterval,
		pkg.ClaimModeOff,
		nil,
	)
	h, ok := handler.(*taskEventHandler)
	if !ok {
		t.Fatalf("expected *taskEventHandler, got %T", handler)
	}

	task := lib.Task{
		TaskIdentifier: lib.TaskIdentifier("tid-terminal-seed"),
		Vault:          indexTerminalSeedVault,
		Frontmatter: lib.TaskFrontmatter{
			"status":         "completed",
			"phase":          string(domain.TaskPhaseExecution),
			"assignee":       "claude",
			"stage":          "prod",
			"current_job":    "job-terminal-seed",
			"job_started_at": "2026-05-17T09:34:00Z",
		},
	}
	store.Store(task.TaskIdentifier, task)

	h.seedDeferredRespawnsFromStore(ctx)
	if got := len(h.deferredRespawns); got != 0 {
		t.Fatalf("seed parked %d entries; want 0", got)
	}
	if _, parked := h.deferredRespawns[task.TaskIdentifier]; parked {
		t.Fatalf("seed parked terminal task %s", task.TaskIdentifier)
	}

	for i := 0; i < 2; i++ {
		if err := h.EvalDeferredRespawns(ctx); err != nil {
			t.Fatalf("EvalDeferredRespawns returned %v; want nil", err)
		}
	}
	if spawner.spawned != 0 {
		t.Fatalf("spawned %d jobs for a terminal task; want 0", spawner.spawned)
	}
}
