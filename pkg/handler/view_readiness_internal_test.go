// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// notReadyView is a TaskView that has not finished replaying. Embedding the
// interface satisfies the rest of it without implementing them: the gate under
// test must not touch anything but Ready, and a panic here would say it did.
type notReadyView struct{ TaskView }

func (notReadyView) Ready() bool { return false }

// stubResolver returns an empty Config for every assignee, which is enough for
// the event to reach the spawn decision.
type stubResolver struct{ pkg.ConfigResolver }

func (stubResolver) Resolve(context.Context, string) (pkg.AgentConfiguration, error) {
	return pkg.AgentConfiguration{}, nil
}

// TestConsumeMessageDoesNotSpawnBeforeTheViewIsReady is the cold-start
// regression. A pod that has just started resumes its consumer from the last
// committed offset, so the first events it sees are a BACKLOG — snapshots of
// tasks as they looked when produced, possibly already handled by another
// cluster. Acting on one spawns a task on the strength of a stale snapshot, and
// the guards that would refuse it cannot help: the spawn path's anti-duplicate
// checks read the task view and the task store, and on a cold pod both are
// empty.
//
// Measured 2026-10-23: five task ids ran on both nuke prod and sun within
// minutes of sun coming back, two of them verified as double successes, spawned
// by a pod that had just started cold.
//
// Reconcile is the source-of-truth floor and already refuses to run before the
// view is ready, so every task skipped here is driven by the next reconcile pass
// once the view has replayed — deferred, never dropped.
func TestConsumeMessageDoesNotSpawnBeforeTheViewIsReady(t *testing.T) {
	spawner := &stubJobSpawner{}
	handler := NewTaskEventHandler(
		spawner,
		"dev",
		"prod",
		"self",
		true,
		true,
		stubResolver{},
		stubResultPublisher{},
		pkg.NewTaskStore(),
		libtime.NewCurrentDateTime(),
		notReadyView{},
		"v",
		DefaultReconcilePassTimeout,
		DefaultReconcileInterval,
		pkg.ClaimModeOff,
		nil,
		WithClaimJobControl(noopJobControl{}),
	)
	h, ok := handler.(*taskEventHandler)
	if !ok {
		t.Fatalf("expected *taskEventHandler, got %T", handler)
	}

	task := lib.Task{
		TaskIdentifier: lib.TaskIdentifier("task-cold-1"),
		Vault:          "v",
		Frontmatter: lib.TaskFrontmatter{
			"status":   "in_progress",
			"phase":    "execution",
			"assignee": "claude",
			"stage":    "prod",
		},
	}
	value, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal task event: %v", err)
	}

	if consumeErr := h.ConsumeMessage(
		context.Background(),
		&sarama.ConsumerMessage{Value: value},
	); consumeErr != nil {
		t.Fatalf(
			"ConsumeMessage returned %v; want nil (the event is deferred, not failed)",
			consumeErr,
		)
	}
	if spawner.spawned != 0 {
		t.Fatalf("spawned %d jobs with an unready view; want 0", spawner.spawned)
	}
}
