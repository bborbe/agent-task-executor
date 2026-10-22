// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"sync"
	"testing"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// stubJobSpawner satisfies spawner.JobSpawner without importing the mocks
// package, which imports pkg/handler and so cannot be used from an internal
// test of it.
type stubJobSpawner struct{ spawned int }

func (s *stubJobSpawner) SpawnJob(
	context.Context, lib.Task, pkg.AgentConfiguration,
) (string, error) {
	s.spawned++
	return "job-1", nil
}

func (s *stubJobSpawner) IsJobActive(
	context.Context, lib.TaskIdentifier,
) (bool, error) {
	return false, nil
}

func (s *stubJobSpawner) CountActiveJobs(context.Context, string) (int, error) {
	return 0, nil
}

func (s *stubJobSpawner) ListActiveJobs(
	context.Context, string,
) (map[lib.TaskIdentifier]bool, error) {
	return map[lib.TaskIdentifier]bool{}, nil
}

// stubResultPublisher satisfies pkg.ResultPublisher for the spawn path, which
// publishes a spawn notification once the Job exists.
type stubResultPublisher struct{}

func (stubResultPublisher) PublishSpawnNotification(
	context.Context, lib.Task, string,
) error {
	return nil
}

func (stubResultPublisher) PublishFailure(
	context.Context, lib.Task, string, string,
) error {
	return nil
}

func (stubResultPublisher) PublishClearCurrentJob(
	context.Context, lib.Task, string,
) error {
	return nil
}

func (stubResultPublisher) PublishIncrementTriggerCount(context.Context, lib.Task) error {
	return nil
}

func (stubResultPublisher) PublishQueueEntry(
	context.Context, lib.Task, string,
) error {
	return nil
}

func (stubResultPublisher) PublishSetTriggerScope(
	context.Context, lib.Task, string, int,
) error {
	return nil
}

func (stubResultPublisher) PublishTypeMismatchFailure(
	context.Context, lib.Task, string,
) error {
	return nil
}

func (stubResultPublisher) PublishRestoreAssignee(
	context.Context, lib.Task, string,
) error {
	return nil
}

func (stubResultPublisher) PublishRaw(
	context.Context, base.CommandOperation, interface{},
) error {
	return nil
}

// TestSpawnMarksPendingBeforeTakingTheClaim pins the ordering the review found:
// the in-flight marker must already be set while the claim PUT is in flight,
// otherwise a terminal event for the previous Job landing in that window sees a
// current-looking store and releases the claim just taken.
func TestSpawnMarksPendingBeforeTakingTheClaim(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	markedDuringPut := false

	client := &countingClaimRefClient{}
	client.onPut = func() {
		mu.Lock()
		defer mu.Unlock()
		markedDuringPut = true
	}

	spawner := &stubJobSpawner{}
	handler := NewTaskEventHandler(
		spawner,
		"prod",
		"self",
		true,
		true,
		nil,
		stubResultPublisher{},
		pkg.NewTaskStore(),
		libtime.NewCurrentDateTime(),
		nil,
		"v",
		DefaultReconcilePassTimeout,
		DefaultReconcileInterval,
		pkg.ClaimModeShadow,
		func() claim.ClaimRefClient { return client },
		noopJobControl{},
	)
	h, ok := handler.(*taskEventHandler)
	if !ok {
		t.Fatalf("expected *taskEventHandler, got %T", handler)
	}

	task := lib.Task{
		TaskIdentifier: lib.TaskIdentifier("task-1"),
		Vault:          "v",
		Frontmatter: lib.TaskFrontmatter{
			"status":   "in_progress",
			"phase":    "execution",
			"assignee": "claude",
		},
	}

	if _, err := h.spawnIfNeededClaimed(ctx, task, &pkg.AgentConfiguration{
		Assignee: "claude",
		Image:    "img",
	}, nil); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if !markedDuringPut {
		t.Fatalf("expected the claim PUT to run with the marker set (spawned=%d)", spawner.spawned)
	}
}
