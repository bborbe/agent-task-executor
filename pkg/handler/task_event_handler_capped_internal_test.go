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

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/claim/claimtest"
)

// stubRedriveStreakStore satisfies claim.RedriveStreakStore with a fixed Count.
// Record and Clear are no-ops: these tests only care which refusal the spawn
// funnel reports.
type stubRedriveStreakStore struct {
	count int
}

func (s *stubRedriveStreakStore) Count(context.Context, string, string) (int, error) {
	return s.count, nil
}

func (s *stubRedriveStreakStore) Snapshot(context.Context) (map[string]map[string]int, error) {
	return nil, nil
}

func (s *stubRedriveStreakStore) Record(context.Context, string, string) error { return nil }

func (s *stubRedriveStreakStore) Clear(context.Context, string) error { return nil }

// newCappedTestHandler builds a handler whose shared streak store is a stub.
//
// The leaseManager warm-up is mandatory. redriveStreakStore returns (nil, nil)
// when claim mode is off, and createLeaseManager overwrites h.redriveStore with
// a real store on its first call — so a stub assigned before that call is
// silently discarded.
func newCappedTestHandler(
	t *testing.T,
	spawner *stubJobSpawner,
	count int,
) *taskEventHandler {
	t.Helper()
	client := &countingClaimRefClient{}
	handler := NewTaskEventHandler(
		spawner,
		"dev",
		"prod",
		"self",
		true,
		true,
		nil,
		stubResultPublisher{},
		pkg.NewTaskStore(),
		libtime.NewCurrentDateTime(),
		&fakeTaskView{},
		"v",
		DefaultReconcilePassTimeout,
		DefaultReconcileInterval,
		pkg.ClaimModeShadow,
		func() claim.ClaimRefClient { return client },
		WithClaimClientFactory(func() claim.ClaimClient {
			return claimtest.NewClient(client, libtime.NewCurrentDateTime().Now)
		}),
		WithClaimJobControl(noopJobControl{}),
	)
	h, ok := handler.(*taskEventHandler)
	if !ok {
		t.Fatalf("expected *taskEventHandler, got %T", handler)
	}
	if _, err := h.leaseManager(context.Background()); err != nil {
		t.Fatalf("lease manager: %v", err)
	}
	h.redriveStore = &stubRedriveStreakStore{count: count}
	return h
}

// cappedTestTask is an eligible task: in this vault, in a non-terminal phase,
// and routed to an assignee.
func cappedTestTask() lib.Task {
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier("task-capped"),
		Vault:          "v",
		Frontmatter: lib.TaskFrontmatter{
			"status":   "in_progress",
			"phase":    "execution",
			"assignee": "claude",
		},
	}
}

// TestReconcileReportsStoreHeldCapAsCapped pins the regression: a task capped
// only in the shared store — the post-restart state, where this executor's
// in-memory streak map is empty — used to be counted as `deferred`, which made
// a fully retired fleet read as a merely busy queue.
func TestReconcileReportsStoreHeldCapAsCapped(t *testing.T) {
	ctx := context.Background()
	h := newCappedTestHandler(t, &stubJobSpawner{}, maxConsecutiveRedrives)

	task := cappedTestTask()
	c := reconcileCounters{}
	c.add(h.driveCandidate(ctx, reconcileCandidate{
		task:   task,
		config: &pkg.AgentConfiguration{Assignee: "claude", Image: "img"},
		claim:  deferredClaim{id: task.TaskIdentifier},
	}))

	if c.capped != 1 {
		t.Fatalf("expected capped=1, got capped=%d deferred=%d", c.capped, c.deferred)
	}
	if c.deferred != 0 {
		t.Fatalf("expected deferred=0, got deferred=%d", c.deferred)
	}
}

// TestReconcileReportsUncappedSpawnAsRedriven keeps the capped assertion honest:
// the same wiring with a store that answers 0 must report a redrive, so the
// first test cannot pass on a hardcoded capped counter.
func TestReconcileReportsUncappedSpawnAsRedriven(t *testing.T) {
	ctx := context.Background()
	h := newCappedTestHandler(t, &stubJobSpawner{}, 0)

	task := cappedTestTask()
	c := reconcileCounters{}
	c.add(h.driveCandidate(ctx, reconcileCandidate{
		task:   task,
		config: &pkg.AgentConfiguration{Assignee: "claude", Image: "img"},
		claim:  deferredClaim{id: task.TaskIdentifier},
	}))

	if c.redriven != 1 {
		t.Fatalf("expected redriven=1, got redriven=%d capped=%d", c.redriven, c.capped)
	}
	if c.capped != 0 {
		t.Fatalf("expected capped=0, got capped=%d", c.capped)
	}
}

// TestReconcileReportsConcurrencyDeferralAsDeferred pins that the new switch
// does not fold a genuine deferral into the capped bucket: a task waiting for a
// slot on another task's Job is deferred, not capped.
func TestReconcileReportsConcurrencyDeferralAsDeferred(t *testing.T) {
	ctx := context.Background()
	spawner := &stubJobSpawner{
		active: map[lib.TaskIdentifier]bool{"other-task": true},
	}
	h := newCappedTestHandler(t, spawner, 0)

	task := cappedTestTask()
	c := reconcileCounters{}
	c.add(h.driveCandidate(ctx, reconcileCandidate{
		task: task,
		config: &pkg.AgentConfiguration{
			Assignee:          "claude",
			Image:             "img",
			MaxConcurrentJobs: 1,
		},
		claim: deferredClaim{id: task.TaskIdentifier},
	}))

	if c.deferred != 1 {
		t.Fatalf("expected deferred=1, got deferred=%d capped=%d", c.deferred, c.capped)
	}
	if c.capped != 0 {
		t.Fatalf("expected capped=0, got capped=%d", c.capped)
	}
}
