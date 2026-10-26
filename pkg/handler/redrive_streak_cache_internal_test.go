// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"fmt"
	"testing"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/claim/claimtest"
)

// countingStreakStore records how the cap check reached the shared store, and
// answers both reads from one fixed snapshot.
type countingStreakStore struct {
	snapshot      map[string]map[string]int
	snapshotCalls int
	countCalls    int
}

func (s *countingStreakStore) Count(context.Context, string, string) (int, error) {
	s.countCalls++
	return 0, nil
}

func (s *countingStreakStore) Snapshot(context.Context) (map[string]map[string]int, error) {
	s.snapshotCalls++
	return s.snapshot, nil
}

func (s *countingStreakStore) Record(context.Context, string, string) error { return nil }

func (s *countingStreakStore) Clear(context.Context, string) error { return nil }

// newStreakCacheTestHandler builds a handler over store. It mirrors
// newCappedTestHandler's mandatory leaseManager warm-up: that call overwrites
// h.redriveStore with a real store, so the stub must be assigned after it.
func newStreakCacheTestHandler(
	t *testing.T,
	spawner *stubJobSpawner,
	store claim.RedriveStreakStore,
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
	h.redriveStore = store
	return h
}

// deferredCandidates drives n candidates whose assignee is already at its
// concurrency cap, so every one of them reaches the streak read and then
// defers. It returns nothing: the assertions are on the store's call counts.
func deferredCandidates(t *testing.T, h *taskEventHandler, ctx context.Context, n int) {
	t.Helper()
	config := &pkg.AgentConfiguration{
		Assignee:          "claude",
		Image:             "img",
		MaxConcurrentJobs: 1,
	}
	for i := range n {
		task := cappedTestTask()
		task.TaskIdentifier = lib.TaskIdentifier(fmt.Sprintf("task-%d", i))
		h.driveCandidate(ctx, reconcileCandidate{
			task:   task,
			config: config,
			claim:  deferredClaim{id: task.TaskIdentifier},
		})
	}
}

// TestReconcilePassReadsStreakStoreOnce pins the batching: the cap check used to
// make one git-rest round-trip per candidate, which is cheap while git-rest is
// healthy and is what turned a degraded git-rest into a whole-pass abort (the
// 2026-10-25 outage aborted every pass at 45 s). Inside a pass the counts must
// come from one prefix list, so the pass's exposure to git-rest latency stops
// scaling with the candidate count.
func TestReconcilePassReadsStreakStoreOnce(t *testing.T) {
	store := &countingStreakStore{snapshot: map[string]map[string]int{}}
	spawner := &stubJobSpawner{active: map[lib.TaskIdentifier]bool{"other": true}}
	h := newStreakCacheTestHandler(t, spawner, store)

	deferredCandidates(t, h, withRedriveStreakCache(context.Background()), 8)

	if store.snapshotCalls != 1 {
		t.Fatalf("expected 1 snapshot call, got %d", store.snapshotCalls)
	}
	if store.countCalls != 0 {
		t.Fatalf("expected 0 per-task count calls inside a pass, got %d", store.countCalls)
	}
}

// TestReconcilePassFetchesSnapshotLazily keeps the cheap case cheap: a pass with
// no candidate must not pay for a prefix list at all.
func TestReconcilePassFetchesSnapshotLazily(t *testing.T) {
	store := &countingStreakStore{snapshot: map[string]map[string]int{}}
	h := newStreakCacheTestHandler(t, &stubJobSpawner{}, store)

	if _, err := h.redriveStreakCount(
		withRedriveStreakCache(context.Background()), store, "never-read", "execution",
	); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The count itself triggers the fetch; what this pins is that the fetch
	// happens once per pass and not once per lookup.
	if store.snapshotCalls != 1 {
		t.Fatalf("expected 1 snapshot call, got %d", store.snapshotCalls)
	}
}

// TestRedriveStreakCountOutsideAPassStaysLive guards the other half: the Kafka
// consumer and the deferred-respawn loop run on their own contexts and must not
// read a count a pass froze before a spawn recorded a new one.
func TestRedriveStreakCountOutsideAPassStaysLive(t *testing.T) {
	store := &countingStreakStore{snapshot: map[string]map[string]int{}}
	h := newStreakCacheTestHandler(t, &stubJobSpawner{}, store)

	for range 3 {
		if _, err := h.redriveStreakCount(
			context.Background(), store, "task-live", "execution",
		); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if store.countCalls != 3 {
		t.Fatalf("expected 3 live count calls outside a pass, got %d", store.countCalls)
	}
	if store.snapshotCalls != 0 {
		t.Fatalf("expected 0 snapshot calls outside a pass, got %d", store.snapshotCalls)
	}
}

// TestReconcilePassCapsFromSnapshot pins that the snapshot is actually consulted:
// a candidate whose stored streak is at the cap must report capped, not deferred,
// so the pass summary keeps naming a stuck task as stuck.
func TestReconcilePassCapsFromSnapshot(t *testing.T) {
	store := &countingStreakStore{snapshot: map[string]map[string]int{
		"task-capped": {"execution": maxConsecutiveRedrives},
	}}
	spawner := &stubJobSpawner{active: map[lib.TaskIdentifier]bool{"other": true}}
	h := newStreakCacheTestHandler(t, spawner, store)

	ctx := withRedriveStreakCache(context.Background())
	task := cappedTestTask()
	c := reconcileCounters{}
	c.add(h.driveCandidate(ctx, reconcileCandidate{
		task:   task,
		config: &pkg.AgentConfiguration{Assignee: "claude", Image: "img", MaxConcurrentJobs: 1},
		claim:  deferredClaim{id: task.TaskIdentifier},
	}))

	if c.capped != 1 {
		t.Fatalf("expected capped=1, got capped=%d deferred=%d", c.capped, c.deferred)
	}
}
