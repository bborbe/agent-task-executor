// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	stderrors "errors"
	"sync"
	"testing"
	"time"

	libtime "github.com/bborbe/time"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// countingSuccessStore answers Snapshot from a fixed map, counts the reads and
// can be told to fail, block, or fail only the first time.
type countingSuccessStore struct {
	mu        sync.Mutex
	snapshot  map[string]map[string]libtime.DateTime
	calls     int
	failFirst bool
	blockFor  func(context.Context) // nil means return immediately
}

func (s *countingSuccessStore) Record(context.Context, string, string, libtime.DateTime) error {
	return nil
}

func (s *countingSuccessStore) Lookup(
	context.Context, string, string,
) (libtime.DateTime, bool, error) {
	return libtime.DateTime{}, false, nil
}

func (s *countingSuccessStore) Snapshot(
	ctx context.Context,
) (map[string]map[string]libtime.DateTime, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	failFirst := s.failFirst
	block := s.blockFor
	s.mu.Unlock()
	if block != nil {
		block(ctx)
	}
	// A real store fails when its context dies mid-read; the fake must too, or a
	// test that blocks past the deadline would look like a successful read.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if failFirst && call == 1 {
		return nil, stderrors.New("connection refused")
	}
	return s.snapshot, nil
}

func (s *countingSuccessStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// TestReconcilePassReadsSuccessStoreOnce pins the batching: one prefix list
// answers every candidate of a pass, never one per candidate.
func TestReconcilePassReadsSuccessStoreOnce(t *testing.T) {
	store := &countingSuccessStore{
		snapshot: map[string]map[string]libtime.DateTime{},
	}
	cache := &passSuccessCache{}

	for i := range 8 {
		if _, _, err := cache.lookup(
			context.Background(), store, string(rune('a'+i)), "execution",
		); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if store.callCount() != 1 {
		t.Fatalf("expected 1 snapshot read for a pass, got %d", store.callCount())
	}
}

// TestSlowSuccessSnapshotLeavesTheSpawnPathItsReserve is the regression this
// change exists for.
//
// The store blocks until its own context is done, which is the shape of the
// 30 s RequestTimeout the real client enforces. The read must be given the
// pass's remaining budget MINUS the reserve, so when it gives up the pass is
// still alive and the resourcequota read and job list that follow can run.
func TestSlowSuccessSnapshotLeavesTheSpawnPathItsReserve(t *testing.T) {
	store := &countingSuccessStore{
		snapshot: map[string]map[string]libtime.DateTime{},
		blockFor: func(ctx context.Context) { <-ctx.Done() },
	}
	cache := &passSuccessCache{}

	// 17 s of pass budget leaves 2 s for the read once the 15 s reserve is
	// subtracted.
	passCtx, cancel := context.WithTimeout(context.Background(), 17*time.Second)
	defer cancel()

	if _, _, err := cache.lookup(passCtx, store, "task-a", "execution"); err == nil {
		t.Fatal("expected the blocked read to report an error")
	}

	if err := passCtx.Err(); err != nil {
		t.Fatalf(
			"the snapshot read expired the pass context (%v) — the spawn reads that follow "+
				"would return `context deadline exceeded`",
			err,
		)
	}
	// A second of tolerance: the read returns a hair before its own deadline.
	// The property under test is that the reserve SURVIVES — before this change
	// the read consumed the whole pass and this was 0.
	if remaining := time.Until(mustDeadline(t, passCtx)); remaining < successSnapshotReserve-time.Second {
		t.Fatalf("expected at least %s of pass budget left, got %s",
			successSnapshotReserve, remaining)
	}
}

// TestSuccessSnapshotRetriesAFastFailure keeps the v0.62.3 recovery: a transient
// failure that clears at once must still get its second attempt.
func TestSuccessSnapshotRetriesAFastFailure(t *testing.T) {
	store := &countingSuccessStore{
		snapshot: map[string]map[string]libtime.DateTime{
			"task-a": {"execution": libtime.DateTime(time.Now())},
		},
		failFirst: true,
	}
	cache := &passSuccessCache{}

	passCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	at, ok, err := cache.lookup(passCtx, store, "task-a", "execution")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok || at.Time().IsZero() {
		t.Fatal("expected the retry's snapshot to answer the lookup")
	}
	if store.callCount() != 2 {
		t.Fatalf("expected a retry after a fast failure, got %d reads", store.callCount())
	}
}

// TestSuccessSnapshotSkipsRetryWithoutBudget stops the retry from spending the
// spawn path's reserve on a second attempt that could not finish.
func TestSuccessSnapshotSkipsRetryWithoutBudget(t *testing.T) {
	store := &countingSuccessStore{
		snapshot:  map[string]map[string]libtime.DateTime{},
		failFirst: true,
	}
	cache := &passSuccessCache{}

	// 20 s leaves 5 s for the first read. After a fast failure the pass has 20 s
	// left, which is short of reserve+min_read (25 s), so no retry.
	passCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, _, err := cache.lookup(passCtx, store, "task-a", "execution"); err == nil {
		t.Fatal("expected the failed read to report an error")
	}
	if store.callCount() != 1 {
		t.Fatalf("expected the retry to be skipped for lack of budget, got %d reads",
			store.callCount())
	}
}

// TestSuccessSnapshotSkippedWithoutBudget refuses to start a read that could
// only starve the spawn path.
func TestSuccessSnapshotSkippedWithoutBudget(t *testing.T) {
	store := &countingSuccessStore{snapshot: map[string]map[string]libtime.DateTime{}}
	cache := &passSuccessCache{}

	passCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _, err := cache.lookup(passCtx, store, "task-a", "execution")
	if err == nil {
		t.Fatal("expected an error when the pass has no budget to spare")
	}
	if !stderrors.Is(err, errSuccessSnapshotNoBudget) {
		t.Fatalf("expected errSuccessSnapshotNoBudget, got %v", err)
	}
	if store.callCount() != 0 {
		t.Fatalf("expected no snapshot read at all, got %d", store.callCount())
	}
}

func mustDeadline(t *testing.T, ctx context.Context) time.Time {
	t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected a bounded context")
	}
	return deadline
}

// The store's own interface is what the cache is written against; this assertion
// fails to compile if it drifts.
var _ claim.SuccessStore = (*countingSuccessStore)(nil)
