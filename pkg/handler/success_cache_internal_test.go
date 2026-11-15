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

// mutableClock is a CurrentDateTimeGetter a test can advance.
type mutableClock struct {
	mu  sync.Mutex
	now libtime.DateTime
}

func newMutableClock() *mutableClock {
	// Seeded from the real clock: the budget arithmetic compares a context
	// deadline (real time) against this getter, so a fixed seed would make
	// `remaining` meaningless. The TTL tests move it forward explicitly.
	return &mutableClock{now: libtime.DateTime(time.Now())}
}

func (c *mutableClock) Now() libtime.DateTime {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mutableClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = libtime.DateTime(time.Time(c.now).Add(d))
}

// countingSuccessStore answers Snapshot from a fixed map and counts the reads.
type countingSuccessStore struct {
	mu        sync.Mutex
	snapshot  map[string]map[string]libtime.DateTime
	calls     int
	blockFor  func(context.Context) // nil means return immediately
	returnErr error
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
	block := s.blockFor
	err := s.returnErr
	s.mu.Unlock()
	if block != nil {
		block(ctx)
	}
	// A real store fails when its context dies mid-read; the fake must too, or a
	// test that blocks past the deadline would look like a successful read.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return nil, err
	}
	return s.snapshot, nil
}

func (s *countingSuccessStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func newTestPassCache(clock libtime.CurrentDateTimeGetter) *passSuccessCache {
	return &passSuccessCache{
		shared:          &successSnapshotCache{},
		currentDateTime: clock,
	}
}

// TestSuccessSnapshotServesManyPassesFromOneRead pins the whole point of the
// cache: a 60 s pass must not pay a ~27 s prefix list on every pass. Before
// this change each pass issued its own read, and on nuke dev 2026-11-15 that
// read took long enough that the pass's spawn reads inherited an expired
// context.
func TestSuccessSnapshotServesManyPassesFromOneRead(t *testing.T) {
	clock := newMutableClock()
	store := &countingSuccessStore{
		snapshot: map[string]map[string]libtime.DateTime{
			"task-a": {"execution": clock.Now()},
		},
	}
	cache := newTestPassCache(clock)

	for range 5 {
		at, ok, err := cache.lookup(context.Background(), store, "task-a", "execution")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ok {
			t.Fatal("expected the cached record to be found")
		}
		if at.Time().IsZero() {
			t.Fatal("expected a non-zero completion time")
		}
	}

	if store.callCount() != 1 {
		t.Fatalf("expected 1 snapshot read across 5 passes, got %d", store.callCount())
	}
}

// TestSuccessSnapshotRefetchedAfterTTL keeps the cache bounded in age: a record
// written by a peer must become visible within the TTL.
func TestSuccessSnapshotRefetchedAfterTTL(t *testing.T) {
	clock := newMutableClock()
	store := &countingSuccessStore{snapshot: map[string]map[string]libtime.DateTime{}}
	cache := newTestPassCache(clock)

	if _, _, err := cache.lookup(context.Background(), store, "task-a", "execution"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	clock.advance(successSnapshotTTL - time.Second)
	if _, _, err := cache.lookup(context.Background(), store, "task-a", "execution"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.callCount() != 1 {
		t.Fatalf("expected the snapshot to still be cached inside its TTL, got %d reads",
			store.callCount())
	}
	clock.advance(2 * time.Second)
	if _, _, err := cache.lookup(context.Background(), store, "task-a", "execution"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.callCount() != 2 {
		t.Fatalf("expected a re-read once the TTL elapsed, got %d reads", store.callCount())
	}
}

// TestSlowSuccessSnapshotLeavesTheSpawnPathItsReserve is the regression this
// change exists for.
//
// The store blocks until its own context is done, which is the shape of the
// 30 s RequestTimeout the real client enforces. The read must be given the
// pass's remaining budget MINUS the reserve, so when it gives up the pass is
// still alive and the resourcequota read and job list that follow can run. The
// retry must not start either: after a read that spent everything down to the
// reserve, a second full read does not fit.
func TestSlowSuccessSnapshotLeavesTheSpawnPathItsReserve(t *testing.T) {
	clock := newMutableClock()
	store := &countingSuccessStore{
		snapshot: map[string]map[string]libtime.DateTime{},
		blockFor: func(ctx context.Context) { <-ctx.Done() },
	}
	cache := newTestPassCache(clock)

	// 17 s of pass budget leaves 2 s for the read once the 15 s reserve is
	// subtracted. The clock is fixed, so the deadline arithmetic is measured
	// against the real wall clock inside context.WithTimeout.
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
	// A second of tolerance: the read returns a hair before its own deadline, so
	// the reserve is measured with scheduling slack rather than exactly. The
	// property under test is that the reserve SURVIVES — before this change the
	// read consumed the whole pass and this was 0.
	if remaining := time.Until(mustDeadline(t, passCtx)); remaining < successSnapshotReserve-time.Second {
		t.Fatalf("expected at least %s of pass budget left, got %s",
			successSnapshotReserve, remaining)
	}
	if store.callCount() != 1 {
		t.Fatalf("expected the retry to be skipped for lack of budget, got %d reads",
			store.callCount())
	}
}

// TestSuccessSnapshotSkippedWithoutBudget refuses to start a read that could
// only starve the spawn path.
func TestSuccessSnapshotSkippedWithoutBudget(t *testing.T) {
	clock := newMutableClock()
	store := &countingSuccessStore{snapshot: map[string]map[string]libtime.DateTime{}}
	cache := newTestPassCache(clock)

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

// TestSuccessSnapshotInvalidatedOnRecord makes a success this pod records
// visible to the next pass instead of hiding it behind the TTL.
func TestSuccessSnapshotInvalidatedOnRecord(t *testing.T) {
	clock := newMutableClock()
	store := &countingSuccessStore{snapshot: map[string]map[string]libtime.DateTime{}}
	cache := newTestPassCache(clock)

	if _, _, err := cache.lookup(context.Background(), store, "task-a", "execution"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cache.shared.invalidate()
	if _, _, err := cache.lookup(context.Background(), store, "task-a", "execution"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.callCount() != 2 {
		t.Fatalf("expected a re-read after invalidation, got %d reads", store.callCount())
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
