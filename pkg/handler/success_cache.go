// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"
	"sync"
	"time"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// successCacheKey is the context key carrying a reconcile pass's success-record
// snapshot.
type successCacheKey struct{}

// successSnapshotReserve is the part of a pass's remaining budget the snapshot
// read must leave untouched.
//
// The resourcequota read and the job list that follow it run on the same
// context as the snapshot, and they are what the spawn path actually needs. On
// nuke dev 2026-11-15 a pass that spent its whole 45 s budget on the
// `claims/success/` read — 6024 refs, ~27 s — left those two reads an expired
// context, so every candidate was skipped with `spawn_error ... context
// deadline exceeded`.
const successSnapshotReserve = 15 * time.Second

// successSnapshotMinRead is the shortest second attempt worth starting.
//
// The retry exists for a transient failure that clears at once — a refused
// connection, a git-rest restart, a mirror that was rebuilding. A retry that
// could only run for a second against a 6024-ref prefix would fail the same way
// and spend budget the spawn path needs, so it is skipped instead.
const successSnapshotMinRead = 10 * time.Second

// errSuccessSnapshotNoBudget reports that the pass had too little budget left
// to start a snapshot read without starving the spawn path.
var errSuccessSnapshotNoBudget = stderrors.New(
	"success snapshot skipped: pass budget reserved for the spawn path",
)

// passSuccessCache answers every candidate's success lookup from ONE list call
// for the whole pass.
//
// The guard needs the record of every candidate, and claim.SuccessStore.Lookup
// reads the task's own ref prefix — so a pass with N candidates would make N
// git-rest round-trips, the same cost that forced passRedriveStreakCache. One
// prefix list answers the question for all of them.
//
// The snapshot is per pass, never shared across passes: the shared record is
// the authority for a peer's newer success, and a cached one would make this
// pod read "no record" — or "cadence elapsed" — for work a peer has already
// finished, which is a duplicate spawn. The read is bounded instead, so it
// cannot take the pass's budget with it.
type passSuccessCache struct {
	once     sync.Once
	snapshot map[string]map[string]libtime.DateTime
	err      error
}

// withSuccessCache returns a context carrying an empty per-pass success cache.
// The snapshot is fetched lazily on the first candidate, so a pass with no
// candidates — the common case for a healthy executor — pays nothing.
//
// The cache travels on the pass's context, never on the handler, so the Kafka
// consumer and the deferred-respawn loop keep reading live records: a success a
// Job just recorded must be visible to them immediately.
func withSuccessCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, successCacheKey{}, &passSuccessCache{})
}

// successCacheFrom returns the pass cache carried by ctx, or nil outside a pass.
func successCacheFrom(ctx context.Context) *passSuccessCache {
	cache, _ := ctx.Value(successCacheKey{}).(*passSuccessCache)
	return cache
}

// lookup returns taskID's recorded success at phase, fetching the pass snapshot
// on the first call. A failed fetch is reported for every candidate of that
// pass, which leaves each one on the in-memory record exactly as a failed
// Lookup does.
func (c *passSuccessCache) lookup(
	ctx context.Context,
	store claim.SuccessStore,
	taskID string,
	phase string,
) (libtime.DateTime, bool, error) {
	c.once.Do(func() {
		c.snapshot, c.err = fetchSuccessSnapshot(ctx, store)
	})
	if c.err != nil {
		return libtime.DateTime{}, false, c.err
	}
	// RedriveStreakPhaseComponent is a pure normalization helper, not a
	// capability: it maps "" to the frozen "none" component so this lookup keys
	// the snapshot exactly the way SuccessStore.Lookup does.
	at, ok := c.snapshot[taskID][claim.RedriveStreakPhaseComponent(phase)]
	return at, ok, nil
}

// fetchSuccessSnapshot reads the whole prefix once, within a budget that leaves
// the spawn path its reserve, and retries once only when a useful second read
// still fits.
//
// Both halves are load-bearing. The reserve is what stops a slow store from
// expiring the pass context before the resourcequota read and the job list run;
// the retry condition is what stops the v0.62.3 retry — guarded only by
// `ctx.Err() == nil` — from spending the spawn path's budget on a read that
// could not finish either.
func fetchSuccessSnapshot(
	ctx context.Context,
	store claim.SuccessStore,
) (map[string]map[string]libtime.DateTime, error) {
	snapshot, err := readSuccessSnapshot(ctx, store, successSnapshotReserve)
	if err == nil {
		glog.V(2).Infof("event=reconcile_success_snapshot tasks=%d err=<nil>", len(snapshot))
		return snapshot, nil
	}
	if ctx.Err() != nil {
		// The pass itself is over; there is nothing left to retry on.
		return nil, err
	}
	glog.V(2).Infof("event=reconcile_success_snapshot_retry err=%v", err)
	if !hasPassBudget(ctx, successSnapshotReserve+successSnapshotMinRead) {
		glog.V(2).Infof(
			"event=reconcile_success_snapshot_retry_skipped reason=budget reserve=%s min_read=%s",
			successSnapshotReserve,
			successSnapshotMinRead,
		)
		return nil, err
	}
	snapshot, retryErr := readSuccessSnapshot(ctx, store, successSnapshotReserve)
	if retryErr != nil {
		glog.V(2).Infof("event=reconcile_success_snapshot tasks=0 err=%v", retryErr)
		return nil, retryErr
	}
	glog.V(2).Infof("event=reconcile_success_snapshot tasks=%d err=<nil>", len(snapshot))
	return snapshot, nil
}

// readSuccessSnapshot performs one budget-bounded read of the whole prefix.
func readSuccessSnapshot(
	ctx context.Context,
	store claim.SuccessStore,
	reserve time.Duration,
) (map[string]map[string]libtime.DateTime, error) {
	readCtx, cancel, err := successSnapshotReadContext(ctx, reserve)
	if err != nil {
		return nil, err
	}
	defer cancel()
	snapshot, err := store.Snapshot(readCtx)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "read success snapshot")
	}
	return snapshot, nil
}

// successSnapshotReadContext bounds one snapshot read by the pass's remaining
// budget minus reserve, capped at the client's own RequestTimeout.
//
// A context with no deadline — a caller that is not a pass — keeps the client's
// own cap and no reserve, which is the behaviour before this change.
func successSnapshotReadContext(
	ctx context.Context,
	reserve time.Duration,
) (context.Context, context.CancelFunc, error) {
	remaining, bounded := remainingPassBudget(ctx)
	if !bounded {
		readCtx, cancel := context.WithTimeout(ctx, claim.RequestTimeout.Duration())
		return readCtx, cancel, nil
	}
	if remaining <= reserve {
		return nil, nil, errors.Wrapf(
			ctx,
			errSuccessSnapshotNoBudget,
			"remaining=%s reserve=%s",
			remaining,
			reserve,
		)
	}
	limit := remaining - reserve
	if limit > claim.RequestTimeout.Duration() {
		limit = claim.RequestTimeout.Duration()
	}
	readCtx, cancel := context.WithTimeout(ctx, limit)
	return readCtx, cancel, nil
}

// remainingPassBudget reports how much budget ctx has left, and whether it is
// bounded at all.
//
// It measures against the wall clock, not an injected getter: ctx.Deadline and
// context.WithTimeout both use the wall clock, so subtracting a handler's
// currentDateTime would compare two different clocks and yield a budget that is
// wrong by however far the injected one is offset.
func remainingPassBudget(ctx context.Context) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return time.Until(deadline), true
}

// hasPassBudget reports whether ctx has more than reserve left. An unbounded
// context always does.
func hasPassBudget(ctx context.Context, reserve time.Duration) bool {
	remaining, bounded := remainingPassBudget(ctx)
	return !bounded || remaining > reserve
}
