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
// cache handle.
type successCacheKey struct{}

// successSnapshotTTL bounds how long one success-record snapshot serves
// reconcile passes.
//
// The guard's answer changes only when a task completes, and its consumer
// already tolerates an hour of staleness: taskRerunWindow is 1 h and
// successRecordRetention is max(taskRerunWindow, longest declared cadence). A
// 60 s pass therefore gains nothing from re-reading a prefix that answers the
// same question, and the read is not cheap: on nuke dev 2026-11-15 the
// `claims/success/` prefix held 6024 records and took ~27 s.
//
// Re-reading it every pass cost the pass its whole budget. When the read
// crossed its own 30 s RequestTimeout, the v0.62.3 retry spent the remaining
// 15 s of the 45 s pass, so the resourcequota read and the job list that follow
// — the two reads the spawn path actually needs — inherited an expired context
// and every candidate was skipped with `spawn_error ... context deadline
// exceeded`.
const successSnapshotTTL = 5 * time.Minute

// successSnapshotReserve is the part of a pass's remaining budget the snapshot
// read must leave untouched. The resourcequota read and the job list that
// follow it run on the same context as the snapshot, and they are what the
// spawn path actually needs.
const successSnapshotReserve = 15 * time.Second

// errSuccessSnapshotNoBudget reports that the pass had too little budget left
// to start a snapshot read without starving the spawn path.
var errSuccessSnapshotNoBudget = stderrors.New(
	"success snapshot skipped: pass budget reserved for the spawn path",
)

// successSnapshotCache holds the last successful success-record snapshot across
// reconcile passes, so one slow read serves many passes instead of one.
//
// It is consulted only from inside a pass (successCacheFrom non-nil). The Kafka
// consumer and the deferred-respawn loop keep reading the store live, because a
// success this pod has just recorded must be visible to them immediately.
//
// A stale snapshot here cannot re-spawn work this pod just finished: the pass's
// guard reads taskStore.LastSucceeded first and suppresses on the in-memory
// record, and this pod invalidates the snapshot whenever it records a success
// of its own. What the snapshot covers is the record that outlives a restart or
// belongs to a peer, and the TTL bounds exactly that record's age.
type successSnapshotCache struct {
	mu       sync.Mutex
	snapshot map[string]map[string]libtime.DateTime
	readAt   time.Time
	valid    bool
}

// get returns the cached snapshot while it is still inside its TTL.
func (c *successSnapshotCache) get(
	now time.Time,
) (map[string]map[string]libtime.DateTime, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.valid || now.Sub(c.readAt) >= successSnapshotTTL {
		return nil, false
	}
	return c.snapshot, true
}

// put records a freshly read snapshot.
func (c *successSnapshotCache) put(
	snapshot map[string]map[string]libtime.DateTime,
	now time.Time,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot = snapshot
	c.readAt = now
	c.valid = true
}

// invalidate drops the cached snapshot so the next pass re-reads the store.
// Called when this pod records a success of its own, which is the one event the
// TTL cannot know about.
func (c *successSnapshotCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.valid = false
}

// passSuccessCache answers every candidate's success lookup from the shared
// snapshot.
//
// The guard needs the record of every candidate, and claim.SuccessStore.Lookup
// reads the task's own ref prefix — so a pass with N candidates would make N
// git-rest round-trips, the same cost that forced passRedriveStreakCache. One
// prefix list answers the question for all of them.
type passSuccessCache struct {
	shared          *successSnapshotCache
	currentDateTime libtime.CurrentDateTimeGetter
}

// withSuccessCache returns a context carrying a handle on the handler's
// success-record snapshot. The snapshot itself lives on the handler so it
// survives the pass; the context value only marks "this is a pass, answer from
// the snapshot".
func (h *taskEventHandler) withSuccessCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, successCacheKey{}, &passSuccessCache{
		shared:          &h.successSnapshot,
		currentDateTime: h.currentDateTime,
	})
}

// successCacheFrom returns the pass handle carried by ctx, or nil outside a
// pass.
func successCacheFrom(ctx context.Context) *passSuccessCache {
	cache, _ := ctx.Value(successCacheKey{}).(*passSuccessCache)
	return cache
}

// lookup returns taskID's recorded success at phase, reading the snapshot only
// when the cached one has aged out. A failed read is reported for every
// candidate of that pass, which leaves each one on the in-memory record exactly
// as a failed Lookup does.
func (c *passSuccessCache) lookup(
	ctx context.Context,
	store claim.SuccessStore,
	taskID string,
	phase string,
) (libtime.DateTime, bool, error) {
	now := c.currentDateTime.Now().Time()
	snapshot, ok := c.shared.get(now)
	if !ok {
		var err error
		snapshot, err = c.fetch(ctx, store)
		if err != nil {
			return libtime.DateTime{}, false, err
		}
		c.shared.put(snapshot, now)
	}
	// RedriveStreakPhaseComponent is a pure normalization helper, not a
	// capability: it maps "" to the frozen "none" component so this lookup keys
	// the snapshot exactly the way SuccessStore.Lookup does.
	at, ok := snapshot[taskID][claim.RedriveStreakPhaseComponent(phase)]
	return at, ok, nil
}

// fetch reads the snapshot within a budget that leaves the spawn path its
// reserve, and retries once only when a whole second read still fits.
//
// Both halves are load-bearing. The reserve is what stops a slow store from
// expiring the pass context before the resourcequota read and the job list run;
// the retry condition is what stops the v0.62.3 retry — guarded only by
// `ctx.Err() == nil` — from spending the spawn path's budget on a read that
// could not finish either.
func (c *passSuccessCache) fetch(
	ctx context.Context,
	store claim.SuccessStore,
) (map[string]map[string]libtime.DateTime, error) {
	readCtx, cancel, err := c.readContext(ctx, successSnapshotReserve)
	if err != nil {
		glog.V(2).Infof("event=reconcile_success_snapshot_skipped err=%v", err)
		return nil, err
	}
	snapshot, err := store.Snapshot(readCtx)
	cancel()
	if err == nil {
		glog.V(2).Infof("event=reconcile_success_snapshot tasks=%d err=<nil>", len(snapshot))
		return snapshot, nil
	}
	if ctx.Err() != nil {
		// The pass itself is over; there is nothing left to retry on.
		return nil, err
	}
	// One retry, and only while a full second read fits beside the reserve. A
	// transient failure — a git-rest restart, a refused connection, a mirror that
	// was rebuilding — usually clears on the second attempt, and losing the whole
	// pass's success records to it is what let a restarted executor re-run tasks
	// it had already done (nuke dev, 2026-11-14).
	glog.V(2).Infof("event=reconcile_success_snapshot_retry err=%v", err)
	retryCtx, retryCancel, budgetErr := c.readContext(
		ctx,
		successSnapshotReserve+claim.RequestTimeout.Duration(),
	)
	if budgetErr != nil {
		glog.V(2).Infof("event=reconcile_success_snapshot_retry_skipped err=%v", budgetErr)
		return nil, err
	}
	snapshot, retryErr := store.Snapshot(retryCtx)
	retryCancel()
	glog.V(2).Infof("event=reconcile_success_snapshot tasks=%d err=%v", len(snapshot), retryErr)
	return snapshot, retryErr
}

// readContext bounds one snapshot read by the pass's remaining budget minus
// reserve, capped at the client's own RequestTimeout.
//
// A context with no deadline — a caller that is not a pass — keeps the client's
// own cap and no reserve, which is the behaviour before this change.
func (c *passSuccessCache) readContext(
	ctx context.Context,
	reserve time.Duration,
) (context.Context, context.CancelFunc, error) {
	remaining, bounded := c.remaining(ctx)
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

// remaining reports how much budget ctx has left, and whether it is bounded at
// all. It measures against the injected clock rather than time.Until so the
// deadline arithmetic is testable without sleeping.
func (c *passSuccessCache) remaining(ctx context.Context) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return deadline.Sub(c.currentDateTime.Now().Time()), true
}
