// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"sync"

	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// redriveStreakCacheKey is the context key carrying a reconcile pass's streak
// snapshot.
type redriveStreakCacheKey struct{}

// passRedriveStreakCache answers every candidate's streak count from ONE list
// call for the whole pass.
//
// The cap check needs a count per candidate, and claim.RedriveStreakStore.Count
// reads the task's own ref prefix — so a pass with N candidates made N git-rest
// round-trips. Measured on dev 2026-10-24, at ~1.4 s per call: trading aborted
// every pass at the 45 s deadline having driven 16 of its 16 candidates, and
// openclaw aborted every second pass at 30, while a pass with 4 candidates
// completed in about a second. The alias change is what pushed those candidate
// counts up — it made tasks the executor had always owned but had filtered as
// foreign eligible to drive — so the cost grew without the budget moving.
//
// This is the same trade ListActiveJobs makes for Jobs: one list per pass
// instead of one read per task.
type passRedriveStreakCache struct {
	once     sync.Once
	snapshot map[string]map[string]int
	err      error

	// The cap check now also needs each candidate's recorded reasons, so the
	// same cache carries a second lazily-fetched snapshot — one list over the
	// reason prefix per pass. It is a separate prefix, so it cannot be folded
	// into the streak list, and a second cache type would only duplicate this
	// one.
	reasonsOnce    sync.Once
	reasonSnapshot map[string]map[string][]string
	reasonsErr     error
}

// withRedriveStreakCache returns a context carrying an empty per-pass streak
// cache. The snapshot is fetched lazily on the first candidate, so a pass with
// no candidates — the common case for a healthy executor — pays nothing.
//
// The cache travels on the pass's context, never on the handler, so the Kafka
// consumer and the deferred-respawn loop keep reading live counts: a streak a
// spawn just recorded must be visible to them immediately.
func withRedriveStreakCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, redriveStreakCacheKey{}, &passRedriveStreakCache{})
}

// redriveStreakCacheFrom returns the pass cache carried by ctx, or nil outside
// a pass.
func redriveStreakCacheFrom(ctx context.Context) *passRedriveStreakCache {
	cache, _ := ctx.Value(redriveStreakCacheKey{}).(*passRedriveStreakCache)
	return cache
}

// count returns taskID's streak at phase, fetching the pass snapshot on the
// first call. A failed fetch is reported for every candidate of that pass, which
// leaves each one on the in-memory floor exactly as a failed Count does today.
func (c *passRedriveStreakCache) count(
	ctx context.Context,
	store claim.RedriveStreakStore,
	taskID string,
	phase string,
) (int, error) {
	c.once.Do(func() {
		c.snapshot, c.err = store.Snapshot(ctx)
		glog.V(2).Infof(
			"event=reconcile_streak_snapshot tasks=%d err=%v", len(c.snapshot), c.err,
		)
	})
	if c.err != nil {
		return 0, c.err
	}
	// RedriveStreakPhaseComponent is a pure normalization helper, not a
	// capability: it maps "" to the frozen "none" component so this lookup keys
	// the snapshot exactly the way RedriveStreakStore.Count does. Nothing is
	// injected for it because there is nothing to substitute.
	return c.snapshot[taskID][claim.RedriveStreakPhaseComponent(phase)], nil
}

// reasons returns taskID's recorded reasons at phase, fetching the pass's
// reason snapshot on the first call. The snapshot's slices are already ordered
// by attempt, exactly as RedriveReasonStore.Reasons returns them, so the
// classifier sees the same window either way.
//
// A failed fetch is reported for every candidate of that pass, which the caller
// treats as no evidence: it refuses the spawn and closes nothing.
func (c *passRedriveStreakCache) reasons(
	ctx context.Context,
	store claim.RedriveReasonStore,
	taskID string,
	phase string,
) ([]string, error) {
	c.reasonsOnce.Do(func() {
		c.reasonSnapshot, c.reasonsErr = store.Snapshot(ctx)
		glog.V(2).Infof(
			"event=reconcile_reason_snapshot tasks=%d err=%v",
			len(c.reasonSnapshot),
			c.reasonsErr,
		)
	})
	if c.reasonsErr != nil {
		return nil, c.reasonsErr
	}
	return c.reasonSnapshot[taskID][claim.RedriveStreakPhaseComponent(phase)], nil
}
