// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"sync"

	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// successCacheKey is the context key carrying a reconcile pass's success-record
// snapshot.
type successCacheKey struct{}

// passSuccessCache answers every candidate's success lookup from ONE list call
// for the whole pass.
//
// The guard needs the record of every candidate, and claim.SuccessStore.Lookup
// reads the task's own ref prefix — so a pass with N candidates would make N
// git-rest round-trips, the same cost that forced passRedriveStreakCache. One
// prefix list answers the question for all of them.
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
// on the first call. A failed fetch is reported for every candidate of that pass,
// which leaves each one on the in-memory record exactly as a failed Lookup does.
func (c *passSuccessCache) lookup(
	ctx context.Context,
	store claim.SuccessStore,
	taskID string,
	phase string,
) (libtime.DateTime, bool, error) {
	c.once.Do(func() {
		c.snapshot, c.err = store.Snapshot(ctx)
		glog.V(2).Infof(
			"event=reconcile_success_snapshot tasks=%d err=%v", len(c.snapshot), c.err,
		)
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
