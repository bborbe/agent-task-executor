// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"sync"
	"time"

	lib "github.com/bborbe/agent"
	libk8s "github.com/bborbe/k8s"
)

// jobCacheKey is the context key carrying a reconcile pass's active-Job cache.
type jobCacheKey struct{}

// passJobCache memoizes ListActiveJobs per assignee for one reconcile pass.
type passJobCache struct {
	mu     sync.Mutex
	active map[string]cachedActiveJobs
}

// cachedActiveJobs is one assignee's active set and the spawn generation it
// was read at.
type cachedActiveJobs struct {
	active map[lib.TaskIdentifier]bool
	gen    uint64
	readAt time.Time
}

// jobCacheTTL bounds how long a pass serves one active-Job read. Completions
// are not tracked, so an entry grows stale-high; on sun a pass runs for minutes
// and, sitting at the cap, never spawns to refresh itself. The TTL keeps the
// cost at one list per assignee per window rather than two per task.
const jobCacheTTL = 10 * time.Second

// withJobCache returns a context carrying a fresh per-pass active-Job cache.
// Inside a pass every lookup is answered from one `list jobs` per assignee.
//
// The cache travels on the pass's context, never on the handler, so only the
// pass's own lookups see it. Until 2026-10-21 it was a handler field armed for
// the whole pass, and the Kafka consumer and the deferred cap retries — which
// run concurrently on their own contexts — were served the pass's frozen
// snapshot. On sun a pass ran for minutes; a snapshot taken at 6 stayed at 6
// while Jobs completed, and the executor logged `concurrency_cap running=6
// cap=6` with only 4 Jobs active.
//
// Added 2026-10-19: spawnIfNeeded made two list calls per eligible task
// (IsJobActive and CountActiveJobs). A pass over ~70 eligible tasks therefore
// issued ~140 calls, and client-go's default 5 QPS limiter consumed the whole
// 30 s pass budget before the first spawn — the `spawn_error ... context
// deadline exceeded` that kept github-update-go at zero dispatch.
func withJobCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, jobCacheKey{}, &passJobCache{
		active: make(map[string]cachedActiveJobs),
	})
}

// jobCacheFrom returns the pass cache carried by ctx, or nil outside a pass.
func jobCacheFrom(ctx context.Context) *passJobCache {
	cache, _ := ctx.Value(jobCacheKey{}).(*passJobCache)
	return cache
}

// spawnGenFor returns the assignee's current spawn generation.
func (h *taskEventHandler) spawnGenFor(assignee string) uint64 {
	h.spawnGenMu.Lock()
	defer h.spawnGenMu.Unlock()
	return h.spawnGens[assignee]
}

// bumpSpawnGen records that a Job was created for the assignee.
func (h *taskEventHandler) bumpSpawnGen(assignee string) {
	h.spawnGenMu.Lock()
	defer h.spawnGenMu.Unlock()
	if h.spawnGens == nil {
		h.spawnGens = make(map[string]uint64)
	}
	h.spawnGens[assignee]++
}

// activeJobsFor returns the set of task identifiers with an active Job for the
// assignee. Inside a reconcile pass (ctx carries its cache) the answer is
// memoized per assignee until any path spawns a Job for that assignee; outside
// a pass it is read live. Completions are not tracked, so a cached count can
// only be too high — which defers, never over-admits — and jobCacheTTL bounds
// how long it stays so. Callers treat the returned map as read-only.
func (h *taskEventHandler) activeJobsFor(
	ctx context.Context,
	namespace libk8s.Namespace,
	assignee string,
) (map[lib.TaskIdentifier]bool, error) {
	cache := jobCacheFrom(ctx)
	cacheKey := namespace.String() + "/" + assignee
	gen := h.spawnGenFor(assignee)
	now := time.Time(h.currentDateTime.Now())
	if cache != nil {
		cache.mu.Lock()
		cached, ok := cache.active[cacheKey]
		cache.mu.Unlock()
		if ok && cached.gen == gen && now.Sub(cached.readAt) < jobCacheTTL {
			return cached.active, nil
		}
	}

	active, err := h.jobSpawner.ListActiveJobs(ctx, namespace, assignee)
	if err != nil {
		return nil, err
	}
	if cache != nil {
		cache.mu.Lock()
		cache.active[cacheKey] = cachedActiveJobs{active: active, gen: gen, readAt: now}
		cache.mu.Unlock()
	}
	return active, nil
}
