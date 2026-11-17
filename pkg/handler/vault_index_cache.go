// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"sync"

	"github.com/bborbe/vault-cli/pkg/domain"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// vaultIndexCacheKey is the context key carrying a reconcile pass's vault-index
// read.
type vaultIndexCacheKey struct{}

// passVaultIndexCache memoizes one vault-api index read per reconcile pass and
// the terminal-id set derived from it. The read is shared between the candidate
// gate and the divergence check, so a pass pays for at most one request.
type passVaultIndexCache struct {
	once     sync.Once
	index    *claim.TaskIndex
	err      error
	terminal map[string]struct{}
}

// withVaultIndexCache returns a context carrying a fresh per-pass vault-index
// cache. The cache travels on the pass's context, never on the handler, so only
// the pass's own lookups see it.
func withVaultIndexCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, vaultIndexCacheKey{}, &passVaultIndexCache{})
}

// vaultIndexCacheFrom returns the pass cache carried by ctx, or nil outside a
// pass.
func vaultIndexCacheFrom(ctx context.Context) *passVaultIndexCache {
	cache, _ := ctx.Value(vaultIndexCacheKey{}).(*passVaultIndexCache)
	return cache
}

// vaultIndexFor returns the vault-api task index for the pass and the set of
// task ids whose index state is terminal.
//
// A nil client makes no call and returns (nil, nil, nil). With a cache on ctx
// the read happens exactly once and both consumers share it; without a cache it
// reads directly, which keeps direct-call unit tests working.
func (h *taskEventHandler) vaultIndexFor(
	ctx context.Context,
) (*claim.TaskIndex, map[string]struct{}, error) {
	if h.vaultIndexClient == nil {
		return nil, nil, nil
	}
	cache := vaultIndexCacheFrom(ctx)
	if cache == nil {
		index, err := h.vaultIndexClient.Read(ctx)
		if err != nil || index == nil {
			return index, nil, err
		}
		return index, indexTerminalIDs(index), nil
	}
	cache.once.Do(func() {
		cache.index, cache.err = h.vaultIndexClient.Read(ctx)
		if cache.err == nil && cache.index != nil {
			cache.terminal = indexTerminalIDs(cache.index)
		}
	})
	return cache.index, cache.terminal, cache.err
}

// indexTerminalIDs builds the set of ids whose index state is terminal. A task
// is terminal when its status or its phase is in the fixed terminal sets.
//
// Items are visited in order so a duplicate id keeps the last item's verdict,
// matching compareViewToIndex's tie-break: a later non-terminal item deletes the
// id again.
func indexTerminalIDs(index *claim.TaskIndex) map[string]struct{} {
	terminal := make(map[string]struct{})
	if index == nil {
		return terminal
	}
	for _, item := range index.Items {
		if IsTerminalStatus(domain.TaskStatus(item.Status)) ||
			IsTerminal(domain.TaskPhase(item.Phase)) {
			terminal[item.ID] = struct{}{}
			continue
		}
		delete(terminal, item.ID)
	}
	return terminal
}
