// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"
	"net/http"
	"sync"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// claimAbsentGrace is how long a record must keep answering a non-retryable
// claim 404 before it is evicted from the view.
//
// One 404 is not proof that the task is gone. vault-api resolves a task claim
// against its task index, and that index lags a fresh write by roughly 50 s — a
// task created through POST /api/v1/tasks answers 404 on every read for that
// window. A record that exists can therefore answer 404 for as long as the
// index is behind, and evicting on the first 404 would drop a live record. No
// later event is guaranteed for it either: publication is edge-triggered on
// vault file changes, so an evicted live task could sit unspawned forever.
//
// Five minutes is six times that window and spans five reconcile passes at the
// default 60 s interval. A genuinely absent record is evicted only after the
// ambiguity has expired; a live one has four more passes to become claimable.
const claimAbsentGrace = 5 * time.Minute

// claimAbsentIndexMaxAge is the oldest vault index whose silence still counts
// as corroboration. A 404 says the claim route could not resolve the task; the
// index says the vault does not list it. Both can be wrong during one outage,
// and a stale index omits every task. Past this age the absence is not evidence
// and the record is left in the view to be retried.
const claimAbsentIndexMaxAge = 10 * time.Minute

// claimAbsentStreak is one record's run of consecutive non-retryable 404s.
// passes counts the observations, not wall-clock ticks, so a pass that is
// skipped or delayed shows up as a smaller count rather than a wrong age.
type claimAbsentStreak struct {
	firstSeen time.Time
	passes    int
}

// claimAbsentTracker counts consecutive non-retryable claim 404s per task id.
//
// The tracker is the ambiguity filter the claim gate was missing: a permanent
// "this task does not exist" and a momentary broker outage are indistinguishable
// to a gate that fails closed on every error, so the absent record is retried
// once a minute forever. The streak turns a repeated 404 into a terminal
// decision without letting a single 404 — which the index lag makes ambiguous —
// end a live record.
//
// Any answer that is not a non-retryable 404 clears the entry, so the count is
// consecutive by construction. The map is bounded by the number of distinct
// records currently answering 404, which is the population this feature exists
// to shrink.
type claimAbsentTracker struct {
	mu      sync.Mutex
	streaks map[lib.TaskIdentifier]claimAbsentStreak
}

// newClaimAbsentTracker returns an empty tracker.
func newClaimAbsentTracker() *claimAbsentTracker {
	return &claimAbsentTracker{streaks: make(map[lib.TaskIdentifier]claimAbsentStreak)}
}

// observe records one 404 for id and reports whether the grace has elapsed,
// together with the streak as recorded. The caller must not mutate the streak.
func (t *claimAbsentTracker) observe(
	id lib.TaskIdentifier,
	now time.Time,
) (bool, claimAbsentStreak) {
	t.mu.Lock()
	defer t.mu.Unlock()
	streak, ok := t.streaks[id]
	if !ok {
		streak = claimAbsentStreak{firstSeen: now}
	}
	streak.passes++
	t.streaks[id] = streak
	return now.Sub(streak.firstSeen) >= claimAbsentGrace, streak
}

// clear forgets the streak for id. It is called for every outcome that is not a
// non-retryable 404, and after an eviction so a record that reappears starts a
// fresh streak rather than being evicted on its first observation.
func (t *claimAbsentTracker) clear(id lib.TaskIdentifier) {
	t.mu.Lock()
	delete(t.streaks, id)
	t.mu.Unlock()
}

// claimNotFound reports whether err is the vault answering 404 to a claim.
//
// Only a 404 means "the vault does not know this task". Other 4xx answers are
// non-retryable too, but they describe a malformed request or a refused
// credential — evicting the record would hide a defect instead of clearing a
// ghost. A 5xx stays retryable and never reaches the tracker.
func claimNotFound(err error) bool {
	var statusErr *claim.StatusError
	if !stderrors.As(err, &statusErr) {
		return false
	}
	return statusErr.Status == http.StatusNotFound && !statusErr.Retryable()
}

// noteClaimSuccess clears any absent streak for a task whose claim was taken,
// so a record that 404s once and then succeeds is not evicted later.
func (h *taskEventHandler) noteClaimSuccess(id lib.TaskIdentifier) {
	if h.claimAbsent == nil {
		return
	}
	h.claimAbsent.clear(id)
}

// indexConfirmsAbsent reports whether a healthy vault index lists no such task.
//
// The 404 and the index answer the same question from two places, and during a
// vault-api outage both can be wrong the same way: an index that failed to
// rebuild omits every task, so a live record would answer 404 and be absent from
// the listing. Requiring both — and refusing a stale index — keeps one outage
// from evicting the whole view.
func (h *taskEventHandler) indexConfirmsAbsent(
	ctx context.Context,
	id lib.TaskIdentifier,
) bool {
	index, _, err := h.vaultIndexFor(ctx)
	if err != nil || index == nil {
		return false
	}
	if index.IndexAgeSeconds > int(claimAbsentIndexMaxAge.Seconds()) {
		return false
	}
	for _, item := range index.Items {
		if item.ID == string(id) {
			return false
		}
	}
	return true
}

// recordClaimFailure decides what a failed claim means for the record's
// lifetime. A non-retryable 404 records a streak and, once the grace has
// elapsed, evicts the record from the view — the only removal a deleted task
// can get, because Kafka compaction never publishes its tombstone. Every other
// failure clears the streak and leaves the record to be retried.
//
// Eviction is confined to claim mode on. In shadow the spawn proceeds without a
// claim, so the record is not looping: removing it would suppress a task the
// mode's contract says must run.
func (h *taskEventHandler) recordClaimFailure(
	ctx context.Context,
	task lib.Task,
	err error,
) {
	if h.claimAbsent == nil {
		return
	}
	if !claimNotFound(err) {
		h.claimAbsent.clear(task.TaskIdentifier)
		return
	}
	if h.claimMode != pkg.ClaimModeOn || h.view == nil {
		return
	}
	now := h.currentDateTime.Now().Time()
	evict, streak := h.claimAbsent.observe(task.TaskIdentifier, now)
	age := now.Sub(streak.firstSeen)
	if !evict {
		glog.V(1).Infof(
			"event=claim_absent task=%s passes=%d age=%s",
			task.TaskIdentifier, streak.passes, age,
		)
		return
	}
	if !h.indexConfirmsAbsent(ctx, task.TaskIdentifier) {
		glog.V(1).Infof(
			"event=claim_absent_unconfirmed task=%s passes=%d age=%s",
			task.TaskIdentifier, streak.passes, age,
		)
		return
	}
	h.view.Evict(task.TaskIdentifier)
	h.claimAbsent.clear(task.TaskIdentifier)
	metrics.ClaimAbsentEvictedTotal.Inc()
	glog.Warningf(
		"event=claim_absent_evicted task=%s passes=%d age=%s status=404",
		task.TaskIdentifier, streak.passes, age,
	)
}
