// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"sort"
	"sync"
	"time"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// viewNotReadyStuckAfter is how long the task view may stay not ready before the
// gate stops reporting a cold start and reports a fault.
//
// A cold start on the measured dev vault replays 37,720 compacted records in
// 4m41s-9m17s (v0.65.2, 2026-11-17/18), so the bound sits clear of the slowest
// observed cold start. Past it the once-a-minute WARN line is the only signal
// that an entire vault is undriven, and it reads exactly like a healthy quiet
// fleet — the shape that let the 2026-11-14 records sit untouched for hours.
const viewNotReadyStuckAfter = 15 * time.Minute

// viewNotReadyPendingCap bounds the events remembered across a not-ready window.
// The reconcile floor is the real backstop for everything the event path drops;
// this set only spares the exact dropped events a wait for the next pass. A
// window that exceeds the cap is already a stuck view, which the stuck event
// reports.
const viewNotReadyPendingCap = 512

// viewNotReadyEntry is one event the readiness gate dropped, kept in the exact
// form the event path would have spawned it: the task as parseAndFilter left it
// and the Config it resolved to.
//
// The pair rather than the id alone is deliberate. parseAndFilter already applied
// every filter — terminal phase, status, foreign cluster, unresolvable assignee —
// so replaying the pair reuses spawnIfNeeded and cannot re-derive those filters
// against a view that has since replayed a different snapshot. Replaying the id
// through a fresh lookup would read whatever the view holds now, which is not the
// event that was refused.
type viewNotReadyEntry struct {
	task   lib.Task
	config *pkg.AgentConfiguration
}

// viewNotReadyState is the readiness gate's bookkeeping. It is a struct rather
// than three handler fields so the handler stays inside its file-length limit and
// the state travels with the helpers that read it.
type viewNotReadyState struct {
	mu sync.Mutex
	// pending holds the events the readiness gate dropped, in the post-filter
	// (task, config) form the event path would have spawned. Replaying them is not
	// acting on a stale snapshot — it is acting on the very event that was
	// refused, which is the freshest input the executor has.
	pending map[lib.TaskIdentifier]viewNotReadyEntry
	// since is when the gate first refused; the zero value means the view is
	// ready. It is what separates a cold start from a view that will never be
	// ready.
	since libtime.DateTime
}

// rememberViewNotReady records an event dropped by the readiness gate so a ready
// view drives it without waiting for the next reconcile pass.
//
// The reconcile floor reaches these events too — the replay carries them — but
// only once the replay finishes, and a view that never finishes has no floor at
// all. That is the hole this closes: the gate's own log line promised the floor
// would drive the task, while the floor refused every pass on the same condition.
//
// Bounded by viewNotReadyPendingCap. Past the cap the entry is dropped and the
// floor is the only driver, which is exactly the pre-existing behaviour: the cap
// is a memory guard, and the view is already the process's largest allocation, so
// this set must not become a second unbounded copy of the task bodies.
func (h *taskEventHandler) rememberViewNotReady(task lib.Task, config *pkg.AgentConfiguration) {
	h.viewNotReady.mu.Lock()
	defer h.viewNotReady.mu.Unlock()

	h.markViewNotReadyStartLocked()

	if h.viewNotReady.pending == nil {
		h.viewNotReady.pending = make(
			map[lib.TaskIdentifier]viewNotReadyEntry,
			viewNotReadyPendingCap,
		)
	}
	if _, known := h.viewNotReady.pending[task.TaskIdentifier]; !known &&
		len(h.viewNotReady.pending) >= viewNotReadyPendingCap {
		glog.Warningf(
			"event=task_view_not_ready_pending_full task=%s cap=%d: the reconcile floor will drive it",
			task.TaskIdentifier,
			viewNotReadyPendingCap,
		)
		return
	}
	h.viewNotReady.pending[task.TaskIdentifier] = viewNotReadyEntry{task: task, config: config}
}

// markViewNotReadyStartLocked stamps the first refusal of the current not-ready
// window. The caller must hold viewNotReady.mu.
func (h *taskEventHandler) markViewNotReadyStartLocked() {
	if h.viewNotReady.since.Time().IsZero() {
		h.viewNotReady.since = h.currentDateTime.Now()
	}
}

// viewNotReadyElapsed returns how long the view has been not ready, and stamps
// the start if this is the first observation.
//
// The stamp is taken here as well as in rememberViewNotReady because the two
// drivers see different firsts: a quiet vault can have its reconcile loop refuse
// many passes before any task event arrives, and measuring from an unstamped
// zero value would report a duration since the epoch.
func (h *taskEventHandler) viewNotReadyElapsed() time.Duration {
	h.viewNotReady.mu.Lock()
	defer h.viewNotReady.mu.Unlock()

	h.markViewNotReadyStartLocked()
	return h.currentDateTime.Now().Time().Sub(h.viewNotReady.since.Time())
}

// viewNotReadyPendingLen reports how many dropped events are held.
func (h *taskEventHandler) viewNotReadyPendingLen() int {
	h.viewNotReady.mu.Lock()
	defer h.viewNotReady.mu.Unlock()
	return len(h.viewNotReady.pending)
}

// takeViewNotReadyPending drains the held events and closes the not-ready window.
// Returned in task-id order so a pass drives a deterministic sequence.
func (h *taskEventHandler) takeViewNotReadyPending() []viewNotReadyEntry {
	h.viewNotReady.mu.Lock()
	defer h.viewNotReady.mu.Unlock()

	entries := make([]viewNotReadyEntry, 0, len(h.viewNotReady.pending))
	for _, entry := range h.viewNotReady.pending {
		entries = append(entries, entry)
	}
	h.viewNotReady.pending = nil
	h.viewNotReady.since = libtime.DateTime{}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].task.TaskIdentifier < entries[j].task.TaskIdentifier
	})
	return entries
}

// noteViewNotReady records a reconcile pass refused by the readiness gate.
//
// Past viewNotReadyStuckAfter the line changes from a cold-start WARN to a
// distinct stuck event at Error level. Without the split the only symptom of a
// view that will never replay is one WARN a minute, which reads exactly like a
// healthy quiet fleet — the shape that let the 2026-11-14 records sit untouched
// for hours while every pass logged the same line.
func (h *taskEventHandler) noteViewNotReady() {
	elapsed := h.viewNotReadyElapsed()
	pending := h.viewNotReadyPendingLen()
	if elapsed < viewNotReadyStuckAfter {
		metrics.ReconcilePassesTotal.WithLabelValues("view_not_ready").Inc()
		glog.Warningf(
			"event=reconcile_view_not_ready task_vault=%s elapsed=%s pending=%d",
			h.vaultMatcher.Vault(),
			elapsed,
			pending,
		)
		return
	}
	metrics.ReconcilePassesTotal.WithLabelValues("view_not_ready_stuck").Inc()
	glog.Errorf(
		"event=task_view_not_ready_stuck task_vault=%s elapsed=%s pending=%d threshold=%s: the task view has not replayed, so neither the event path nor this floor is driving any task in this vault",
		h.vaultMatcher.Vault(),
		elapsed,
		pending,
		viewNotReadyStuckAfter,
	)
}

// driveViewNotReadyPending spawns every event the readiness gate dropped, once
// the view is ready. Each spawn goes through spawnIfNeeded, the same call the
// event path would have made, so every guard — foreign vault, claim, cap,
// trigger cap, duplicate Job — still applies.
//
// A failure is logged and skipped rather than returned: the reconcile floor
// retries the task on the next pass, and one bad entry must not tear down the
// consumer, which is the same reason ConsumeMessage swallows a refusal-write
// error.
func (h *taskEventHandler) driveViewNotReadyPending(ctx context.Context) {
	entries := h.takeViewNotReadyPending()
	if len(entries) == 0 {
		return
	}
	spawned := 0
	for _, entry := range entries {
		if h.passDone(ctx) {
			glog.Warningf(
				"event=task_view_not_ready_drain_aborted pending=%d driven=%d: pass deadline reached, the reconcile floor drives the rest",
				len(entries),
				spawned,
			)
			return
		}
		ok, err := h.spawnIfNeeded(ctx, entry.task, entry.config)
		if err != nil {
			glog.Warningf(
				"event=task_view_not_ready_drain_failed task=%s err=%v: the reconcile floor retries",
				entry.task.TaskIdentifier,
				err,
			)
			continue
		}
		if ok {
			spawned++
		}
	}
	glog.Infof(
		"event=task_view_not_ready_drained pending=%d spawned=%d",
		len(entries),
		spawned,
	)
}
