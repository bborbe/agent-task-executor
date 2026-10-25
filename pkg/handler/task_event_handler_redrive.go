// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// redriveStreak is the executor's record of consecutive spawns for one task at
// one phase: how many, and which phase they accrued in. See maxConsecutiveRedrives
// for why the phase is the reset and why the task file is not used for it — the
// agent pods rewrite the frontmatter from a pre-spawn snapshot and would clobber
// any counter kept there.
type redriveStreak struct {
	count int
	phase string
}

// redriveStreakStore returns this process's streak store, or nil when claim
// mode takes no claim. It triggers the one-time lease-manager construction, so
// the store and the manager share one claim-ref client. An unparsed mode errors
// through leaseManager, which makes the caller fall back to the in-memory floor.
func (h *taskEventHandler) redriveStreakStore(
	ctx context.Context,
) (claim.RedriveStreakStore, error) {
	if h.claimMode == pkg.ClaimModeOff {
		return nil, nil
	}
	if _, err := h.leaseManager(ctx); err != nil {
		return nil, err
	}
	return h.redriveStore, nil
}

// inMemoryRedriveCount returns the streak this process has recorded for id at
// phase, or 0 when the recorded streak belongs to another phase.
func (h *taskEventHandler) inMemoryRedriveCount(id lib.TaskIdentifier, phase string) int {
	h.redriveMu.Lock()
	defer h.redriveMu.Unlock()
	streak, ok := h.redriveStreaks[id]
	if !ok || streak.phase != phase {
		return 0
	}
	return streak.count
}

// signalRedriveCapped is the only place that increments
// executor_redrive_capped_total or logs event=redrive_capped.
//
// Logged at V(1), not V(0): a capped task repeats this line on every event and
// every pass for as long as it stays capped, which is an expected steady state —
// the same reason event=concurrency_cap is V(1). The metric carries the alerting
// signal.
func (h *taskEventHandler) signalRedriveCapped(
	id lib.TaskIdentifier,
	taskType string,
	streak int,
	phase string,
) {
	metrics.RedriveCappedTotal.WithLabelValues(taskType, phase).Inc()
	glog.V(1).Infof(
		"event=redrive_capped task=%s task_type=%s streak=%d cap=%d phase=%q",
		id, taskType, streak, maxConsecutiveRedrives, phase,
	)
}

// redriveStreakCappedInMemory is the cheap pre-check the reconcile pass uses
// for its summary. It never reads the store; the authoritative check runs in
// the spawn funnel.
func (h *taskEventHandler) redriveStreakCappedInMemory(task lib.Task) bool {
	id := task.TaskIdentifier
	phase := taskPhaseString(task)
	count := h.inMemoryRedriveCount(id, phase)
	if count < maxConsecutiveRedrives {
		return false
	}
	h.signalRedriveCapped(id, task.Frontmatter.TaskType().String(), count, phase)
	return true
}

// redriveStreakCapped reports whether the spawn must be refused because the
// task has been spawned maxConsecutiveRedrives times at this phase. Memory is
// checked first; when it already reaches the cap the store is not read. A
// failed store read logs event=redrive_streak_read_failed and uses memory
// alone. The refusal keeps today's metric and log line, with streak showing
// the larger count actually used.
//
// Skipping is the whole bound. The streak is deliberately NOT cleared here, so
// the task stays capped until something moves it — clearing on the cap would
// let the next spawn through and make the cap a no-op.
//
// The phase comparison is not redundant with recordRedriveSpawn's reset. The
// streak is only ever rewritten AFTER a spawn succeeds, so a task that advanced
// to a new phase still carries its old phase in the streak until that spawn
// happens — and a count-only check would cap it before it ever could, stranding
// exactly the task the reset exists to release.
func (h *taskEventHandler) redriveStreakCapped(ctx context.Context, task lib.Task) bool {
	id := task.TaskIdentifier
	phase := taskPhaseString(task)
	count := h.inMemoryRedriveCount(id, phase)
	if count >= maxConsecutiveRedrives {
		h.signalRedriveCapped(id, task.Frontmatter.TaskType().String(), count, phase)
		return true
	}
	store, err := h.redriveStreakStore(ctx)
	if err != nil {
		glog.Warningf(
			"event=redrive_streak_read_failed task=%s phase=%q error=%v",
			id, phase, err,
		)
	} else if store != nil {
		stored, storeErr := h.redriveStreakCount(ctx, store, string(id), phase)
		switch {
		case storeErr != nil:
			glog.Warningf(
				"event=redrive_streak_read_failed task=%s phase=%q error=%v",
				id, phase, storeErr,
			)
		case stored > count:
			count = stored
		}
	}
	if count < maxConsecutiveRedrives {
		return false
	}
	h.signalRedriveCapped(id, task.Frontmatter.TaskType().String(), count, phase)
	return true
}

// redriveStreakCount answers one task's stored streak count. Inside a reconcile
// pass the count comes from the pass's single prefix snapshot; everywhere else
// it is read live for the task, so the Kafka consumer and the deferred-respawn
// loop never read a count this pass froze before a spawn recorded a new one.
func (h *taskEventHandler) redriveStreakCount(
	ctx context.Context,
	store claim.RedriveStreakStore,
	taskID string,
	phase string,
) (int, error) {
	if cache := redriveStreakCacheFrom(ctx); cache != nil {
		return cache.count(ctx, store, taskID, phase)
	}
	return store.Count(ctx, taskID, phase)
}

// taskPhaseString returns the task's phase as a string, or "" when it carries
// none. Shared by the cap check and the streak record so the two can never
// disagree about which phase a streak belongs to.
func taskPhaseString(task lib.Task) string {
	if p := task.Frontmatter.Phase(); p != nil {
		return string(*p)
	}
	return ""
}

// recordRedriveSpawn extends this task's in-memory streak with the spawn that
// just happened, resetting it when the task has moved phase, then records the
// spawn in the shared store. The in-memory update happens first and always: a
// store failure must not roll back the Job or stop the floor from counting.
//
// Called only after SpawnJob returned successfully, so a suppressed, deferred
// or capped spawn does not count — the streak measures spawns, not attempts.
// Record's network calls run under the per-assignee spawn lock, bounded by the
// 30 s per-request cap.
func (h *taskEventHandler) recordRedriveSpawn(ctx context.Context, task lib.Task) {
	id := task.TaskIdentifier
	phase := taskPhaseString(task)
	h.redriveMu.Lock()
	if prev, ok := h.redriveStreaks[id]; ok && prev.phase == phase {
		h.redriveStreaks[id] = redriveStreak{count: prev.count + 1, phase: phase}
	} else {
		h.redriveStreaks[id] = redriveStreak{count: 1, phase: phase}
	}
	h.redriveMu.Unlock()

	store, err := h.redriveStreakStore(ctx)
	if err != nil {
		glog.Warningf(
			"event=redrive_streak_write_failed task=%s phase=%q error=%v",
			id, phase, err,
		)
		return
	}
	if store == nil {
		return
	}
	if recordErr := store.Record(ctx, string(id), phase); recordErr != nil {
		glog.Warningf(
			"event=redrive_streak_write_failed task=%s phase=%q error=%v",
			id, phase, recordErr,
		)
	}
}

// clearRedriveStreak ends this task's streak in memory and in the shared store.
// Called when the task reaches a terminal state, where a further spawn is an
// operator decision rather than a re-drive of the loop this cap bounds. A store
// failure is logged and never blocks the terminal handling.
func (h *taskEventHandler) clearRedriveStreak(ctx context.Context, id lib.TaskIdentifier) {
	h.redriveMu.Lock()
	delete(h.redriveStreaks, id)
	h.redriveMu.Unlock()

	store, err := h.redriveStreakStore(ctx)
	if err != nil {
		glog.Warningf("event=redrive_streak_write_failed task=%s error=%v", id, err)
		return
	}
	if store == nil {
		return
	}
	if clearErr := store.Clear(ctx, string(id)); clearErr != nil {
		glog.Warningf("event=redrive_streak_write_failed task=%s error=%v", id, clearErr)
	}
}
