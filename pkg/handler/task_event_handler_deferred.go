// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"
	"sort"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// deferredEntry tracks a task whose respawn was suppressed by the grace window.
// The executor re-evaluates it once retryAfter is reached.
type deferredEntry struct {
	task       lib.Task
	config     pkg.AgentConfiguration
	retryAfter time.Time
}

// deferredClaim pins the removal epoch a deferred entry was collected at. Every
// removeDeferredEntry bumps the task's epoch, so a replay can tell that its
// snapshot was evicted while it was in flight and must neither spawn nor re-park.
type deferredClaim struct {
	id    lib.TaskIdentifier
	epoch uint64
}

// deferredReplay is one due deferred entry and the claim it was collected with.
type deferredReplay struct {
	entry deferredEntry
	claim deferredClaim
}

// sortDeferredByAdmissionPriority orders the due deferred entries in place so the
// first one the loop reaches is the one that most deserves the freed slot. The
// collection step ranges a Go map, whose iteration order is randomized per tick,
// so without this sort the winner is drawn at random — the shape that lets the
// same task ids recur in the concurrency_cap log for hours while others are
// dispatched.
func sortDeferredByAdmissionPriority(replays []deferredReplay) {
	sort.SliceStable(replays, func(i, j int) bool {
		return admissionPriority(
			replays[i].entry.task,
		).Before(admissionPriority(replays[j].entry.task))
	})
}

// evalDeferredRespawns checks all pending deferred-respawn entries and spawns a job
// for each entry whose retryAfter has been reached. Entries are removed once processed.
// The respawn_after_grace_window metric and log line fire ONLY when the call actually
// results in a spawn (spec 037 AC #6: "recorded each time the follow-up evaluation
// results in a spawn"); evaluations that no-op (active job already, trigger cap hit,
// terminal phase) do not increment the metric.
//
// A failed Job create is logged, counted and skipped rather than returned: one
// rejected Job must not exit the process and starve every other due entry. The
// skipped entry is not re-parked; the reconcile floor re-drives the task.
func (h *taskEventHandler) evalDeferredRespawns(ctx context.Context) error {
	now := h.currentDateTime.Now().Time()

	h.deferredMu.Lock()
	var ready []deferredReplay
	for taskID, entry := range h.deferredRespawns {
		if !now.Before(entry.retryAfter) {
			// Capture the epoch in the same critical section that removes the
			// entry: an eviction after this point bumps it and invalidates the
			// replay below.
			ready = append(ready, deferredReplay{
				entry: entry,
				claim: deferredClaim{id: taskID, epoch: h.deferredEpochs[taskID]},
			})
			delete(h.deferredRespawns, taskID)
		}
	}
	h.deferredMu.Unlock()

	// The collection above ranges a map, so `ready` arrives in a randomized order.
	// Sort before spawning: only the first entry can win the free slot (every later
	// one re-enters deferIfAtConcurrencyCap and is re-deferred), so leaving the
	// order random is what lets the same task ids recur in the cap log indefinitely.
	sortDeferredByAdmissionPriority(ready)

	for _, replay := range ready {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		replay := replay // capture for closure
		entry := replay.entry
		outcome, err := h.spawnIfNeededClaimed(ctx, entry.task, &entry.config, &replay.claim)
		spawned := outcome.spawned()
		if err != nil {
			// A single failed Job create (e.g. a Job the API server rejects) must
			// not exit the process and starve every other due entry. The entry is
			// not re-parked; the reconcile floor re-drives the task. Every other
			// error stays fatal.
			var refusalErr *triggerRefusalWriteError
			if stderrors.As(err, &refusalErr) {
				glog.Warningf(
					"event=deferred_respawn_refusal_write_failed task=%s err=%v",
					entry.task.TaskIdentifier, err,
				)
				metrics.TaskEventsTotal.WithLabelValues("deferred_respawn_refusal_write_failed").
					Inc()
				h.clearQueuedAtStampIfNotDeferred(entry.task.TaskIdentifier)
				continue
			}
			var spawnErr *spawnJobError
			if stderrors.As(err, &spawnErr) {
				glog.Warningf(
					"event=deferred_respawn_spawn_failed task=%s err=%v",
					entry.task.TaskIdentifier, err,
				)
				metrics.TaskEventsTotal.WithLabelValues("deferred_respawn_spawn_failed").Inc()
				// The entry was removed during collection and no spawn replaced
				// it, so the queued_at record must not outlive the deferral.
				h.clearQueuedAtStampIfNotDeferred(entry.task.TaskIdentifier)
				continue
			}
			return errors.Wrapf(
				ctx, err, "deferred respawn for task %s", entry.task.TaskIdentifier,
			)
		}
		if !spawned {
			// A skip branch (foreign vault, suppressed current job, active job,
			// recently succeeded, capped redrive, trigger budget) ended the
			// deferral without a spawn. Clear only if nothing re-parked it: a
			// re-parked entry is still waiting and needs its record.
			h.clearQueuedAtStampIfNotDeferred(entry.task.TaskIdentifier)
			continue
		}
		// The task is no longer waiting behind the cap, so the queued_at record
		// has served its purpose. Dropping it keeps the map bounded by the
		// deferred working set.
		h.clearQueuedAtStampIfNotDeferred(entry.task.TaskIdentifier)
		jobStartedAt, _ := entry.task.Frontmatter.JobStartedAt()
		elapsed := now.Sub(jobStartedAt)
		glog.V(1).Infof(
			"event=respawn_after_grace_window task=%s current_job=%s elapsed=%.0fs",
			entry.task.TaskIdentifier, entry.task.Frontmatter.CurrentJob(), elapsed.Seconds(),
		)
		metrics.TaskEventsTotal.WithLabelValues("respawn_after_grace_window").Inc()
	}
	return nil
}

// removeDeferredEntry removes any pending deferred-respawn entry for the given
// task and bumps its removal epoch, in one critical section. The epoch is bumped
// even when no entry exists, and is never deleted, so an in-flight replay holding
// an older claim is invalidated rather than silently re-validated.
//
// Callers: cleanupTerminalTask, every skip branch of parseAndFilter (resolveConfig,
// type mismatch, status, terminal phase, non-trigger phase, stage, empty assignee,
// foreign cluster), and reconcileTask's foreign branch.
func (h *taskEventHandler) removeDeferredEntry(id lib.TaskIdentifier) {
	h.deferredMu.Lock()
	delete(h.deferredRespawns, id)
	delete(h.queuedAtStamps, id)
	h.deferredEpochs[id]++
	h.deferredMu.Unlock()
}

// clearQueuedAtStampIfNotDeferred drops the record for id unless the task is
// parked in deferredRespawns, whose entry still needs it.
//
// This is the correct clear for every path that ends a deferral, because a
// deferral can end in two ways: the task is re-parked (it is still waiting, so
// the record must survive) or it is not (a skip branch, a failed spawn, an
// evicted claim — the record must go). Clearing unconditionally would drop a
// stamp a re-parked entry still depends on; not clearing at all leaves one
// string behind per task that ever left the deferred state this way, and lets a
// later, unrelated wait reuse a stale value — which admissionPriority then sorts
// on as though the task had waited since then.
func (h *taskEventHandler) clearQueuedAtStampIfNotDeferred(id lib.TaskIdentifier) {
	h.deferredMu.Lock()
	defer h.deferredMu.Unlock()
	if _, parked := h.deferredRespawns[id]; !parked {
		delete(h.queuedAtStamps, id)
	}
}

// taskWithFrontmatterValue returns a copy of the task whose frontmatter carries
// one extra key. The frontmatter map is copied rather than mutated in place: the
// caller's map is shared with the event and may still be held by a deferred
// entry, so an in-place write would corrupt that entry.
func taskWithFrontmatterValue(task lib.Task, key string, value interface{}) lib.Task {
	copied := task
	copied.Frontmatter = make(lib.TaskFrontmatter, len(task.Frontmatter)+1)
	for k, v := range task.Frontmatter {
		copied.Frontmatter[k] = v
	}
	copied.Frontmatter[key] = value
	return copied
}

// claimInvalidatedLocked reports whether claim was superseded by a removal. The
// caller must hold deferredMu.
func (h *taskEventHandler) claimInvalidatedLocked(claim *deferredClaim) bool {
	if claim == nil {
		return false
	}
	return h.deferredEpochs[claim.id] != claim.epoch
}

// claimStillValid reports whether a claimed replay may still spawn. It takes
// deferredMu only for the check and releases it explicitly before returning, so
// the caller never holds it across SpawnJob.
func (h *taskEventHandler) claimStillValid(claim *deferredClaim) bool {
	if claim == nil {
		return true
	}
	h.deferredMu.Lock()
	valid := !h.claimInvalidatedLocked(claim)
	h.deferredMu.Unlock()
	return valid
}

// recordClaimInvalidated counts and logs a replay whose entry was evicted before
// it could spawn or re-park.
func (h *taskEventHandler) recordClaimInvalidated(task lib.Task) {
	glog.V(1).Infof("event=deferred_claim_invalidated task=%s", task.TaskIdentifier)
	metrics.TaskEventsTotal.WithLabelValues("deferred_claim_invalidated").Inc()
}

// storeGraceWindowDeferral parks a task whose grace window has not elapsed. A
// claimed replay is effectively never here — its retryAfter is already past the
// grace window — but the guard keeps an evicted claim from re-parking the stale
// snapshot regardless.
func (h *taskEventHandler) storeGraceWindowDeferral(
	task lib.Task,
	config *pkg.AgentConfiguration,
	retryAfter time.Time,
	claim *deferredClaim,
) {
	if config == nil {
		return
	}
	h.deferredMu.Lock()
	if !h.claimInvalidatedLocked(claim) {
		h.deferredRespawns[task.TaskIdentifier] = deferredEntry{
			task:       task,
			config:     *config,
			retryAfter: retryAfter,
		}
	}
	h.deferredMu.Unlock()
}

// EvalDeferredRespawns implements TaskEventHandler.
func (h *taskEventHandler) EvalDeferredRespawns(ctx context.Context) error {
	return h.evalDeferredRespawns(ctx)
}

// seedDeferredRespawnsFromStore scans the in-memory taskStore for tasks that look
// like in-flight work (current_job set, phase non-terminal) and adds them to
// deferredRespawns with retryAfter = job_started_at + defaultRespawnGracePeriod.
// Called once from RunDeferredRespawnLoop on startup. Idempotent: any entry already
// present in deferredRespawns is left untouched. This restores deferred state lost
// when the in-memory map is wiped by an executor restart, so a task deferred
// behind the concurrency cap does not remain stuck for want of a Kafka event that
// will never arrive.
//
// The agent configuration is resolved here at seed time (not left zero-valued):
// a seeded entry is later evaluated by evalDeferredRespawns → spawnIfNeeded, which
// spawns with the entry's config — a zero-valued config would spawn a Job with an
// empty image. Tasks whose assignee config cannot be resolved are skipped; they
// are re-evaluated when a genuine Kafka event for the task arrives.
func (h *taskEventHandler) seedDeferredRespawnsFromStore(ctx context.Context) {
	snapshot := h.taskStore.Snapshot()

	h.deferredMu.Lock()
	defer h.deferredMu.Unlock()
	for taskID, task := range snapshot {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if _, exists := h.deferredRespawns[taskID]; exists {
			continue
		}
		currentJob := task.Frontmatter.CurrentJob()
		if currentJob == "" {
			continue
		}
		phase := task.Frontmatter.Phase()
		if phase != nil && IsTerminal(*phase) {
			continue
		}
		jobStartedAt, jobStartedErr := task.Frontmatter.JobStartedAt()
		if jobStartedErr != nil {
			continue
		}
		config, resolveErr := h.resolver.Resolve(ctx, task.Frontmatter.Assignee().String())
		if resolveErr != nil {
			glog.V(2).Infof(
				"seed deferred respawn: skip task %s (config unresolvable for assignee %s): %v",
				taskID, task.Frontmatter.Assignee(), resolveErr,
			)
			continue
		}
		h.deferredRespawns[taskID] = deferredEntry{
			task:       task,
			config:     config,
			retryAfter: jobStartedAt.Add(defaultRespawnGracePeriod),
		}
	}
}

// RunDeferredRespawnLoop implements TaskEventHandler.
func (h *taskEventHandler) RunDeferredRespawnLoop(ctx context.Context) error {
	// Startup reconciliation: recover deferred entries lost across an executor
	// restart by scanning the in-memory taskStore. See seedDeferredRespawnsFromStore
	// for the restart-safety rationale (spec 037 AC #5).
	h.seedDeferredRespawnsFromStore(ctx)

	// Fire one eval immediately after seeding so that tasks whose grace has
	// already elapsed at startup are picked up without waiting for the first tick.
	if err := h.evalDeferredRespawns(ctx); err != nil {
		return errors.Wrapf(ctx, err, "deferred respawn loop initial eval")
	}

	ticker := time.NewTicker(deferredRespawnInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := h.evalDeferredRespawns(ctx); err != nil {
				return errors.Wrapf(ctx, err, "deferred respawn loop tick")
			}
		}
	}
}
