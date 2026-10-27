// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"strings"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// transientRedriveBudget is how many extra spawns a transient failure earns
// beyond maxConsecutiveRedrives. It is a bounded extension, not an exemption:
// reaching maxConsecutiveRedrives + transientRedriveBudget closes the record
// rather than extending it again.
//
// Declared here rather than beside maxConsecutiveRedrives in
// task_event_handler.go, which is already at the revive file-length limit.
const transientRedriveBudget = 3

// The classifier's three classes. They are also the values logged as class=.
const (
	redriveClassUnknown   = "unknown"
	redriveClassBroken    = "broken"
	redriveClassTransient = "transient"
)

// brokenRedriveReasons are the failure signatures that say retrying the same
// work at the same phase will not help: the pod produced no output and crashed,
// was OOM-killed, or the task type does not match the agent.
//
// type_mismatch is in the taxonomy's broken set but cannot in practice reach the
// store: a type-mismatched task is filtered before the spawn and its failure is
// published by PublishTypeMismatchFailure, not PublishFailure, so its streak
// never grows. The classifier still handles it so the broken set is complete
// against the taxonomy.
var brokenRedriveReasons = map[string]struct{}{
	pkg.ZombieReasonPodCrashNoStdout.String(): {},
	pkg.ZombieReasonPodOOMKilled.String():     {},
	pkg.ZombieReasonTypeMismatch.String():     {},
}

// transientRedriveReasons are failures that may well succeed on a retry: image
// pulls back off, deadlines, evictions, scheduling and a lost watch.
//
// The classifier does not consult this set. Everything that is not the cap's
// worth of identical broken signatures is transient, so membership here
// documents the taxonomy rather than driving a branch — a value outside it is
// transient too. TestTransientVocabularyIsTransient keeps the set honest by
// asserting no entry is classified broken.
var transientRedriveReasons = map[string]struct{}{
	pkg.ZombieReasonImagePullBackOff.String():  {},
	pkg.ZombieReasonDeadlineExceeded.String():  {},
	pkg.ZombieReasonPodEvicted.String():        {},
	pkg.ZombieReasonPodNotScheduled.String():   {},
	pkg.ZombieReasonExecutorWatchLost.String(): {},
}

// redriveStreak is the executor's record of consecutive spawns for one task at
// one phase: how many, and which phase they accrued in. See maxConsecutiveRedrives
// for why the phase is the reset and why the task file is not used for it — the
// agent pods rewrite the frontmatter from a pre-spawn snapshot and would clobber
// any counter kept there.
type redriveStreak struct {
	count int
	phase string
	// since is when this streak last advanced. The cap check takes
	// max(memory, stored), so a permanent in-memory 3 would keep a task capped
	// after the store's window had released it — the floor has to expire on the
	// same window or the release path this change adds is defeated by the
	// process's own memory.
	since libtime.DateTime
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

// redriveReasonStore returns this process's reason store, or nil when claim
// mode takes no claim. It mirrors redriveStreakStore exactly, including the
// lease-manager warm-up that builds both stores.
func (h *taskEventHandler) redriveReasonStore(
	ctx context.Context,
) (claim.RedriveReasonStore, error) {
	if h.claimMode == pkg.ClaimModeOff {
		return nil, nil
	}
	if _, err := h.leaseManager(ctx); err != nil {
		return nil, err
	}
	return h.redriveReasons, nil
}

// redriveStreakExpired reports whether an in-memory streak has outlived the
// window the shared store applies to its refs, so the two release together.
//
// The window runs from the streak's last advance, which is the conservative
// reading: the store ages each ref from its own write, so it starts counting
// down as soon as the first ref is written. Anchoring memory to the last write
// can therefore hold the floor up to one window longer than the store would,
// and never shorter — max(memory, stored) then delays a release slightly but
// cannot suppress one, and cannot weaken the cap either.
func redriveStreakExpired(streak redriveStreak, now libtime.DateTime) bool {
	if streak.since.IsZero() {
		return false
	}
	return !now.Time().Before(streak.since.Add(claim.RedriveStreakLeaseDuration).Time())
}

// inMemoryRedriveCount returns the streak this process has recorded for id at
// phase, or 0 when the recorded streak belongs to another phase or has outlived
// the release window.
func (h *taskEventHandler) inMemoryRedriveCount(id lib.TaskIdentifier, phase string) int {
	h.redriveMu.Lock()
	defer h.redriveMu.Unlock()
	streak, ok := h.redriveStreaks[id]
	if !ok || streak.phase != phase {
		return 0
	}
	if redriveStreakExpired(streak, h.currentDateTime.Now()) {
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
	class string,
	reasons string,
) {
	metrics.RedriveCappedTotal.WithLabelValues(taskType, phase).Inc()
	glog.V(1).Infof(
		"event=redrive_capped task=%s task_type=%s streak=%d cap=%d phase=%q class=%s reasons=%s",
		id, taskType, streak, maxConsecutiveRedrives, phase, class, reasons,
	)
}

// redriveStreakCapped reports whether the spawn must be refused because the
// task has been spawned maxConsecutiveRedrives times at this phase, and, when it
// is, decides what to do about it: close a broken record, extend a transient
// one, or refuse without closing when there is no evidence.
//
// Memory is checked first, but reaching the cap in memory no longer short-
// circuits: the classification needs the stored reasons, and a task this
// process itself counted to the cap is the ordinary case, not a reason to skip
// the read. Below the cap nothing is read at all.
//
// A failed store read logs event=redrive_streak_read_failed and uses memory
// alone. A failed reason read logs event=redrive_reason_read_failed and is
// treated as no evidence, which refuses but closes nothing.
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

	reasons, reasonsErr := h.redriveReasonsFor(ctx, id, phase)
	if reasonsErr != nil {
		glog.Warningf(
			"event=redrive_reason_read_failed task=%s phase=%q error=%v",
			id, phase, reasonsErr,
		)
	}
	class := classifyRedriveReasons(reasons, count)
	if class == redriveClassTransient && count < maxConsecutiveRedrives+transientRedriveBudget {
		// The bounded extension: a transient failure earns more attempts, and
		// the spawn is allowed. Nothing was refused, so nothing is signalled.
		return false
	}

	h.signalRedriveCapped(
		id,
		task.Frontmatter.TaskType().String(),
		count,
		phase,
		class,
		joinRedriveReasons(reasons),
	)
	if class != redriveClassUnknown {
		h.abortRedrive(ctx, task, abortReasonForClass(class), count)
	}
	return true
}

// abortReasonForClass maps a closeable class to its abort reason. unknown never
// reaches here: it refuses without closing.
func abortReasonForClass(class string) string {
	if class == redriveClassBroken {
		return pkg.AbortReasonBrokenSignature
	}
	return pkg.AbortReasonTransientExhausted
}

// abortRedrive closes the record. A publish failure is logged and swallowed:
// the spawn is refused either way, and the next event retries the write.
func (h *taskEventHandler) abortRedrive(
	ctx context.Context,
	task lib.Task,
	reason string,
	streak int,
) {
	if err := h.resultPublisher.PublishAbort(ctx, task, reason, streak); err != nil {
		glog.Warningf(
			"event=redrive_abort_failed task=%s reason=%s error=%v",
			task.TaskIdentifier, reason, err,
		)
	}
}

// joinRedriveReasons renders the reason set for the log line. "-" means none.
func joinRedriveReasons(reasons []string) string {
	if len(reasons) == 0 {
		return "-"
	}
	return strings.Join(reasons, ",")
}

// classifyRedriveReasons decides why a streak reached the cap. The rules are
// evaluated in the order unknown, broken, transient and the first match wins.
//
// The classifier reads the FAILURES, not the spawns. The streak counts every
// successful spawn on every path, while a reason is recorded only from
// PublishFailure, so a spawn that publishes no failure — a reconcile re-drive
// whose job vanished, for instance — advances the streak without adding a
// reason. Requiring len(reasons) >= count would therefore strand such a record
// permanently: the spawn is refused, so it can never earn the missing reason,
// and nothing is ever closed. The window is the most recent
// min(count, len(reasons)) reasons instead, and the streak only bounds how far
// back to look.
//
//   - unknown: no reasons at all, or every reason in the window is the literal
//     "unknown" — there is no evidence to act on in either direction.
//   - broken: the window holds at least maxConsecutiveRedrives reasons and they
//     are all one value from brokenRedriveReasons. The cap's worth of identical
//     signatures is what makes a close safe; fewer is partial evidence.
//   - transient: everything else — differing reasons, a transient value, or a
//     window shorter than the cap. A retry may help, and the bounded extension
//     ends the record either way, so no record is left abandoned at the cap.
//
// The function is total: every input falls into exactly one class.
func classifyRedriveReasons(reasons []string, count int) string {
	if count < 1 || len(reasons) == 0 {
		return redriveClassUnknown
	}
	window := count
	if window > len(reasons) {
		window = len(reasons)
	}
	recent := reasons[len(reasons)-window:]

	allUnknown := true
	for _, reason := range recent {
		if reason != claim.RedriveReasonUnknown {
			allUnknown = false
			break
		}
	}
	if allUnknown {
		return redriveClassUnknown
	}
	if len(recent) >= maxConsecutiveRedrives && allSameRedriveReason(recent) {
		if _, broken := brokenRedriveReasons[recent[0]]; broken {
			return redriveClassBroken
		}
	}
	// Differing reasons, a transient value, or fewer reasons than the cap: a
	// retry may help, and the bounded extension ends the record either way.
	return redriveClassTransient
}

// allSameRedriveReason reports whether every entry holds the same value.
func allSameRedriveReason(reasons []string) bool {
	for _, reason := range reasons[1:] {
		if reason != reasons[0] {
			return false
		}
	}
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

// redriveReasonsFor answers one task's recorded reasons. Inside a reconcile pass
// they come from the pass's single reason snapshot; everywhere else they are
// read live, for the same reason the count is.
func (h *taskEventHandler) redriveReasonsFor(
	ctx context.Context,
	id lib.TaskIdentifier,
	phase string,
) ([]string, error) {
	store, err := h.redriveReasonStore(ctx)
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, nil
	}
	if cache := redriveStreakCacheFrom(ctx); cache != nil {
		return cache.reasons(ctx, store, string(id), phase)
	}
	return store.Reasons(ctx, string(id), phase)
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
// When the shared store says this is a NEW streak, the task's reason refs are
// cleared before the spawn is recorded. The classifier's short-count guard
// compares the reason count against the streak count, so a reason set left by an
// earlier streak would satisfy the guard on partial evidence and could close the
// record as broken. The shared Count is read directly, not through the pass
// cache: the cache can be older than a spawn this pass already recorded, and a
// stale 0 would discard the reasons of a streak the store already counted.
//
// Called only after SpawnJob returned successfully, so a suppressed, deferred
// or capped spawn does not count — the streak measures spawns, not attempts.
// Record's network calls run under the per-assignee spawn lock, bounded by the
// 30 s per-request cap.
func (h *taskEventHandler) recordRedriveSpawn(ctx context.Context, task lib.Task) {
	id := task.TaskIdentifier
	phase := taskPhaseString(task)
	now := h.currentDateTime.Now()
	h.redriveMu.Lock()
	prev, found := h.redriveStreaks[id]
	switch {
	case !found || prev.phase != phase || redriveStreakExpired(prev, now):
		// A new phase or an expired window starts a fresh streak: the store has
		// released the old one, so carrying its count forward would re-cap the
		// task the moment it spawns again.
		h.redriveStreaks[id] = redriveStreak{count: 1, phase: phase, since: now}
	default:
		h.redriveStreaks[id] = redriveStreak{
			count: prev.count + 1,
			phase: phase,
			since: now,
		}
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
	h.clearReasonsForNewStreak(ctx, store, id, phase)
	if recordErr := store.Record(ctx, string(id), phase); recordErr != nil {
		glog.Warningf(
			"event=redrive_streak_write_failed task=%s phase=%q error=%v",
			id, phase, recordErr,
		)
	}
}

// clearReasonsForNewStreak clears the task's reason refs when the shared streak
// is still at 0, i.e. the spawn about to be recorded starts a new streak. Both
// the read and the clear are logged and swallowed.
func (h *taskEventHandler) clearReasonsForNewStreak(
	ctx context.Context,
	streakStore claim.RedriveStreakStore,
	id lib.TaskIdentifier,
	phase string,
) {
	count, countErr := streakStore.Count(ctx, string(id), phase)
	if countErr != nil {
		glog.Warningf(
			"event=redrive_streak_read_failed task=%s phase=%q error=%v",
			id, phase, countErr,
		)
		return
	}
	if count != 0 {
		return
	}
	reasonStore, err := h.redriveReasonStore(ctx)
	if err != nil {
		glog.Warningf(
			"event=redrive_reason_write_failed task=%s phase=%q error=%v",
			id, phase, err,
		)
		return
	}
	if reasonStore == nil {
		return
	}
	if clearErr := reasonStore.Clear(ctx, string(id)); clearErr != nil {
		glog.Warningf(
			"event=redrive_reason_write_failed task=%s phase=%q error=%v",
			id, phase, clearErr,
		)
	}
}

// clearRedriveStreak ends this task's streak in memory and in both shared
// stores. Called when the task reaches a terminal state, where a further spawn
// is an operator decision rather than a re-drive of the loop this cap bounds. A
// store failure is logged and never blocks the terminal handling.
func (h *taskEventHandler) clearRedriveStreak(ctx context.Context, id lib.TaskIdentifier) {
	h.redriveMu.Lock()
	delete(h.redriveStreaks, id)
	h.redriveMu.Unlock()

	if store, err := h.redriveStreakStore(ctx); err != nil {
		glog.Warningf("event=redrive_streak_write_failed task=%s error=%v", id, err)
	} else if store != nil {
		if clearErr := store.Clear(ctx, string(id)); clearErr != nil {
			glog.Warningf(
				"event=redrive_streak_write_failed task=%s error=%v",
				id, clearErr,
			)
		}
	}

	if store, err := h.redriveReasonStore(ctx); err != nil {
		glog.Warningf("event=redrive_reason_write_failed task=%s error=%v", id, err)
	} else if store != nil {
		if clearErr := store.Clear(ctx, string(id)); clearErr != nil {
			glog.Warningf(
				"event=redrive_reason_write_failed task=%s error=%v",
				id, clearErr,
			)
		}
	}
}
