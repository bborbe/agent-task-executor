// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
	"github.com/bborbe/agent-task-executor/pkg/spawner"
)

// defaultTriggerPhases is the fallback phase allow-list when the per-Config Trigger is absent or empty.
var defaultTriggerPhases = domain.TaskPhases{
	domain.TaskPhasePlanning,
	domain.TaskPhaseExecution,
	domain.TaskPhaseAIReview,
}

// defaultTriggerStatuses is the fallback status allow-list when the per-Config Trigger is absent or empty.
var defaultTriggerStatuses = domain.TaskStatuses{
	domain.TaskStatusInProgress,
}

// defaultRespawnGracePeriod is the window after job_started_at during which the executor
// suppresses respawn when the K8s Job is inactive but no terminal phase has been observed.
// The window gives the agent's terminal-phase write time to propagate through the vault pipeline.
// defaultRespawnGracePeriod is the spawn path's half of the success window: it
// refuses to re-spawn a task whose Job succeeded this recently. It aliases the
// claim package's SuccessGracePeriod deliberately — the other half holds the
// claim for the same length — so the two cannot drift apart and leave a gap
// where the claim is gone but the spawn guard has also expired.
const defaultRespawnGracePeriod = claim.SuccessGracePeriod

// deferredRespawnInterval is the polling interval for the deferred-respawn reconciliation loop.
// Must be ≤ 60s to satisfy the R ≤ 60s bound from spec 037 (R = interval + per-tick
// comparison/spawn overhead; 30 s leaves headroom under the 60 s bound).
const deferredRespawnInterval = 30 * time.Second

// concurrencyCapRetryDelay is how long a spawn blocked by MaxConcurrentJobs
// waits before being re-evaluated. Short relative to a Job's runtime (a
// github-update-go Job measured 11-15 minutes uncontended on 2026-08-10), so a
// freed slot is picked up promptly without busy-polling the API server.
const concurrencyCapRetryDelay = 60 * time.Second

// DefaultReconcileInterval is the default interval between reconcile-loop
// passes (spec 005), overridable per deployment via RECONCILE_INTERVAL. 60s is
// a reasonable recovery latency for a task whose deferral was lost across a
// restart.
const DefaultReconcileInterval = 60 * time.Second

// maxConsecutiveRedrives bounds how many times in a row the executor may spawn
// the same task at the same phase with nothing advancing it.
//
// It is the bound for the case where neither of the other two engages. A task
// with no `ref` and no `max_triggers` keeps the trigger budget disarmed by
// design (see applyTriggerBudget — arming it would re-create the 2026-08-27
// Daily Sentry Triage incident), and a task whose scope reads as changed on
// every spawn is handed a fresh budget of 1 each time (see triggerBudgetWrite).
// Between them, a task that stops advancing is spawned forever.
//
// The streak counts EVERY spawn of a task, on every path, because it is checked
// and recorded inside spawnIfNeeded — the one funnel the Kafka consumer, the
// deferred-respawn loop and the reconcile floor all pass through.
//
// Counting only the floor's own spawns does not bound the loop, and dev proved
// it on 2026-10-14: four Jobs in 70 min, `capped=0` on all 68 reconcile passes.
// The floor initiated only two of the four; the other two arrived on the Kafka
// path, and each one cleared the floor-only streak before it could reach the cap.
//
// The reset is the phase. A task that moved phase has advanced and earns a fresh
// streak; a task pinned at one phase across three consecutive spawns has not, and
// stays capped until something moves it — an operator re-arm (which changes the
// phase) or a restart (which empties the streak).
//
// The streak is in-memory, and that is load-bearing rather than an optimisation.
// A counter persisted in the task file is not merely redundant, it is defeated:
// the agent pods rewrite the frontmatter from a snapshot taken at the start of
// their run, so `trigger_scope` and `job_started_at` are clobbered back to their
// pre-spawn values every time a Job completes (measured on the dev fixture,
// 2026-10-14 — the file showed `trigger_scope: 'planning:'` under
// `phase: execution`, and `job_started_at` from the task's creation). That
// clobber is what makes `scopeChanged` true on every spawn and resets the
// persisted `trigger_count` to 1, so any counter living in that file is reset by
// the very loop it must count.
//
// The cost of in-memory: a task stuck across a restart is re-driven up to N more
// times, not that it loops forever.
const maxConsecutiveRedrives = 3

// DefaultReconcilePassTimeout bounds one reconcile pass so a slow or large vault
// cannot hold a pass open indefinitely (spec 005 Failure Modes: "Pass bounded
// by a per-pass timeout; logs evaluate count so cost is visible"). Three
// quarters of the interval: a pass completes within a tick and leaves headroom
// for the next one. Raised from 30s on 2026-10-19 — a pass over the openclaw
// vault aborted on `context_cancelled` every minute after evaluating only 8-73
// of ~200 non-terminal tasks, so the tasks ordered after the cut were never
// reached. The per-assignee active-Job cache added in the same change removes
// most of the cost; this is the headroom that keeps a slow pass from re-opening
// the same window. Overridable per deployment via RECONCILE_PASS_TIMEOUT: on
// 2026-10-21 a sun executor reading the 15k-file openclaw vault aborted every
// pass at 45s with evaluated=0.
const DefaultReconcilePassTimeout = 45 * time.Second

// terminalPhases is the set of phases that must never trigger a new spawn.
// Extending this set requires a follow-up spec if vault-cli adds new terminal phases.
var terminalPhases = map[domain.TaskPhase]struct{}{
	domain.TaskPhaseHumanReview: {},
	domain.TaskPhaseDone:        {},
}

// knownPhases contains all phase constants exported by vault-cli v0.64.3.
// Values outside this set trigger enum-drift logging (event=unknown_phase).
var knownPhases = map[domain.TaskPhase]struct{}{
	domain.TaskPhaseTodo:        {},
	domain.TaskPhasePlanning:    {},
	domain.TaskPhaseExecution:   {}, // canonical (was TaskPhaseInProgress)
	domain.TaskPhaseInProgress:  {}, // legacy alias — still a known phase string
	domain.TaskPhaseAIReview:    {},
	domain.TaskPhaseHumanReview: {},
	domain.TaskPhaseDone:        {},
}

// IsTerminal reports whether the given phase is in the terminal set.
// Tasks at a terminal phase must not be re-spawned; operator intervention is required.
func IsTerminal(phase domain.TaskPhase) bool {
	_, ok := terminalPhases[phase]
	return ok
}

// applyPhaseGate emits metrics/logs for terminal and unknown phases.
// Returns true when the task must be skipped (terminal phase suppressed).
func applyPhaseGate(task lib.Task, phase domain.TaskPhase) bool {
	if IsTerminal(phase) {
		glog.Infof("event=spawn_suppressed phase=%s task=%s", phase, task.TaskIdentifier)
		metrics.TaskEventsTotal.WithLabelValues("spawn_suppressed_terminal_phase").Inc()
		return true
	}
	if _, inKnown := knownPhases[phase]; !inKnown {
		glog.Infof("event=unknown_phase phase=%s task=%s", phase, task.TaskIdentifier)
		metrics.TaskEventsTotal.WithLabelValues("unknown_phase").Inc()
	}
	return false
}

//counterfeiter:generate -o ../../mocks/task_event_handler.go --fake-name FakeTaskEventHandler . TaskEventHandler

// TaskEventHandler processes task event messages from Kafka and manages deferred respawns.
type TaskEventHandler interface {
	ConsumeMessage(ctx context.Context, msg *sarama.ConsumerMessage) error
	// EvalDeferredRespawns evaluates all pending deferred-respawn entries immediately.
	// Called by RunDeferredRespawnLoop on each tick; also callable directly in tests.
	EvalDeferredRespawns(ctx context.Context) error
	// RunDeferredRespawnLoop polls evalDeferredRespawns every deferredRespawnInterval
	// until ctx is cancelled. Must be run alongside the Kafka consumer.
	RunDeferredRespawnLoop(ctx context.Context) error
	// ReconcileOnce runs one reconcile pass: it snapshots the in-memory task
	// view, applies the eligibility filter, and re-drives every eligible owned
	// task through the existing spawn path. Called by RunReconcileLoop on each
	// tick; also callable directly in tests.
	ReconcileOnce(ctx context.Context) error
	// RunReconcileLoop runs one pass immediately at startup, then every
	// reconcile interval until ctx is cancelled. Must be run
	// alongside the Kafka consumer.
	RunReconcileLoop(ctx context.Context) error
}

// NewTaskEventHandler creates a new TaskEventHandler.
func NewTaskEventHandler(
	jobSpawner spawner.JobSpawner,
	namespace libk8s.Namespace,
	branch base.Branch,
	cluster string,
	stageFilter bool,
	runUntagged bool,
	resolver pkg.ConfigResolver,
	resultPublisher pkg.ResultPublisher,
	taskStore *pkg.TaskStore,
	currentDateTime libtime.CurrentDateTimeGetter,
	view TaskView,
	taskVault string,
	reconcilePassTimeout time.Duration,
	reconcileInterval time.Duration,
	claimMode pkg.ClaimMode,
	claimRefClientFactory ClaimRefClientFactory,
	// claimJobControl is optional so the many test call sites stay readable; a
	// claim-taking mode without one fails loudly at lease-manager construction.
	claimJobControl ...claim.JobControl,
) ClaimLifecycleEventHandler {
	handler := &taskEventHandler{
		jobSpawner:            jobSpawner,
		namespace:             namespace,
		branch:                branch,
		cluster:               cluster,
		stageFilter:           stageFilter,
		runUntagged:           runUntagged,
		resolver:              resolver,
		resultPublisher:       resultPublisher,
		taskStore:             taskStore,
		currentDateTime:       currentDateTime,
		deferredRespawns:      make(map[lib.TaskIdentifier]deferredEntry),
		deferredEpochs:        make(map[lib.TaskIdentifier]uint64),
		queuedAtStamps:        make(map[lib.TaskIdentifier]string),
		spawnLocks:            make(map[string]*sync.Mutex),
		view:                  view,
		taskVault:             taskVault,
		reconcilePassTimeout:  reconcilePassTimeout,
		reconcileInterval:     reconcileInterval,
		claimMode:             claimMode,
		claimRefClientFactory: claimRefClientFactory,
		redriveStreaks:        make(map[lib.TaskIdentifier]redriveStreak),
	}
	if len(claimJobControl) > 0 {
		handler.claimJobControl = claimJobControl[0]
	}
	return handler
}

type taskEventHandler struct {
	jobSpawner       spawner.JobSpawner
	namespace        libk8s.Namespace
	branch           base.Branch
	cluster          string
	stageFilter      bool
	runUntagged      bool
	resolver         pkg.ConfigResolver
	resultPublisher  pkg.ResultPublisher
	taskStore        *pkg.TaskStore
	currentDateTime  libtime.CurrentDateTimeGetter
	deferredMu       sync.Mutex
	deferredRespawns map[lib.TaskIdentifier]deferredEntry
	// deferredEpochs counts removals per task, so a replay collected before an
	// eviction can detect it. Entries are never deleted: resetting an epoch would
	// re-validate a stale claim.
	deferredEpochs map[lib.TaskIdentifier]uint64
	// queuedAtStamps records the queued_at this executor already published for a
	// task, so a deferral never re-stamps while the write is unlandable. The
	// frontmatter and view checks in stampQueueEntry both read state that only a
	// SUCCESSFUL vault write can update; a task whose file is gone never updates
	// either, so without this map it re-stamps on every deferral, forever.
	// Guarded by deferredMu.
	queuedAtStamps        map[lib.TaskIdentifier]string
	spawnLocksMu          sync.Mutex
	spawnLocks            map[string]*sync.Mutex
	view                  TaskView
	taskVault             string
	reconcilePassTimeout  time.Duration
	reconcileInterval     time.Duration
	claimMode             pkg.ClaimMode
	claimRefClientFactory ClaimRefClientFactory
	// claimJobControl lets the lease manager's fence and restart recovery act on
	// a Job. Nil falls back to the loud unmanaged control, so a mode that takes a
	// claim cannot silently run without it.
	claimJobControl  claim.JobControl
	claimManagerOnce sync.Once
	claimManager     claim.LeaseManager
	claimManagerErr  error
	// spawnPendingSet marks tasks whose spawn is between taking the claim and
	// recording the new Job. A terminal event for the previous Job arriving in
	// that window names the only Job the store knows, so without this marker it
	// would release the claim just taken for the replacement.
	spawnPendingMu  sync.Mutex
	spawnPendingSet map[lib.TaskIdentifier]struct{}
	// claimLocks serialises, per task, the spawn path's mark+claim against a
	// release path's check+snapshot+delete. Without it the pending check is
	// check-then-act: a spawn could mark and take its claim between the check
	// and the HeldClaims snapshot, and the release would delete the fresh claim.
	claimLocksMu   sync.Mutex
	claimLocks     map[lib.TaskIdentifier]*sync.Mutex
	redriveMu      sync.Mutex
	redriveStreaks map[lib.TaskIdentifier]redriveStreak
	// spawnGens counts Jobs created by this executor per assignee, on any path.
	// A pass cache entry read at an older generation is refetched, so a Job
	// spawned by the Kafka consumer or the deferred loop mid-pass is never
	// missed, and a spawn for one assignee leaves the others' entries valid.
	spawnGenMu sync.Mutex
	spawnGens  map[string]uint64
}

// lockAssigneeSpawn serializes the count-then-spawn sequence for one assignee and
// returns the unlock func. It is the mutual exclusion that makes MaxConcurrentJobs
// an actual cap rather than an advisory hint.
//
// Without it the cap is a check-then-act race: spawnIfNeeded is reached from two
// goroutines — the Kafka consumer (consumer.Consume) and the deferred-respawn loop
// (RunDeferredRespawnLoop), both started by service.Run in main.go — and each reads
// the same live Job count before either has created its Job, so both conclude they
// are under the cap. Measured in prod on 2026-08-15 with cap=1: 36 tasks released at
// once admitted 17 Jobs, 15 released admitted 15. Every over-cap Job is then rejected
// by the agent's ResourceQuota, loops on FailedCreate and burns its full
// activeDeadlineSeconds while merely queued, before being killed without ever running.
//
// Per-assignee rather than global: a fleet-wide lock would make one agent's spawn
// latency everyone's. Serializing per assignee costs nothing — spawns are rare
// against Job runtimes measured in the 11-15 minute range.
//
// Taken unconditionally since spec 005: the reconcile loop and the Kafka consumer
// must also serialize for uncapped assignees, or a reconcile tick racing the event
// path for the same task would double-spawn.
//
// WARNING: correct only while the executor runs replicas: 1 (verified in quant dev
// and prod). Scaling to 2 reinstates the race across processes and requires a lease
// or leader election instead.
func (h *taskEventHandler) lockAssigneeSpawn(assignee string) func() {
	h.spawnLocksMu.Lock()
	mu, ok := h.spawnLocks[assignee]
	if !ok {
		mu = &sync.Mutex{}
		h.spawnLocks[assignee] = mu
	}
	h.spawnLocksMu.Unlock()

	mu.Lock()
	return mu.Unlock
}

// isJobActive reports whether an active Job exists for the task. Answered from
// the per-assignee active set when the assignee is known, so a reconcile pass
// costs one list per assignee rather than one per task. An empty assignee cannot
// key that set, so it falls back to the direct lookup.
func (h *taskEventHandler) isJobActive(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
) (bool, error) {
	namespace, err := h.jobNamespaceForTask(ctx, config, task.TaskIdentifier)
	if err != nil {
		return false, err
	}
	if config == nil || config.Assignee == "" {
		active, err := h.jobSpawner.IsJobActive(ctx, namespace, task.TaskIdentifier)
		if err != nil {
			return false, errors.Wrapf(
				ctx,
				err,
				"check active job for task %s",
				task.TaskIdentifier,
			)
		}
		return active, nil
	}
	activeSet, err := h.activeJobsFor(ctx, namespace, config.Assignee)
	if err != nil {
		return false, errors.Wrapf(ctx, err, "check active job for task %s", task.TaskIdentifier)
	}
	return activeSet[task.TaskIdentifier], nil
}

// markSpawnPending records that taskID is between taking its claim and having a
// Job name to record.
func (h *taskEventHandler) markSpawnPending(taskID lib.TaskIdentifier) {
	h.spawnPendingMu.Lock()
	defer h.spawnPendingMu.Unlock()
	if h.spawnPendingSet == nil {
		h.spawnPendingSet = make(map[lib.TaskIdentifier]struct{})
	}
	h.spawnPendingSet[taskID] = struct{}{}
}

// clearSpawnPending ends the window opened by markSpawnPending.
func (h *taskEventHandler) clearSpawnPending(taskID lib.TaskIdentifier) {
	h.spawnPendingMu.Lock()
	defer h.spawnPendingMu.Unlock()
	delete(h.spawnPendingSet, taskID)
}

// lockClaimRelease takes the per-task claim lock and returns its unlock. The
// spawn path holds it across mark+claim; every release path holds it across
// pending-check+snapshot+delete, so the two can never interleave.
func (h *taskEventHandler) lockClaimRelease(taskID lib.TaskIdentifier) func() {
	h.claimLocksMu.Lock()
	if h.claimLocks == nil {
		h.claimLocks = make(map[lib.TaskIdentifier]*sync.Mutex)
	}
	lock, ok := h.claimLocks[taskID]
	if !ok {
		lock = &sync.Mutex{}
		h.claimLocks[taskID] = lock
	}
	h.claimLocksMu.Unlock()
	lock.Lock()
	return lock.Unlock
}

// spawnPendingFor reports whether taskID is inside the claim-then-store window.
func (h *taskEventHandler) spawnPendingFor(taskID lib.TaskIdentifier) bool {
	h.spawnPendingMu.Lock()
	defer h.spawnPendingMu.Unlock()
	_, ok := h.spawnPendingSet[taskID]
	return ok
}

// staleEventForTask reports whether the task store holds an entry that tracks a
// DIFFERENT Job than the event carries, i.e. the event belongs to a superseded
// job cycle and must not act on the one that is running now. An absent entry is
// not stale: after a restart the store holds nothing and the release must still
// happen.
func (h *taskEventHandler) staleEventForTask(task lib.Task) bool {
	storedJob := h.storedCurrentJob(task)
	return storedJob != "" && storedJob != task.Frontmatter.CurrentJob()
}

// storedCurrentJob returns the store entry's current_job, or "" when the store
// holds no entry for the task.
func (h *taskEventHandler) storedCurrentJob(task lib.Task) string {
	stored, ok := h.taskStore.Load(task.TaskIdentifier)
	if !ok {
		return ""
	}
	return stored.Frontmatter.CurrentJob()
}

// cleanupTerminalTask clears in-flight state when a task has reached a terminal
// status (completed/aborted). taskStore.Delete stops the job informer from emitting
// a spurious synthetic failure after the agent published success. removeDeferredEntry
// cancels any pending grace-window respawn — the terminal event is skipped by the
// status filter in parseAndFilter BEFORE reaching the terminal-phase gate that would
// otherwise clear the entry, so without this a deferred entry created during the grace
// window fires ~300s later and respawns a job for an already-done task (the "path C"
// respawn observed on dev 2026-07-13 for probe 095c58d7).
func (h *taskEventHandler) cleanupTerminalTask(ctx context.Context, task lib.Task) {
	status := string(task.Frontmatter.Status())
	if status != "completed" && status != "aborted" {
		return
	}
	// Guard against stale terminal events from an earlier job cycle. A terminal
	// event whose current_job differs from the in-store task's means the task was
	// re-dispatched and re-spawned under a DIFFERENT job since the event was
	// emitted (respawned-second-success race, observed prod 2026-09-03 on task
	// 893c33b9). Evicting here drops the only store entry the new job's success
	// path uses to publish the current_job clear, leaving current_job set forever
	// with no operator visibility. Evict only when the store entry tracks the
	// same job as the terminal event — or no job at all (defensive). A stale
	// entry kept here is cleaned when the tracked job reaches terminal via
	// HandleJob, the same mechanism the success path already relies on.
	if h.staleEventForTask(task) {
		glog.V(3).Infof(
			"task %s %s: stale terminal event (store current_job=%s, event current_job=%s) — keeping store entry",
			task.TaskIdentifier, status, h.storedCurrentJob(task), task.Frontmatter.CurrentJob(),
		)
		return
	}
	// Release after the stale-event guard: releasing before it would drop the
	// claim of a job that is still running under a newer current_job.
	h.ReleaseTaskClaim(ctx, task.TaskIdentifier)
	h.taskStore.Delete(task.TaskIdentifier)
	h.removeDeferredEntry(task.TaskIdentifier)
	h.clearRedriveStreak(task.TaskIdentifier)
	glog.V(3).
		Infof("task %s %s: cleared task store + deferred respawn", task.TaskIdentifier, status)
}

func (h *taskEventHandler) ConsumeMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	task, config, skip, err := h.parseAndFilter(ctx, msg)
	if err != nil {
		return err
	}
	if skip {
		return nil
	}
	// A cold pod resumes its consumer from the last committed offset, so the first
	// events it processes are a BACKLOG — snapshots of tasks as they looked when
	// the event was produced, possibly minutes ago and possibly already handled by
	// another cluster. Acting on one spawns a task on the strength of a stale
	// snapshot, and none of the guards that would refuse it can help yet: the
	// spawn path's anti-duplicate checks read the task view and the task store,
	// and on a cold pod both are empty.
	//
	// The view is the durable half — it replays the compacted topic from the
	// oldest offset — so waiting for it to be ready is waiting for the picture
	// that makes those guards meaningful. Reconcile already refuses to run before
	// then (event=reconcile_view_not_ready), and it is the source-of-truth floor:
	// every task skipped here is driven by the next reconcile pass once the view
	// is ready, so nothing is dropped, only deferred.
	if h.view != nil && !h.view.Ready() {
		metrics.TaskEventsTotal.WithLabelValues("skipped_view_not_ready").Inc()
		glog.Infof(
			"event=event_view_not_ready task=%s: task view has not replayed yet; reconcile will drive it",
			task.TaskIdentifier,
		)
		return nil
	}
	_, err = h.spawnIfNeeded(ctx, task, config)
	return err
}

// parseAndFilter unmarshals the message and applies all pre-spawn filter checks.
// Returns (task, config, true, nil) when the message should be silently skipped.
// Returns (task, config, false, nil) when the task qualifies for spawning.
// Returns (_, _, false, err) when an unexpected error occurred.
func (h *taskEventHandler) parseAndFilter(
	ctx context.Context,
	msg *sarama.ConsumerMessage,
) (lib.Task, *pkg.AgentConfiguration, bool, error) {
	task, err := decodeTaskEvent(ctx, msg.Value)
	if err != nil {
		switch {
		case stderrors.Is(err, errEmptyTaskEvent):
			glog.V(3).Infof("skip empty message at offset %d", msg.Offset)
		case stderrors.Is(err, errEmptyTaskIdentifier):
			glog.Warningf("task event at offset %d has empty TaskIdentifier, skipping", msg.Offset)
		default:
			glog.Warningf("failed to unmarshal task event at offset %d: %v", msg.Offset, err)
		}
		return lib.Task{}, nil, true, nil
	}

	h.cleanupTerminalTask(ctx, task)

	if h.skipForeignCluster(task) { // before Config resolution: not an unknown assignee
		// A pending deferred entry holds a pre-retag snapshot; it must not outlive this.
		h.removeDeferredEntry(task.TaskIdentifier)
		return lib.Task{}, nil, true, nil
	}

	// Resolve the per-agent Config before the status/phase checks so both filters
	// can use the per-Config trigger. Skip lookup when assignee is empty.
	config, skip, err := h.resolveConfig(ctx, task)
	if err != nil {
		return lib.Task{}, nil, false, err
	}
	if skip {
		// An unresolvable assignee must not leave an older snapshot parked.
		h.removeDeferredEntry(task.TaskIdentifier)
		return lib.Task{}, nil, true, nil
	}

	// Type filter: effective set = {cfg.TaskType} ∪ cfg.TaskTypes.
	// Skipped when the effective set is empty (agent has no task types configured).
	if mismatch := taskTypeMismatchReason(task, config); mismatch != "" {
		if err := h.resultPublisher.PublishTypeMismatchFailure(ctx, task, mismatch); err != nil {
			metrics.TaskEventsTotal.WithLabelValues("error").Inc()
			return lib.Task{}, nil, false, errors.Wrapf(
				ctx, err, "publish type mismatch failure for task %s", task.TaskIdentifier,
			)
		}
		glog.V(2).Infof("type mismatch: %s (task %s)", mismatch, task.TaskIdentifier)
		return h.skipAndEvict(task, "type_mismatch")
	}

	if !effectiveTriggerStatuses(config).Contains(task.Frontmatter.Status()) {
		glog.V(3).
			Infof("skip task %s with status %s", task.TaskIdentifier, task.Frontmatter.Status())
		// This also evicts for a stale terminal event that cleanupTerminalTask
		// deliberately kept (store current_job != event current_job). Accepted:
		// the reconcile floor re-drives a task still at in_progress within one
		// interval, so the kept store entry is recovered rather than lost.
		return h.skipAndEvict(task, "skipped_status")
	}

	phase := task.Frontmatter.Phase()
	// terminal phases must not be spawned again — operator escalation required
	if phase != nil && applyPhaseGate(task, *phase) {
		// Same stale-event guard as cleanupTerminalTask: a replayed terminal-phase
		// event for a task that was moved back to execution and re-spawned under a
		// newer Job must not release the live claim, or a peer re-claims the key and
		// runs the same task twice.
		if !h.staleEventForTask(task) {
			h.ReleaseTaskClaim(ctx, task.TaskIdentifier)
		}
		h.removeDeferredEntry(task.TaskIdentifier)
		return lib.Task{}, nil, true, nil
	}
	if phase == nil || !effectiveTriggerPhases(config).Contains(*phase) {
		glog.V(3).Infof("skip task %s with phase %v", task.TaskIdentifier, phase)
		return h.skipAndEvict(task, "skipped_phase")
	}

	stage := task.Frontmatter.Stage()
	if h.stageFilter && stage != string(h.branch) {
		glog.V(3).Infof(
			"skip task %s with stage %s (executor branch %s)",
			task.TaskIdentifier, stage, h.branch,
		)
		return h.skipAndEvict(task, "skipped_stage")
	}

	if task.Frontmatter.Assignee() == "" {
		glog.V(3).Infof("skip task %s with empty assignee", task.TaskIdentifier)
		return h.skipAndEvict(task, "skipped_assignee")
	}

	return task, config, false, nil
}

// skipAndEvict records a filter-skip metric, drops any parked respawn for the
// task, and returns the parseAndFilter skip tuple. Every filter skip evicts:
// an event that no longer qualifies must not leave an older snapshot to spawn.
func (h *taskEventHandler) skipAndEvict(
	task lib.Task,
	result string,
) (lib.Task, *pkg.AgentConfiguration, bool, error) {
	metrics.TaskEventsTotal.WithLabelValues(result).Inc()
	h.removeDeferredEntry(task.TaskIdentifier)
	return lib.Task{}, nil, true, nil
}

// resolveConfig looks up the agent Config CR for the task's assignee.
// Returns (nil, false, nil) when assignee is empty (caller handles the empty-assignee path).
// Returns (nil, true, nil) when the assignee is unknown (ErrConfigNotFound).
// Returns (nil, false, err) on unexpected resolver errors.
func (h *taskEventHandler) resolveConfig(
	ctx context.Context,
	task lib.Task,
) (*pkg.AgentConfiguration, bool, error) {
	if task.Frontmatter.Assignee() == "" {
		return nil, false, nil
	}
	resolved, err := h.resolver.Resolve(ctx, string(task.Frontmatter.Assignee()))
	if err != nil {
		if stderrors.Is(err, pkg.ErrConfigNotFound) {
			glog.Warningf(
				"skip task %s: unknown assignee %s",
				task.TaskIdentifier,
				task.Frontmatter.Assignee(),
			)
			metrics.TaskEventsTotal.WithLabelValues("skipped_unknown_assignee").Inc()
			metrics.SkippedUnknownAssigneeTotal.WithLabelValues(
				string(task.Frontmatter.Assignee()),
			).Inc()
			return nil, true, nil
		}
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return nil, false, errors.Wrapf(
			ctx,
			err,
			"resolve agent config for task %s",
			task.TaskIdentifier,
		)
	}
	return &resolved, false, nil
}

// resolveConfigForReconcile resolves the agent Config for a task on the reconcile
// floor's recovery path. It calls resolveConfig first (which resolves by assignee
// only) and falls back to a lookup by the task's declared task_type when the
// assignee is empty and the task_type is non-empty.
//
// It exists separately from resolveConfig because resolveConfig is also called by
// parseAndFilter — the Kafka consumer path. That path must NOT gain this fallback:
// an ambiguous declared type would return an error out of ConsumeMessage and tear
// the consumer loop down with the offset unmarked, wedging on redelivery. Only
// reconcileTask calls this function.
//
// Returns (nil, true, nil) when neither the assignee nor the declared task_type
// resolves to a Config: the task carries no routing information this executor can
// use. The skip is attributed to the existing skipped_unknown_assignee label on
// TaskEventsTotal; SkippedUnknownAssigneeTotal is deliberately NOT incremented,
// because that counter is keyed on a routing value and there is none.
// ErrConfigAmbiguous and any other error are returned unchanged so the caller can
// log them with the task in hand.
func (h *taskEventHandler) resolveConfigForReconcile(
	ctx context.Context,
	task lib.Task,
) (*pkg.AgentConfiguration, bool, error) {
	config, skip, err := h.resolveConfig(ctx, task)
	if err != nil || skip || config != nil {
		return config, skip, err
	}
	taskType := task.Frontmatter.TaskType()
	if taskType == "" {
		return nil, false, nil
	}
	// previous_assignee (written at escalation) only breaks a tie between several
	// Configs declaring this task_type; it never routes a task on its own.
	previousAssignee, _ := task.Frontmatter.String("previous_assignee")
	resolved, err := h.resolver.ResolveByTaskType(ctx, string(taskType), previousAssignee)
	if err != nil {
		if stderrors.Is(err, pkg.ErrConfigNotFound) {
			glog.V(2).Infof(
				"skip task %s: task_type %s resolves to no config",
				task.TaskIdentifier,
				taskType,
			)
			metrics.TaskEventsTotal.WithLabelValues("skipped_unknown_assignee").Inc()
			return nil, true, nil
		}
		return nil, false, err
	}
	return &resolved, false, nil
}

// taskTypeMismatchReason returns a non-empty reason string when the task's task_type is not in the
// agent's effective type set. Returns "" when the filter passes (match or effective set is empty).
func taskTypeMismatchReason(task lib.Task, cfg *pkg.AgentConfiguration) string {
	if cfg == nil {
		return ""
	}
	effectiveTypes := pkg.EffectiveTaskTypes(cfg.TaskType, cfg.TaskTypes)
	if len(effectiveTypes) == 0 {
		return ""
	}
	taskType := task.Frontmatter.TaskType()
	if pkg.TaskTypeInSet(string(taskType), effectiveTypes) {
		return ""
	}
	if taskType == "" {
		return fmt.Sprintf(
			"task has no task_type; agent %q accepts %v",
			cfg.Assignee,
			effectiveTypes,
		)
	}
	return fmt.Sprintf(
		"task_type %q not in effective set %v of agent %q",
		taskType, effectiveTypes, cfg.Assignee,
	)
}

// effectiveTriggerPhases returns the phase allow-list from the Config trigger,
// falling back to defaultTriggerPhases when Trigger is absent or the list is empty.
func effectiveTriggerPhases(cfg *pkg.AgentConfiguration) domain.TaskPhases {
	if cfg == nil || cfg.Trigger == nil || len(cfg.Trigger.Phases) == 0 {
		return defaultTriggerPhases
	}
	return cfg.Trigger.Phases
}

// effectiveTriggerStatuses returns the status allow-list from the Config trigger,
// falling back to defaultTriggerStatuses when Trigger is absent or the list is empty.
func effectiveTriggerStatuses(cfg *pkg.AgentConfiguration) domain.TaskStatuses {
	if cfg == nil || cfg.Trigger == nil || len(cfg.Trigger.Statuses) == 0 {
		return defaultTriggerStatuses
	}
	return cfg.Trigger.Statuses
}

// spawnIfNeeded returns (spawned, err): spawned is true iff a new k8s Job was actually launched
// (i.e. the call reached SpawnJob successfully). All early-return branches (suppression, trigger
// cap, active job, terminal phase, errors) return spawned=false.
//
// The Kafka consumer's path passes no claim; a replay (deferred loop or reconcile
// drive) goes through spawnIfNeededClaimed with the epoch its snapshot was
// collected at, so an eviction landing mid-flight stops that snapshot.
func (h *taskEventHandler) spawnIfNeeded(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
) (bool, error) {
	return h.spawnIfNeededClaimed(ctx, task, config, nil)
}

// spawnJobError marks a failed JobSpawner.SpawnJob call so the deferred-respawn
// loop can skip that entry instead of exiting the whole process. Every other
// error from the spawn path stays fatal.
type spawnJobError struct {
	err error
}

func (e *spawnJobError) Error() string { return e.err.Error() }

func (e *spawnJobError) Unwrap() error { return e.err }

// spawnIfNeededClaimed is spawnIfNeeded with an optional removal claim. With a
// non-nil claim, both deferral stores and the final spawn decision re-check the
// task's epoch, so a removeDeferredEntry that lands after the entry was
// collected wins unless it arrives after the final claim check: the remaining
// window is the duration of the SpawnJob call, which deliberately runs outside
// deferredMu (an eviction blocked behind an in-flight create could not stop that
// Job anyway). That window is accepted: the next reconcile pass's foreign read
// only stops the task being re-parked, it does not remove a Job already created.
func (h *taskEventHandler) spawnIfNeededClaimed(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
	claim *deferredClaim,
) (bool, error) {
	// Not this instance's task — see skipForeignVaultTask.
	if h.skipForeignVaultTask(task) {
		return false, nil
	}

	// A Config whose target namespace is unusable is left untouched for a human:
	// no claim, no frontmatter write, no Job.
	if h.rejectInvalidJobNamespace(ctx, task, config) {
		return false, nil
	}

	// If current_job is set in frontmatter, a prior spawn notification was written
	// to the task file. Verify the job is still active; if not, proceed to spawn.
	if currentJob := task.Frontmatter.CurrentJob(); currentJob != "" {
		suppress, err := h.checkActiveCurrentJob(ctx, task, currentJob, config, claim)
		if err != nil {
			return false, err
		}
		if suppress {
			return false, nil
		}
	}

	// Held across the cap check AND the SpawnJob call below, so no other goroutine
	// can observe a stale Job count in between. Taken unconditionally (for any
	// resolved config, capped or not) because three goroutines reach spawnIfNeeded
	// — the Kafka consumer, the deferred-respawn loop, and the reconcile loop
	// (spec 005) — and for an UNCAPPED assignee they must still serialize
	// count-then-spawn so two of them cannot both read IsJobActive=false before
	// either creates the Job and double-spawn the same task. Spawns are rare
	// against Job runtimes measured in minutes, so the per-assignee serialization
	// costs nothing measurable. config is nil only for tasks that never reach this
	// code (empty assignee is filtered upstream); the nil guard stays defensive.
	if config != nil {
		defer h.lockAssigneeSpawn(config.Assignee)()
	}

	active, err := h.isJobActive(ctx, task, config)
	if err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return false, err
	}
	if active {
		glog.V(3).Infof("skip task %s: active job exists", task.TaskIdentifier)
		metrics.TaskEventsTotal.WithLabelValues("skipped_active_job").Inc()
		return false, nil
	}

	if h.recentlySucceeded(task) {
		return false, nil
	}

	// The last bound, and the only one that covers every path — see
	// maxConsecutiveRedrives.
	if h.redriveStreakCapped(task) {
		return false, nil
	}

	capped, err := h.deferIfAtConcurrencyCap(ctx, task, config, claim)
	if err != nil {
		return false, err
	}
	if capped {
		return false, nil
	}

	// Scoped trigger budget: check only. The counter write is published by
	// recordTriggerSpawn after SpawnJob succeeds, so an infrastructure failure
	// cannot burn one of the task's attempts.
	if h.checkTriggerBudget(task) {
		return false, nil
	}

	// Claimed replays decide under deferredMu, released before SpawnJob; the
	// lock order is assignee spawn lock → deferredMu. An eviction landing after
	// this check and during SpawnJob does not stop the spawn — see
	// spawnIfNeededClaimed. See claimStillValid.
	if !h.claimStillValid(claim) {
		h.recordClaimInvalidated(task)
		return false, nil
	}

	// Lease claim: taken after every existing gate and immediately before the Job
	// is created, under the per-assignee spawn lock taken above. Taken earlier it
	// would hold a lease for a task a gate skips; taken later it could not prevent
	// the duplicate it exists to prevent. Do not confuse this lease with the
	// deferred-removal claim argument of this function.
	// Marked BEFORE the claim: acquireSpawnClaim stores the fresh claim, so a
	// marker set after it would leave the window between the claim and the marker
	// wide enough for a terminal event for the previous Job to see a
	// current-looking store and release the claim just taken. Held until this
	// function returns, i.e. past storeSpawnedJob.
	h.markSpawnPending(task.TaskIdentifier)
	defer h.clearSpawnPending(task.TaskIdentifier)

	unlockClaim := h.lockClaimRelease(task.TaskIdentifier)
	acquired := h.acquireSpawnClaim(ctx, task)
	unlockClaim()
	if !acquired {
		return false, nil
	}

	return h.spawnClaimedJob(ctx, task, config)
}

// spawnClaimedJob creates the Job for a task whose claim is already held, and
// records it. The caller holds the in-flight marker across this call — from
// before the claim through storeSpawnedJob — because between the claim and the
// store entry there is no Job name to compare against, so a terminal event for
// the previous Job would otherwise look current and release the claim.
func (h *taskEventHandler) spawnClaimedJob(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
) (bool, error) {
	jobName, err := h.jobSpawner.SpawnJob(ctx, task, *config)
	if err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		// A claim taken for a Job that was never created must not stay held.
		// Every SpawnJob error but one precedes the Job create, and an
		// AlreadyExists answer is success; the accepted window is a lost
		// Create response, where the Job exists and this release drops its
		// claim until the lease expires. The pending marker is cleared first:
		// this is the one release that must go through.
		unlockRelease := h.lockClaimRelease(task.TaskIdentifier)
		h.clearSpawnPending(task.TaskIdentifier)
		h.releaseTaskClaimLocked(ctx, task.TaskIdentifier)
		unlockRelease()
		return false, errors.Wrapf(
			ctx,
			&spawnJobError{err: err},
			"spawn job for task %s failed",
			task.TaskIdentifier,
		)
	}

	if err := h.finishSpawn(ctx, task, config, jobName); err != nil {
		return false, err
	}
	return true, nil
}

// finishSpawn records a Job that was just created. A counter write that cannot
// be recorded is a real fault and still propagates, but it no longer prevents
// the spawn: the Job exists. The Job is stored before the counter write, so if
// the write fails the in-memory store still knows the Job exists.
func (h *taskEventHandler) finishSpawn(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
	jobName string,
) error {
	h.bumpSpawnGen(config.Assignee)
	h.storeSpawnedJob(task, jobName)

	if err := h.recordTriggerSpawn(ctx, task); err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return err
	}

	if err := h.resultPublisher.PublishSpawnNotification(ctx, task, jobName); err != nil {
		// Log but don't fail — job is already spawned, spawn notification is best-effort
		glog.Warningf("publish spawn notification for task %s failed (job %s still running): %v",
			task.TaskIdentifier, jobName, err)
	}

	glog.V(2).Infof(
		"spawned job for task %s (assignee=%s image=%s)",
		task.TaskIdentifier, task.Frontmatter.Assignee(), config.Image,
	)
	metrics.TaskEventsTotal.WithLabelValues("spawned").Inc()
	metrics.JobsSpawnedTotal.Inc()
	h.recordRedriveSpawn(task)
	return nil
}

// storeSpawnedJob records the task under the job that was actually spawned. The
// spawn notification is published to the vault asynchronously; without this the
// in-memory entry keeps the pre-spawn event's current_job (the previous job or
// none), so cleanupTerminalTask cannot tell a stale terminal event from a current
// one and evicts a task whose replacement job is still running — the
// respawned-second-success race.
//
// Clone the frontmatter: the event's task may still be held by a deferred-respawn
// entry, so mutating it in place would corrupt that entry.
func (h *taskEventHandler) storeSpawnedJob(task lib.Task, jobName string) {
	stored := task
	stored.Frontmatter = make(lib.TaskFrontmatter, len(task.Frontmatter))
	for k, v := range task.Frontmatter {
		stored.Frontmatter[k] = v
	}
	stored.Frontmatter["current_job"] = jobName
	stored.Frontmatter["job_started_at"] = h.currentDateTime.Now().
		UTC().
		Format("2006-01-02T15:04:05Z07:00")
	h.taskStore.Store(task.TaskIdentifier, stored)
}

// deferIfAtConcurrencyCap reports whether the agent is already running its
// configured maximum number of Jobs, and if so registers the task for a
// deferred respawn instead of spawning now.
//
// Deferring rather than skipping is the whole point. Task publication is
// edge-triggered on vault file changes; being over the cap produces no such
// change, so a skipped spawn would never be re-driven and the task would sit
// at in_progress forever, indistinguishable from work in flight.
func (h *taskEventHandler) deferIfAtConcurrencyCap(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
	claim *deferredClaim,
) (bool, error) {
	if config == nil || config.MaxConcurrentJobs <= 0 {
		return false, nil
	}
	namespace, err := h.jobNamespaceForTask(ctx, config, task.TaskIdentifier)
	if err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return false, err
	}
	activeSet, err := h.activeJobsFor(ctx, namespace, config.Assignee)
	if err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return false, errors.Wrapf(ctx, err, "count active jobs for assignee %s", config.Assignee)
	}
	running := len(activeSet)
	if running < config.MaxConcurrentJobs {
		// Log the ADMIT decision, not just the deferral. Without this, an
		// over-cap spawn leaves no trace and overshoot can only be found by
		// counting Jobs in the cluster by hand — which is how the 2026-08-15
		// race went unnoticed until 17 Jobs existed against a cap of 1.
		// Same V(1) rationale as the deferral log below.
		glog.V(1).Infof(
			"event=concurrency_admit task=%s assignee=%s running=%d cap=%d",
			task.TaskIdentifier, config.Assignee, running, config.MaxConcurrentJobs,
		)
		return false, nil
	}
	// Cheap pre-check before the side effects below: an evicted claim must not
	// publish a queued_at or count a deferral. The authoritative check is at the
	// store, under the same lock as the write.
	h.deferredMu.Lock()
	invalid := h.claimInvalidatedLocked(claim)
	h.deferredMu.Unlock()
	if invalid {
		h.recordClaimInvalidated(task)
		return true, nil
	}
	// V(1), not V(0): a capped deferral is an expected steady state and repeats
	// once per retry interval per waiting task, so V(0) would be a constant
	// stream during any backlog. V(1) is inside the deployed verbosity, so it
	// stays visible to an operator looking — unlike the V(3) skip that hid the
	// wedge fixed in #12.
	glog.V(1).Infof(
		"event=concurrency_cap task=%s assignee=%s running=%d cap=%d retry_after=%s",
		task.TaskIdentifier, config.Assignee, running, config.MaxConcurrentJobs,
		concurrencyCapRetryDelay,
	)
	metrics.TaskEventsTotal.WithLabelValues("deferred_concurrency_cap").Inc()
	// The bare counter above is kept for continuity; this one carries the
	// assignee so an alert can exclude an agent that is capped on purpose.
	metrics.DeferredConcurrencyCapTotal.WithLabelValues(config.Assignee).Inc()
	task = h.stampQueueEntry(ctx, task)
	h.deferredMu.Lock()
	if h.claimInvalidatedLocked(claim) {
		h.deferredMu.Unlock()
		h.recordClaimInvalidated(task)
		// The deferral ends here without re-parking, so the record stamped above
		// must not outlive it.
		h.clearQueuedAtStampIfNotDeferred(task.TaskIdentifier)
		return true, nil
	}
	// A claimed replay stores only when nothing is parked: a concurrent fresh
	// event may have parked a newer snapshot under the same id.
	if claim != nil {
		if _, exists := h.deferredRespawns[task.TaskIdentifier]; exists {
			h.deferredMu.Unlock()
			// Nothing is parked by this call. The existing entry keeps its own
			// stamp, so this is a no-op when the task is parked and a clear when
			// it is not.
			h.clearQueuedAtStampIfNotDeferred(task.TaskIdentifier)
			return true, nil
		}
	}
	h.deferredRespawns[task.TaskIdentifier] = deferredEntry{
		task:       task,
		config:     *config,
		retryAfter: h.currentDateTime.Now().Time().Add(concurrencyCapRetryDelay),
	}
	h.deferredMu.Unlock()
	return true, nil
}

// stampQueueEntry returns the task carrying a queued_at value, writing that
// value to the vault the first time the task is deferred behind the concurrency
// cap. queued_at is the moment the FIRST wait began: it is written once and never
// moved by a later re-deferral, so the field can be read as a queue-entry time.
//
// Four cases, in order:
//
//   - the task already carries queued_at (a re-deferred in-memory entry) — return
//     it unchanged and publish nothing.
//   - the task view already carries queued_at — return a copy carrying it. The
//     view is the authority here because a duplicate Kafka event can arrive after
//     the publish but before the controller has written the field; re-stamping
//     would move the timestamp forward.
//   - this executor already published queued_at for the task — return a copy
//     carrying that value and publish nothing. This is the case a missing task
//     file depends on: both checks above read state only a SUCCESSFUL vault write
//     updates, so for a task whose file was deleted (or whose write keeps failing)
//     neither ever becomes true and the deferral would stamp a fresh value on
//     every retry — once per concurrencyCapRetryDelay, indefinitely. Measured on
//     nukedev 2026-10-23: one update-frontmatter command per minute for a task id
//     with no file anywhere, each one a not-found at the controller.
//   - none of the above — stamp the injected clock's current UTC time, record it,
//     publish it, and return a copy carrying it.
//
// A failed publish is deliberately not retried. The returned copy still carries
// the value, so the next re-deferral sees it and publishes nothing: that wait
// reports no queue age rather than publishing a later, wrong one.
func (h *taskEventHandler) stampQueueEntry(ctx context.Context, task lib.Task) lib.Task {
	if v, ok := task.Frontmatter.String("queued_at"); ok && v != "" {
		return task
	}
	if v := h.queuedAtFromView(task.TaskIdentifier); v != "" {
		return taskWithFrontmatterValue(task, "queued_at", v)
	}
	if v := h.queuedAtStamp(task.TaskIdentifier); v != "" {
		return taskWithFrontmatterValue(task, "queued_at", v)
	}
	queuedAt := h.currentDateTime.Now().UTC().Format("2006-01-02T15:04:05Z07:00")
	h.recordQueuedAtStamp(task.TaskIdentifier, queuedAt)
	if err := h.resultPublisher.PublishQueueEntry(ctx, task, queuedAt); err != nil {
		glog.Warningf(
			"event=queued_at_publish_failed task=%s queued_at=%s err=%v",
			task.TaskIdentifier, queuedAt, err,
		)
	} else {
		glog.V(1).Infof(
			"event=queued_at_written task=%s queued_at=%s",
			task.TaskIdentifier, queuedAt,
		)
	}
	return taskWithFrontmatterValue(task, "queued_at", queuedAt)
}

// queuedAtFromView returns the queued_at the view holds for id, or "".
//
// It exists for a duplicate event that arrives after PublishQueueEntry but
// before the controller has applied the write: without this lookup that event
// would stamp a second, later timestamp. The view is already in memory, so this
// costs a map lookup on the deferral path rather than a vault read.
func (h *taskEventHandler) queuedAtFromView(id lib.TaskIdentifier) string {
	viewTask, ok := h.view.Get(id)
	if !ok {
		return ""
	}
	v, ok := viewTask.Frontmatter.String("queued_at")
	if !ok || v == "" {
		return ""
	}
	return v
}

// queuedAtStamp returns the queued_at this executor already published for id, or "".
//
// It is the fallback for a task whose write can never land. queuedAtFromView only
// knows a value the vault accepted, so a task whose file was deleted keeps
// returning "" there and would re-stamp on every deferral; the record written by
// recordQueuedAtStamp survives that, because it is executor-local.
func (h *taskEventHandler) queuedAtStamp(id lib.TaskIdentifier) string {
	h.deferredMu.Lock()
	defer h.deferredMu.Unlock()
	return h.queuedAtStamps[id]
}

// recordQueuedAtStamp remembers the queued_at published for id, so a later
// deferral reuses it instead of publishing a later, wrong one.
//
// Recorded before the publish is attempted, not after: a failed publish is
// deliberately not retried (see stampQueueEntry), and recording only on success
// would let a task whose publish keeps failing re-stamp on every deferral — the
// same unbounded loop this record exists to stop.
func (h *taskEventHandler) recordQueuedAtStamp(id lib.TaskIdentifier, value string) {
	h.deferredMu.Lock()
	defer h.deferredMu.Unlock()
	h.queuedAtStamps[id] = value
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

// admissionPriority is the tie-break that decides which waiting task gets a free
// slot when several are eligible at once.
//
// Key, in order: a task that has NEVER been attempted (no job_started_at) sorts
// ahead of every task that has; among attempted tasks the earliest queue-entry
// time wins — queued_at, the moment the first wait began, falling back to
// job_started_at for a task that ran without ever being deferred.
//
// The "never attempted first" clause is the load-bearing one. Without it the order
// is whichever order the caller happens to walk first: on the old reconcile floor
// that was vault path order, which for the trading vault is filename order, so a
// child delegated today (named 20261021-…) sorts behind every older child and is
// never admitted while any of them is still eligible. Measured on prod 2026-10-21:
// 67 eligible children against maxConcurrentJobs 1, and both admissions in a
// 20-minute window were the first two files in filename order. Those older tasks
// are the ones already given up on — maxConsecutiveRedrives retires a task after
// three spawns at an unchanged phase — so replaying them ahead of fresh work is
// exactly what starves the pipeline.
func admissionPriority(task lib.Task) time.Time {
	startedAt, err := task.Frontmatter.JobStartedAt()
	if err != nil || startedAt.IsZero() {
		return time.Time{}
	}
	if v, ok := task.Frontmatter.String("queued_at"); ok && v != "" {
		if queuedAt, parseErr := time.Parse(time.RFC3339, v); parseErr == nil {
			return queuedAt.UTC()
		}
	}
	return startedAt.UTC()
}

// ReconcileOnce runs a single reconcile pass over a point-in-time snapshot of
// the in-memory task view: it applies the eligibility filter and drives every
// eligible task owned by this executor. There are two outcomes for an eligible
// task: a parked task — one whose routing value was cleared but whose declared
// task_type resolves to a Config — gets that Config's assignee written back onto
// it (no Job is spawned; the write's TaskUpdated drives the normal spawn path),
// and every other eligible task is re-driven through the existing
// spawnIfNeeded path.
//
// The reconcile loop is the source-of-truth floor (spec 005): it derives state
// from the world — the replayed task topic and live Jobs via the spawner —
// rather than from the in-memory maps a restart wipes, so a task whose
// concurrency-cap deferral was lost across an executor restart is re-driven
// without a new Kafka event and without a current_job value. It does NOT consult
// deferredRespawns.
//
// View-side conditions degrade gracefully: a not-ready view, a snapshot with no
// task from this executor's vault, and a snapshot in which every task is
// unstamped each skip the pass with a log line and the next tick retries.
// ReconcileOnce returns nil for all of them so the loop can never tear down the
// executor via service.Run.
func (h *taskEventHandler) ReconcileOnce(ctx context.Context) error {
	// Bound the whole pass so a large view cannot hold it open indefinitely
	// (spec 005 Failure Modes: "Pass bounded by a per-pass timeout; logs
	// evaluate count so cost is visible"). The snapshot itself is in memory; the
	// deadline bounds the per-task Job lookups and spawns that follow.
	passCtx, cancel := context.WithTimeout(ctx, h.reconcilePassTimeout)
	defer cancel()

	// One `list jobs` per assignee for the whole pass, instead of two per task.
	// Scoped to passCtx so the event path never reads a frozen snapshot.
	passCtx = withJobCache(passCtx)

	if !h.view.Ready() {
		metrics.ReconcilePassesTotal.WithLabelValues("view_not_ready").Inc()
		glog.Warningf("event=reconcile_view_not_ready task_vault=%s", h.taskVault)
		return nil
	}
	snapshot := h.view.Snapshot()
	outcome, seen := classifyVaults(snapshot, h.taskVault)
	switch outcome {
	case vaultOutcomeUnstamped:
		metrics.ReconcilePassesTotal.WithLabelValues("task_vault_unstamped").Inc()
		glog.Warningf("event=task_vault_unstamped tasks=%d", len(snapshot))
		return nil
	case vaultOutcomeMismatch:
		metrics.ReconcilePassesTotal.WithLabelValues("task_vault_mismatch").Inc()
		glog.Warningf(
			"event=task_vault_mismatch task_vault=%s seen=%s",
			h.taskVault, strings.Join(seen, ","),
		)
		return nil
	}

	// Once per pass: forget successes older than the grace window, so the
	// record stays bounded to results that may still be in flight.
	h.taskStore.PruneSucceededBefore(
		h.currentDateTime.Now().Time().Add(-defaultRespawnGracePeriod),
	)
	owned := ownedTasks(snapshot, h.taskVault)
	c := reconcileCounters{}
	// Two phases, deliberately. The gather below filters every task and holds
	// the eligible ones; the drive loop then admits them in admission order.
	// Spawning inside the gather loop would admit whichever task the map
	// iteration happened to yield first, so a child delegated today sorts behind
	// every older child and is never reached while any of them is still eligible.
	candidates := h.gatherCandidates(passCtx, owned, &c)
	if h.passDone(passCtx) {
		c.logAborted()
		return nil
	}
	sortCandidatesByAdmissionPriority(candidates)
	for _, candidate := range candidates {
		if h.passDone(passCtx) {
			c.logAborted()
			return nil
		}
		c.add(h.driveCandidate(passCtx, candidate))
	}
	// Counted on every pass that got past the vault classification, so
	// rate(ok)==0 is a true "the backstop is not running" signal rather than an
	// absence of errors.
	metrics.ReconcilePassesTotal.WithLabelValues("ok").Inc()
	glog.Infof(
		"event=reconcile evaluated=%d re_driven=%d deferred=%d skipped=%d capped=%d task_vault=%q",
		c.evaluated, c.redriven, c.deferred, c.skipped, c.capped, h.taskVault,
	)
	return nil
}

// vaultPassOutcome is what a ready view snapshot says about task ownership.
type vaultPassOutcome int

const (
	vaultOutcomeOK vaultPassOutcome = iota
	vaultOutcomeMismatch
	vaultOutcomeUnstamped
)

// classifyVaults decides whether a ready view snapshot lets the pass run and
// returns the distinct non-empty vault slugs it saw, sorted.
//
// The mismatch and unstamped outcomes are unconditional: a view that holds
// tasks but none from this executor's vault means the deployment is
// misconfigured, and idling would look identical to a healthy quiet fleet.
func classifyVaults(tasks []lib.Task, taskVault string) (vaultPassOutcome, []string) {
	if len(tasks) == 0 {
		return vaultOutcomeOK, nil
	}
	distinct := make(map[string]struct{}, len(tasks))
	owned := false
	for _, task := range tasks {
		if task.Vault == "" {
			continue
		}
		distinct[task.Vault] = struct{}{}
		if task.Vault == taskVault {
			owned = true
		}
	}
	if len(distinct) == 0 {
		return vaultOutcomeUnstamped, nil
	}
	seen := make([]string, 0, len(distinct))
	for vault := range distinct {
		seen = append(seen, vault)
	}
	sort.Strings(seen)
	if !owned {
		return vaultOutcomeMismatch, seen
	}
	return vaultOutcomeOK, seen
}

// ownedTasks returns the tasks whose stamped vault equals taskVault.
func ownedTasks(tasks []lib.Task, taskVault string) []lib.Task {
	owned := make([]lib.Task, 0, len(tasks))
	for _, task := range tasks {
		if task.Vault == taskVault {
			owned = append(owned, task)
		}
	}
	return owned
}

// passDone reports whether the reconcile pass has run out of time. It receives
// from Done rather than reading Err so the cancellation check keeps the shape the
// rest of this file uses; the pass is bounded by reconcilePassTimeout.
func (h *taskEventHandler) passDone(passCtx context.Context) bool {
	select {
	case <-passCtx.Done():
		return true
	default:
		return false
	}
}

// reconcileCounters is the per-pass tally. A struct rather than five locals so the
// gather helper, the drive loop and the abort path share it without ReconcileOnce
// growing past the funlen bound.
type reconcileCounters struct {
	evaluated, redriven, deferred, skipped, capped int
}

// add folds one outcome into the tally. reconcileOutcomeCollected is never passed
// here: a collected candidate is driven in the second phase, which reports its own
// outcome.
func (c *reconcileCounters) add(outcome reconcileOutcome) {
	switch outcome {
	case reconcileOutcomeRedriven:
		c.evaluated++
		c.redriven++
	case reconcileOutcomeDeferred:
		c.evaluated++
		c.deferred++
	case reconcileOutcomeCapped:
		c.evaluated++
		c.capped++
	default:
		c.skipped++
	}
}

// logAborted records a pass cut short by its context, with the tally reached so far.
func (c *reconcileCounters) logAborted() {
	metrics.ReconcilePassesTotal.WithLabelValues("aborted").Inc()
	glog.Infof(
		"event=reconcile_aborted reason=context_cancelled evaluated=%d re_driven=%d deferred=%d skipped=%d",
		c.evaluated,
		c.redriven,
		c.deferred,
		c.skipped,
	)
}

// gatherCandidates runs the pass's filter phase, returning the eligible tasks
// and tallying everything else into c. It stops early when the pass context is
// done; the caller checks passDone to tell an early stop from a complete gather.
func (h *taskEventHandler) gatherCandidates(
	passCtx context.Context,
	tasks []lib.Task,
	c *reconcileCounters,
) []reconcileCandidate {
	candidates := make([]reconcileCandidate, 0, len(tasks))
	for _, task := range tasks {
		if h.passDone(passCtx) {
			return candidates
		}
		candidate, outcome := h.gatherCandidate(passCtx, task)
		if outcome == reconcileOutcomeCollected {
			candidates = append(candidates, candidate)
			continue
		}
		// The parked-assignee restore publishes a routing value and consumes no
		// spawn budget, so it reports from here rather than competing for a slot.
		c.add(outcome)
	}
	return candidates
}

// reconcileOutcome is what one task contributed to a pass's counters.
// The three decision outcomes count as evaluated; Skipped covers an ineligible
// task or a spawn error.
//
// One deliberate difference from the pre-extraction accounting: a spawn error
// used to increment BOTH evaluated and skipped, because evaluated was incremented
// before spawnIfNeeded was called. It now counts as skipped only, so evaluated is
// lower by one per spawn error.
type reconcileOutcome int

const (
	reconcileOutcomeSkipped reconcileOutcome = iota
	reconcileOutcomeRedriven
	reconcileOutcomeDeferred
	reconcileOutcomeCapped
	// reconcileOutcomeCollected means the task is eligible and has been held for
	// the pass's admission-ordered drive phase; it contributes nothing to the
	// counters yet.
	reconcileOutcomeCollected
)

// reconcileCandidate is one eligible task waiting to be driven. The pass
// collects these first and sorts them before spawning anything, so admission
// follows the queue's own order rather than map iteration order.
type reconcileCandidate struct {
	task   lib.Task
	config *pkg.AgentConfiguration
	// claim is the removal epoch captured at gather time, so an eviction landing
	// between the gather and the drive invalidates the drive.
	claim deferredClaim
}

// sortCandidatesByAdmissionPriority orders the pass's candidates in place so the
// first one driven is the one that most deserves a free slot. See
// admissionPriority for the rule and the measurement behind it.
func sortCandidatesByAdmissionPriority(candidates []reconcileCandidate) {
	sort.SliceStable(candidates, func(i, j int) bool {
		return admissionPriority(candidates[i].task).Before(admissionPriority(candidates[j].task))
	})
}

// gatherCandidate applies the eligibility filter to one snapshot task,
// returning the candidate for the pass's drive phase. It owns everything that
// does not compete for a spawn slot: the filter and the parked-assignee restore.
//
// Snapshot tasks are treated as read-only. The snapshot is a shallow copy and
// its frontmatter map is shared with the view, so nothing here may write into it
// in place.
func (h *taskEventHandler) gatherCandidate(
	ctx context.Context,
	task lib.Task,
) (reconcileCandidate, reconcileOutcome) {
	task, config, ok, err := h.reconcileTask(ctx, task)
	if err != nil {
		glog.Warningf(
			"event=reconcile_skip task=%s reason=resolve_error task_type=%s err=%v",
			task.TaskIdentifier,
			task.Frontmatter.TaskType(),
			err,
		)
		return reconcileCandidate{}, reconcileOutcomeSkipped
	}
	if !ok {
		// Left the eligibility filter — it advanced, or it carries no routing
		// information at all (an empty assignee whose declared task_type resolves
		// to no Config). Nothing to clear here: the streak's phase key is what
		// retires a task that genuinely moved on, and a task that left the filter
		// is not spawned again either way.
		return reconcileCandidate{}, reconcileOutcomeSkipped
	}
	// A parked task whose declared task_type resolved to a Config: restoring the
	// routing value is the whole action. The restore consumes no spawn budget, so
	// it happens here in the gather phase rather than being ordered against the
	// admission queue — a task the floor already re-drove maxConsecutiveRedrives
	// times at this phase must still get its routing value back, otherwise it
	// stays parked forever, which is exactly the re-drive-and-re-park class this
	// recovery path exists for. No Job is spawned here: the restore changes the
	// file, the controller publishes TaskUpdated for it, and that event drives the
	// normal spawn path.
	if task.Frontmatter.Assignee() == "" {
		return reconcileCandidate{}, h.restoreParkedAssignee(ctx, task, config)
	}
	candidate := reconcileCandidate{task: task, config: config}
	// Pin the removal epoch now: a retag eviction between this gather and the
	// drive must invalidate the drive rather than being lost.
	if task.TaskIdentifier != "" {
		h.deferredMu.Lock()
		candidate.claim = deferredClaim{
			id:    task.TaskIdentifier,
			epoch: h.deferredEpochs[task.TaskIdentifier],
		}
		h.deferredMu.Unlock()
	}
	return candidate, reconcileOutcomeCollected
}

// driveCandidate drives one collected candidate: the consecutive-re-drive cap and
// the spawn. ReconcileOnce calls it in admission order, so the first candidate
// that finds a free slot takes it and every later one is deferred by
// deferIfAtConcurrencyCap.
func (h *taskEventHandler) driveCandidate(
	ctx context.Context,
	candidate reconcileCandidate,
) reconcileOutcome {
	// Checked here as well as in spawnIfNeeded so the pass summary can report the
	// skip as `capped` rather than folding it into `deferred`. Only the capped
	// branch signals, so the two checks cannot double-count.
	if h.redriveStreakCapped(candidate.task) {
		return reconcileOutcomeCapped
	}
	spawned, err := h.spawnIfNeededClaimed(
		ctx,
		candidate.task,
		candidate.config,
		&candidate.claim,
	)
	if err != nil {
		glog.Warningf(
			"event=reconcile_skip task=%s reason=spawn_error err=%v",
			candidate.task.TaskIdentifier,
			err,
		)
		return reconcileOutcomeSkipped
	}
	if !spawned {
		return reconcileOutcomeDeferred
	}
	metrics.ReconcileRedrivenTotal.Inc()
	glog.Infof("event=reconcile_redrive task=%s", candidate.task.TaskIdentifier)
	return reconcileOutcomeRedriven
}

// restoreParkedAssignee publishes the resolved configuration's assignee back onto
// a parked task and reports the outcome. See reconcileOne for why it branches
// before the consecutive-re-drive cap and why no Job is spawned here.
//
// A publish failure is a skip, not a pass failure: ReconcileOnce never returns a
// vault-side error, and the next tick retries.
func (h *taskEventHandler) restoreParkedAssignee(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
) reconcileOutcome {
	if err := h.resultPublisher.PublishRestoreAssignee(ctx, task, config.Assignee); err != nil {
		glog.Warningf(
			"event=reconcile_skip task=%s reason=restore_error err=%v",
			task.TaskIdentifier,
			err,
		)
		return reconcileOutcomeSkipped
	}
	metrics.ReconcileRedrivenTotal.Inc()
	glog.Infof(
		"event=reconcile_redrive task=%s reason=restore_assignee assignee=%s",
		task.TaskIdentifier,
		config.Assignee,
	)
	return reconcileOutcomeRedriven
}

// reconcileTask applies the reconcile eligibility filter to one snapshot task.
// Returns (task, config, ok, err): ok=false when the task is not eligible (status
// not in the Config's trigger set, phase not in the Config's trigger set, stage
// mismatch, or no routing information — an empty assignee whose declared
// task_type resolves to no Config) — mirroring parseAndFilter's gates, except
// that a parked task whose declared task_type resolves IS eligible here and is
// not eligible on the Kafka path.
//
// The trigger sets come from the resolved Config via effectiveTriggerPhases /
// effectiveTriggerStatuses, NOT from the hardcoded defaults. Those helpers still
// fall back to the defaults when the Config carries no trigger, so a Config that
// omits the block behaves exactly as before; a Config that declares a narrower
// list is now honoured on this path too.
//
// This supersedes spec 005 AC 1, which wrote the default set into the criterion
// ("phase in {planning, execution, ai_review}"). That wording predates per-Config
// trigger narrowing and made the Config authoritative on the Kafka path only:
// parseAndFilter honoured trigger.phases while this floor ignored it, so a Config
// narrowing had no effect here and a task sitting at a dropped phase was re-driven
// every tick forever. Observed 2026-09-24 on the build-fix lane — narrowing its
// Config to [execution] left a `planning` fixture re-driven at ~1/min by
// `event=reconcile_redrive`, because this function gated on the default set.
// The floor's purpose (recover tasks a restart orphaned) is unaffected: an
// orphaned task is at a phase its own Config lists, so it is still re-driven.
func (h *taskEventHandler) reconcileTask(
	ctx context.Context,
	task lib.Task,
) (lib.Task, *pkg.AgentConfiguration, bool, error) {
	if h.isForeignCluster(task) {
		// Backstop for a reconcile-driven cap deferral that stored the pre-retag
		// snapshot after the Kafka retag event was already consumed.
		h.removeDeferredEntry(task.TaskIdentifier)
		return task, nil, false, nil
	}
	config, skip, err := h.resolveConfigForReconcile(ctx, task)
	if err != nil {
		return task, nil, false, err
	}
	if skip {
		return task, nil, false, nil
	}
	if !effectiveTriggerStatuses(config).Contains(task.Frontmatter.Status()) {
		return task, nil, false, nil
	}
	phase := task.Frontmatter.Phase()
	if phase == nil || !effectiveTriggerPhases(config).Contains(*phase) {
		return task, nil, false, nil
	}
	// A terminal task is never driven, even when a Config lists its phase in
	// trigger.phases. The event path already refuses these in applyPhaseGate; this
	// floor needs the same rule because the task view retains a task's body only
	// while it can still spawn, so driving a terminal task here would hand the
	// agent an empty task file instead of the task text.
	if IsTerminal(*phase) {
		return task, nil, false, nil
	}
	if h.stageFilter && task.Frontmatter.Stage() != string(h.branch) {
		return task, nil, false, nil
	}
	// A parked task (empty routing value) is eligible when its declared
	// task_type resolved to a Config on this floor-only path. resolveConfig
	// returns no Config for an empty assignee, so a non-nil config here means
	// the declared type resolved. A task with neither is still ineligible.
	if task.Frontmatter.Assignee() == "" && config == nil {
		return task, nil, false, nil
	}
	return task, config, true, nil
}

// redriveStreak is the executor's record of consecutive spawns for one task at
// one phase: how many, and which phase they accrued in. See maxConsecutiveRedrives
// for why the phase is the reset and why the streak is deliberately not persisted.
type redriveStreak struct {
	count int
	phase string
}

// redriveStreakCapped reports whether the executor must skip this task because it
// has spawned it maxConsecutiveRedrives times in a row at an unchanged phase, and
// records the skip signal when it must.
//
// Skipping is the whole bound. The streak is deliberately NOT cleared here, so the
// task stays capped until something moves it — clearing on the cap would let the
// next spawn through and make the cap a no-op.
//
// Logged at V(1), not V(0): a capped task repeats this line on every event and
// every pass for as long as it stays capped, which is an expected steady state —
// the same reason event=concurrency_cap is V(1). The metric carries the alerting
// signal.
//
// The phase comparison is not redundant with recordRedriveSpawn's reset. The
// streak is only ever rewritten AFTER a spawn succeeds, so a task that advanced
// to a new phase still carries its old phase in the streak until that spawn
// happens — and a count-only check would cap it before it ever could, stranding
// exactly the task the reset exists to release.
func (h *taskEventHandler) redriveStreakCapped(task lib.Task) bool {
	id := task.TaskIdentifier
	phase := taskPhaseString(task)
	h.redriveMu.Lock()
	streak, ok := h.redriveStreaks[id]
	h.redriveMu.Unlock()
	if !ok || streak.phase != phase || streak.count < maxConsecutiveRedrives {
		return false
	}
	metrics.RedriveCappedTotal.Inc()
	glog.V(1).Infof(
		"event=redrive_capped task=%s streak=%d cap=%d phase=%q",
		id, streak.count, maxConsecutiveRedrives, streak.phase,
	)
	return true
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

// recordRedriveSpawn extends this task's streak with the spawn that just happened,
// resetting it when the task has moved phase since the previous one.
//
// Called only after SpawnJob returned successfully, so a suppressed, deferred or
// capped spawn does not count — the streak measures spawns, not attempts.
func (h *taskEventHandler) recordRedriveSpawn(task lib.Task) {
	id := task.TaskIdentifier
	phase := taskPhaseString(task)
	h.redriveMu.Lock()
	defer h.redriveMu.Unlock()
	if prev, ok := h.redriveStreaks[id]; ok && prev.phase == phase {
		h.redriveStreaks[id] = redriveStreak{count: prev.count + 1, phase: phase}
		return
	}
	h.redriveStreaks[id] = redriveStreak{count: 1, phase: phase}
}

// clearRedriveStreak ends this task's streak. Called when the task reaches a
// terminal state, where a further spawn is an operator decision rather than a
// re-drive of the loop this cap exists to bound.
func (h *taskEventHandler) clearRedriveStreak(id lib.TaskIdentifier) {
	h.redriveMu.Lock()
	defer h.redriveMu.Unlock()
	delete(h.redriveStreaks, id)
}

// skipForeignVaultTask reports whether the spawn must be dropped because the
// task is not owned by this executor, recording the skip signal when it must.
//
// Ownership is a plain comparison: the task's stamped vault must equal this
// executor's TASK_VAULT. The Kafka consumer group is shared across every
// executor deployment — a task's events reach whichever instance owns the
// partition, not the instance whose vault holds the file — so without this test
// every instance drives every vault's tasks. A task with no stamped vault is
// never owned.
//
// It is the cheapest and the widest gate in spawnIfNeeded — a foreign task is
// not this instance's to spawn, whatever path delivered the event — so it runs
// first, and it is factored out only to keep spawnIfNeeded inside funlen.
func (h *taskEventHandler) skipForeignVaultTask(task lib.Task) bool {
	if task.Vault == h.taskVault {
		return false
	}
	metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_vault").Inc()
	glog.V(2).Infof(
		"skip task %s: foreign vault vault=%s task_vault=%s",
		task.TaskIdentifier, task.Vault, h.taskVault,
	)
	return true
}

// RunReconcileLoop runs one reconcile pass immediately at startup, then every
// reconcile interval until ctx is cancelled. ReconcileOnce returns nil for all
// view-side conditions, so this loop cannot crash the executor — the next tick
// just retries (spec 005 Failure Modes).
func (h *taskEventHandler) RunReconcileLoop(ctx context.Context) error {
	if err := h.ReconcileOnce(ctx); err != nil {
		glog.Errorf("event=reconcile initial pass failed: %v", err)
	}
	ticker := time.NewTicker(h.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := h.ReconcileOnce(ctx); err != nil {
				glog.Errorf("event=reconcile tick failed: %v", err)
			}
		}
	}
}

// triggerScopeRefLen is how much of the ref is kept in a scope key. A short prefix
// is enough to distinguish commits while keeping the frontmatter value readable in
// the task file, where an operator has to be able to eyeball it.
const triggerScopeRefLen = 8

// triggerScope keys the spawn budget to the work being attempted rather than to the
// task's whole lifetime.
//
// A re-dispatch that represents real progress — a new lifecycle phase, or a new
// commit on the target repo — produces a different scope and earns a fresh budget.
// A deterministic failure retried against the same phase and the same ref keeps the
// same scope and burns the budget down. That difference is what lets the cap run by
// default instead of opt-in.
//
// Phase comes from the normalizing accessor, so a phase alias does not split a scope
// and silently hand out a second budget. ref is absent on human-authored tasks
// (it is written by the agents that clone a repo, alongside clone_url and base_ref);
// those tasks scope on phase alone, which is the correct degradation — there is no
// commit for them to make progress against.
func triggerScope(fm lib.TaskFrontmatter) string {
	var phase string
	if p := fm.Phase(); p != nil {
		phase = string(*p)
	}
	ref, _ := fm.String("ref")
	if len(ref) > triggerScopeRefLen {
		ref = ref[:triggerScopeRefLen]
	}
	return phase + ":" + ref
}

// applyTriggerBudget evaluates the scoped trigger cap for one spawn decision and,
// when the spawn may proceed, publishes exactly one counter write recording it.
// Returns capped=true when the cap is reached and the spawn must be skipped.
//
// The cap default-engages for repo-backed tasks: a task whose frontmatter
// carries a ref (written by the agents that clone a repo, alongside clone_url
// and base_ref) scopes its budget to phase+ref via triggerScope, and that scope
// moves with the repo, so accrual against a constant scope cannot occur — the
// safe default-on form. An explicit max_triggers engages the cap for any task,
// with or without a ref. A task with neither ref nor max_triggers is a
// recurring task: it is not repo-backed, sits at a stable phase, and its scope
// is a constant, so a default cap would accrue across re-dispatches and fire at
// 3, re-creating the 2026-08-27 Daily Sentry Triage incident (the v0.7.1
// regression). That task stays uncapped by default.
//
// The cap value is always MaxTriggers(): the explicit max_triggers field, or
// the lib default of 3 when it is absent — a repo-backed task with no
// max_triggers is therefore capped at 3.
//
// Scoping also fixes the opt-in cap's correctness: a task that opts in no
// longer burns its budget across unrelated attempts, so an operator can set a
// tight max_triggers without it leaking into the next phase or the next commit.
// checkTriggerBudget reports whether the task's scoped trigger budget is
// exhausted. It writes nothing.
//
// The counter write is deliberately split out into recordTriggerSpawn and
// published only AFTER a Job was actually created. Until 2026-10-19 the
// increment was published here, before SpawnJob, so every infrastructure
// failure — the `spawn_error ... context deadline exceeded` this change also
// fixes — consumed one of the task's attempts without the agent ever running.
// That is how github-update-go tasks reached trigger_count 5-7 while producing
// no work, and why a trigger_count reset alone could not hold.
func (h *taskEventHandler) checkTriggerBudget(task lib.Task) bool {
	currentScope := triggerScope(task.Frontmatter)
	storedScope, hasScope := task.Frontmatter.String("trigger_scope")

	// An ABSENT scope is not a CHANGED scope. Every task in flight today predates
	// this field, so treating absent as changed would hand each of them a free
	// budget reset on the first event after deploy — including one already looping
	// at cap, which would resume for another N. Absent means "adopt the current
	// scope, keep the count".
	scopeChanged := hasScope && storedScope != currentScope

	_, optedIn := task.Frontmatter["max_triggers"]
	_, hasRef := task.Frontmatter.String("ref")
	if (optedIn || hasRef) && !scopeChanged &&
		task.Frontmatter.TriggerCount() >= task.Frontmatter.MaxTriggers() {
		glog.V(2).Infof("skip task %s: trigger_count %d >= max_triggers %d in scope %q",
			task.TaskIdentifier,
			task.Frontmatter.TriggerCount(),
			task.Frontmatter.MaxTriggers(),
			currentScope,
		)
		metrics.TaskEventsTotal.WithLabelValues("skipped_trigger_cap").Inc()
		return true
	}
	return false
}

// recordTriggerSpawn publishes the counter write that charges one spawn against
// the task's scoped budget. Called only after SpawnJob succeeded.
//
// Exactly one publish per spawn in every branch. Splitting "write the scope"
// and "count the spawn" into two commands would emit two writes for one spawn
// and let them interleave with a concurrent write.
func (h *taskEventHandler) recordTriggerSpawn(ctx context.Context, task lib.Task) error {
	currentScope := triggerScope(task.Frontmatter)
	storedScope, hasScope := task.Frontmatter.String("trigger_scope")
	scopeChanged := hasScope && storedScope != currentScope

	count, useScopeWrite := triggerBudgetWrite(task.Frontmatter, scopeChanged, hasScope)
	if !useScopeWrite {
		if err := h.resultPublisher.PublishIncrementTriggerCount(ctx, task); err != nil {
			return errors.Wrapf(
				ctx, err, "publish increment trigger_count for task %s", task.TaskIdentifier,
			)
		}
		return nil
	}

	glog.V(2).Infof("task %s: trigger scope %q -> %q, trigger_count -> %d",
		task.TaskIdentifier, storedScope, currentScope, count,
	)
	if err := h.resultPublisher.PublishSetTriggerScope(ctx, task, currentScope, count); err != nil {
		return errors.Wrapf(
			ctx, err, "publish set trigger scope for task %s", task.TaskIdentifier,
		)
	}
	return nil
}

// triggerBudgetWrite decides which counter write a spawn needs. useScopeWrite
// reports whether the scope itself must be persisted alongside the count.
//
//   - scope changed: fresh budget at 1, counting the spawn about to happen.
//   - scope absent: adopt at count+1 — carry the existing attempts forward rather
//     than resetting, so the migration to this field grants nobody a free budget.
//   - scope unchanged: no scope write; the ordinary increment path applies.
func triggerBudgetWrite(
	fm lib.TaskFrontmatter,
	scopeChanged bool,
	hasScope bool,
) (int, bool) {
	switch {
	case scopeChanged:
		return 1, true
	case !hasScope:
		return fm.TriggerCount() + 1, true
	default:
		return 0, false
	}
}

// taskCluster returns the task's optional "cluster" frontmatter value, or ""
// when the task names no cluster.
func taskCluster(task lib.Task) string {
	v, _ := task.Frontmatter["cluster"].(string)
	return v
}

// isForeignCluster reports whether the task names a cluster other than the one
// this executor runs in. A task without a cluster is foreign only when the
// executor opted out of untagged tasks (RUN_UNTAGGED=false), so tasks filed
// before the field existed keep their previous routing on every other cluster.
func (h *taskEventHandler) isForeignCluster(task lib.Task) bool {
	c := taskCluster(task)
	if c == "" {
		return !h.runUntagged
	}
	return c != h.cluster
}

// skipForeignCluster logs and counts a task routed to another cluster and
// reports whether the Kafka path must skip it.
func (h *taskEventHandler) skipForeignCluster(task lib.Task) bool {
	if !h.isForeignCluster(task) {
		return false
	}
	if taskCluster(task) == "" {
		glog.V(2).
			Infof("skip task %s: untagged (executor cluster %s runs only tagged tasks)", task.TaskIdentifier, h.cluster)
		metrics.TaskEventsTotal.WithLabelValues("skipped_untagged").Inc()
		return true
	}
	glog.V(2).Infof(
		"skip task %s: foreign cluster %s (executor cluster %s)",
		task.TaskIdentifier, taskCluster(task), h.cluster,
	)
	metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_cluster").Inc()
	return true
}
