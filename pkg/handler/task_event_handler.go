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
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
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
const defaultRespawnGracePeriod = 300 * time.Second

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
// 2026-10-21 a sun executor reading the 15k-file openclaw vault over the
// git-rest ingress (~7 ms per GET) aborted every pass at 45s with evaluated=0.
const DefaultReconcilePassTimeout = 45 * time.Second

// deferredEntry tracks a task whose respawn was suppressed by the grace window.
// The executor re-evaluates it once retryAfter is reached.
type deferredEntry struct {
	task       lib.Task
	config     pkg.AgentConfiguration
	retryAfter time.Time
}

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
) TaskEventHandler {
	return &taskEventHandler{
		jobSpawner:           jobSpawner,
		branch:               branch,
		cluster:              cluster,
		stageFilter:          stageFilter,
		runUntagged:          runUntagged,
		resolver:             resolver,
		resultPublisher:      resultPublisher,
		taskStore:            taskStore,
		currentDateTime:      currentDateTime,
		deferredRespawns:     make(map[lib.TaskIdentifier]deferredEntry),
		spawnLocks:           make(map[string]*sync.Mutex),
		view:                 view,
		taskVault:            taskVault,
		reconcilePassTimeout: reconcilePassTimeout,
		reconcileInterval:    reconcileInterval,
		redriveStreaks:       make(map[lib.TaskIdentifier]redriveStreak),
	}
}

type taskEventHandler struct {
	jobSpawner           spawner.JobSpawner
	branch               base.Branch
	cluster              string
	stageFilter          bool
	runUntagged          bool
	resolver             pkg.ConfigResolver
	resultPublisher      pkg.ResultPublisher
	taskStore            *pkg.TaskStore
	currentDateTime      libtime.CurrentDateTimeGetter
	deferredMu           sync.Mutex
	deferredRespawns     map[lib.TaskIdentifier]deferredEntry
	spawnLocksMu         sync.Mutex
	spawnLocks           map[string]*sync.Mutex
	view                 TaskView
	taskVault            string
	reconcilePassTimeout time.Duration
	reconcileInterval    time.Duration
	redriveMu            sync.Mutex
	redriveStreaks       map[lib.TaskIdentifier]redriveStreak
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
	if config == nil || config.Assignee == "" {
		active, err := h.jobSpawner.IsJobActive(ctx, task.TaskIdentifier)
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
	activeSet, err := h.activeJobsFor(ctx, config.Assignee)
	if err != nil {
		return false, errors.Wrapf(ctx, err, "check active job for task %s", task.TaskIdentifier)
	}
	return activeSet[task.TaskIdentifier], nil
}

// cleanupTerminalTask clears in-flight state when a task has reached a terminal
// status (completed/aborted). taskStore.Delete stops the job informer from emitting
// a spurious synthetic failure after the agent published success. removeDeferredEntry
// cancels any pending grace-window respawn — the terminal event is skipped by the
// status filter in parseAndFilter BEFORE reaching the terminal-phase gate that would
// otherwise clear the entry, so without this a deferred entry created during the grace
// window fires ~300s later and respawns a job for an already-done task (the "path C"
// respawn observed on dev 2026-07-13 for probe 095c58d7).
func (h *taskEventHandler) cleanupTerminalTask(task lib.Task) {
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
	if stored, ok := h.taskStore.Load(task.TaskIdentifier); ok {
		if storedJob := stored.Frontmatter.CurrentJob(); storedJob != "" &&
			storedJob != task.Frontmatter.CurrentJob() {
			glog.V(3).Infof(
				"task %s %s: stale terminal event (store current_job=%s, event current_job=%s) — keeping store entry",
				task.TaskIdentifier, status, storedJob, task.Frontmatter.CurrentJob(),
			)
			return
		}
	}
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

	h.cleanupTerminalTask(task)

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
		metrics.TaskEventsTotal.WithLabelValues("type_mismatch").Inc()
		return lib.Task{}, nil, true, nil
	}

	if !effectiveTriggerStatuses(config).Contains(task.Frontmatter.Status()) {
		glog.V(3).
			Infof("skip task %s with status %s", task.TaskIdentifier, task.Frontmatter.Status())
		metrics.TaskEventsTotal.WithLabelValues("skipped_status").Inc()
		return lib.Task{}, nil, true, nil
	}

	phase := task.Frontmatter.Phase()
	// terminal phases must not be spawned again — operator escalation required
	if phase != nil && applyPhaseGate(task, *phase) {
		h.removeDeferredEntry(task.TaskIdentifier)
		return lib.Task{}, nil, true, nil
	}
	if phase == nil || !effectiveTriggerPhases(config).Contains(*phase) {
		glog.V(3).Infof("skip task %s with phase %v", task.TaskIdentifier, phase)
		metrics.TaskEventsTotal.WithLabelValues("skipped_phase").Inc()
		return lib.Task{}, nil, true, nil
	}

	stage := task.Frontmatter.Stage()
	if h.stageFilter && stage != string(h.branch) {
		glog.V(3).Infof(
			"skip task %s with stage %s (executor branch %s)",
			task.TaskIdentifier, stage, h.branch,
		)
		metrics.TaskEventsTotal.WithLabelValues("skipped_stage").Inc()
		return lib.Task{}, nil, true, nil
	}

	if task.Frontmatter.Assignee() == "" {
		glog.V(3).Infof("skip task %s with empty assignee", task.TaskIdentifier)
		metrics.TaskEventsTotal.WithLabelValues("skipped_assignee").Inc()
		return lib.Task{}, nil, true, nil
	}

	return task, config, false, nil
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
	resolved, err := h.resolver.ResolveByTaskType(ctx, string(taskType))
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

// checkActiveCurrentJob verifies whether spawn must be suppressed due to current_job state.
// Returns (true, nil) when the spawn must be suppressed (job still active or inside grace window).
// Returns (false, nil) when spawn may proceed. Returns (false, err) on unexpected errors.
func (h *taskEventHandler) checkActiveCurrentJob(
	ctx context.Context,
	task lib.Task,
	currentJob string,
	config *pkg.AgentConfiguration,
) (bool, error) {
	active, err := h.jobSpawner.IsJobActive(ctx, task.TaskIdentifier)
	if err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return false, errors.Wrapf(
			ctx,
			err,
			"check current_job active for task %s",
			task.TaskIdentifier,
		)
	}
	if active {
		glog.V(3).Infof(
			"skip task %s: current_job %s still active (from frontmatter)",
			task.TaskIdentifier, currentJob,
		)
		metrics.TaskEventsTotal.WithLabelValues("skipped_active_job").Inc()
		return true, nil
	}
	// Grace window: suppress respawn while the agent's terminal-phase write propagates.
	// Treat missing or unparseable job_started_at as elapsed (preserves legacy-task behavior).
	jobStartedAt, parseErr := task.Frontmatter.JobStartedAt()
	if parseErr != nil {
		glog.Warningf(
			"task %s: failed to parse job_started_at: %v; treating grace period as elapsed",
			task.TaskIdentifier, parseErr,
		)
	}
	if parseErr == nil && !jobStartedAt.IsZero() {
		elapsed := h.currentDateTime.Now().Time().Sub(jobStartedAt)
		if elapsed < defaultRespawnGracePeriod {
			glog.Infof(
				"event=respawn_grace_window task=%s current_job=%s elapsed=%.0fs",
				task.TaskIdentifier, currentJob, elapsed.Seconds(),
			)
			metrics.TaskEventsTotal.WithLabelValues("respawn_grace_window").Inc()
			if config != nil {
				retryAfter := jobStartedAt.Add(defaultRespawnGracePeriod)
				h.deferredMu.Lock()
				h.deferredRespawns[task.TaskIdentifier] = deferredEntry{
					task:       task,
					config:     *config,
					retryAfter: retryAfter,
				}
				h.deferredMu.Unlock()
			}
			return true, nil
		}
	}
	glog.V(2).Infof(
		"task %s: current_job %s no longer active, proceeding to spawn",
		task.TaskIdentifier, currentJob,
	)
	return false, nil
}

// spawnIfNeeded returns (spawned, err): spawned is true iff a new k8s Job was actually launched
// (i.e. the call reached SpawnJob successfully). All early-return branches (suppression, trigger
// cap, active job, terminal phase, errors) return spawned=false.
func (h *taskEventHandler) spawnIfNeeded(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
) (bool, error) {
	// Not this instance's task — see skipForeignVaultTask.
	if h.skipForeignVaultTask(task) {
		return false, nil
	}

	// If current_job is set in frontmatter, a prior spawn notification was written
	// to the task file. Verify the job is still active; if not, proceed to spawn.
	if currentJob := task.Frontmatter.CurrentJob(); currentJob != "" {
		suppress, err := h.checkActiveCurrentJob(ctx, task, currentJob, config)
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

	// The last bound, and the only one that covers every path: a task that has
	// been spawned maxConsecutiveRedrives times in a row at an unchanged phase is
	// looping, not advancing. Checked here rather than at the call sites because
	// the Kafka consumer, the deferred-respawn loop and the reconcile floor all
	// funnel through this function — see maxConsecutiveRedrives.
	if h.redriveStreakCapped(task) {
		return false, nil
	}

	capped, err := h.deferIfAtConcurrencyCap(ctx, task, config)
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

	jobName, err := h.jobSpawner.SpawnJob(ctx, task, *config)
	if err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return false, errors.Wrapf(ctx, err, "spawn job for task %s failed", task.TaskIdentifier)
	}

	// A Job now exists: charge the attempt. The error still propagates — a
	// counter write that cannot be recorded is a real fault, and the existing
	// callers treat it as one. It no longer prevents the spawn, which is the
	// point of moving the write here: a spawn that fails for infrastructure
	// reasons leaves the budget untouched, and a Job that did start is charged.
	// Record the Job before the counter write: if the write fails and this
	// returns an error, the in-memory store must still know the Job exists.
	h.bumpSpawnGen(config.Assignee)
	h.storeSpawnedJob(task, jobName)

	if err := h.recordTriggerSpawn(ctx, task); err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return false, err
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
	return true, nil
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
) (bool, error) {
	if config == nil || config.MaxConcurrentJobs <= 0 {
		return false, nil
	}
	activeSet, err := h.activeJobsFor(ctx, config.Assignee)
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
// Three cases, in order:
//
//   - the task already carries queued_at (a re-deferred in-memory entry) — return
//     it unchanged and publish nothing.
//   - the vault file already carries queued_at — return a copy carrying it. The
//     vault is the authority here because a duplicate Kafka event can arrive after
//     the publish but before the controller has written the field; re-stamping
//     would move the timestamp forward.
//   - neither — stamp the injected clock's current UTC time, publish it, and
//     return a copy carrying it so a later re-deferral takes the first branch.
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
	queuedAt := h.currentDateTime.Now().UTC().Format("2006-01-02T15:04:05Z07:00")
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
// is whichever path the caller happens to walk first: on the reconcile floor that
// is git-rest's path order, which for the trading vault is filename order, so a
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

// sortDeferredByAdmissionPriority orders the due deferred entries in place so the
// first one the loop reaches is the one that most deserves the freed slot. The
// collection step ranges a Go map, whose iteration order is randomized per tick,
// so without this sort the winner is drawn at random — the shape that lets the
// same task ids recur in the concurrency_cap log for hours while others are
// dispatched.
func sortDeferredByAdmissionPriority(entries []deferredEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		return admissionPriority(entries[i].task).Before(admissionPriority(entries[j].task))
	})
}

// evalDeferredRespawns checks all pending deferred-respawn entries and spawns a job
// for each entry whose retryAfter has been reached. Entries are removed once processed.
// The respawn_after_grace_window metric and log line fire ONLY when the call actually
// results in a spawn (spec 037 AC #6: "recorded each time the follow-up evaluation
// results in a spawn"); evaluations that no-op (active job already, trigger cap hit,
// terminal phase) do not increment the metric.
func (h *taskEventHandler) evalDeferredRespawns(ctx context.Context) error {
	now := h.currentDateTime.Now().Time()

	h.deferredMu.Lock()
	var ready []deferredEntry
	for taskID, entry := range h.deferredRespawns {
		if !now.Before(entry.retryAfter) {
			ready = append(ready, entry)
			delete(h.deferredRespawns, taskID)
		}
	}
	h.deferredMu.Unlock()

	// The collection above ranges a map, so `ready` arrives in a randomized order.
	// Sort before spawning: only the first entry can win the free slot (every later
	// one re-enters deferIfAtConcurrencyCap and is re-deferred), so leaving the
	// order random is what lets the same task ids recur in the cap log indefinitely.
	sortDeferredByAdmissionPriority(ready)

	for _, entry := range ready {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		entry := entry // capture for closure
		spawned, err := h.spawnIfNeeded(ctx, entry.task, &entry.config)
		if err != nil {
			return errors.Wrapf(
				ctx, err, "deferred respawn for task %s", entry.task.TaskIdentifier,
			)
		}
		if !spawned {
			continue
		}
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

// removeDeferredEntry removes any pending deferred-respawn entry for the given task.
// Called from parseAndFilter when a terminal-phase event arrives or when a foreign-cluster
// event skips the task, and from reconcileTask when the vault reads the task as foreign,
// so no stale spawn fires.
func (h *taskEventHandler) removeDeferredEntry(id lib.TaskIdentifier) {
	h.deferredMu.Lock()
	delete(h.deferredRespawns, id)
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
	return reconcileCandidate{task: task, config: config}, reconcileOutcomeCollected
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
	spawned, err := h.spawnIfNeeded(ctx, candidate.task, candidate.config)
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
