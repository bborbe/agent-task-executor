// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
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
	"github.com/bborbe/agent-task-executor/pkg/gitrestclient"
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

// defaultReconcileInterval is the interval between reconcile-loop passes
// (spec 005). Mirrors the DefaultZombieSweeperIntervalSeconds CRD knob: a
// package constant, not a CRD field and not env-configurable in this change.
// 60s is a reasonable recovery latency for a task whose deferral was lost
// across a restart.
const defaultReconcileInterval = 60 * time.Second

// reconcilePassTimeout bounds one reconcile pass so a slow or large vault
// cannot hold a pass open indefinitely (spec 005 Failure Modes: "Pass bounded
// by a per-pass timeout; logs evaluate count so cost is visible"). Half the
// interval: a pass completes within a tick and leaves headroom for the next
// tick. Package constant — not configurable in this change.
const reconcilePassTimeout = 30 * time.Second

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
	// ReconcileOnce runs one reconcile pass: lists the task files under the
	// configured task glob via git-rest, parses each file's frontmatter, and
	// re-drives every eligible task through the existing spawn path. Called by
	// RunReconcileLoop on each tick; also callable directly in tests.
	ReconcileOnce(ctx context.Context) error
	// RunReconcileLoop runs one pass immediately at startup, then every
	// defaultReconcileInterval until ctx is cancelled. Must be run
	// alongside the Kafka consumer.
	RunReconcileLoop(ctx context.Context) error
}

// NewTaskEventHandler creates a new TaskEventHandler.
func NewTaskEventHandler(
	jobSpawner spawner.JobSpawner,
	branch base.Branch,
	resolver pkg.ConfigResolver,
	resultPublisher pkg.ResultPublisher,
	taskStore *pkg.TaskStore,
	currentDateTime libtime.CurrentDateTimeGetter,
	gitRestClient gitrestclient.GitRestClient,
	taskGlob string,
) TaskEventHandler {
	return &taskEventHandler{
		jobSpawner:       jobSpawner,
		branch:           branch,
		resolver:         resolver,
		resultPublisher:  resultPublisher,
		taskStore:        taskStore,
		currentDateTime:  currentDateTime,
		deferredRespawns: make(map[lib.TaskIdentifier]deferredEntry),
		spawnLocks:       make(map[string]*sync.Mutex),
		gitRestClient:    gitRestClient,
		taskGlob:         taskGlob,
	}
}

type taskEventHandler struct {
	jobSpawner       spawner.JobSpawner
	branch           base.Branch
	resolver         pkg.ConfigResolver
	resultPublisher  pkg.ResultPublisher
	taskStore        *pkg.TaskStore
	currentDateTime  libtime.CurrentDateTimeGetter
	deferredMu       sync.Mutex
	deferredRespawns map[lib.TaskIdentifier]deferredEntry
	spawnLocksMu     sync.Mutex
	spawnLocks       map[string]*sync.Mutex
	gitRestClient    gitrestclient.GitRestClient
	taskGlob         string
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
	h.taskStore.Delete(task.TaskIdentifier)
	h.removeDeferredEntry(task.TaskIdentifier)
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
	if len(msg.Value) == 0 {
		glog.V(3).Infof("skip empty message at offset %d", msg.Offset)
		return lib.Task{}, nil, true, nil
	}

	var task lib.Task
	if err := json.Unmarshal(msg.Value, &task); err != nil {
		glog.Warningf("failed to unmarshal task event at offset %d: %v", msg.Offset, err)
		return lib.Task{}, nil, true, nil
	}

	if task.TaskIdentifier == "" {
		glog.Warningf("task event at offset %d has empty TaskIdentifier, skipping", msg.Offset)
		return lib.Task{}, nil, true, nil
	}

	h.cleanupTerminalTask(task)

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
		glog.V(3).Infof(
			"skip task %s with status %s", task.TaskIdentifier, task.Frontmatter.Status(),
		)
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
	if stage != string(h.branch) {
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

	active, err := h.jobSpawner.IsJobActive(ctx, task.TaskIdentifier)
	if err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return false, errors.Wrapf(ctx, err, "check active job for task %s", task.TaskIdentifier)
	}
	if active {
		glog.V(3).Infof("skip task %s: active job exists", task.TaskIdentifier)
		metrics.TaskEventsTotal.WithLabelValues("skipped_active_job").Inc()
		return false, nil
	}

	capped, err := h.deferIfAtConcurrencyCap(ctx, task, config)
	if err != nil {
		return false, err
	}
	if capped {
		return false, nil
	}

	// Scoped trigger budget: evaluates the opt-in cap against the task's current
	// phase+ref scope and publishes the counter write for the spawn about to
	// happen. Extracted so spawnIfNeeded stays legible; it owns one decision.
	capped, err = h.applyTriggerBudget(ctx, task)
	if err != nil {
		return false, err
	}
	if capped {
		return false, nil
	}

	jobName, err := h.jobSpawner.SpawnJob(ctx, task, *config)
	if err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return false, errors.Wrapf(ctx, err, "spawn job for task %s failed", task.TaskIdentifier)
	}

	h.taskStore.Store(task.TaskIdentifier, task)
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
	return true, nil
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
	running, err := h.jobSpawner.CountActiveJobs(ctx, config.Assignee)
	if err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return false, errors.Wrapf(ctx, err, "count active jobs for assignee %s", config.Assignee)
	}
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
	h.deferredMu.Lock()
	h.deferredRespawns[task.TaskIdentifier] = deferredEntry{
		task:       task,
		config:     *config,
		retryAfter: h.currentDateTime.Now().Time().Add(concurrencyCapRetryDelay),
	}
	h.deferredMu.Unlock()
	return true, nil
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
// Called from parseAndFilter when a terminal-phase event arrives so no stale spawn fires.
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

// ReconcileOnce runs a single reconcile pass: lists the task files under the
// configured task glob via git-rest, parses each file's frontmatter, and
// re-drives every eligible task through the existing spawnIfNeeded path.
//
// The reconcile loop is the source-of-truth floor (spec 005): it derives state
// from the world — the vault via git-rest, live Jobs via the spawner — rather
// than from the in-memory maps a restart wipes, so a task whose concurrency-cap
// deferral was lost across an executor restart is re-driven without a new Kafka
// event and without a current_job value. It does NOT consult deferredRespawns.
//
// git-rest unavailability degrades gracefully (spec 005 Failure Modes): a
// failed readiness, List, or Get skips the pass with a log line and the next
// tick retries. ReconcileOnce returns nil for all vault-side failures so the
// loop can never tear down the executor via service.Run.
func (h *taskEventHandler) ReconcileOnce(ctx context.Context) error {
	// Bound the whole pass so a slow or large vault cannot hold it open
	// indefinitely (spec 005 Failure Modes: "Pass bounded by a per-pass
	// timeout; logs evaluate count so cost is visible"). On timeout the
	// in-flight git-rest call returns a deadline error, which the gates below
	// log and skip; the next tick retries.
	passCtx, cancel := context.WithTimeout(ctx, reconcilePassTimeout)
	defer cancel()

	ready, err := h.gitRestClient.IsReady(passCtx)
	if err != nil {
		glog.Warningf("event=reconcile_vault_unavailable reason=readiness_error err=%v", err)
		return nil
	}
	if !ready {
		glog.Warningf("event=reconcile_vault_unavailable reason=not_ready")
		return nil
	}
	paths, err := h.gitRestClient.List(passCtx, h.taskGlob)
	if err != nil {
		glog.Warningf("event=reconcile_list_failed glob=%q err=%v", h.taskGlob, err)
		return nil
	}
	evaluated, redriven, deferred, skipped := 0, 0, 0, 0
	for _, relPath := range paths {
		select {
		case <-passCtx.Done():
			glog.Infof(
				"event=reconcile_aborted reason=context_cancelled evaluated=%d re_driven=%d deferred=%d skipped=%d",
				evaluated,
				redriven,
				deferred,
				skipped,
			)
			return nil
		default:
		}
		content, err := h.gitRestClient.Get(passCtx, relPath)
		if err != nil {
			glog.Warningf("event=reconcile_skip path=%s reason=get_failed err=%v", relPath, err)
			skipped++
			continue
		}
		task, config, ok, err := h.reconcileTask(passCtx, content)
		if err != nil {
			glog.Warningf("event=reconcile_skip path=%s reason=resolve_error err=%v", relPath, err)
			skipped++
			continue
		}
		if !ok {
			skipped++
			continue
		}
		evaluated++
		spawned, err := h.spawnIfNeeded(passCtx, task, config)
		if err != nil {
			glog.Warningf(
				"event=reconcile_skip task=%s reason=spawn_error err=%v",
				task.TaskIdentifier,
				err,
			)
			skipped++
			continue
		}
		if spawned {
			redriven++
			metrics.ReconcileRedrivenTotal.Inc()
			glog.Infof("event=reconcile_redrive task=%s path=%s", task.TaskIdentifier, relPath)
		} else {
			deferred++
		}
	}
	glog.Infof(
		"event=reconcile evaluated=%d re_driven=%d deferred=%d skipped=%d glob=%q",
		evaluated, redriven, deferred, skipped, h.taskGlob,
	)
	return nil
}

// reconcileTask parses a task file and applies the reconcile eligibility filter.
// Returns (task, config, ok, err): ok=false when the file is not an eligible
// task (unparseable frontmatter, missing task_identifier, status != in_progress,
// phase not in the default trigger set, stage mismatch, empty assignee, or
// unresolvable Config) — mirroring parseAndFilter's gates. Reconcile uses the
// DEFAULT trigger sets, not per-Config trigger overrides (spec 005 AC 1:
// "status in_progress, phase in {planning, execution, ai_review}, assignee set");
// the reconcile floor is the baseline contract.
func (h *taskEventHandler) reconcileTask(
	ctx context.Context,
	content []byte,
) (lib.Task, *pkg.AgentConfiguration, bool, error) {
	task, ok := parseTaskFile(ctx, content)
	if !ok {
		return lib.Task{}, nil, false, nil
	}
	config, skip, err := h.resolveConfig(ctx, task)
	if err != nil {
		return lib.Task{}, nil, false, err
	}
	if skip {
		return lib.Task{}, nil, false, nil
	}
	if !defaultTriggerStatuses.Contains(task.Frontmatter.Status()) {
		return lib.Task{}, nil, false, nil
	}
	phase := task.Frontmatter.Phase()
	if phase == nil || !defaultTriggerPhases.Contains(*phase) {
		return lib.Task{}, nil, false, nil
	}
	if task.Frontmatter.Stage() != string(h.branch) {
		return lib.Task{}, nil, false, nil
	}
	if task.Frontmatter.Assignee() == "" {
		return lib.Task{}, nil, false, nil
	}
	return task, config, true, nil
}

// RunReconcileLoop runs one reconcile pass immediately at startup, then every
// defaultReconcileInterval until ctx is cancelled. ReconcileOnce returns
// nil for all vault-side failures, so this loop cannot crash the executor when
// git-rest is unreachable — the next tick just retries (spec 005 Failure Modes).
func (h *taskEventHandler) RunReconcileLoop(ctx context.Context) error {
	if err := h.ReconcileOnce(ctx); err != nil {
		glog.Errorf("event=reconcile initial pass failed: %v", err)
	}
	ticker := time.NewTicker(defaultReconcileInterval)
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
// The cap stays OPT-IN: an absent max_triggers still means no cap.
//
// Scoping does not make it safe to default on, which was the original plan for
// this change. A recurring task is exactly the case v0.7.1 protected after the
// 2026-08-27 prod incident (Daily Sentry Triage: the lib default-3 fallback
// stripped assignee on the 3rd trigger and silently killed the re-dispatch loop),
// and such a task is NOT repo-backed — it carries no ref and sits at a stable
// phase, so its scope is a constant. The counter would accrue across re-dispatches
// exactly as before and the cap would fire at 3, re-creating the incident.
// Scoping only helps where the scope actually moves.
//
// What scoping DOES fix is the opt-in cap's correctness: a task that opts in no
// longer burns its budget across unrelated attempts, so an operator can set a
// tight max_triggers without it leaking into the next phase or the next commit.
func (h *taskEventHandler) applyTriggerBudget(
	ctx context.Context,
	task lib.Task,
) (bool, error) {
	currentScope := triggerScope(task.Frontmatter)
	storedScope, hasScope := task.Frontmatter.String("trigger_scope")

	// An ABSENT scope is not a CHANGED scope. Every task in flight today predates
	// this field, so treating absent as changed would hand each of them a free
	// budget reset on the first event after deploy — including one already looping
	// at cap, which would resume for another N. Absent means "adopt the current
	// scope, keep the count".
	scopeChanged := hasScope && storedScope != currentScope

	_, optedIn := task.Frontmatter["max_triggers"]
	if optedIn && !scopeChanged &&
		task.Frontmatter.TriggerCount() >= task.Frontmatter.MaxTriggers() {
		glog.V(2).Infof("skip task %s: trigger_count %d >= max_triggers %d in scope %q",
			task.TaskIdentifier,
			task.Frontmatter.TriggerCount(),
			task.Frontmatter.MaxTriggers(),
			currentScope,
		)
		metrics.TaskEventsTotal.WithLabelValues("skipped_trigger_cap").Inc()
		return true, nil
	}

	// Exactly one publish per spawn in every branch. Splitting "write the scope"
	// and "count the spawn" into two commands would emit two writes for one spawn
	// and let them interleave with a concurrent write.
	count, useScopeWrite := triggerBudgetWrite(task.Frontmatter, scopeChanged, hasScope)
	if !useScopeWrite {
		if err := h.resultPublisher.PublishIncrementTriggerCount(ctx, task); err != nil {
			metrics.TaskEventsTotal.WithLabelValues("error").Inc()
			return false, errors.Wrapf(
				ctx, err, "publish increment trigger_count for task %s", task.TaskIdentifier,
			)
		}
		return false, nil
	}

	glog.V(2).Infof("task %s: trigger scope %q -> %q, trigger_count -> %d",
		task.TaskIdentifier, storedScope, currentScope, count,
	)
	if err := h.resultPublisher.PublishSetTriggerScope(ctx, task, currentScope, count); err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return false, errors.Wrapf(
			ctx, err, "publish set trigger scope for task %s", task.TaskIdentifier,
		)
	}
	return false, nil
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
