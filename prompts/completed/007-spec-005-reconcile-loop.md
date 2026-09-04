---
status: completed
spec: [005-bug-executor-restart-drops-deferred-respawn-queue]
summary: Added a periodic reconcile loop to taskEventHandler that re-derives running tasks from the vault via git-rest and live Jobs, re-driving eligible tasks through the existing spawnIfNeeded path with per-pass observability and graceful git-rest degradation
execution_id: agent-task-executor-exec-007-spec-005-reconcile-loop
dark-factory-version: dev
created: "2026-09-03T18:04:37Z"
queued: "2026-09-03T18:18:58Z"
started: "2026-09-03T18:26:14Z"
completed: "2026-09-03T18:34:57Z"
branch: dark-factory/bug-executor-restart-drops-deferred-respawn-queue
---

<summary>
- The executor gains a periodic reconcile loop that derives task state from the world — the vault via git-rest and live Jobs via the spawner — instead of the in-memory maps a restart wipes, so a task whose concurrency-cap deferral was lost across an executor restart is re-driven without any Kafka event, vault edit, or `current_job` value.
- One pass runs immediately at startup, then every 60 seconds (package constant, mirroring the zombie sweeper interval; no CRD field or env override).
- Each pass lists the configured task glob via git-rest, parses every file's frontmatter, filters to eligible tasks (status `in_progress`, phase in `{planning, execution, ai_review}`, matching stage, assignee set, config resolvable), and re-drives each through the existing `spawnIfNeeded` path — so the concurrency cap, trigger budget, and `current_job` grace window apply identically to event-driven and reconcile-driven spawns.
- git-rest unavailability degrades gracefully: a failed readiness, List, or Get skips that pass with a log line and the next tick retries; the loop can never crash the executor.
- Each pass records queue depth: a `executor_reconcile_redriven_total` Prometheus counter per re-driven task and an `event=reconcile` summary log line (`evaluated` / `re_driven` / `deferred` / `skipped`); each re-drive logs `event=reconcile_redrive task=<id>`.
- The existing in-memory `deferredRespawns` map stays untouched as a fast path; the reconcile loop is the source-of-truth floor and does not consult it.
- The wiring threads the git-rest client and task glob from `main.go` through the factory into the handler; the gateway secret value is read from K8s by name at startup.
- A new Ginkgo test file covers all acceptance criteria and failure modes; a `## Unreleased` `feat:` changelog bullet is added.
</summary>

<objective>
Make an executor restart non-destructive by adding a reconcile loop to `taskEventHandler` that periodically re-derives the set of tasks that should be running from the vault (via git-rest) and live Jobs, and re-drives every eligible task through the existing `spawnIfNeeded` path, with per-pass observability and graceful degradation when git-rest is unreachable.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for project conventions (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, counterfeiter mocks, glog V(n) gating, coverage ≥80% for new code).

Read these files fully before changing anything:
- `/workspace/pkg/handler/task_event_handler.go` — the whole file. Key anchors: the `TaskEventHandler` interface (lines 105-116), `NewTaskEventHandler` (lines 118-137), the `taskEventHandler` struct (lines 139-150), `parseAndFilter` (lines 220-303, the status/phase/stage/assignee gates to mirror), `resolveConfig` (lines 309-336), `spawnIfNeeded` (lines 450-525), `deferIfAtConcurrencyCap` (lines 535-579), `defaultTriggerStatuses`/`defaultTriggerPhases` (lines 28-38), `deferredRespawnInterval` (line 48), and `RunDeferredRespawnLoop` (lines 674-699, the loop shape to mirror).
- `/workspace/main.go` — `Run` (lines 74-182): the wiring sequence where the git-rest client is built and `taskEventHandler.RunReconcileLoop` is registered in `service.Run` (lines 170-181). Note `kubeClient` (line 98) and `a.Namespace` are available for reading the gateway secret.
- `/workspace/pkg/factory/factory.go` — `CreateConsumer` (lines 97-147) where `NewTaskEventHandler` is called (lines 124-131); it must forward the two new arguments.
- `/workspace/pkg/handler/task_event_handler_test.go` — the `BeforeEach` (lines 52-76) and the five `NewTaskEventHandler` call sites at lines 63, 876, 922, 1656, 1702 that must gain the two new arguments. This file is 1909 lines (revive file-length-limit is 2000) — put ALL new reconcile specs in the NEW test file created here, never in this file.
- `/workspace/pkg/zombie_sweeper.go` — `Run`/`SweepOnce` (lines 90-171): the run-once-plus-ticker loop and per-tick log-and-continue pattern to mirror for `RunReconcileLoop`/`ReconcileOnce`.
- `/workspace/pkg/metrics/metrics.go` and `/workspace/pkg/metrics/metrics_test.go` — the `promauto` counter pattern and the registered-names assertion to extend.
- `/workspace/pkg/agent_configuration.go` — `AgentConfiguration` (lines 28-57, `MaxConcurrentJobs` at line 57, `Assignee` at line 30).

Reference for the frontmatter/body extraction helpers (ported, do NOT import): `/home/node/go/pkg/mod/github.com/bborbe/agent-task-controller@v0.6.6/pkg/scanner/frontmatter.go` — `extractFrontmatter` (returns an error when the `---` delimiters are missing/unclosed) and `extractBody`. If that module is not in the container cache, the target code in requirement 2 below is authoritative — do not add `agent-task-controller` to go.mod.

Library contracts already verified in this repo (use them as-is):
- `lib "github.com/bborbe/agent"` — `lib.Task{TaskIdentifier: lib.TaskIdentifier(...), Frontmatter: lib.TaskFrontmatter(map[string]interface{}), Content: lib.TaskContent(...)}`; `lib.TaskFrontmatter.Status() domain.TaskStatus`, `.Phase() *domain.TaskPhase`, `.Assignee() lib.TaskAssignee`, `.Stage() string` (returns `"prod"` when the key is absent or empty).
- `"github.com/bborbe/vault-cli/pkg/domain"` — `domain.TaskStatuses.Contains(TaskStatus) bool`, `domain.TaskPhases.Contains(TaskPhase) bool`.
- `"github.com/bborbe/agent-task-executor/pkg/gitrestclient"` (created by the previous prompt) — `GitRestClient` interface with `Get(ctx, relPath) ([]byte, error)`, `List(ctx, glob) ([]string, error)`, `IsReady(ctx) (bool, error)`.

Decision note for the reviewer: the reconcile eligibility filter uses the DEFAULT trigger sets (status `in_progress`; phase `{planning, execution, ai_review}`), NOT per-Config `trigger.phases`/`trigger.statuses` overrides — this matches AC 1's literal wording ("status `in_progress`, phase in `{planning, execution, ai_review}`, assignee set") and keeps the reconcile loop a baseline floor. If the intended semantics were per-Config triggers, that would be a spec change, not an executor-side judgment call.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega, external test packages, coverage ≥80%.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — never `fmt.Errorf`; `fmt.Sprintf` for building test fixture strings is fine.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md` — `glog.Infof` for the per-pass and per-redrive events, `glog.Warningf` for the degraded/unavailable events.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-context-cancellation-in-loops.md` — non-blocking `select` context check inside the per-file loop.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` format and prefix rules.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.
</context>

<requirements>
1. Add the reconcile interval constant to `/workspace/pkg/handler/task_event_handler.go`, directly after the `concurrencyCapRetryDelay` const (line 54):
```go
// defaultReconcileIntervalSeconds is the interval between reconcile-loop passes
// (spec 005). Mirrors DefaultZombieSweeperIntervalSeconds: a package constant,
// not a CRD field and not env-configurable in this change. 60s is a reasonable
// recovery latency for a task whose deferral was lost across a restart.
const defaultReconcileIntervalSeconds = 60 * time.Second

// reconcilePassTimeout bounds one reconcile pass so a slow or large vault
// cannot hold a pass open indefinitely (spec 005 Failure Modes: "Pass bounded
// by a per-pass timeout; logs evaluate count so cost is visible"). Half the
// interval: a pass completes within a tick and leaves headroom for the next
// tick. Package constant — not configurable in this change.
const reconcilePassTimeout = 30 * time.Second
```

2. Create `/workspace/pkg/handler/task_file.go` (package `handler`) with the frontmatter/body extraction and task-file parsing helpers, ported from the controller's scanner (keep the BSD header):
```go
// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"strings"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	"github.com/golang/glog"
	"gopkg.in/yaml.v3"
)

// extractFrontmatter returns the YAML frontmatter block between the leading
// "---" and the closing "\n---" delimiters. Ported from the controller's
// pkg/scanner/frontmatter.go (spec 005 — the reconcile loop reads vault task
// files through git-rest and needs the same parsing the controller uses).
func extractFrontmatter(ctx context.Context, content []byte) (string, error) {
	s := string(content)
	const delim = "---"
	if !strings.HasPrefix(s, delim) {
		return "", errors.Errorf(ctx, "no frontmatter delimiter found")
	}
	rest := strings.TrimPrefix(s, delim)
	if strings.HasPrefix(rest, "\r\n") {
		rest = rest[2:]
	} else if strings.HasPrefix(rest, "\n") {
		rest = rest[1:]
	}
	idx := strings.Index(rest, "\n---")
	if idx == -1 {
		return "", errors.Errorf(ctx, "frontmatter not closed")
	}
	return rest[:idx], nil
}

// extractBody returns the markdown body after the closing frontmatter delimiter.
func extractBody(content []byte) string {
	s := string(content)
	const delim = "---"
	if !strings.HasPrefix(s, delim) {
		return s
	}
	rest := strings.TrimPrefix(s, delim)
	if strings.HasPrefix(rest, "\r\n") {
		rest = rest[2:]
	} else if strings.HasPrefix(rest, "\n") {
		rest = rest[1:]
	}
	idx := strings.Index(rest, "\n---")
	if idx == -1 {
		return s
	}
	after := rest[idx+4:] // skip "\n---"
	if strings.HasPrefix(after, "\r\n") {
		after = after[2:]
	} else if strings.HasPrefix(after, "\n") {
		after = after[1:]
	}
	return after
}

// parseTaskFile parses a vault task file into a lib.Task. ok=false when the
// file has no valid frontmatter or no task_identifier (the file is not a task
// the executor can drive). The Content is the markdown body — it is preserved
// verbatim because SpawnJob renders it into the spawned Job's TASK_CONTENT env
// (renderTaskContent in pkg/spawner), so a dropped body would spawn an agent
// with no instructions.
func parseTaskFile(ctx context.Context, content []byte) (lib.Task, bool) {
	fmYAML, err := extractFrontmatter(ctx, content)
	if err != nil {
		glog.Warningf("event=reconcile_skip reason=invalid_frontmatter err=%v", err)
		return lib.Task{}, false
	}
	var fmMap map[string]interface{}
	if err := yaml.Unmarshal([]byte(fmYAML), &fmMap); err != nil {
		glog.Warningf("event=reconcile_skip reason=unparseable_frontmatter err=%v", err)
		return lib.Task{}, false
	}
	taskID, _ := fmMap["task_identifier"].(string)
	if taskID == "" {
		glog.Warningf("event=reconcile_skip reason=missing_task_identifier")
		return lib.Task{}, false
	}
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier(taskID),
		Frontmatter:    lib.TaskFrontmatter(fmMap),
		Content:        lib.TaskContent(extractBody(content)),
	}, true
}
```

3. Change the `TaskEventHandler` interface in `/workspace/pkg/handler/task_event_handler.go` (lines 105-116) to add two methods (keep `ConsumeMessage`/`EvalDeferredRespawns`/`RunDeferredRespawnLoop` unchanged):
```go
	// ReconcileOnce runs one reconcile pass: lists the task files under the
	// configured task glob via git-rest, parses each file's frontmatter, and
	// re-drives every eligible task through the existing spawn path. Called by
	// RunReconcileLoop on each tick; also callable directly in tests.
	ReconcileOnce(ctx context.Context) error
	// RunReconcileLoop runs one pass immediately at startup, then every
	// defaultReconcileIntervalSeconds until ctx is cancelled. Must be run
	// alongside the Kafka consumer.
	RunReconcileLoop(ctx context.Context) error
```

4. Change `NewTaskEventHandler` (lines 118-137) to accept and store the git-rest client and task glob. New signature (append two parameters after `currentDateTime`):
```go
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
```
Add `gitRestClient gitrestclient.GitRestClient` and `taskGlob string` fields to the `taskEventHandler` struct (lines 139-150), and add the import `"github.com/bborbe/agent-task-executor/pkg/gitrestclient"` to the local import group (with `pkg`, `metrics`, `spawner`).

5. Add the reconcile methods to `/workspace/pkg/handler/task_event_handler.go`. Place `ReconcileOnce` immediately before `RunReconcileLoop`, and place both after `RunDeferredRespawnLoop` (after line 699). Target code:
```go
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
				evaluated, redriven, deferred, skipped,
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
			glog.Warningf("event=reconcile_skip task=%s reason=spawn_error err=%v", task.TaskIdentifier, err)
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
// defaultReconcileIntervalSeconds until ctx is cancelled. ReconcileOnce returns
// nil for all vault-side failures, so this loop cannot crash the executor when
// git-rest is unreachable — the next tick just retries (spec 005 Failure Modes).
func (h *taskEventHandler) RunReconcileLoop(ctx context.Context) error {
	if err := h.ReconcileOnce(ctx); err != nil {
		glog.Errorf("event=reconcile initial pass failed: %v", err)
	}
	ticker := time.NewTicker(defaultReconcileIntervalSeconds)
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
```

6. Add the counter to `/workspace/pkg/metrics/metrics.go`, after `JobsSpawnedTotal` (after line 27):
```go
// ReconcileRedrivenTotal counts tasks re-driven by the reconcile loop after a
// restart dropped their in-memory deferral state (spec 005).
var ReconcileRedrivenTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "executor_reconcile_redriven_total",
		Help: "Total number of tasks re-driven by the reconcile loop.",
	},
)
```
Extend `/workspace/pkg/metrics/metrics_test.go` (the first `It` block, line ~19) so the expected registered names also include `"executor_reconcile_redriven_total"`.

7. Wire the factory in `/workspace/pkg/factory/factory.go`:
   a. Add a new factory function (zero logic — forward only):
   ```go
   // CreateGitRestClient returns the read-only git-rest vault reader the
   // reconcile loop uses. gatewayInitiator is fixed to the executor's identity
   // for git-rest's auth-failure logging.
   func CreateGitRestClient(baseURL, gatewaySecret string) gitrestclient.GitRestClient {
       return gitrestclient.NewGitRestClient(baseURL, gatewaySecret, "agent-task-executor")
   }
   ```
   Add the import `"github.com/bborbe/agent-task-executor/pkg/gitrestclient"`.
   b. Change `CreateConsumer` to accept the two new values as the final two parameters (append after `jobKafkaCaCertSecret string`):
   ```go
   func CreateConsumer(
       saramaClient sarama.Client,
       branch base.Branch,
       topicPrefix base.TopicPrefix,
       kubeClient kubernetes.Interface,
       namespace libk8s.Namespace,
       kafkaBrokers string,
       resolver pkg.ConfigResolver,
       logSamplerFactory log.SamplerFactory,
       currentDateTimeGetter libtime.CurrentDateTimeGetter,
       resultPublisher pkg.ResultPublisher,
       taskStore *pkg.TaskStore,
       jobTTLSecondsAfterFinished int32,
       jobKafkaClientCertSecret string,
       jobKafkaCaCertSecret string,
       gitRestClient gitrestclient.GitRestClient,
       taskGlob string,
   ) (libkafka.Consumer, handler.TaskEventHandler) {
   ```
   and inside, pass both new values as the final two arguments of the `handler.NewTaskEventHandler(...)` call (after `currentDateTimeGetter`).

8. Wire `main.go`:
   a. After the `resultPublisher`/`taskStore`/`jobWatcher`/`zombieSweeper` block and BEFORE the `consumer, taskEventHandler := factory.CreateConsumer(...)` call (i.e., after line 151), add:
   ```go
   // The git-rest gateway secret is read by NAME from K8s (never logged, never
   // in env) so the reconcile loop can authenticate to git-rest without secret
   // material entering the Deployment manifest (spec 005 Security).
   gitRestGatewaySecretValue, err := pkg.ReadGitRestGatewaySecret(
       ctx,
       kubeClient,
       a.Namespace,
       a.GitRestGatewaySecret,
   )
   if err != nil {
       return errors.Wrapf(ctx, err, "read git-rest gateway secret")
   }
   gitRestClient := factory.CreateGitRestClient(a.GitRestURL, gitRestGatewaySecretValue)
   ```
   b. Pass `gitRestClient` and `a.TaskGlob` as the final two arguments of the `factory.CreateConsumer(...)` call (after `a.JobKafkaCaCertSecret`).
   c. Register the loop in `service.Run` (lines 170-181), immediately after `taskEventHandler.RunDeferredRespawnLoop`:
   ```go
   taskEventHandler.RunDeferredRespawnLoop,
   taskEventHandler.RunReconcileLoop,
   ```

9. Update the five existing `NewTaskEventHandler` call sites in `/workspace/pkg/handler/task_event_handler_test.go` — each gains the two new arguments `fakeGitRestClient, "24 Tasks/*.md"` after `currentDateTime` (or `libtime.NewCurrentDateTime()` / the local clock var). The call sites are at lines 63 (the BeforeEach), 876, 922, 1656, and 1702. Do NOT add any new Ginkgo specs to this file (it is 1909 lines; the 2000-line revive limit is near). Also add the BeforeEach setup for the fake client (the `var` block at line 43-51 gains `fakeGitRestClient *mocks.FakeGitRestClient`, and the BeforeEach gains):
   ```go
   fakeGitRestClient = &mocks.FakeGitRestClient{}
   fakeGitRestClient.IsReadyReturns(true, nil)
   ```

10. Create `/workspace/pkg/handler/task_reconcile_test.go` in `package handler_test` — a new file (do NOT extend the 1909-line existing test file). It registers into the existing "Handler Suite" launched by `TestHandler` in `task_event_handler_test.go`; give it its OWN `var _ = Describe("TaskEventHandler reconcile loop", func() {...})` block with its own `BeforeEach` replicating the setup in `task_event_handler_test.go:52-76` plus the fake git-rest client (default `IsReadyReturns(true, nil)`), constructing the handler via `handler.NewTaskEventHandler(fakeSpawner, base.Branch("prod"), fakeResolver, fakeResultPublisher, taskStore, currentDateTime, fakeGitRestClient, "24 Tasks/*.md")`.
    Helper in the file:
    ```go
    // renderTaskFile serializes a task into the vault markdown shape the
    // reconcile loop parses (frontmatter delimited by "---", body after).
    renderTaskFile := func(task lib.Task) string {
        fm := make(map[string]any, len(task.Frontmatter)+1)
        for k, v := range task.Frontmatter {
            fm[k] = v
        }
        fm["task_identifier"] = string(task.TaskIdentifier)
        out, err := yaml.Marshal(fm)
        Expect(err).To(BeNil())
        return "---\n" + string(out) + "---\n# Task body\n"
    }
    ```
    (imports: `gopkg.in/yaml.v3`, `github.com/bborbe/agent-task-executor/mocks`, `lib "github.com/bborbe/agent"`, `"github.com/bborbe/cqrs/base"`, `"github.com/bborbe/vault-cli/pkg/domain"`, `pkg "github.com/bborbe/agent-task-executor/pkg"`, `"github.com/bborbe/agent-task-executor/pkg/handler"`, `"github.com/bborbe/agent-task-executor/pkg/metrics"`, `"github.com/bborbe/errors"`, `libtime "github.com/bborbe/time"`, `"github.com/prometheus/client_golang/prometheus/testutil"`, Ginkgo/Gomega.)
    Then add these specs:
    a. AC 1 (happy path): `fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)`, `GetReturns([]byte(renderTaskFile(task)), nil)` for an eligible task (status in_progress, phase planning, assignee claude, stage prod, resolver returns `pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"}`), `fakeSpawner.IsJobActiveReturns(false, nil)`. Run `h.ReconcileOnce(ctx)`; assert `fakeSpawner.SpawnJobCallCount() == 1` and the counter delta (`testutil.ToFloat64(metrics.ReconcileRedrivenTotal)` before vs after) is +1.
    b. AC 2 (restart recovery with empty store): fresh `pkg.NewTaskStore()` (empty — simulating the post-restart wipe), git-rest returns a deferred-shape task whose frontmatter has NO `current_job` key (status in_progress, phase execution, assignee claude, stage prod), no Kafka message is sent; run one `ReconcileOnce`; assert `SpawnJobCallCount() == 1` and the counter delta is +1. This is the exact shape `deferIfAtConcurrencyCap` leaves behind.
    c. AC 3 (skip table — `SpawnJob` NOT called): table of `Entry` cases where each constructs a task file and asserts `SpawnJobCallCount() == 0`:
       - status `todo` (not in_progress)
       - phase `todo` (not in default trigger set)
       - phase `human_review` (terminal)
       - empty assignee (`assignee` key absent)
       - unresolvable config (`fakeResolver.ResolveReturns(pkg.AgentConfiguration{}, pkg.ErrConfigNotFound)`)
       - live Job (`fakeSpawner.IsJobActiveReturns(true, nil)`)
       - stage mismatch (`stage: dev` with executor branch `prod`)
    d. AC 4 (concurrency cap respected): `fakeResolver.ResolveReturns(pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest", MaxConcurrentJobs: 1}, nil)`, `fakeSpawner.CountActiveJobsReturns(1, nil)` (at cap), `fakeSpawner.IsJobActiveReturns(false, nil)`; run `ReconcileOnce`; assert `SpawnJobCallCount() == 0` (deferred, not immediately spawned).
    e. AC 6 (counter increments): covered by (a) and (b) — assert the `executor_reconcile_redriven_total` delta explicitly in both.
    f. Failure mode — git-rest not ready: `fakeGitRestClient.IsReadyReturns(false, nil)`; run `ReconcileOnce`; assert `fakeGitRestClient.ListCallCount() == 0` and `SpawnJobCallCount() == 0` (pass skipped, next tick retries).
    g. Failure mode — readiness error: `fakeGitRestClient.IsReadyReturns(false, errors.Errorf(ctx, "boom"))`; assert `ListCallCount() == 0` and `SpawnJobCallCount() == 0`.
    h. Failure mode — List error: `IsReadyReturns(true, nil)`, `ListReturns(nil, errors.Errorf(ctx, "rate limited"))`; assert `SpawnJobCallCount() == 0` (pass skipped, no panic).
    i. Failure mode — Get error for one path: `ListReturns([]string{"24 Tasks/bad.md"}, nil)`, `GetReturns(nil, errors.Errorf(ctx, "boom"))`; assert `SpawnJobCallCount() == 0` and `ReconcileOnce` returns nil (no crash).
    j. Startup pass (DB 1): run `RunReconcileLoop` in a short-lived context (`context.WithCancel(ctx)`), with `fakeGitRestClient.ListReturns` set; `Eventually(fakeGitRestClient.ListCallCount).Should(BeNumerically(">=", 1))` (the immediate startup pass runs before any ticker fire), then `cancel()` and assert `<-done` is nil. Since the interval is 60s, no ticker tick occurs in the test.
    k. Per-pass abort (DB 7 / failure-mode "slow or vault large"): with `fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)` set and an already-cancelled parent context (`cancelCtx, cancel := context.WithCancel(ctx); cancel()`), run `ReconcileOnce(cancelCtx)` and assert it returns nil (the `passCtx.Done()` select aborts the pass; no panic, no `SpawnJob`).

11. Add the CHANGELOG entry in `/workspace/CHANGELOG.md` under the existing `## Unreleased` section (append after the current chore bullet):
    ```
    - feat: add a periodic reconcile loop that re-derives running tasks from the vault (via git-rest) and live Jobs, so a task deferred behind `maxConcurrentJobs` whose in-memory deferral was lost across an executor restart resumes without a Kafka event or vault edit; observable via `executor_reconcile_redriven_total` and `event=reconcile*` log lines, with the per-assignee spawn lock taken unconditionally to prevent reconcile/event double-spawns
    ```

12. Do NOT touch `seedDeferredRespawnsFromStore` (lines 642-672) — it remains as-is; the reconcile loop is the new restart-recovery floor and must not be gated on `deferredRespawns` contents. Do NOT change `deferIfAtConcurrencyCap`, `checkActiveCurrentJob`, or the Kafka event path.
</requirements>

<constraints>
- Repo conventions are frozen: Ginkgo/Gomega v2 tests (no stdlib table tests), `github.com/bborbe/errors` wrapping (never `fmt.Errorf`), counterfeiter mocks for any new dependency, glog `V(n)` gating.
- The reconcile interval is a package constant `defaultReconcileIntervalSeconds = 60 * time.Second` — NO CRD field and NO env override (spec Non-goal).
- The reconcile loop is the source-of-truth floor: it reads the vault via git-rest and live Jobs via the spawner. It must NOT consult or be gated on the in-memory `deferredRespawns` map or `taskStore` (which a restart wipes).
- Re-driving an eligible task MUST go through the existing `spawnIfNeeded` path so the concurrency cap, trigger budget, and `current_job` grace-window logic apply identically to event-driven and reconcile-driven spawns. Do NOT duplicate any of that logic in the reconcile code.
- The reconcile eligibility uses the DEFAULT trigger sets (status `in_progress`, phase `{planning, execution, ai_review}`) plus stage/assignee/config gates, per AC 1 — not per-Config trigger overrides.
- git-rest unavailability must NEVER crash the executor: pass skipped with a log line, next tick retries, Kafka consumption unaffected.
- The Kafka event path and `deferIfAtConcurrencyCap`/`checkActiveCurrentJob` behavior are unchanged for event-driven spawns.
- Do NOT add the unconditional spawn lock in this prompt — that is the next prompt.
- CHANGELOG: append the single `feat:` bullet to the existing `## Unreleased` section (it already exists at HEAD v0.8.3).
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `go test -mod=mod ./pkg/handler/... ./pkg/metrics/...` iteratively after each meaningful change (fast feedback loop), then `make precommit` ONCE at the very end.

- `go test -mod=mod ./pkg/handler/...` — exits 0.
- `go test -mod=mod ./pkg/metrics/...` — exits 0.
- `make precommit` — exits 0 (regenerates the `FakeTaskEventHandler` mock for the two new interface methods via `go generate -mod=mod ./...`; also regenerates the git-rest mock).
- `grep -n 'reconcile' pkg/handler/task_event_handler.go` — returns ≥1 line (acceptance-criterion evidence).
- `grep -rn 'executor_reconcile_redriven_total' pkg/` — returns ≥1 line (acceptance-criterion evidence).
- `grep -rn 'event=reconcile_redrive' pkg/handler/` — the per-redrive log line format is present.
- `grep -n 'RunReconcileLoop' main.go pkg/handler/task_event_handler.go` — the loop is registered in service.Run and defined on the handler.
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/handler/... && go tool cover -func=/tmp/cover.out` — confirm `ReconcileOnce`, `reconcileTask`, `RunReconcileLoop`, `parseTaskFile`, `extractFrontmatter`, `extractBody` are each exercised by at least one test (≥80% statement coverage for new code).

Do NOT run `docker`, `make build`, `kubectl`, `dark-factory`, `gh`, or `scripts/*.sh` commands in this prompt — those are operator-executable and belong on the spec's verification ladder.
</verification>
