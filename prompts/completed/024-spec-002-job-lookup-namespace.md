---
status: completed
spec: [002-config-job-namespace]
summary: Threaded the resolved Job namespace through all four Job lookups, handler call sites, cache key, regenerated fake, and tests.
execution_id: agent-task-executor-job-namespace-exec-024-spec-002-job-lookup-namespace
dark-factory-version: v0.198.7
created: "2026-10-23T11:05:00Z"
queued: "2026-10-23T11:28:33Z"
started: "2026-10-23T11:28:34Z"
completed: "2026-10-23T11:40:21Z"
branch: dark-factory/config-job-namespace
---

<summary>
- Every Job lookup now searches the same namespace the Job was created in, not always the executor's namespace.
- Without this, a Job in a sandbox namespace is invisible: the executor would spawn a duplicate on every pass and the concurrency cap would never fire.
- The four lookups (`IsJobActive`, `JobExists`, `CountActiveJobs`, `ListActiveJobs`) take the resolved namespace as an explicit argument.
- The handler resolves the namespace once per call site, where the Config is in scope, and passes it down.
- The per-pass active-Job cache is keyed by namespace and assignee, so two namespaces cannot share one stale snapshot.
- The counterfeiter fake is regenerated, and every handler test double and stub is updated to the new signatures.
- New tests assert that each of the four lookups issues its list/get against the resolved namespace, and that the handler helper resolves the three cases correctly.
</summary>

<objective>
Thread the resolved Job namespace through every Job lookup. End state: `JobSpawner.IsJobActive`, `JobExists`, `CountActiveJobs` and `ListActiveJobs` each take a `namespace` argument and issue their Kubernetes call against it; the event handler resolves that namespace from the in-scope `AgentConfiguration` (empty means its own namespace) and passes it to every call; the generated fake and all handler test doubles compile against the new signatures.
</objective>

<context>
Prompts 1 and 2 have landed: `pkg.ResolveJobNamespace` exists, `SpawnJob` creates in the resolved namespace, and `taskEventHandler` has a `namespace libk8s.Namespace` field plus a second constructor parameter. Read these files fully before editing:

- `pkg/spawner/job_spawner.go`
  - `type JobSpawner interface` — the four lookup methods currently have no namespace argument:
    ```go
    IsJobActive(ctx context.Context, taskIdentifier lib.TaskIdentifier) (bool, error)
    CountActiveJobs(ctx context.Context, assignee string) (int, error)
    ListActiveJobs(ctx context.Context, assignee string) (map[lib.TaskIdentifier]bool, error)
    JobExists(ctx context.Context, jobName string) (bool, error)
    ```
  - Their implementations each call `s.kubeClient.BatchV1().Jobs(s.namespace.String())`. The package imports the type as `k8s "github.com/bborbe/k8s"`.
  - The counterfeiter annotation sits above the interface: `//counterfeiter:generate -o ../../mocks/job_spawner.go --fake-name FakeJobSpawner . JobSpawner`, and `pkg/spawner/spawner_suite_test.go` carries the `//go:generate ... -generate` directive.
- `pkg/handler/task_event_handler.go`
  - `func (h *taskEventHandler) isJobActive(ctx context.Context, task lib.Task, config *pkg.AgentConfiguration) (bool, error)` — the nil/empty-assignee branch calls `h.jobSpawner.IsJobActive(ctx, task.TaskIdentifier)`; the other branch calls `h.activeJobsFor(ctx, config.Assignee)`.
  - `func (h *taskEventHandler) deferIfAtConcurrencyCap(ctx context.Context, task lib.Task, config *pkg.AgentConfiguration, claim *deferredClaim) (bool, error)` — calls `h.activeJobsFor(ctx, config.Assignee)`.
- `pkg/handler/task_event_handler_current_job.go`
  - `func (h *taskEventHandler) checkActiveCurrentJob(ctx context.Context, task lib.Task, currentJob string, config *pkg.AgentConfiguration, claim *deferredClaim) (bool, error)` — calls `h.jobSpawner.IsJobActive(ctx, task.TaskIdentifier)` and later `h.jobProvablyFinished(ctx, task, currentJob)`.
  - `func (h *taskEventHandler) jobProvablyFinished(ctx context.Context, task lib.Task, currentJob string) (bool, error)` — calls `h.jobSpawner.JobExists(ctx, stored)`.
- `pkg/handler/job_cache.go`
  - `func (h *taskEventHandler) activeJobsFor(ctx context.Context, assignee string) (map[lib.TaskIdentifier]bool, error)` — memoizes `ListActiveJobs` in `passJobCache.active map[string]cachedActiveJobs`, currently keyed by assignee alone. Its final call is `h.jobSpawner.ListActiveJobs(ctx, assignee)`.
- `pkg/handler/task_event_handler.go` imports `pkg`, `errors`, `glog`, `metrics`; the handler package has no `libk8s` import yet.
- Test doubles to update (all currently pass the old signatures):
  - `pkg/handler/task_event_handler_deferred_test.go` — `ListActiveJobsStub` at lines 444, 519, 671, 701, 757, 814, 978, 1016, 1063.
  - `pkg/handler/task_reconcile_test.go` — `ListActiveJobsStub` at lines 490, 587, 965, 1015, 1099; inline single-line stubs at 1177 (`func(_ context.Context, _ string)`) and 1203 (`func(_ context.Context, assignee string)`).
  - `pkg/handler/task_event_handler_test.go` — `ListActiveJobsStub` at line 365.
  - None of these three files imports `github.com/bborbe/k8s` today.
- `mocks/job_spawner.go` is generated by counterfeiter (`Code generated by counterfeiter. DO NOT EDIT.`) and must be regenerated, not hand-edited.
- `pkg/spawner/job_namespace_test.go` exists from prompt 2, with the helper `newSpawnerForNamespace(t *testing.T, executorNamespace string) (*fake.Clientset, spawner.JobSpawner)` and `countJobCreateActions`.
</context>

<requirements>
1. Change the four `JobSpawner` interface methods in `pkg/spawner/job_spawner.go` to take the namespace as the first argument after `ctx`:
   ```go
   IsJobActive(ctx context.Context, namespace k8s.Namespace, taskIdentifier lib.TaskIdentifier) (bool, error)
   CountActiveJobs(ctx context.Context, namespace k8s.Namespace, assignee string) (int, error)
   ListActiveJobs(ctx context.Context, namespace k8s.Namespace, assignee string) (map[lib.TaskIdentifier]bool, error)
   JobExists(ctx context.Context, namespace k8s.Namespace, jobName string) (bool, error)
   ```
   Update each doc comment to say the lookup is scoped to `namespace`.
2. In each implementation, replace `s.namespace.String()` with `namespace.String()`:
   - `IsJobActive`: the `Jobs(...).List` call;
   - `JobExists`: the `Jobs(...).Get` call;
   - `CountActiveJobs`: the `Jobs(...).List` call;
   - `ListActiveJobs`: the `Jobs(...).List` call.
   Do not change terminal-state detection, label selectors or error wrapping.
3. Create `pkg/handler/job_namespace.go` (`package handler`):
   ```go
   // Copyright (c) 2026 Benjamin Borbe All rights reserved.
   // Use of this source code is governed by a BSD-style
   // license that can be found in the LICENSE file.

   package handler

   import (
       "context"

       libk8s "github.com/bborbe/k8s"

       pkg "github.com/bborbe/agent-task-executor/pkg"
   )

   // jobNamespace resolves the namespace this Config's Jobs live in. A nil config
   // has no Config-supplied namespace, so it uses the executor's own.
   func (h *taskEventHandler) jobNamespace(
       ctx context.Context,
       config *pkg.AgentConfiguration,
   ) (libk8s.Namespace, error) {
       if config == nil {
           return h.namespace, nil
       }
       return pkg.ResolveJobNamespace(ctx, h.namespace, config.JobNamespace)
   }
   ```
4. Update the handler call sites:
   - `isJobActive`: resolve `namespace, err := h.jobNamespace(ctx, config)` first; on error return `false, errors.Wrapf(ctx, err, "resolve job namespace for task %s", task.TaskIdentifier)`. Pass `namespace` to `h.jobSpawner.IsJobActive(ctx, namespace, task.TaskIdentifier)` and to `h.activeJobsFor(ctx, namespace, config.Assignee)`.
   - `deferIfAtConcurrencyCap`: after the `config == nil || config.MaxConcurrentJobs <= 0` early return, resolve the namespace the same way (on error, `metrics.TaskEventsTotal.WithLabelValues("error").Inc()` and return the wrapped error), then call `h.activeJobsFor(ctx, namespace, config.Assignee)`.
   - `checkActiveCurrentJob`: resolve the namespace at the top (same error handling as above), pass it to `h.jobSpawner.IsJobActive(ctx, namespace, task.TaskIdentifier)` and to `h.jobProvablyFinished(ctx, task, currentJob, namespace)`.
   - `jobProvablyFinished`: change its signature to `func (h *taskEventHandler) jobProvablyFinished(ctx context.Context, task lib.Task, currentJob string, namespace libk8s.Namespace) (bool, error)` and call `h.jobSpawner.JobExists(ctx, namespace, stored)`. Add the `libk8s "github.com/bborbe/k8s"` import to this file.
   - `activeJobsFor`: change its signature to `func (h *taskEventHandler) activeJobsFor(ctx context.Context, namespace libk8s.Namespace, assignee string) (map[lib.TaskIdentifier]bool, error)`. Key the cache entry by `namespace.String() + "/" + assignee` instead of `assignee` alone, and call `h.jobSpawner.ListActiveJobs(ctx, namespace, assignee)`. Add the `libk8s` import to `pkg/handler/job_cache.go`.
5. Regenerate the fake:
   ```bash
   go generate ./pkg/spawner/...
   ```
   Confirm `mocks/job_spawner.go` now declares e.g. `IsJobActiveStub func(context.Context, k8s.Namespace, lib.TaskIdentifier) (bool, error)` and imports `k8s "github.com/bborbe/k8s"`. Do not hand-edit the generated file; if generation fails, fix the interface or the generate directive rather than patching the fake.
6. Update every test double/stub listed in the context section:
   - Multi-line stubs gain a `_ libk8s.Namespace,` parameter between `ctx context.Context` and the assignee/identifier parameter.
   - The two inline stubs at `task_reconcile_test.go:1177` and `:1203` become `func(_ context.Context, _ libk8s.Namespace, _ string)` and `func(_ context.Context, _ libk8s.Namespace, assignee string)` respectively.
   - Add `libk8s "github.com/bborbe/k8s"` to the imports of `task_event_handler_deferred_test.go`, `task_reconcile_test.go` and `task_event_handler_test.go` only if the file contains a typed namespace parameter after the edit.
   - Do not change any `...Returns(...)` setup, call-count assertion or behavioural expectation.
7. Append to `pkg/spawner/job_namespace_test.go` a plain test `func TestJobLookupsUseResolvedNamespace(t *testing.T)`:
   - Start with `ctx := context.Background()`; the file already imports `context`, `lib`, `libk8s`, `pkg` and the fake clientset from prompt 2. Add any import that is genuinely missing.
   - Build `client, s := newSpawnerForNamespace(t, "dev")` and `namespace := libk8s.Namespace("dev-agents-sandbox")`.
   - Call all four methods with `namespace`:
     ```go
     _, _ = s.IsJobActive(ctx, namespace, lib.TaskIdentifier("abc12345"))
     _, _ = s.JobExists(ctx, namespace, "some-job")
     _, _ = s.CountActiveJobs(ctx, namespace, "claude")
     _, _ = s.ListActiveJobs(ctx, namespace, "claude")
     ```
   - Iterate `client.Actions()`; for every action whose resource is `jobs` and whose verb is `list` or `get`, assert `action.GetNamespace() == "dev-agents-sandbox"`. Assert there are at least 4 such actions (one list per IsJobActive/CountActiveJobs/ListActiveJobs, one get for JobExists).
8. Add `pkg/handler/job_namespace_internal_test.go` (`package handler`, plain `testing`, imports `context`, `testing`, `libk8s "github.com/bborbe/k8s"`, `pkg "github.com/bborbe/agent-task-executor/pkg"`) with `func TestJobNamespaceHelper(t *testing.T)`:
   - `h := &taskEventHandler{namespace: libk8s.Namespace("dev")}`;
   - nil config → `"dev"`;
   - `&pkg.AgentConfiguration{JobNamespace: ""}` → `"dev"`;
   - `&pkg.AgentConfiguration{JobNamespace: "dev-agents-sandbox"}` → `"dev-agents-sandbox"`;
   - `&pkg.AgentConfiguration{JobNamespace: "dev"}` → non-nil error whose message contains `may not equal the executor namespace`.
9. New files start with the repository's BSD license header shown in step 3.
</requirements>

<constraints>
- The empty `JobNamespace` default must preserve today's behaviour exactly: every lookup for such a Config still targets the executor's namespace.
- Do NOT change the Kafka result-publishing path, `TaskType`/`TaskTypes` routing, `pkg/job_watcher.go`, or the `SpawnJob` create path from prompt 2.
- Do NOT add a cluster-scoped informer, a new RBAC grant or a capability — the Job informer deliberately stays scoped to the executor's namespace (spec Non-goals and Follow-ups).
- Do NOT edit the spec file.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass; the only permitted test edits are the added namespace parameter and the new assertions.
</constraints>

<verification>
Run from the repository root:

```bash
go generate ./pkg/spawner/...
go test ./... -run 'TestJobLookupsUseResolvedNamespace|TestJobNamespaceHelper' -v
```

The stdout must contain `--- PASS: TestJobLookupsUseResolvedNamespace` and `--- PASS: TestJobNamespaceHelper`.

Then:

```bash
go test ./pkg/handler/... ./pkg/spawner/... ./pkg/factory/...
ROOTDIR=/workspace make precommit
```

Both commands must exit 0.
</verification>
