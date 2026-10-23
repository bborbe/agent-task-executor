---
status: completed
spec: [002-config-job-namespace]
summary: SpawnJob now creates Jobs in the Config-resolved namespace, the event handler rejects an executor-namespace Config with an ERROR log and no Job, and the app:agent pod label is pinned by tests.
execution_id: agent-task-executor-job-namespace-exec-023-spec-002-spawner-create-path
dark-factory-version: v0.198.7
created: "2026-10-23T11:05:00Z"
queued: "2026-10-23T11:19:16Z"
started: "2026-10-23T11:19:17Z"
completed: "2026-10-23T11:28:12Z"
branch: dark-factory/config-job-namespace
---

<summary>
- A Config that sets a target namespace now has its Job created in that namespace instead of the executor's.
- A Config with no target namespace keeps creating its Job in the executor's namespace, exactly as before.
- A Config that names the executor's own namespace is refused before anything is created: the rejection is logged at ERROR with the task, the Config assignee and the rejected namespace, and the task is left untouched for a human.
- A refused Config creates no Job at all — the rejection happens before the Kubernetes create call.
- Every spawned Job's pod template carries `app: agent`, and that behaviour is now pinned by a regression test rather than left implicit.
- The spawner itself performs the derivation, so every caller of `SpawnJob` gets the same behaviour.
- The event handler gains the executor's namespace so it can log and refuse the bad Config at the call site where the Config is in scope.
- New tests pin the target namespace, the zero-create rejection and the pod label.
</summary>

<objective>
Make the spawner create each Job in the namespace resolved from the Config, and make the event handler refuse a Config whose `JobNamespace` equals the executor's own namespace with an ERROR log and no Job. End state: `SpawnJob` resolves `config.JobNamespace` against the spawner's own namespace, uses the result for both the Job's `metadata.namespace` and the `Jobs(ns).Create` call, and returns an error before creating anything when resolution fails; the handler logs and skips such a Config before any gate or claim. Also lock the existing unconditional `app: agent` pod label with a named test.
</objective>

<context>
Prompt 1 has already landed: `ConfigSpec.JobNamespace`, `AgentConfiguration.JobNamespace`, `pkg.ResolveJobNamespace(ctx, executorNamespace, jobNamespace) (libk8s.Namespace, error)` and the resolver round-trip exist. Read these files fully before editing:

- `pkg/spawner/job_spawner.go`
  - `func (s *jobSpawner) SpawnJob(ctx context.Context, task lib.Task, config pkg.AgentConfiguration) (string, error)` — builds the Job, then:
    ```go
    objectMetaBuilder.SetNamespace(s.namespace)
    ...
    jobBuilder.SetApp("agent")
    ...
    _, err = s.kubeClient.BatchV1().
        Jobs(s.namespace.String()).
        Create(ctx, job, metav1.CreateOptions{})
    ```
    The `SetApp("agent")` call is already unconditional; the new test only pins it.
  - `type jobSpawner struct` holds `namespace k8s.Namespace` (imported as `k8s "github.com/bborbe/k8s"`).
- `pkg/handler/task_event_handler.go`
  - `func NewTaskEventHandler(jobSpawner spawner.JobSpawner, branch base.Branch, cluster string, ...)` — 15 required parameters plus a variadic `claimJobControl ...claim.JobControl`.
  - `type taskEventHandler struct` and the constructor body that fills it.
  - `func (h *taskEventHandler) spawnIfNeededClaimed(ctx context.Context, task lib.Task, config *pkg.AgentConfiguration, claim *deferredClaim) (bool, error)` — its first statement is `if h.skipForeignVaultTask(task) { return false, nil }`, followed by the `current_job` check. The new guard goes between those two.
  - Imports already include `pkg "github.com/bborbe/agent-task-executor/pkg"`, `"github.com/golang/glog"` and `"github.com/bborbe/agent-task-executor/pkg/metrics"`. Add `libk8s "github.com/bborbe/k8s"` (not currently imported here).
- `pkg/factory/factory.go`
  - `func CreateConsumer(... namespace libk8s.Namespace, ...)` calls `handler.NewTaskEventHandler(jobSpawner, branch, ...)`. Pass the existing `namespace` value.
- All `handler.NewTaskEventHandler(` call sites that must gain the new argument (19 total, excluding the definition):
  - `pkg/factory/factory.go:209`
  - `pkg/handler/task_event_handler_deferred_test.go:74, 488, 1173, 1228`
  - `pkg/handler/task_event_handler_claim_test.go:87`
  - `pkg/handler/task_event_handler_respawn_race_test.go:57`
  - `pkg/handler/task_event_handler_test.go:82, 1009, 1064, 1138, 1180, 1235`
  - `pkg/handler/task_reconcile_test.go:59, 120, 943, 1284`
  - `pkg/handler/claim_spawn_order_internal_test.go:128` (package `handler`, no `handler.` prefix)
  - `pkg/handler/view_readiness_internal_test.go:55` (package `handler`, no `handler.` prefix)
- `pkg/spawner/job_spawner_test.go` — `package spawner_test`; `fakeClient = fake.NewClientset()`; `spawner.NewJobSpawner(fakeClient, "test-ns", "kafka:9092", "develop", "test-prefix", currentDateTime, 1800, "", "", 0)`. Existing specs already assert `job.Spec.Template.Labels` contains `app: agent`.
</context>

<requirements>
1. In `pkg/spawner/job_spawner.go`, at the start of `(*jobSpawner).SpawnJob` (before `jobNameFromTask`), resolve the target namespace:
   ```go
   jobNamespace, err := pkg.ResolveJobNamespace(ctx, s.namespace, config.JobNamespace)
   if err != nil {
       return "", err
   }
   ```
   The later `envBuilder, err := s.buildJobEnvBuilder(ctx, task, config)` and `job, err := jobBuilder.Build(ctx)` statements remain valid as written: each uses `:=` and declares a new variable (`envBuilder`, `job`) alongside the existing `err`.
2. Use `jobNamespace` in both places that currently use `s.namespace` in `SpawnJob`:
   - `objectMetaBuilder.SetNamespace(jobNamespace)`
   - `s.kubeClient.BatchV1().Jobs(jobNamespace.String()).Create(ctx, job, metav1.CreateOptions{})`
   Leave the `AlreadyExists` handling unchanged; it now reads in the resolved namespace.
   Do NOT change `IsJobActive`, `JobExists`, `CountActiveJobs` or `ListActiveJobs` in this prompt — prompt 3 owns those.
3. In `pkg/handler/task_event_handler.go`, add `namespace libk8s.Namespace` as the second parameter of `NewTaskEventHandler` (immediately after `jobSpawner spawner.JobSpawner`), add `namespace libk8s.Namespace` as a field on `taskEventHandler`, and set `namespace: namespace` in the constructor's struct literal. Add the import `libk8s "github.com/bborbe/k8s"`.
4. Update every call site listed in the context section by inserting a namespace argument immediately after the spawner argument:
   - `pkg/factory/factory.go` → `namespace,`
   - every test call site → `"dev",` (an untyped string constant is assignable to `libk8s.Namespace`, so no import change is needed in those files)
5. In `spawnIfNeededClaimed`, immediately after the `skipForeignVaultTask` block and before the `current_job` block, add:
   ```go
   if config != nil {
       if _, err := pkg.ResolveJobNamespace(ctx, h.namespace, config.JobNamespace); err != nil {
           glog.Errorf(
               "event=job_namespace_rejected task=%s config=%s jobNamespace=%q: %v",
               task.TaskIdentifier, config.Assignee, config.JobNamespace, err,
           )
           metrics.TaskEventsTotal.WithLabelValues("error").Inc()
           return false, nil
       }
   }
   ```
   Returning `false, nil` (not an error) is deliberate: the task is left untouched, no claim is taken, and the next reconcile pass logs the same rejection until a human corrects the Config.
6. Add the required named tests in a new file `pkg/spawner/job_namespace_test.go` (`package spawner_test`, plain `testing`, no Ginkgo). Every test starts with `ctx := context.Background()` and imports `context`. Use `fake.NewClientset()` and this helper:
   ```go
   func newSpawnerForNamespace(
       t *testing.T,
       executorNamespace string,
   ) (*fake.Clientset, spawner.JobSpawner) {
       t.Helper()
       client := fake.NewClientset()
       return client, spawner.NewJobSpawner(
           client,
           libk8s.Namespace(executorNamespace),
           "kafka:9092",
           "dev",
           "",
           libtime.NewCurrentDateTime(),
           1800,
           "",
           "",
           0,
       )
   }
   ```
   Import `libk8s "github.com/bborbe/k8s"`, `lib "github.com/bborbe/agent"`, `libtime "github.com/bborbe/time"`, `"k8s.io/client-go/kubernetes/fake"`, and `pkg "github.com/bborbe/agent-task-executor/pkg"`. Use this task shape:
   ```go
   task := lib.Task{
       TaskIdentifier: lib.TaskIdentifier("abc12345"),
       Frontmatter:    lib.TaskFrontmatter{"assignee": "claude"},
   }
   config := pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"}
   ```
   - `func TestSpawnRejectsExecutorNamespace(t *testing.T)`: executor namespace `"dev"`, `config.JobNamespace = "dev"`. Assert `SpawnJob` returns a non-nil error whose `Error()` contains `may not equal the executor namespace`, and that the number of recorded actions with verb `create` and resource `jobs` is 0:
     ```go
     func countJobCreateActions(client *fake.Clientset) int {
         n := 0
         for _, action := range client.Actions() {
             if action.GetVerb() == "create" && action.GetResource().Resource == "jobs" {
                 n++
             }
         }
         return n
     }
     ```
   - `func TestSpawnTargetNamespace(t *testing.T)`: two sub-cases, both with executor namespace `"dev"`:
     - `config.JobNamespace = "dev-agents-sandbox"` → the recorded `create` action for resource `jobs` has `GetNamespace() == "dev-agents-sandbox"`;
     - `config.JobNamespace = ""` → the recorded `create` action has `GetNamespace() == "dev"`.
     Find the action with:
     ```go
     var createdNamespace string
     for _, action := range client.Actions() {
         if action.GetVerb() == "create" && action.GetResource().Resource == "jobs" {
             createdNamespace = action.GetNamespace()
         }
     }
     ```
   - `func TestJobPodLabels(t *testing.T)`: executor namespace `"dev"`, `config.JobNamespace = ""`. After a successful `SpawnJob`, list Jobs in `"dev"` and assert `jobs.Items[0].Spec.Template.Labels["app"] == "agent"`.
7. Every new file starts with the repository's BSD license header:
   ```go
   // Copyright (c) 2026 Benjamin Borbe All rights reserved.
   // Use of this source code is governed by a BSD-style
   // license that can be found in the LICENSE file.
   ```
</requirements>

<constraints>
- Existing `Config` CRs must remain valid unedited and unset `JobNamespace` must preserve today's behaviour exactly.
- A resolution error must create nothing and leave the task untouched; do not take a claim, do not write frontmatter, do not defer.
- The Kafka result-publishing path, `TaskType`/`TaskTypes` routing and `pkg/job_watcher.go` must not change.
- Do NOT introduce a new RBAC grant, capability or `securityContext` change.
- Do NOT edit the spec file.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass; update only the constructor arguments they need.
</constraints>

<verification>
Run from the repository root:

```bash
go test ./... -run 'TestSpawnRejectsExecutorNamespace|TestJobPodLabels|TestSpawnTargetNamespace' -v
```

The stdout must contain `--- PASS: TestSpawnRejectsExecutorNamespace`, `--- PASS: TestJobPodLabels` and `--- PASS: TestSpawnTargetNamespace`.

Then:

```bash
go test ./pkg/handler/... ./pkg/spawner/... ./pkg/factory/...
ROOTDIR=/workspace make precommit
```

Both commands must exit 0.
</verification>
