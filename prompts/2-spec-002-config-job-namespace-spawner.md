---
status: draft
spec: [002-config-job-namespace]
created: "2026-09-03T17:50:40Z"
branch: dark-factory/config-job-namespace
---

<summary>
- Spawned Jobs are now created in the resolved namespace (default `<executor-namespace>-agents-sandbox`) instead of the executor's own namespace.
- A Config that sets `jobNamespace` to the executor's own namespace is refused: the job is not created, an ERROR log names the Config and the rejected namespace, and the error propagates so the task stays untouched.
- Every spawned Job's pod template carries the label `app: agent` unconditionally — a conflicting `app` value set directly on the pod template is overwritten, so no Config can suppress the NetworkPolicy selector.
- The Job's object metadata namespace matches the resolved target namespace, not the executor's.
- Existing spawner tests are updated for the new default target namespace, and the new acceptance tests lock the target-namespace, refuse-on-error, and label-lock behaviors.
- The `## Unreleased` changelog gains one feature entry for the whole capability.
</summary>

<objective>
Wire the spawner to consume the resolved `JobNamespace` from the previous prompt: create every Job in the resolved namespace (defaulting to the per-environment sandbox), refuse with an ERROR log when a Config tries to use the executor's own namespace, and stamp `app: agent` on every Job pod template unconditionally — so no Config can produce an unisolated Job, by omission or by explicit override.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for project conventions (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, glog V(n) gating, coverage ≥80% for new code).

Read these files fully before changing anything:
- `/workspace/pkg/spawner/job_spawner.go` — `SpawnJob` (the whole flow: `jobNameFromTask` → `buildJobEnvBuilder` → `k8s.NewJobBuilder()` + `jobBuilder.SetApp("agent")` → `jobBuilder.Build(ctx)` → the `applyTaskIDLabel`/`applyAssigneeLabel`/... apply block → `s.kubeClient.BatchV1().Jobs(s.namespace.String()).Create(...)`), the `jobSpawner` struct field `namespace k8s.Namespace`, and the existing apply-helpers (`applyTaskIDLabel`, `applyAssigneeLabel`) that mutate `job.Spec.Template.Labels` with nil-map guards — `applyAppLabel` mirrors them. Also `IsJobActive` and `CountActiveJobs` (both list `s.namespace.String()` — they are NOT changed by this prompt).
- `/workspace/pkg/spawner/job_spawner_test.go` — the Ginkgo suite. The `BeforeEach` builds a spawner with executor namespace `"test-ns"`; with the new code the default sandbox for that executor is `"test-ns-agents-sandbox"`, so every SpawnJob verification that reads created jobs from `Jobs("test-ns")` must move to `Jobs("test-ns-agents-sandbox")`. Note the existing `Describe("applyTaskIDLabel", ...)` test that captures the created job via a `PrependReactor("create", "jobs", ...)` — that capture pattern is namespace-independent and is the pattern `TestJobPodLabels` uses.
- `/workspace/pkg/spawner/job_namespace_test.go` does not exist yet — create it (this prompt).
- `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/types.go` — `ResolveJobNamespace(ctx, jobNamespace, executorNamespace string) (string, error)` from the previous prompt (its error message contains `may not equal the executor namespace`).
- `/workspace/pkg/agent_configuration.go` — `AgentConfiguration.JobNamespace` field from the previous prompt (the spawner reads `config.JobNamespace` from here).
- `/workspace/pkg/k8s_connector.go` — nothing to change here in this prompt; listed so you can confirm the `jobNamespace` schema property already landed from the previous prompt.
- `/workspace/CHANGELOG.md` — `## Unreleased` already exists at the top; append to it, do not create it.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md` — package-level apply-helper style (see `applyTaskIDLabel`).
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — never `fmt.Errorf`; `errors.Wrapf(ctx, err, ...)`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md` — `glog.Errorf` for the ERROR-level rejection log, `glog.V(2)` for the existing success/AlreadyExists logs.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — coverage ≥80% for new code; internal (`package spawner`) vs external (`package spawner_test`) test files can coexist in one directory.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` format and prefix rules.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.
</context>

<requirements>
The end state: `SpawnJob` resolves the target namespace per Config (empty → `<executor>-agents-sandbox`, explicit → that value), refuses the executor's own namespace with an ERROR log naming the Config and the rejected namespace, creates the Job in the resolved namespace with `app: agent` stamped unconditionally on the pod template, and all four spec acceptance tests pass.

1. In `/workspace/pkg/spawner/job_spawner.go`, add the import `agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"` to the project import group (the same alias used in `pkg/config_resolver.go` and `pkg/spawner/job_spawner_test.go`).

2. At the top of `SpawnJob`, immediately after the `jobName := jobNameFromTask(...)` line and before `buildJobEnvBuilder`, resolve the target namespace and refuse on error (spec DB 5: resolution error → no Job created, ERROR log naming the Config and the rejected namespace, task left untouched):
   ```go
   targetNamespace, err := agentv1.ResolveJobNamespace(ctx, config.JobNamespace, s.namespace.String())
   if err != nil {
       glog.Errorf(
           "config %q jobNamespace %q rejected, not spawning job for task %s: %v",
           config.Assignee,
           config.JobNamespace,
           task.TaskIdentifier,
           err,
       )
       return "", err
   }
   ```
   `config.Assignee` is the Config name and `config.JobNamespace` is the rejected namespace. `err` is not yet declared at this point, so `:=` introduces both names; the later `envBuilder, err := ...` line continues to compile unchanged (reuses `err`). Returning `("", err)` propagates to the handler, which does NOT store the task, publish a spawn notification, or mark the task — the task stays untouched.

3. Set the built Job's object metadata namespace to the target (replace the current `objectMetaBuilder.SetNamespace(s.namespace)`):
   ```go
   objectMetaBuilder.SetNamespace(k8s.Namespace(targetNamespace))
   ```
   `k8s.Namespace` is `type Namespace string`; `SetNamespace(namespace Namespace)` is the `ObjectMetaBuilder` method (both already in use in this file).

4. Create the Job against the target namespace (replace the current `Jobs(s.namespace.String())`):
   ```go
   _, err = s.kubeClient.BatchV1().
       Jobs(targetNamespace).
       Create(ctx, job, metav1.CreateOptions{})
   ```
   Keep the existing `k8serrors.IsAlreadyExists` handling and the `errors.Wrapf(ctx, err, "create job %s for task %s failed", ...)` unchanged.

5. Add the unconditional label stamp (spec DB 4 / AC 6) and call it in the apply block. The `jobBuilder.SetApp("agent")` call stays (it seeds the builder labels), and the new helper is the post-build guard that makes the label unconditional and overwrites any conflicting value. Add to the apply block after `applyAssigneeLabel(config.Assignee, job)`:
   ```go
   applyAppLabel(job)
   ```
   Add the helper next to `applyTaskIDLabel` / `applyAssigneeLabel` (same nil-map-guard style):
   ```go
   // applyAppLabel stamps the app: agent label on the Job's pod template
   // unconditionally so a NetworkPolicy selector can always match spawned Job
   // pods. Any conflicting app value is overwritten — the label is set by the
   // spawner and not overridable by Config-supplied labels.
   func applyAppLabel(job *batchv1.Job) {
       if job.Spec.Template.Labels == nil {
           job.Spec.Template.Labels = map[string]string{}
       }
       job.Spec.Template.Labels["app"] = "agent"
   }
   ```

6. Create `/workspace/pkg/spawner/job_namespace_test.go` in `package spawner` (internal test package so it can call the unexported `applyAppLabel`; a plain stdlib test file — the spec's acceptance criteria require `--- PASS: TestSpawnRejectsExecutorNamespace` / `--- PASS: TestJobPodLabels` / `--- PASS: TestSpawnTargetNamespace` output, which the Ginkgo suite cannot produce). This file coexists with the Ginkgo suite in `job_spawner_test.go` (`package spawner_test`); both run under `go test`. Include a file-scoped helper mirroring the suite's `BeforeEach` construction:
   ```go
   func newNamespaceTestSpawner(t *testing.T, executorNS string) (*fake.Clientset, JobSpawner) {
       fakeClient := fake.NewClientset()
       currentDateTime := libtime.NewCurrentDateTime()
       currentDateTime.SetNow(libtimetest.ParseDateTime("2026-04-03T17:35:00Z"))
       sp := NewJobSpawner(fakeClient, k8s.Namespace(executorNS), "kafka:9092", "develop", "test-prefix", currentDateTime, 1800, "", "")
       return fakeClient, sp
   }
   ```
   plus a `makeTask()` / `makeConfig()` pair matching the suite's fixtures. Then the three acceptance tests:

   a. `TestSpawnTargetNamespace` (AC 7): spawn with executor `"dev"` and an empty `JobNamespace`, then scan `fakeClient.Actions()` for the create action and assert the recorded action namespace is `dev-agents-sandbox`:
   ```go
   func TestSpawnTargetNamespace(t *testing.T) {
       fakeClient, sp := newNamespaceTestSpawner(t, "dev")
       task := lib.Task{TaskIdentifier: lib.TaskIdentifier("abc12345-rest-ignored"), Frontmatter: lib.TaskFrontmatter{"assignee": "claude"}}
       config := pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest", Env: map[string]string{}}
       if _, err := sp.SpawnJob(context.Background(), task, config); err != nil {
           t.Fatalf("SpawnJob: %v", err)
       }
       var createNS string
       for _, action := range fakeClient.Actions() {
           if action.Matches("create", "jobs") {
               if ca, ok := action.(k8stesting.CreateAction); ok {
                   createNS = ca.GetNamespace()
               }
           }
       }
       if createNS != "dev-agents-sandbox" {
           t.Fatalf("create action namespace = %q, want %q", createNS, "dev-agents-sandbox")
       }
   }
   ```
   Imports for this file: `context`, `testing`, `lib "github.com/bborbe/agent"`, `libtime "github.com/bborbe/time"`, `libtimetest "github.com/bborbe/time/test"`, `k8s "github.com/bborbe/k8s"`, `batchv1 "k8s.io/api/batch/v1"`, `corev1 "k8s.io/api/core/v1"`, `metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"`, `"k8s.io/apimachinery/pkg/runtime"`, `"k8s.io/client-go/kubernetes/fake"`, `k8stesting "k8s.io/client-go/testing"`, `pkg "github.com/bborbe/agent-task-executor/pkg"`.

   b. `TestSpawnRejectsExecutorNamespace` (AC 5): executor `"dev"`, config with `JobNamespace: "dev"` (equal to the executor namespace). Assert `SpawnJob` returns a non-nil error whose message contains `may not equal the executor namespace`, and that the fake clientset recorded ZERO create actions on `jobs` (negative evidence):
   ```go
   func TestSpawnRejectsExecutorNamespace(t *testing.T) {
       fakeClient, sp := newNamespaceTestSpawner(t, "dev")
       task := lib.Task{TaskIdentifier: lib.TaskIdentifier("abc12345-rest-ignored"), Frontmatter: lib.TaskFrontmatter{"assignee": "claude"}}
       config := pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest", Env: map[string]string{}, JobNamespace: "dev"}
       _, err := sp.SpawnJob(context.Background(), task, config)
       if err == nil {
           t.Fatal("SpawnJob expected error for JobNamespace == executor namespace, got nil")
       }
       if !strings.Contains(err.Error(), "may not equal the executor namespace") {
           t.Fatalf("error %q does not contain %q", err.Error(), "may not equal the executor namespace")
       }
       createCount := 0
       for _, action := range fakeClient.Actions() {
           if action.Matches("create", "jobs") {
               createCount++
           }
       }
       if createCount != 0 {
           t.Fatalf("expected 0 create actions on jobs, got %d", createCount)
       }
   }
   ```
   (add `strings` to the imports.)

   c. `TestJobPodLabels` (AC 6): part 1 — capture the generated `batchv1.Job` via a `PrependReactor("create", "jobs", ...)` (mirror the capture pattern in the suite's `Describe("applyTaskIDLabel", ...)`) and assert `job.Spec.Template.Labels["app"] == "agent"`; part 2 — build a `batchv1.Job` whose pod template labels already contain a conflicting `"app"` value set directly, call `applyAppLabel(&job)`, and assert the label was overwritten to `"agent"`:
   ```go
   func TestJobPodLabels(t *testing.T) {
       fakeClient, sp := newNamespaceTestSpawner(t, "test-ns")
       var created *batchv1.Job
       fakeClient.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
           ca, ok := action.(k8stesting.CreateAction)
           if !ok {
               return false, nil, nil
           }
           created, ok = ca.GetObject().(*batchv1.Job)
           if !ok {
               return false, nil, nil
           }
           return false, nil, nil
       })
       task := lib.Task{TaskIdentifier: lib.TaskIdentifier("label-lock-task"), Frontmatter: lib.TaskFrontmatter{"assignee": "claude"}}
       config := pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest", Env: map[string]string{}}
       if _, err := sp.SpawnJob(context.Background(), task, config); err != nil {
           t.Fatalf("SpawnJob: %v", err)
       }
       if created == nil {
           t.Fatal("no created job captured")
       }
       if got := created.Spec.Template.Labels["app"]; got != "agent" {
           t.Fatalf("pod template app label = %q, want %q", got, "agent")
       }

       conflicting := &batchv1.Job{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "not-agent"}}}}}
       applyAppLabel(conflicting)
       if got := conflicting.Spec.Template.Labels["app"]; got != "agent" {
           t.Fatalf("applyAppLabel did not overwrite conflicting app label: got %q, want %q", got, "agent")
       }
   }
   ```

7. Update the existing Ginkgo suite `/workspace/pkg/spawner/job_spawner_test.go` for the new default target namespace. With executor namespace `"test-ns"` and an empty `JobNamespace`, `SpawnJob` now creates Jobs in `"test-ns-agents-sandbox"`. Mechanically:
   - Replace every `fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})` and `.Jobs("test-ns").Get(...)` call that verifies a Job created by `SpawnJob` with the same call against `"test-ns-agents-sandbox"`. This covers the SpawnJob `It` blocks (job name/env vars, TOPIC_PREFIX, TTL, TASK_CONTENT round-trips, task-id label, env vars, job name, PVC, secret envFrom, image pull secret, resources, priorityClassName, TASK_TYPE) and the `Describe("ActiveDeadlineSeconds", ...)` `Get` calls.
   - In the Kafka mTLS `DescribeTable`, change the verification `fakeClient.BatchV1().Jobs("test-ns").List(...)` to `Jobs("test-ns-agents-sandbox")`.
   - In `It("returns job name when job already exists (AlreadyExists)", ...)`, the pre-seeded `existingJob` must live in the namespace SpawnJob now creates into: change its `ObjectMeta.Namespace` from `"test-ns"` to `"test-ns-agents-sandbox"`.
   - Do NOT change the `IsJobActive` / `CountActiveJobs` specs' fixtures (they pre-seed jobs in `"test-ns"` and query through `IsJobActive`/`CountActiveJobs`, which remain executor-namespace-scoped and unchanged), and do NOT change `It("returns error on unexpected K8s error", ...)` (its create reactor fires regardless of namespace) or `It("returns error when VolumeClaim is set but VolumeMountPath is empty", ...)` (errors before create).
   - Leave the `Describe("applyTaskIDLabel", ...)` capture-reactor specs unchanged (they assert on the captured object, namespace-independent).

8. Update the `Describe("SpawnJob + IsJobActive label contract", ...)` block (two `It`s) — it currently asserts that a job just spawned via `SpawnJob` is visible to `IsJobActive`, which is no longer true because `SpawnJob` places the job in `"test-ns-agents-sandbox"` while `IsJobActive` queries `"test-ns"`. Preserve the block's actual regression intent — that SpawnJob and IsJobActive agree on the `agent.benjamin-borbe.de/task-id` label key — without depending on cross-namespace visibility:
   - First `It` ("IsJobActive returns true for a Job just spawned via SpawnJob"): spawn a task via `SpawnJob`; read the created job back from `"test-ns-agents-sandbox"` and assert its `Labels["agent.benjamin-borbe.de/task-id"]` equals the task id (the label SpawnJob stamps); then seed a `batchv1.Job` in the executor namespace `"test-ns"` carrying exactly that task-id label value (`ObjectMeta.Namespace: "test-ns"`, `Labels: {"agent.benjamin-borbe.de/task-id": <taskID>}`, `Status: batchv1.JobStatus{}`) via `fake.NewClientset(...)` re-wiring the spawner, and assert `IsJobActive` returns true for that task id.
   - Second `It` ("IsJobActive returns false for a different taskID"): seed an executor-namespace job labelled with a different task id and assert `IsJobActive` returns false for the spawned task's id.
   - Add a comment on the `Describe` block noting that `SpawnJob` now creates Jobs in the resolved namespace while `IsJobActive`/`CountActiveJobs` remain executor-namespace-scoped, and that cross-namespace visibility is a spec-level follow-up (see the OPEN QUESTION note below) — do NOT change `IsJobActive`, `CountActiveJobs`, or the JobWatcher in this prompt.

9. Append one feature entry to the existing `## Unreleased` section in `/workspace/CHANGELOG.md` (it already exists — do not create a second one), following the changelog-guide format:
   ```
   - feat: Configs can place agent Jobs in a configurable namespace via `spec.jobNamespace` — unset resolves to a per-environment `<executor-namespace>-agents-sandbox` sandbox, setting the executor's own namespace is rejected, and every spawned Job pod carries an unconditional `app: agent` label
   ```

10. Do NOT change `IsJobActive`, `CountActiveJobs`, the `JobSpawner` interface, `NewJobSpawner`'s signature, `pkg/factory/factory.go`, `main.go`, or the JobWatcher — the spec scopes this prompt to the create path.
</requirements>

<constraints>
- Existing `Config` CRs must remain valid unedited — `JobNamespace` is additive and optional.
- The default must be the safe value: omitting the field may never place a Job in the executor's namespace.
- The Kafka result-publishing path must not change; the agent still publishes its own result.
- No new capability, securityContext change, or RBAC grant is introduced by this spec.
- `TaskType`/`TaskTypes` routing behavior is untouched.
- OPEN QUESTION (for the human reviewer — the container agent must NOT act on this): this spec changes where `SpawnJob` places Jobs (the sandbox) but does not re-scope the executor's Job queries. `IsJobActive` and `CountActiveJobs` (both in `/workspace/pkg/spawner/job_spawner.go`, executor-namespace `List`) and the `JobWatcher` informer (`/workspace/pkg/job_watcher.go`, `k8sinformers.WithNamespace(string(w.namespace))`) continue to observe only the executor namespace, so sandboxed Jobs are invisible to them — no duplicate-spawn guard, no concurrency cap, no zombie/deadline/failure handling for sandboxed Jobs. The spec's ACs and DBs do not address these paths. Do NOT change them in this prompt; the reviewer should decide whether a follow-up spec must scope these to the resolved namespace. Requirement 8 documents this in the affected test.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `make test` iteratively after each meaningful change (fast feedback loop), then `make precommit` ONCE at the very end.

- `make test` — exits 0.
- `make precommit` — exits 0.
- `go test ./... -run 'TestResolveJobNamespace|TestSpawnRejectsExecutorNamespace|TestJobPodLabels|TestSpawnTargetNamespace' -v` — stdout contains `--- PASS:` for each of the four tests.
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/spawner/... && go tool cover -func=/tmp/cover.out` — the new error branch of `SpawnJob` (refuse-on-error) and `applyAppLabel` are each exercised by at least one test (≥80% statement coverage for new code).
- `grep -n 'ResolveJobNamespace\|Jobs(targetNamespace)\|applyAppLabel' pkg/spawner/job_spawner.go` — shows the resolution call, the target-namespace Create, and the label stamp in production code.

Do NOT run `docker`, `make build`, `kubectl`, `dark-factory`, or `git` commands in this prompt — the container cannot execute them and the daemon does not check their exit codes.
</verification>
