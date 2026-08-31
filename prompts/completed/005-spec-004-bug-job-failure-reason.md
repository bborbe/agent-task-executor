---
status: completed
spec: [004-bug-job-failure-reason]
summary: 'Split BackoffLimitExceeded from DeadlineExceeded in the job failure classifier: added pod_oom_killed reason, pod-resolution reason composition (reason + exit code in published text), internal regression tests, and CHANGELOG entry'
execution_id: agent-task-executor-failreason-exec-005-spec-004-bug-job-failure-reason
dark-factory-version: dev
created: "2026-08-31T23:55:00Z"
queued: "2026-08-31T22:03:30Z"
started: "2026-08-31T22:03:31Z"
completed: "2026-08-31T22:11:18Z"
branch: dark-factory/bug-job-failure-reason
---

<summary>
- A Job that exhausted its retry backoff limit is no longer mislabeled `deadline_exceeded`; only a genuine active-deadline kill still reports `deadline_exceeded`.
- A new machine-readable reason value `pod_oom_killed` is added to the closed reason set for OOM-killed pods, so operators can grep for OOM kills specifically.
- For backoff-limit failures, the published failure text now carries the pod's actual termination reason and exit code (e.g. `OOMKilled`/`137`) — no manual `kubectl get pod -o json` needed.
- Non-OOM pod deaths under a backoff-limit failure surface their own termination reason + exit code (e.g. `Error`/`1`) under the existing generic pod-failure reason, never `deadline_exceeded` and never `pod_oom_killed`.
- When the failed pod is no longer resolvable (GC'd or lister not synced), the Job still publishes a distinct non-deadline reason, just without pod detail.
- The executor log line and the vault task's `## Failure` body show the same specific reason string.
- The two existing tests that pinned the wrong `deadline_exceeded` mapping are corrected; new regression tests lock the OOM path and the non-OOM path.
- A `## Unreleased` CHANGELOG section is created for this fix.
</summary>

<objective>
Fix the job-failure classifier so a Job that hit `BackoffLimitExceeded` publishes a distinct pod-failure reason that surfaces the pod's terminated reason + exit code (with a dedicated `pod_oom_killed` for OOM kills), never the misleading `deadline_exceeded`, while leaving the genuine deadline path unchanged. Motivating incident (2026-08-16): a pod OOM-killed at ~6 min of a 30-min budget was reported as `deadline_exceeded`, and three genuinely different failures were all logged identically, misdirecting a post-mortem that tallied 82 Jobs as quota-starved.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for project conventions (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, counterfeiter mocks, glog V(n) gating, coverage ≥80% for new code).

Read these files fully before changing anything:
- `/workspace/pkg/job_watcher.go` — `JobFailureReason` (lines ~249-267), `HandleJob` (lines ~139-162), `handleTerminal` (lines ~164-182), `publishSyntheticFailure` (lines ~184-197), `PodLister()` (lines ~131-137), the `podLister atomic.Pointer[corev1listers.PodLister]` field (line 65), and the existing `classifyPodFailure`/`ownerJobName` helpers (lines ~320-363). Note `ownerJobName(pod)` already returns the owning Job name from a Pod's ownerRefs — reuse it.
- `/workspace/pkg/zombie_reason.go` — the closed `ZombieReason` set and its doc contract ("Adding a new value requires updating this list and the documentation").
- `/workspace/pkg/job_watcher_test.go` — the two `BackoffLimitExceeded` assertions to correct (HandleJob path ~line 240, direct-mapping path ~line 271) and the two `DeadlineExceeded` assertions that must stay (lines ~220 and ~262).
- `/workspace/pkg/zombie_reason_test.go` — the per-value `String()` assertions to extend.
- `/workspace/pkg/zombie_sweeper_test.go` — the informer-indexer pattern for building a test `PodLister` without starting informers (search `podInformer.GetIndexer().Add` / `informerFactory.Core().V1().Pods().Lister()`); mirror it in the new internal test file below.
- `/workspace/pkg/result_publisher.go` — `PublishFailure` (lines ~171-230) writes the reason verbatim into the vault `## Failure` body's `- **Reason:**` line; this is why the composed reason string must keep the greppable value as its leading token.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega, external test packages, coverage ≥80%.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — never `fmt.Errorf`; `fmt.Sprintf` for string building is an established pattern (see `result_publisher.go`).
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md` — `glog.V(2)` for per-failure events, `glog.Errorf` for lister lookup errors.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` format and prefix rules.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.
</context>

<requirements>
1. Add the new reason value in `/workspace/pkg/zombie_reason.go`. Add `ZombieReasonPodOOMKilled` to the const block after `ZombieReasonTypeMismatch`, keeping the existing alignment style (gofmt normalizes):
   ```go
   ZombieReasonPodOOMKilled      ZombieReason = "pod_oom_killed"
   ```
   Update the package doc comment (the `ZombieReason` block comment) so it states the new contract: OOM kills use the dedicated `pod_oom_killed` value so operators can grep for OOM specifically; other non-zero pod terminations fall back to the generic `pod_crash_no_stdout`. Keep the existing "adding a value requires updating this list and the documentation" sentence.

2. Add the matching `String()` assertion in `/workspace/pkg/zombie_reason_test.go`, inside the `Describe("String()", ...)` block (after the `type_mismatch` `It`):
   ```go
   It("returns pod_oom_killed for ZombieReasonPodOOMKilled", func() {
       Expect(pkg.ZombieReasonPodOOMKilled.String()).To(Equal("pod_oom_killed"))
   })
   ```

3. Split the classifier in `/workspace/pkg/job_watcher.go`. Replace the body of `JobFailureReason` so `BackoffLimitExceeded` maps to the generic pod-failure reason instead of `ZombieReasonDeadlineExceeded`, and update its doc comment. Do NOT merge the two conditions — a `DeadlineExceeded` condition always yields `deadline_exceeded`, even if a `BackoffLimitExceeded` condition is also present. New form:
   ```go
   // JobFailureReason maps a failed Job's conditions to a ZombieReason. Returns
   // ZombieReasonDeadlineExceeded when a Failed condition has Reason
   // "DeadlineExceeded" (the kubelet killed the pod for running past
   // activeDeadlineSeconds). Returns the generic pod-failure reason
   // ZombieReasonPodCrashNoStdout for "BackoffLimitExceeded" (the Job exhausted
   // its retry backoff limit — a distinct reason from a deadline kill; HandleJob
   // resolves the pod's terminated reason + exit code and surfaces them in the
   // published reason) and for any other Failed condition (the pod terminated
   // non-zero and no AgentResult was observed; the Job-condition informer only
   // fires AFTER terminal state, so absence of an AgentResult is implicit at this
   // point).
   func JobFailureReason(job *batchv1.Job) ZombieReason {
       for _, c := range job.Status.Conditions {
           if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
               switch c.Reason {
               case "DeadlineExceeded":
                   return ZombieReasonDeadlineExceeded
               case "BackoffLimitExceeded":
                   return ZombieReasonPodCrashNoStdout
               }
           }
       }
       return ZombieReasonPodCrashNoStdout
   }
   ```

4. Add a package-level condition helper near `IsJobFailed`:
   ```go
   // isBackoffLimitExceeded reports whether the Job's terminal Failed condition
   // has Reason "BackoffLimitExceeded" (the Job exhausted its retry backoff
   // limit).
   func isBackoffLimitExceeded(job *batchv1.Job) bool {
       for _, c := range job.Status.Conditions {
           if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue && c.Reason == "BackoffLimitExceeded" {
               return true
           }
       }
       return false
   }
   ```

5. Add a package-level pod-state helper:
   ```go
   // hasNonZeroTerminatedContainer reports whether any container terminated with
   // a non-zero exit code.
   func hasNonZeroTerminatedContainer(pod *corev1.Pod) bool {
       for _, cs := range pod.Status.ContainerStatuses {
           if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
               return true
           }
       }
       return false
   }
   ```

6. Add the pod-resolution method on `*jobWatcher`. It resolves the Job's most recently created pod from the shared pod lister (matched by Job owner reference via the existing `ownerJobName` helper) and returns its `.state.terminated.reason` and `exitCode`. Every failure path returns `ok=false` with no side effects — never a panic:
   ```go
   // terminalPodDetail returns the terminated reason and exit code of the Job's
   // most recently created pod that has a non-zero terminated container, matched
   // by Job owner reference. ok is false when the pod lister is not yet synced,
   // the lister lookup fails, or no owned pod carries a non-zero terminated
   // container.
   func (w *jobWatcher) terminalPodDetail(job *batchv1.Job) (terminatedReason string, exitCode int32, ok bool) {
       lister := w.PodLister()
       if lister == nil {
           return "", 0, false
       }
       pods, err := lister.Pods(job.Namespace).List(labels.Everything())
       if err != nil {
           glog.Errorf("job watcher: list pods for job %s/%s: %v", job.Namespace, job.Name, err)
           return "", 0, false
       }
       var best *corev1.Pod
       for _, pod := range pods {
           if ownerJobName(pod) != job.Name {
               continue
           }
           if !hasNonZeroTerminatedContainer(pod) {
               continue
           }
           if best == nil || pod.CreationTimestamp.After(best.CreationTimestamp.Time) {
               best = pod
           }
       }
       if best == nil {
           return "", 0, false
       }
       for _, cs := range best.Status.ContainerStatuses {
           if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
               return cs.State.Terminated.Reason, cs.State.Terminated.ExitCode, true
           }
       }
       return "", 0, false
   }
   ```

7. Add the reason-composition methods on `*jobWatcher`. The published reason string keeps the machine-readable `ZombieReason` value as its leading token and appends the pod detail in parentheses; the spec leaves the exact detail format to you and this is the chosen format. Use `fmt.Sprintf` (established in `result_publisher.go`), never `fmt.Errorf`:
   ```go
   // failureReason returns the reason string to publish for a failed Job. A
   // BackoffLimitExceeded failure surfaces the pod's terminated reason + exit
   // code when the pod is still resolvable (see backoffFailureReason); every
   // other failure publishes the bare ZombieReason value.
   func (w *jobWatcher) failureReason(job *batchv1.Job) string {
       reason := JobFailureReason(job)
       if reason == ZombieReasonDeadlineExceeded || !isBackoffLimitExceeded(job) {
           return reason.String()
       }
       return w.backoffFailureReason(job, reason)
   }

   // backoffFailureReason classifies a BackoffLimitExceeded Job using its
   // resolved pod's terminal state: the dedicated pod_oom_killed for an OOMKilled
   // pod, else the generic pod-failure reason, always appending the terminated
   // reason + exit code when a pod is resolvable so operators see the cause
   // without inspecting the pod.
   func (w *jobWatcher) backoffFailureReason(job *batchv1.Job, base ZombieReason) string {
       terminatedReason, exitCode, ok := w.terminalPodDetail(job)
       if !ok {
           return base.String()
       }
       if terminatedReason == "OOMKilled" {
           return fmt.Sprintf("%s (%s/%d)", ZombieReasonPodOOMKilled, terminatedReason, exitCode)
       }
       return fmt.Sprintf("%s (%s/%d)", base.String(), terminatedReason, exitCode)
   }
   ```
   Add `"fmt"` to the stdlib import group and `"k8s.io/apimachinery/pkg/labels"` to the k8s import group in this file. Do not worry about final import ordering — the precommit `format` step (goimports-reviser) normalizes it.

8. Wire the composition into `HandleJob` and the publish path (no interface change to `ResultPublisher`):
   - In `HandleJob`, change `reason := JobFailureReason(job)` to `reason := w.failureReason(job)`. The variable becomes a `string`; the existing `glog.V(2).Infof("job %s/%s failed (task %s): %s", job.Namespace, job.Name, taskID, reason)` and `w.handleTerminal(ctx, taskID, job, reason, true)` calls keep working unchanged. This makes the executor log line show the same specific reason as the published failure.
   - Change `handleTerminal`'s `reason ZombieReason` parameter to `reason string` (no other behavior change).
   - Change `publishSyntheticFailure`'s `reason ZombieReason` parameter to `reason string`, and change the `PublishFailure` call from `reason.String()` to `reason` — the reason string flows through `PublishFailure`'s existing `(ctx, task, jobName, reason string)` signature as-is.
   - Do NOT change `ResultPublisher` or the `PublishFailure` signature.
   - Spec Failure Mode row 5 (executor crashes mid-publish after classifying) is pre-existing behavior, NOT new work: `PublishFailure` is at-least-once via Kafka and its dedupe (keyed on job name) in `/workspace/pkg/result_publisher.go` already absorbs re-delivery. Do not modify that dedupe code.

9. Correct the two wrong `BackoffLimitExceeded` tests in `/workspace/pkg/job_watcher_test.go`:
   - HandleJob path (~line 240): rename the spec to "maps BackoffLimitExceeded job condition to pod_crash_no_stdout (no resolvable pod)" and change the final assertion from `Expect(calledReason).To(Equal("deadline_exceeded"))` to `Expect(calledReason).To(Equal("pod_crash_no_stdout"))`. This test runs against a watcher whose pod lister was never populated (`Run()` is never called, so `PodLister()` returns nil) — it exercises the nil-lister path of `terminalPodDetail` and proves the reason is distinct from `deadline_exceeded`.
   - Direct-mapping path (~line 271): rename to "returns pod_crash_no_stdout for BackoffLimitExceeded" and change the assertion from `pkg.ZombieReasonDeadlineExceeded` to `pkg.ZombieReasonPodCrashNoStdout`.
   - Do NOT touch the two `DeadlineExceeded` assertions (lines ~220 and ~262) — they must keep asserting `deadline_exceeded`.

10. Add an internal test file `/workspace/pkg/job_watcher_internal_test.go` in `package pkg` (this pairs an internal test file with the existing external one, matching the repo's `main_test.go` + `main_internal_test.go` precedent). It exists because the OOM/backoff pod-resolution path needs a populated pod lister on the real watcher, and `Run()` is never called in tests so `PodLister()` would otherwise return nil.
    - Helper `newTestPodLister(pods ...*corev1.Pod) corev1listers.PodLister`: create `fake.NewSimpleClientset()`, then `k8sinformers.NewSharedInformerFactoryWithOptions(fakeClient, 0, k8sinformers.WithNamespace("test-ns"))`, add each pod with `_ = informerFactory.Core().V1().Pods().Informer().GetIndexer().Add(pod)`, and return `informerFactory.Core().V1().Pods().Lister()` — mirror the exact pattern in `/workspace/pkg/zombie_sweeper_test.go`.
    - Helper `setPodLister(w JobWatcher, pods ...*corev1.Pod)`: `lister := newTestPodLister(pods...)` then `w.(*jobWatcher).podLister.Store(&lister)`.
    - `BeforeEach`: `ctx = context.Background()`, `fakePublisher = &mocks.FakeResultPublisher{}`, `taskStore = NewTaskStore()`, `watcher = NewJobWatcher(fake.NewSimpleClientset(), "test-ns", taskStore, fakePublisher)`, and a `testTask`/`testTaskID` pair mirroring the external file's setup (task type `lib.Task` from `github.com/bborbe/agent`, frontmatter with `status`/`assignee`).
    - Helper `makeJob(name, taskID string, conditions ...batchv1.JobCondition) *batchv1.Job`: same shape as the external file (namespace `"test-ns"`, label `agent.benjamin-borbe.de/task-id`, `Status.Conditions` from args).
    - Helper `backoffCondition() batchv1.JobCondition`: `{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"}`.
    - Helper `makeOwnedTerminatedPod(name, jobName, taskID, terminatedReason string, exitCode int32, createdAt string) *corev1.Pod`: namespace `"test-ns"`, labels `{"agent.benjamin-borbe.de/task-id": taskID}`, `OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: jobName}}`, `Status.Phase = corev1.PodFailed`, `Status.ContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: terminatedReason, ExitCode: exitCode}}}}`, and `CreationTimestamp = metav1.NewTime(t)` parsed from `createdAt` (RFC3339) when non-empty.
    - Then add these Ginkgo specs (each: `taskStore.Store(testTaskID, testTask)`, `watcher.HandleJob(ctx, job)`, assert `fakePublisher.PublishFailureCallCount()` is 1, capture `_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)`):
      a. OOMKilled pod resolvable → `Expect(calledReason).To(Equal("pod_oom_killed (OOMKilled/137)"))` and `Expect(calledReason).NotTo(ContainSubstring("deadline_exceeded"))`. This one spec satisfies ACs 2a, 3, and 4.
      b. Non-OOM terminated pod (`Error`/exit `1`) → `Expect(calledReason).To(Equal("pod_crash_no_stdout (Error/1)"))`, plus `NotTo(ContainSubstring("deadline_exceeded"))` and `NotTo(ContainSubstring("pod_oom_killed"))`. Satisfies AC 2b.
      c. Two pods owned by the same job with different terminated reasons (earlier `Error`/`1`, later `OOMKilled`/`137` — distinct `createdAt` timestamps) → `Expect(calledReason).To(Equal("pod_oom_killed (OOMKilled/137)"))`, proving the terminal (last) pod's state is used (spec Failure Mode row 4).
      d. Empty lister cache (no pods) → `Expect(calledReason).To(Equal("pod_crash_no_stdout"))` with no detail (spec Failure Mode rows 1 and 3).
      e. An owned pod with no non-zero terminated container (e.g. `corev1.PodRunning` with no terminated state) → `Expect(calledReason).To(Equal("pod_crash_no_stdout"))` with no detail.

11. Add the CHANGELOG entry in `/workspace/CHANGELOG.md`: create `## Unreleased` directly under the `# Changelog` heading (none exists at HEAD v0.8.0) with one bullet following the changelog-guide format:
    ```
    ## Unreleased

    - fix: split `BackoffLimitExceeded` from `DeadlineExceeded` in the job failure classifier — a Job that exhausted its backoff limit now publishes the pod's terminated reason + exit code (dedicated `pod_oom_killed` for OOM kills) instead of a misleading `deadline_exceeded`
    ```

12. Do NOT touch `pkg/zombie_sweeper.go`'s `classify` deadline fallback — the spec's Non-goal explicitly defers it as a follow-up.
</requirements>

<constraints>
- Repo conventions are frozen: Ginkgo/Gomega v2 tests (no stdlib table tests), `github.com/bborbe/errors` wrapping (never `fmt.Errorf`), counterfeiter mocks for any new dependency, glog `V(n)` gating.
- `ZombieReason` is a closed machine-readable set (`pkg/zombie_reason.go`). The new `pod_oom_killed` value is added deliberately and documented in the doc comment, which states that adding a value requires updating the list and the documentation.
- Do NOT change `PublishFailure`'s interface or signature — the reason string flows through as-is. You may change the internal `handleTerminal`/`publishSyntheticFailure` parameter type from `ZombieReason` to `string`.
- Assumption: the failed Job's pod is usually still resolvable at `HandleJob` time (`TTLSecondsAfterFinished` keeps it until TTL expiry). When it is not (pod GC'd / lister not synced / lister nil), the Job-condition path still publishes a distinct pod-failure reason without pod detail — never `deadline_exceeded`.
- The two existing `DeadlineExceeded` assertions in `pkg/job_watcher_test.go` stay; the two `BackoffLimitExceeded` assertions that pin the wrong behavior are updated.
- CHANGELOG: add an `## Unreleased` bullet for this fix (creating the section if absent — none exists at HEAD v0.8.0).
- Non-goals are load-bearing: do NOT raise the Job memory limit, do NOT fix the GitHub App `workflows`-scope permission, do NOT touch the resync `not in task store` warning noise, do NOT touch `zombie_sweeper.go`'s `classify` deadline fallback.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `make test` iteratively after each meaningful change (fast feedback loop), then `make precommit` ONCE at the very end.

- `make test` — exits 0.
- `make precommit` — exits 0.
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/... && go tool cover -func=/tmp/cover.out` — confirm the new functions (`failureReason`, `backoffFailureReason`, `terminalPodDetail`, `isBackoffLimitExceeded`, `hasNonZeroTerminatedContainer`) are each exercised by at least one test (≥80% statement coverage for new code).
- `grep -n 'BackoffLimitExceeded' pkg/job_watcher.go` — returns at least one line that does NOT map to `ZombieReasonDeadlineExceeded` (acceptance-criterion evidence).
- `grep -n 'pod_oom_killed' pkg/zombie_reason.go pkg/zombie_reason_test.go pkg/job_watcher.go pkg/job_watcher_internal_test.go` — shows the new value in the closed set, its `String()` assertion, and its production usage.

Do NOT run `docker`, `make build`, `kubectl`, or `dark-factory` commands in this prompt — those are operator-executable and belong on the spec's verification ladder.
</verification>
