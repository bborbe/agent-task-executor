---
status: failed
spec: [021-bug-deadline-kill-never-arms-trigger-cap]
execution_id: agent-task-executor-deadline-cap-exec-056-spec-021-deadline-arm-trigger-cap
dark-factory-version: v0.198.7
created: "2026-11-03T20:00:11Z"
queued: "2026-11-03T20:06:30Z"
started: "2026-11-03T20:10:57Z"
completed: "2026-11-03T20:10:58Z"
lastFailReason: 'execute prompt: docker run failed: wait command: exit status 1'
---

# Deadline kill: arm the trigger cap when the pod never scheduled

<summary>
- A task whose Job dies on `activeDeadlineSeconds` without ever being placed on a node now arms the trigger cap, so it stops re-queuing forever
- The cap is written on the first such failure (`max_triggers: 3`), and the controller clears the assignee on the third
- A Job that actually ran and then hit its deadline keeps today's behaviour: no cap is written
- A Job whose pod is gone or unresolvable when the failure is handled also keeps today's behaviour: no cap is written
- The zombie sweeper's failure publications never arm the cap, because it supplies no pod
- The store-miss failure path never arms the cap, because its nil `max_triggers` sentinel is preserved
- An operator-set `max_triggers` is never lowered or overwritten on the deadline path
- The existing crash (`pod_crash_no_stdout`) arming behaviour is unchanged
- The arm decision is visible in the log as `parsed="deadline_exceeded"` plus `decision=arm`, distinguishable from the crash arm
- CHANGELOG records the fix under `## Unreleased`
</summary>

<objective>
Close the retry-cap hole for deadline-killed Jobs that were never scheduled: carry the failed Job's pod into the failure publish path, and let the existing arm gate in `crashFailureUpdates` write `max_triggers: 3` when — and only when — that pod carries `PodScheduled=False`. Every other failure class, every already-capped record, the store-miss path, and the zombie sweeper keep their current behaviour.
</objective>

<context>
Read these files in full before changing anything:

- `pkg/result_publisher.go` — the `ResultPublisher` interface and `resultPublisher` implementation. The arm gate is `crashFailureUpdates` (currently `func crashFailureUpdates(task lib.Task, jobName, reason string) lib.TaskFrontmatter`, around line 515), called from the unexported `func (p *resultPublisher) publishFailure(ctx context.Context, task lib.Task, jobName string, reason string, note string) error` (around line 614). `PublishFailure` and `PublishStoreMissFailure` are thin wrappers over `publishFailure`. `defaultMaxTriggers = 3` is the cap constant. `ParseZombieReason` (`pkg/zombie_reason.go`) matches up to the first space and maps Kubernetes condition aliases; `ZombieReasonDeadlineExceeded == "deadline_exceeded"`.
- `pkg/job_watcher.go` — the `jobWatcher` and its three failure publish sites. `HandleJob` classifies a failed Job via `failureReason(job)` and calls `handleTerminal`, which calls `publishSyntheticFailure(ctx, taskID, task, job, reason)` (around line 808) for a store hit and `publishStoreMissFailure` (around line 721) for a store miss. `HandlePod` (around line 1016) publishes directly. `PodLister(namespace libk8s.Namespace) corev1listers.PodLister` (line 352) is the existing accessor; `terminalPodDetail` (line 1127) is the existing example of listing a Job's pods from it. `ownerJobName(pod *corev1.Pod) string` matches a pod to its Job.
- `pkg/zombie_sweeper.go` — `SweepOnce` publishes at line 159. `hasPodScheduledFalse(pod *corev1.Pod) bool` already exists here (line 276) and is the single definition of "never scheduled" that this change must reuse.
- `pkg/result_publisher_test.go` (package `pkg_test`) — the `Describe("PublishFailure trigger cap", ...)` block (around line 443), `captureGlogStderr` (line 41), `decodeUpdateFrontmatterCommand`, and `capturingSyncProducer`. Note the existing comment that a command read back through JSON decodes numbers as `float64`, so `max_triggers` must be asserted with `BeNumerically("==", 3)`.
- `pkg/job_watcher_internal_test.go` (package `pkg`) — the test-local `fakeResultPublisher` (line 347), `resultPublisherCall` (line 359), `setPodLister` (line 284), and the `HandleJob` fixtures. This fake exists because the `mocks` package imports `pkg`, so an internal test cannot import it.
- `pkg/zombie_sweeper_test.go` — the `PublishFailureArgsForCall` destructuring at lines 141, 281, 333 and the `PublishFailureStub` at line 515.
- `pkg/envtest/job_watcher_envtest_test.go` — `//go:build envtest`; the `PublishFailureArgsForCall` destructuring at line 175. Not compiled by a default `go test`, but it must still compile under the `envtest` tag.

Coding-plugin guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `errors.Wrapf(ctx, err, ...)`, never `fmt.Errorf`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md` — `V(2)` and the `event=` convention the existing `crash_arm_decision` line follows
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2 conventions
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` bullet format

Verified facts (do not re-derive, do not "improve"):
- `hasPodScheduledFalse` iterates `pod.Status.Conditions` looking for `Type == corev1.PodScheduled && Status == corev1.ConditionFalse`. It dereferences its argument, so callers must guard against nil.
- `ownerJobName` returns the name of the first `OwnerReference` with `Kind == "Job"`, else `""`.
- `terminalPodDetail` cannot be reused for this lookup: it filters on a non-zero terminated container, and a never-scheduled pod has none.
- `PublishStoreMissFailure` builds its task with `lib.TaskFrontmatter{"max_triggers": nil}` (`taskFromFailedJob`); the gate treats a present key as already-armed, so that path must stay unarmed.
- `specs/in-progress/004-bug-job-failure-reason.md` already landed the `DeadlineExceeded` / `BackoffLimitExceeded` split in `JobFailureReason` and `failureReason`. Do not re-open it.
</context>

<requirements>
1. **Widen the publisher interface with the pod.** In `pkg/result_publisher.go`, add the import `corev1 "k8s.io/api/core/v1"` and change the `ResultPublisher` interface method to:

   ```go
   PublishFailure(ctx context.Context, task lib.Task, jobName string, reason string, pod *corev1.Pod) error
   ```

   `pod` is the failed Job's pod as observed by the caller; `nil` means the caller has no pod (the zombie sweeper, or an unresolvable pod). Extend the existing GoDoc on the method with one paragraph stating: the pod is used only to discriminate the never-scheduled deadline class at the arm gate; `nil` never arms; and the pod is never written to the record. Keep the existing crash-class documentation intact. Do **not** change `PublishStoreMissFailure`'s signature.

2. **Thread the pod through `publishFailure`.** Change the unexported helper to:

   ```go
   func (p *resultPublisher) publishFailure(
   	ctx context.Context,
   	task lib.Task,
   	jobName string,
   	reason string,
   	note string,
   	pod *corev1.Pod,
   ) error
   ```

   - `PublishFailure` passes its `pod` argument through.
   - `PublishStoreMissFailure` passes `nil` for `pod`. This is deliberate and must be commented: the store-miss path carries no pod, and its `max_triggers: nil` sentinel already suppresses arming; passing a pod here would be the only way to break that sentinel's guarantee.
   - The call inside `publishFailure` becomes `crashFailureUpdates(task, jobName, reason, pod)`.

3. **Rewrite the arm gate.** Replace the body of `crashFailureUpdates` (keep its name, package-level visibility, and every existing explanatory paragraph that is still true) with the logic below. Add the `pod *corev1.Pod` parameter. The returned `Updates` map still always starts as `lib.TaskFrontmatter{"current_job": ""}`; `max_triggers` is added only on the arm path.

   ```go
   func crashFailureUpdates(
   	task lib.Task,
   	jobName, reason string,
   	pod *corev1.Pod,
   ) lib.TaskFrontmatter {
   	updates := lib.TaskFrontmatter{"current_job": ""}
   	parsed, parsedOK := ParseZombieReason(reason)
   	armClass := parsedOK && (parsed == ZombieReasonPodCrashNoStdout ||
   		(parsed == ZombieReasonDeadlineExceeded && pod != nil && hasPodScheduledFalse(pod)))
   	if !armClass {
   		if parsedOK && parsed == ZombieReasonDeadlineExceeded {
   			glog.V(2).Infof(
   				"event=crash_arm_decision task=%s job=%s reason=%q parsed=%q parsed_ok=%t "+
   					"decision=not_never_scheduled pod_resolved=%t",
   				task.TaskIdentifier, jobName, reason, parsed.String(), parsedOK, pod != nil,
   			)
   			return updates
   		}
   		glog.V(2).Infof(
   			"event=crash_arm_decision task=%s job=%s reason=%q parsed=%q parsed_ok=%t "+
   				"decision=not_crash_class",
   			task.TaskIdentifier, jobName, reason, parsed.String(), parsedOK,
   		)
   		return updates
   	}
   	cachedMax, maxPresent := task.Frontmatter["max_triggers"]
   	if maxPresent {
   		glog.V(2).Infof(
   			"event=crash_arm_decision task=%s job=%s reason=%q parsed=%q parsed_ok=%t "+
   				"decision=max_triggers_already_present cached_max_triggers=%v",
   			task.TaskIdentifier, jobName, reason, parsed.String(), parsedOK, cachedMax,
   		)
   		return updates
   	}
   	glog.V(2).Infof(
   		"event=crash_arm_decision task=%s job=%s reason=%q parsed=%q parsed_ok=%t "+
   			"decision=arm cached_max_triggers=<absent> max_triggers=%d",
   		task.TaskIdentifier, jobName, reason, parsed.String(), parsedOK, defaultMaxTriggers,
   	)
   	updates["max_triggers"] = defaultMaxTriggers
   	return updates
   }
   ```

   Notes that must be reflected in the function's GoDoc (rewrite the prose, do not paste this list):
   - Reuse `hasPodScheduledFalse` — do not add a second `PodScheduled` check anywhere.
   - A pod that reached a terminal phase before the deadline carries no `PodScheduled=False` condition, so the same predicate covers Desired Behavior 3 without a phase check.
   - `nil` is not evidence of never having scheduled, so it never arms.
   - The already-present `max_triggers` check now sits *after* the class gate, so it protects both classes; an operator budget of 5 or 6 is never lowered to 3.
   - The arm log line must keep the existing `event=crash_arm_decision`, `parsed=%q`, and `decision=arm` shape so a deadline arm is greppable and distinguishable from a crash arm.

4. **Resolve the pod on the Job-informer path.** In `pkg/job_watcher.go`, add this unexported helper next to `terminalPodDetail` (same file, same package):

   ```go
   // jobPod returns the most recently created Pod owned by job, or nil when the
   // namespace's PodLister is not synced, the lookup fails, or the Job owns no
   // pod. Unlike terminalPodDetail it does not require a terminated container:
   // a never-scheduled pod has none, and that pod is exactly the one the deadline
   // arm gate needs.
   func (w *jobWatcher) jobPod(job *batchv1.Job) *corev1.Pod {
   	lister := w.PodLister(libk8s.Namespace(job.Namespace))
   	if lister == nil {
   		return nil
   	}
   	pods, err := lister.Pods(job.Namespace).List(labels.Everything())
   	if err != nil {
   		glog.Errorf("job watcher: list pods for job %s/%s: %v", job.Namespace, job.Name, err)
   		return nil
   	}
   	var best *corev1.Pod
   	for _, pod := range pods {
   		if ownerJobName(pod) != job.Name {
   			continue
   		}
   		if best == nil || pod.CreationTimestamp.After(best.CreationTimestamp.Time) {
   			best = pod
   		}
   	}
   	return best
   }
   ```

   Then:
   - In `publishSyntheticFailure`, pass `w.jobPod(job)` as the new final argument to `w.publisher.PublishFailure`.
   - In `HandlePod`, pass its own `pod` argument as the new final argument.
   - Leave `publishStoreMissFailure` calling `PublishStoreMissFailure` with its unchanged four-argument signature.

5. **Zombie sweeper passes no pod.** In `pkg/zombie_sweeper.go` line 159, call `s.publisher.PublishFailure(ctx, task, jobName, reason.String(), nil)`. Add a short comment: the sweeper's `classify` can fall through to `ZombieReasonDeadlineExceeded` for a never-scheduled retry pod, so letting the gate resolve the pod here would silently change sweeper behaviour; a sweeper-published failure never arms.

6. **Regenerate the counterfeiter fake.** The `//counterfeiter:generate` directive at the top of `pkg/result_publisher.go` is unchanged. Run `ROOTDIR=/workspace make generate`. Confirm `mocks/result_publisher.go` now has `PublishFailure` taking five parameters, and `PublishFailureArgsForCall` returning five values (`context.Context, lib.Task, string, string, *corev1.Pod`). Never hand-edit `mocks/`.

7. **Update the three hand-written implementations** of `ResultPublisher` (they will not compile otherwise):
   - `pkg/job_watcher_internal_test.go` — `fakeResultPublisher.PublishFailure` gains the `pod *corev1.Pod` parameter and records it; add `pod *corev1.Pod` to the `resultPublisherCall` struct; add `func (f *fakeResultPublisher) PublishFailurePodArgsForCall(i int) *corev1.Pod`. Keep `PublishFailureArgsForCall` returning its existing four values so its many call sites are untouched.
   - `pkg/handler/claim_spawn_order_internal_test.go` — `stubResultPublisher.PublishFailure` gains `_ *corev1.Pod`; add the `corev1 "k8s.io/api/core/v1"` import.
   - `pkg/handler/claim_wiring_internal_test.go` — `parkRecordingPublisher.PublishFailure` gains `_ *corev1.Pod`; add the same import.

8. **Mechanically update every remaining call site** of the fake's five-value `PublishFailureArgsForCall` and of `PublishFailure`:
   - `pkg/result_publisher_test.go` — every `publisher.PublishFailure(...)` call gains a final pod argument. Pass `nil` everywhere except the new deadline specs. (There are roughly 28; use the compiler's errors as the checklist.)
   - `pkg/zombie_sweeper_test.go` — the three `PublishFailureArgsForCall` destructures at lines 141, 281, 333 gain a fifth `_`; the `PublishFailureStub` at line 515 gains `_ *corev1.Pod`.
   - `pkg/envtest/job_watcher_envtest_test.go` — line 175 becomes `_, _, gotJobName, gotReason, _ := publisher.PublishFailureArgsForCall(0)`.
   - `pkg/job_watcher_namespaces_test.go` uses the test-local fake's four-value accessor and needs no change.

9. **Add the publisher-level specs** to the existing `Describe("PublishFailure trigger cap", ...)` block in `pkg/result_publisher_test.go`. Add the `corev1 "k8s.io/api/core/v1"` import. Every spec name below contains `never-scheduled` so the focused run in `<verification>` selects exactly these plus requirement 10's spec. Use a helper pod in the block:

   ```go
   neverScheduledPod := &corev1.Pod{
   	Status: corev1.PodStatus{
   		Phase: corev1.PodPending,
   		Conditions: []corev1.PodCondition{{
   			Type:   corev1.PodScheduled,
   			Status: corev1.ConditionFalse,
   		}},
   	},
   }
   ```

   - `It("arms max_triggers=3 for a never-scheduled deadline_exceeded failure")` — task with no `max_triggers`; `PublishFailure(ctx, task, "job-deadline", "deadline_exceeded", neverScheduledPod)`; assert `updateCmd.Updates` has exactly 2 entries, `current_job == ""`, and `max_triggers` is `BeNumerically("==", 3)`. This is Acceptance Criterion 1.
   - `It("does not arm a never-scheduled deadline when the pod ran to a terminal phase")` — pod `{Status: corev1.PodStatus{Phase: corev1.PodFailed}}` with no `PodScheduled` condition; assert exactly 1 entry, `current_job == ""`, no `max_triggers` key. Acceptance Criterion 2 (the spec's `Pending` + `PodScheduled=True` Failure-Modes row shares this predicate, so this one pod shape covers both).
   - `DescribeTable("does not arm a never-scheduled deadline failure with no pod", ...)` with two entries — `Entry("sweeper publishes with no pod")` and `Entry("job watcher could not resolve the pod")` — both call `PublishFailure` with a `nil` pod and reason `"deadline_exceeded"`, asserting exactly 1 entry and no `max_triggers` key. This covers Acceptance Criteria 3 and 7 at the publisher seam; the sweeper's own call shape is pinned in requirement 11.
   - `It("preserves an operator max_triggers on the never-scheduled deadline path")` — task with `"max_triggers": 5`, never-scheduled pod, reason `"deadline_exceeded"`; assert exactly 1 entry and no `max_triggers` key. Acceptance Criterion 5.
   - `It("does not arm through PublishStoreMissFailure for a never-scheduled deadline failure")` — task `lib.Task{TaskIdentifier: ..., Frontmatter: lib.TaskFrontmatter{"max_triggers": nil}}`; call `PublishStoreMissFailure(ctx, task, "job-store-miss", "deadline_exceeded")` (four arguments — that signature is unchanged); decode the first message and assert `updateCmd.Updates` has exactly 1 entry and no `max_triggers` key. Acceptance Criterion 8.
   - `It("logs decision=arm for a never-scheduled deadline failure")` — wrap the `PublishFailure` call in `captureGlogStderr`; assert the captured output contains `event=crash_arm_decision `, `parsed="deadline_exceeded"`, and `decision=arm`; assert `strings.Count(out, "event=crash_arm_decision ") == 1`. Acceptance Criterion 9.
   - Add `Entry("deadline_exceeded with no pod", "deadline_exceeded")` to the existing `DescribeTable("leaves every other class uncapped", ...)` so the nil-pod deadline reason stays in the uncapped table alongside the seven other classes.

10. **Add the watcher-level spec** to `pkg/job_watcher_internal_test.go`, in the `Describe("jobWatcher backoff-limit pod detail", ...)` block (or a sibling `Describe` that reuses its `BeforeEach` and `makeJob`/`setPodLister` helpers):

    `It("passes the never-scheduled deadline pod into PublishFailure")` — store the test task; build a Job whose only condition is `{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"}`; seed the lister with a pod that has an `OwnerReference{Kind: "Job", Name: <job name>}`, `Namespace: "test-ns"`, and `Status.Conditions: [{Type: corev1.PodScheduled, Status: corev1.ConditionFalse}]`; call `watcher.HandleJob(ctx, job)`; assert `PublishFailureCallCount() == 1`, then:

    ```go
    calledPod := fakePublisher.PublishFailurePodArgsForCall(0)
    Expect(calledPod).NotTo(BeNil())
    Expect(hasPodScheduledFalse(calledPod)).To(BeTrue())
    ```

    The internal package can call `hasPodScheduledFalse` directly; this also pins that the gate and the sweeper share one predicate. A `nil`-passing implementation fails this spec. Acceptance Criterion 6.

11. **Pin the sweeper's call shape** in `pkg/zombie_sweeper_test.go`: in the existing `It("publishes failure with deadline_exceeded", ...)` spec (around line 98), after the existing reason assertion, add:

    ```go
    _, _, _, _, calledPod := fakePublisher.PublishFailureArgsForCall(0)
    Expect(calledPod).To(BeNil())
    ```

    This is Acceptance Criterion 7's call-site half: the sweeper supplies no pod, so a sweeper-published `deadline_exceeded` can never arm.

12. **CHANGELOG.** `CHANGELOG.md` HEAD is `## v0.55.1`; there is no `## Unreleased` yet. Insert one directly below the `All notable changes...` paragraph and above `## v0.55.1`, with exactly one bullet:

    ```markdown
    ## Unreleased

    - fix: **a Job killed at its deadline before it was ever scheduled now arms the trigger cap, so a task that can never run stops re-queuing.** `crashFailureUpdates` armed `max_triggers` only for `pod_crash_no_stdout`, so a never-scheduled `deadline_exceeded` failure accrued `trigger_count` without bound and its assignee was never cleared. The failed Job's pod is now resolved from the existing `PodLister` and passed to `PublishFailure`; the gate arms only when that pod carries `PodScheduled=False`, reusing the sweeper's `hasPodScheduledFalse`. A pod that ran and hit its deadline, an unresolvable pod, the store-miss path and every sweeper-published failure stay uncapped, and an operator-set `max_triggers` is never overwritten.
    ```

13. Walk every requirement above against the diff before finishing. Run `<verification>` and fix anything it reports.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass; `ROOTDIR=/workspace make precommit` must exit 0.
- Ginkgo/Gomega v2 for new tests; plain Go tests only in `pkg/handler` internal test files.
- Errors: `errors.Wrapf(ctx, err, ...)` from `github.com/bborbe/errors`; never `fmt.Errorf`, never `context.Background()` inside `pkg/`.
- Reuse `hasPodScheduledFalse` from `pkg/zombie_sweeper.go`. Do not add a second `PodScheduled` predicate, and do not move or rename the existing one.
- The published reason string for a never-scheduled deadline kill stays `deadline_exceeded`. Do not add a new `ZombieReason`, do not change `ParseZombieReason`, and do not change `JobFailureReason` / `failureReason`.
- Do not arm on the store-miss path (`taskFromFailedJob`'s `max_triggers: nil` sentinel is deliberate) and do not arm for the zombie sweeper.
- Do not overwrite an already-present `max_triggers` on any path.
- Do not add a recurring-task special case. A recurring task (no `ref`, no `max_triggers`) whose pod never schedules arms exactly like any other task; that is the deliberate, narrow amendment to `specs/completed/006-bug-trigger-cap-never-engages-for-repo-backed-tasks.md` and spec 006 itself is not edited. A recurring task whose pod ran and hit its deadline stays uncapped.
- Do not touch `classify` or `podNotScheduledGraceWindow` in `pkg/zombie_sweeper.go`, `activeDeadlineSeconds`, cluster sizing, or `pkg/spawner/`.
- Do not add a metric, a counter label, a config field, or a feature flag. The only new observability is the existing `event=crash_arm_decision` line's `decision=not_never_scheduled` value and the deadline arm reusing `decision=arm`.
- Do not add a new body section and do not change the `## Failure` body format.
- Never hand-edit `mocks/` — regenerate it.
- Keep `publishFailure`'s error wrapping and the caller-side `glog.Errorf` behaviour unchanged; a failed publish writes no key and the next failure cycle re-arms.
</constraints>

<verification>
- `ROOTDIR=/workspace make precommit` — exits 0.
- `go test ./pkg/... -mod=mod` — exits 0.
- `go test ./pkg/ -mod=mod -v -ginkgo.v -ginkgo.focus='never-scheduled'` — exits 0 and prints `Ran 8 of` (the seven publisher specs of requirement 9 plus the watcher spec of requirement 10; a focus matching no spec still exits 0, so the count is the check).
- `grep -n 'ZombieReasonDeadlineExceeded' pkg/result_publisher.go` — returns at least one line.
- `grep -n 'hasPodScheduledFalse' pkg/result_publisher.go` — returns at least one line.
- `grep -n 'decision=not_never_scheduled' pkg/result_publisher.go` — returns at least one line.
- `grep -n 'func (w \*jobWatcher) jobPod' pkg/job_watcher.go` — returns at least one line.
- `grep -n 'pod \*corev1.Pod' pkg/result_publisher.go` — returns at least one line (the interface parameter and/or the gate parameter).
- `grep -n 'PublishFailureArgsForCall' mocks/result_publisher.go` — the generated accessor returns five values; inspect the `func (fake *FakeResultPublisher) PublishFailureArgsForCall` signature.
- `grep -A2 '## Unreleased' CHANGELOG.md | grep -i deadline` — returns at least one line.
- `go vet -mod=mod -tags envtest ./pkg/envtest/` — exits 0 (the `envtest`-tagged file must still compile).
</verification>
