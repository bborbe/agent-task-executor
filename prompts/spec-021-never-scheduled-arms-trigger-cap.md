---
status: draft
spec: [021-bug-deadline-kill-never-arms-trigger-cap]
created: "2026-11-10T17:50:51Z"
branch: dark-factory/bug-deadline-kill-never-arms-trigger-cap
---

# Arm the trigger cap when a Job's pod can never be scheduled

<summary>
- A task whose Job can never be placed on a node now counts toward the retry cap, so it stops re-queuing forever
- The cap is armed on the first never-scheduled failure (`max_triggers: 3`), and the controller clears the assignee on the third
- The never-scheduled signal comes from the zombie sweeper, which can still see the pod while it is `Pending` and unschedulable
- The sweeper publishes `pod_not_scheduled` at a 10-minute arm window, well before the 30-minute deadline
- The cap is decided on the failure reason alone; the failed Job's pod is not consulted by the arm gate
- A Job that actually ran and then hit its deadline still never arms the cap
- The store-miss path still never arms the cap, and an operator-set `max_triggers` is never lowered
- The existing crash (`pod_crash_no_stdout`) arming behaviour is unchanged
- The arm decision is visible in the log as `parsed="pod_not_scheduled"` plus `decision=arm`, distinguishable from a crash arm
- CHANGELOG records the fix

</summary>

<objective>
Make a Job whose pod cannot be scheduled count toward the trigger cap: the zombie sweeper publishes `pod_not_scheduled` once the pod has been `Pending` with `PodScheduled=False` for its arm window, and the existing arm gate in `crashFailureUpdates` writes `max_triggers: 3` on that reason. Every other failure class — including a Job that ran and hit its deadline — keeps today's behaviour of never arming.
</objective>

<context>
<!--
REVIEWER NOTE (open question, resolve at audit): this spec's final design is
already present in the working tree and was shipped in CHANGELOG v0.56.2, while
the spec still says HEAD is v0.55.1. The prompt therefore targets the end state
and instructs the executor to verify/gap-fill rather than duplicate. The one
genuine missing acceptance test is AC7 (store-miss + pod_not_scheduled). If the
spec is in fact being re-run against a pre-v0.56.2 tree, the same requirements
implement it from scratch.

The working tree may also carry an unrelated in-flight `claude_session_id`
change (prompts/in-progress/065). Do not touch it; if it leaves the tree
non-compiling, run the spec-021 tests directly and report the unrelated
breakage rather than repairing it here.
-->

Read these files in full before changing anything:

- `pkg/result_publisher.go` — the `ResultPublisher` interface and `resultPublisher` implementation. The arm gate is `crashFailureUpdates`; `publishFailure` composes the `## Failure` section and calls it; `PublishFailure` and `PublishStoreMissFailure` are the two entry points. `defaultMaxTriggers = 3`.
- `pkg/zombie_reason.go` — `ZombieReasonPodNotScheduled == "pod_not_scheduled"`, `ZombieReasonDeadlineExceeded == "deadline_exceeded"`, `ZombieReasonPodCrashNoStdout == "pod_crash_no_stdout"`, and `ParseZombieReason`.
- `pkg/zombie_sweeper.go` — `sweepTask`, `classify`, `podNotScheduledGraceWindow`, `podNotScheduledArmWindow`, and `hasPodScheduledFalse`.
- `pkg/result_publisher_test.go` — the `Describe("PublishFailure trigger cap", ...)` and `Describe("PublishStoreMissFailure", ...)` blocks, `decodeUpdateFrontmatterCommand`, `capturingSyncProducer`.
- `pkg/glog_capture_test.go` — `captureGlogStderr(func()) string`.
- `pkg/job_watcher_internal_test.go` — the test-local `fakeResultPublisher`, `setPodLister`, `makeJob`, and `deadlineCondition`.
- `pkg/zombie_sweeper_test.go` — `makePod`, `podScheduledFalseCondition`, and the existing early-publish specs.
- `CHANGELOG.md` — inspect whether this fix is already recorded.

Coding-plugin guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `errors.Wrapf(ctx, err, ...)`, never `fmt.Errorf`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2 conventions
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` bullet format

Verified facts (do not re-derive, do not "improve"):
- `hasPodScheduledFalse(pod *corev1.Pod) bool` lives in `pkg/zombie_sweeper.go` and is the single definition of "never scheduled". It is used by the sweeper's `classify` only; the arm gate must not call it.
- `podNotScheduledGraceWindow = 2 * time.Minute` is the classification floor. `podNotScheduledArmWindow = 10 * time.Minute` is how long the sweeper waits before publishing the class ahead of the deadline.
- `PublishFailure` keeps its `pod *corev1.Pod` final argument because the log-tail capture (`logTailBlock`) consumes it. The arm gate does not read it.
- `PublishStoreMissFailure` builds a task whose `max_triggers` is present-but-nil; the gate treats a present key as already-armed, which is what keeps that path unarmed.
- `agentv1.DefaultZombieJobTimeoutSeconds` is `1800` (30 minutes).

The measured reason the pod cannot gate the arm (do not re-litigate): the Job controller deletes the pod one second BEFORE it stamps the Job's `DeadlineExceeded` condition (nuke dev, 2026-11-04). A failure-time pod lookup therefore returns nil for exactly the class that matters. The first shipped attempt gated on `pod != nil && hasPodScheduledFalse(pod)` and never armed; a live replay ran `trigger_count` from 1 to 11 with no `max_triggers` written.
</context>

<requirements>
1. **Arm gate decides on the reason alone.** In `pkg/result_publisher.go`, `crashFailureUpdates` must have the signature `func crashFailureUpdates(task lib.Task, jobName, reason string) lib.TaskFrontmatter` — no pod parameter. Its class gate must be exactly:

   ```go
   armClass := parsedOK && (parsed == ZombieReasonPodCrashNoStdout ||
       parsed == ZombieReasonPodNotScheduled)
   ```

   The returned map starts as `lib.TaskFrontmatter{"current_job": ""}`. The `max_triggers` presence check (`cachedMax, maxPresent := task.Frontmatter["max_triggers"]`) sits AFTER the class gate and returns unarmed when the key is present, for both arming classes. Only the absent-key arm path adds `updates["max_triggers"] = defaultMaxTriggers`.

   If the current code instead gates on a pod (`pod != nil && hasPodScheduledFalse(pod)`, or any `deadline_exceeded && PodScheduled=False` branch), remove that branch: it was proven not to arm at runtime. Do not add a second `PodScheduled` predicate anywhere in this file, and do not import or call `hasPodScheduledFalse` from `result_publisher.go`.

2. **Keep the pod argument, but ignore it at the gate.** `PublishFailure` keeps `pod *corev1.Pod` as its final parameter and passes it through `publishFailure` to `logTailBlock` only. `PublishStoreMissFailure` passes `nil` for the pod. `publishFailure` calls `crashFailureUpdates(task, jobName, reason)` with no pod. Do not change `PublishStoreMissFailure`'s public signature.

3. **Every arm decision is logged on `event=crash_arm_decision`.** The arm path must emit a `glog.V(2).Infof` line carrying `task=%s`, `job=%s`, `reason=%q`, `parsed=%q`, `parsed_ok=%t`, and `decision=arm`. The non-arming paths emit their own `decision=` values (`not_crash_class`, `max_triggers_already_present`). A `pod_not_scheduled` arm must therefore be greppable as `parsed="pod_not_scheduled"` together with `decision=arm`. Keep the existing line shape; do not add a metric, counter, or config field.

4. **Sweeper publishes the class ahead of the deadline.** In `pkg/zombie_sweeper.go`, `sweepTask` must:
   - compute `floor := podNotScheduledGraceWindow`, clamp it to the task deadline (`if deadline < floor { floor = deadline }`), and return when `elapsed < floor` — so the pod is read from the grace window onward, not only past the deadline;
   - resolve the namespace and fetch the pod lister as today; a nil lister skips the task for this tick;
   - call `s.classify(...)` as today;
   - compute `armWindow := podNotScheduledArmWindow`, clamp it to the deadline, and set `early := reason == ZombieReasonPodNotScheduled && elapsed >= armWindow`;
   - publish only when `early || elapsed >= deadline`, and call `s.publisher.PublishFailure(ctx, task, jobName, reason.String(), nil)`.
   Do not change `classify` or `podNotScheduledGraceWindow`. Do not publish before the arm window merely because the class matched.

5. **Publisher-seam specs** in `pkg/result_publisher_test.go`, inside `Describe("PublishFailure trigger cap", ...)`:
   - `It("arms max_triggers=3 for a pod_not_scheduled failure")` — task with no `max_triggers`, reason `pod_not_scheduled`, nil pod; assert exactly two updates, `current_job == ""`, and `max_triggers` is `BeNumerically("==", 3)` (JSON round-trip decodes numbers as `float64`). Acceptance Criterion 1.
   - `It("does not arm a deadline_exceeded failure even when the pod is unschedulable")` — task with no `max_triggers`, reason `deadline_exceeded`, a pod `Pending` with `PodScheduled=False`; assert exactly one update, `current_job == ""`, and no `max_triggers` key. This pins that the gate ignores the pod. Acceptance Criterion 2.
   - `It("preserves an operator max_triggers on the pod_not_scheduled path")` — task with `"max_triggers": 5`, reason `pod_not_scheduled`; assert one update and no `max_triggers` key. Acceptance Criterion 5.
   - `It("does not arm through PublishStoreMissFailure for a pod_not_scheduled failure")` — task `lib.Task{TaskIdentifier: ..., Frontmatter: lib.TaskFrontmatter{"max_triggers": nil}}`; call `PublishStoreMissFailure(ctx, task, "<job>", "pod_not_scheduled")` (four arguments); decode the first message and assert exactly one update and no `max_triggers` key. This is Acceptance Criterion 7 and is currently missing — add it.
   - `It("logs decision=arm for a pod_not_scheduled failure")` — wrap the `PublishFailure` call in `captureGlogStderr`; assert the output contains `event=crash_arm_decision `, `parsed="pod_not_scheduled"`, and `decision=arm`, and that `strings.Count(out, "event=crash_arm_decision ") == 1`. Acceptance Criterion 8.
   - Keep the existing `pod_crash_no_stdout` arming spec unchanged and green. Acceptance Criterion 4.
   If any of these specs already exists, verify it matches the assertions above rather than adding a duplicate.

6. **Watcher-seam spec** in `pkg/job_watcher_internal_test.go`: driving `HandleJob` with a Job whose only condition is `{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"}` must call `PublishFailure` exactly once with reason `deadline_exceeded`. Do not assert that the pod is nil: the watcher may still resolve the pod for the log tail, and the gate ignores it. Acceptance Criterion 6.

7. **Sweeper specs** in `pkg/zombie_sweeper_test.go`:
   - A task well inside its `ZombieJobTimeoutSeconds` (e.g. 12 minutes elapsed against the 1800 s default) whose pod is `Pending` with `PodScheduled=False` past the 2-minute grace window publishes exactly one `PublishFailure` with reason `pod_not_scheduled`. Acceptance Criterion 3.
   - A task past the grace window but inside the 10-minute arm window (e.g. 5 minutes) does not publish, so a cluster-autoscaler scale-up is not mistaken for never-scheduled.
   - Assert the sweeper supplies a nil pod: `_, _, _, _, calledPod := fakePublisher.PublishFailureArgsForCall(0); Expect(calledPod).To(BeNil())`.

8. **CHANGELOG.** If `CHANGELOG.md` does not already record this fix, add an `## Unreleased` section directly above the newest `## v` heading with one `- fix:` bullet describing it. If a released section already records the fix, do not add a duplicate — leave the released entry and say so in the completion report.

9. **Self-check.** Before finishing, re-run `<verification>` and confirm it passes, then walk each requirement above against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass; `ROOTDIR=/workspace make precommit` must exit 0.
- Ginkgo/Gomega v2 for new tests. Errors use `errors.Wrapf(ctx, err, ...)` from `github.com/bborbe/errors`; never `fmt.Errorf`, never `context.Background()` inside `pkg/`.
- The arm gate decides on the reason alone. Do not thread a pod into `crashFailureUpdates`, do not add a `jobPod` helper for the arm, and do not gate on `hasPodScheduledFalse` from the publisher.
- Reuse `hasPodScheduledFalse` only in the sweeper's `classify`. Do not add a second `PodScheduled` predicate and do not move or rename the existing one.
- The published reason for a never-scheduled kill stays `pod_not_scheduled`; a deadline kill that ran stays `deadline_exceeded`. Do not add a new `ZombieReason`, do not change `ParseZombieReason`, and do not change `JobFailureReason` / `failureReason`.
- Do not arm on `deadline_exceeded`, do not arm on the store-miss path, and do not overwrite an already-present `max_triggers` on any path.
- A recurring task (no `ref`, no `max_triggers`) whose pod never schedules arms exactly like any other task. A recurring task whose pod ran and hit its deadline stays uncapped. This is the deliberate, narrow amendment to `specs/completed/006-bug-trigger-cap-never-engages-for-repo-backed-tasks.md`; do not edit spec 006.
- Do not touch `classify`, `podNotScheduledGraceWindow`, `activeDeadlineSeconds`, cluster sizing, or `pkg/spawner/`.
- Do not add a metric, a counter label, a config field, or a feature flag. The only new observability is the existing `event=crash_arm_decision` line's `decision=arm` on the `pod_not_scheduled` class.
- Do not change the `## Failure` body format.
- Never hand-edit `mocks/` — `make precommit` regenerates it via `generate`.
- Emit the `DARK-FACTORY-REPORT` completion template appended by dark-factory. Report `status: success` only when `make precommit` exits 0; otherwise report `failed` or `partial` honestly, and append the required `## Improvements` section after the report.
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` — must pass.

Then run the meaningful test pass (the repo's `hideGit` masking can empty `go list` inside the Makefile, so run the package tests directly):

- `GOFLAGS=-buildvcs=false go test -mod=mod ./pkg/...` — exits 0.
- `GOFLAGS=-buildvcs=false go vet -mod=mod -tags envtest ./pkg/envtest/` — exits 0 (the envtest-tagged files must still compile).

Confirm the design points that precommit does not:

- `grep -n 'podNotScheduledArmWindow' pkg/zombie_sweeper.go` — at least one line.
- `grep -n 'ZombieReasonPodNotScheduled' pkg/result_publisher.go` — at least one line.
- `grep -n 'hasPodScheduledFalse' pkg/result_publisher.go` — must print nothing: `! grep -q 'hasPodScheduledFalse' pkg/result_publisher.go`
- `grep -n 'pod \*corev1.Pod' pkg/result_publisher.go` — at least one line (the interface parameter).
- `grep -n 'decision=arm' pkg/result_publisher.go` — at least one line.
- `grep -n 'does not arm through PublishStoreMissFailure for a pod_not_scheduled failure' pkg/result_publisher_test.go` — at least one line.
- `grep -qi 'never be scheduled' CHANGELOG.md` — exits 0.
</verification>
