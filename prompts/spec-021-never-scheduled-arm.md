---
spec: ["021-bug-deadline-kill-never-arms-trigger-cap"]
status: draft
created: "2026-11-09T17:35:00Z"
---

# Never-scheduled Job arms the trigger cap via the sweeper's classification

<summary>
- A Job whose pod can never be placed on a node now counts toward the trigger cap, so the task stops re-queuing on its own after three failures
- The cap is armed from the zombie sweeper's existing `pod_not_scheduled` classification, which is the only component that still sees the pod while it is `Pending`
- The sweeper publishes `pod_not_scheduled` at a 10-minute arm window, about 28 minutes before the 1800 s deadline would have expired, instead of waiting for a deadline that arrives after the pod is deleted
- A Job that actually ran and then hit its deadline still publishes `deadline_exceeded` and still does not arm the cap
- An operator-set `max_triggers` is never lowered or overwritten on the never-scheduled path
- The store-miss path keeps its present-but-nil `max_triggers` sentinel and still does not arm
- The existing `pod_crash_no_stdout` arming behavior is unchanged
- The arm decision stays on the existing `event=crash_arm_decision` line, so an operator can tell a never-scheduled arm from a crash arm
- The failed Job's pod argument on `PublishFailure` is retained for the log-tail capture but no longer influences the arm decision
- CHANGELOG records the fix exactly once, under `## Unreleased` if it is not already recorded
</summary>

<objective>
Arm the trigger cap for a Job whose pod was never scheduled, using the zombie sweeper's `pod_not_scheduled` classification as the signal, while leaving `deadline_exceeded`, the crash class, the store-miss path, and every operator-set budget exactly as they are today.
</objective>

<context>
Read these files in full before changing anything.

- `pkg/result_publisher.go` — `ResultPublisher` interface (the `PublishFailure` method takes `pod *corev1.Pod`; `PublishStoreMissFailure` does not), the unexported `publishFailure` (it passes the pod only to `logTailBlock` and calls `crashFailureUpdates(task, jobName, reason)`), `crashFailureUpdates`, `defaultMaxTriggers = 3`, and the long GoDoc blocks above `crashFailureUpdates` and `PublishFailure`.
- `pkg/zombie_sweeper.go` — `sweepTask`, `classify`, `hasPodScheduledFalse`, `resolveTaskNamespace`, `podNotScheduledGraceWindow`, `podNotScheduledArmWindow`, and the `PublishFailure` call at the end of `sweepTask`.
- `pkg/zombie_reason.go` — the closed `ZombieReason` set, in particular `ZombieReasonDeadlineExceeded`, `ZombieReasonPodNotScheduled`, `ZombieReasonPodCrashNoStdout`, and `ParseZombieReason`.
- `pkg/job_watcher.go` — `publishSyntheticFailure`, `publishStoreMissFailure`, `taskFromFailedJob`, and `jobPod`. The Job-informer path resolves a pod for the log tail; the arm gate must not depend on it.
- `pkg/result_publisher_test.go` — the `Describe("PublishFailure trigger cap", ...)` block, `decodeUpdateFrontmatterCommand`, and `capturingSyncProducer`.
- `pkg/glog_capture_test.go` — `captureGlogStderr`.
- `pkg/zombie_sweeper_test.go` — the `makeTask`, `makePod`, `podScheduledFalseCondition`, and `ptrInt32` helpers and the `pod_not_scheduled` contexts.
- `pkg/job_watcher_internal_test.go` — the test-local `fakeResultPublisher`, `PublishFailureArgsForCall`, `PublishFailurePodArgsForCall`, `setPodLister`, and the `HandleJob` fixtures.

Coding-plugin guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `errors.Wrapf(ctx, err, ...)`, never `fmt.Errorf`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-logging-guide.md` — `glog` `V(n)` gating and the existing `event=` convention
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2 conventions
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` bullet format
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules

Current implementation status (verified against this working tree on 2026-11-09):
- `crashFailureUpdates` already has the two-class gate described in requirement 1, and `pkg/result_publisher_test.go` already carries specs named `arms the cap when the sweeper classified the pod as never scheduled`, `does not arm a deadline kill, whose pod is already gone`, `preserves an operator-set max_triggers on the never-scheduled path`, and `logs the never-scheduled arm distinctly`.
- `pkg/zombie_sweeper.go` already defines `podNotScheduledArmWindow = 10 * time.Minute` and has the early-publish gate described in requirement 2; `pkg/zombie_sweeper_test.go` carries `publishes pod_not_scheduled before the deadline` and `does not publish early, so a scale-up is not mistaken for never-scheduled`.
- `CHANGELOG.md` already records this fix under `## v0.56.2`.
- Therefore: treat this prompt as a verify-then-align task. For each requirement, read the named function first. If the tree already matches, leave the code unchanged and confirm the named spec exists and passes. Change only what diverges. Do not re-add an implementation that is already present.

<!-- OPEN QUESTION (human reviewer, audit time): the spec's Suggested Decomposition row still says "carry pod state from the watcher into the arm gate, arm on PodScheduled=False". That row predates the measured replay and contradicts the spec body (Constraints, Desired Behavior 4 and 9): a failure-time pod lookup cannot work because the Job controller deletes the pod one second before stamping DeadlineExceeded. This prompt follows the spec BODY — the arm gate decides on the reason, and the sweeper owns the never-scheduled signal. If the reviewer still wants the pod-based gate, this prompt is wrong and the spec body must be rewritten first. -->

<!-- OPEN QUESTION (human reviewer, audit time): Acceptance Criterion 3 says a task at 5 minutes elapsed whose pod is Pending with PodScheduled=False past the 2-minute grace window must publish pod_not_scheduled. Desired Behavior 2 and the code use a 10-minute arm window, and a 5-minute-elapsed task is deliberately NOT published (a cluster-autoscaler scale-up can take that long and then schedule). The tree pins the 10-minute behavior and a 5-minute negative spec. This prompt follows Desired Behavior 2. The reviewer should amend AC 3 to use an elapsed time past the arm window (e.g. 12 minutes) or drop the arm-window design. -->

<!-- OPEN QUESTION (human reviewer, audit time): Acceptance Criterion 10 requires `grep -A2 '## Unreleased' CHANGELOG.md | grep -i deadline` to return a line. The fix is already released in `## v0.56.2` and there is no `## Unreleased` section today. Requirement 6 therefore forbids adding a duplicate Unreleased bullet and verifies the existing released entry instead. If the release process still requires an Unreleased line, the reviewer should state that explicitly. -->
</context>

<requirements>
1. **Arm gate in `pkg/result_publisher.go`.** `crashFailureUpdates` must keep the signature `func crashFailureUpdates(task lib.Task, jobName, reason string) lib.TaskFrontmatter` (no pod parameter) and must implement exactly this decision:

   ```go
   func crashFailureUpdates(
   	task lib.Task,
   	jobName, reason string,
   ) lib.TaskFrontmatter {
   	updates := lib.TaskFrontmatter{"current_job": ""}
   	parsed, parsedOK := ParseZombieReason(reason)
   	armClass := parsedOK && (parsed == ZombieReasonPodCrashNoStdout ||
   		parsed == ZombieReasonPodNotScheduled)
   	if !armClass {
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

   - The gate is `ParseZombieReason`, not string equality, so a suffixed reason still classifies.
   - `ZombieReasonDeadlineExceeded` must not be an arm class, and the gate must not consult a pod value. A deadline kill that ran publishes `current_job: ""` only.
   - The present-`max_triggers` check sits after the class gate, so it protects both arm classes. An operator budget of 5 or 6 is never lowered to 3.
   - Keep the existing GoDoc prose above the function but correct it where it still says the pod gates the arm. It must state that the pod argument on `PublishFailure` feeds the log tail only.

2. **Sweeper early classification and publish in `pkg/zombie_sweeper.go`.** Keep the two constants as named: `podNotScheduledGraceWindow = 2 * time.Minute` and `podNotScheduledArmWindow = 10 * time.Minute`. In `sweepTask`, after `deadline` and `elapsed` are computed:

   ```go
   floor := podNotScheduledGraceWindow
   if deadline < floor {
   	floor = deadline
   }
   if elapsed < floor {
   	return
   }
   namespace := resolveTaskNamespace(ctx, s.namespace, task, cfgs)
   lister := s.jobWatcher.PodLister(namespace)
   if lister == nil {
   	return
   }
   reason := s.classify(lister, namespace, taskID, now)
   if reason == "" {
   	return
   }
   armWindow := podNotScheduledArmWindow
   if deadline < armWindow {
   	armWindow = deadline
   }
   early := reason == ZombieReasonPodNotScheduled && elapsed >= armWindow
   if !early && elapsed < deadline {
   	return
   }
   if err := s.publisher.PublishFailure(ctx, task, jobName, reason.String(), nil); err != nil {
   	glog.Errorf(
   		"zombie sweeper: publish failure for task %s (job %s reason %s): %v",
   		taskID, jobName, reason, err,
   	)
   	return
   }
   ```

   - Preserve the existing `V(2)`/`V(3)` log lines and the error-wrapping style.
   - The sweeper passes a nil pod; the arm decision does not need one.
   - Do not change `classify`, `hasPodScheduledFalse`, or the grace window. `pod_not_scheduled` keeps its existing meaning.
   - Failure behavior: a nil lister skips the task for this tick (retried next tick); a publish error is logged and the task is retried on a later cycle.

3. **Publisher plumbing stays as it is.** `PublishFailure(ctx context.Context, task lib.Task, jobName string, reason string, pod *corev1.Pod) error` and `publishFailure(ctx, task, jobName, reason, note, pod)` keep their current signatures. `publishFailure` passes the pod only to `logTailBlock` and calls `crashFailureUpdates(task, jobName, reason)`. `PublishStoreMissFailure(ctx, task, jobName, reason)` keeps its four-argument signature and passes `nil` for the pod.

4. **Store-miss path stays unarmed.** `taskFromFailedJob` in `pkg/job_watcher.go` keeps `lib.TaskFrontmatter{"max_triggers": nil}`. That present-but-nil key is what makes the gate treat the record as already armed. Do not remove or change the sentinel.

5. **Regression specs.** Each spec below must exist and pass. If a spec is already present with an equivalent name and shape, leave it; otherwise add it.

   In `pkg/result_publisher_test.go`, inside `Describe("PublishFailure trigger cap", ...)`:
   - `It("arms the cap when the sweeper classified the pod as never scheduled")` — task with no `max_triggers`; `publisher.PublishFailure(ctx, task, "claude-20260418120000", "pod_not_scheduled", nil)`; assert the decoded update has exactly 2 entries, `current_job == ""`, and `max_triggers` is `BeNumerically("==", 3)`.
   - `It("does not arm a deadline kill, whose pod is already gone")` — reason `"deadline_exceeded"`, even with a `Pending` pod carrying `PodScheduled=False`; assert exactly 1 entry and no `max_triggers` key.
   - `It("preserves an operator-set max_triggers on the never-scheduled path")` — task with `"max_triggers": 5`, reason `"pod_not_scheduled"`; assert exactly 1 entry and no `max_triggers` key.
   - `It("does not arm through PublishStoreMissFailure for a never-scheduled failure")` — task `lib.Task{TaskIdentifier: ..., Frontmatter: lib.TaskFrontmatter{"max_triggers": nil}}`; call `publisher.PublishStoreMissFailure(ctx, task, "job-store-miss", "pod_not_scheduled")`; assert no `max_triggers` key.
   - `It("logs the never-scheduled arm distinctly")` — wrap the `PublishFailure` call in `captureGlogStderr`; assert the output contains `event=crash_arm_decision `, `parsed="pod_not_scheduled"`, and `decision=arm`.
   - The existing `pod_crash_no_stdout` arming spec must still pass unchanged.

   In `pkg/zombie_sweeper_test.go`, keep/add:
   - `It("publishes pod_not_scheduled before the deadline")` — `job_started_at` 12 minutes before the sweep's clock, default 1800 s deadline, pod `Pending` with `PodScheduled=False` created 5 minutes ago; assert exactly one `PublishFailure` call with reason `pod_not_scheduled`.
   - `It("does not publish early, so a scale-up is not mistaken for never-scheduled")` — same pod, `job_started_at` 5 minutes before the sweep's clock (past the 2-minute grace, inside the 10-minute arm window); assert zero `PublishFailure` calls.
   - Existing deadline-gated contexts for other classes must still pass.

   In `pkg/job_watcher_internal_test.go`, keep/add a spec that drives `HandleJob` with a Job whose only condition is `{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"}` and asserts `PublishFailureCallCount() == 1` with reason `deadline_exceeded`. A resolvable pod may still be passed for the log tail; the arm decision must not depend on it.

6. **CHANGELOG.** Search `CHANGELOG.md` for an entry that already records this fix. If one exists (as of 2026-11-09 it is the `## v0.56.2` bullet beginning `fix: arm the trigger cap for a Job whose pod can never be scheduled`), do not add a duplicate. If no entry exists, insert a `## Unreleased` section directly below the `All notable changes...` paragraph and add exactly one bullet:

   ```markdown
   - fix: arm the trigger cap for a Job whose pod can never be scheduled — the cap was armed only for the pod-crash class, so a never-scheduled kill left `max_triggers` absent and the task re-queued until a human cleared the assignee. The arm gate now also admits `pod_not_scheduled`, and the zombie sweeper publishes that classification at a 10-minute arm window instead of waiting for the deadline: the Job controller deletes the pod one second before it stamps `DeadlineExceeded`, so a failure-time pod lookup returns nothing for exactly the class that matters. A Job that ran to its deadline, an unresolvable pod, and the store-miss path stay unarmed, and the recurring-task guard is unchanged
   ```

7. **Final review.** Walk every requirement above against the diff before finishing. Run `<verification>` and fix anything it reports.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass; `make precommit` must exit 0.
- Ginkgo/Gomega v2 for new specs; plain Go tests only in `pkg/handler` internal test files.
- Errors: `errors.Wrapf(ctx, err, ...)` from `github.com/bborbe/errors`; never `fmt.Errorf`, never `context.Background()` inside `pkg/`.
- Reuse `hasPodScheduledFalse` from `pkg/zombie_sweeper.go`. Do not add a second `PodScheduled` predicate, and do not move or rename the existing one.
- The published reason string for a never-scheduled deadline kill stays `deadline_exceeded`. Do not add a new `ZombieReason`, do not change `ParseZombieReason`, and do not change `JobFailureReason` / `failureReason`.
- Do not arm on `deadline_exceeded`. Do not arm on the store-miss path. Do not arm when the pod lister is not synced.
- Do not overwrite an already-present `max_triggers` on any path.
- Do not change `classify`, `hasPodScheduledFalse`, or `podNotScheduledGraceWindow`.
- Do not add a recurring-task special case. A recurring task (no `ref`, no `max_triggers`) whose pod never schedules arms exactly like any other task; that is the deliberate, narrow amendment to `specs/completed/006-bug-trigger-cap-never-engages-for-repo-backed-tasks.md`. A recurring task whose pod ran and hit its deadline stays uncapped.
- Do not touch `specs/in-progress/004-bug-job-failure-reason.md`; it owns the `DeadlineExceeded` / `BackoffLimitExceeded` split and is at `status: verifying`.
- Do not raise `activeDeadlineSeconds`, resize the cluster, or change `pkg/spawner/`.
- Do not add a metric, a counter label, a config field, or a feature flag. The only observability is the existing `event=crash_arm_decision` line.
- Do not add a new body section and do not change the `## Failure` body format.
- Keep `publishFailure`'s error wrapping and the caller-side `glog.Errorf` behaviour unchanged; a failed publish writes no key and the next failure cycle re-arms.
- Do not edit the spec.
</constraints>

<verification>
- `make precommit` — exits 0.
- `go test -mod=mod ./pkg/...` — exits 0.
- `go test -mod=mod ./pkg/ -ginkgo.focus='pod_not_scheduled|never-scheduled|never scheduled'` — exits 0.
- `grep -n 'ZombieReasonPodNotScheduled' pkg/result_publisher.go` — returns at least one line.
- `grep -n 'decision=arm' pkg/result_publisher.go` — returns at least one line.
- `grep -n 'podNotScheduledArmWindow' pkg/zombie_sweeper.go` — returns at least one line.
- `grep -n 'pod_not_scheduled' CHANGELOG.md` — returns at least one line (the existing `## v0.56.2` entry, or a new `## Unreleased` entry if one was required).
</verification>
