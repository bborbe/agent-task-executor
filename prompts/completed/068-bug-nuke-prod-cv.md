---
status: completed
spec: [bug-nuke-prod-cv]
summary: 'Preserved the failed container''s termination message in the ## Failure section, redacted and single-lined, with tests and a changelog entry; make precommit passed.'
execution_id: 265db5d1-bc7c-5f52-9f84-9dfe2e58e844-exec-068-bug-nuke-prod-cv
dark-factory-version: dev
created: "2026-11-19T07:35:00Z"
queued: "2026-11-19T06:45:00Z"
started: "2026-11-19T06:45:02Z"
completed: "2026-11-19T06:57:52Z"
---

# Include the failed container's termination message in the failure record

<!--
OPEN QUESTION FOR THE HUMAN REVIEWER (read before approving):

The spec `bug-nuke-prod-cv` names `exitErrorForStatus` (main.go:235, called from
`finishRun` at main.go:218) and `result.Message` in `bborbe/agent-task-executor`.
Those symbols do not exist in this repository at HEAD or anywhere in its git
history:

    git log --all -S 'exitErrorForStatus' -- '*.go'   # no commits
    git log --all -S 'finishRun'          -- '*.go'   # no commits
    grep -rn 'exitErrorForStatus\|finishRun' .        # no matches

`main.go` is the executor: it spawns Kubernetes Jobs and never runs an agent
in-process. The `finishRun` / `exitErrorForStatus` pair is the agent binary that
runs INSIDE the spawned Job (image supplied by the Config CR), which is not in
this tree. The spec therefore appears mis-attributed to this repository.

This prompt does NOT fabricate those functions. It fixes the same bug class where
it actually occurs in this repo: the executor already computes a failed pod's
termination state, but it drops the container's termination `Message`, so
distinct crashed runs collapse into the same `pod_crash_no_stdout` record. If the
reviewer confirms the spec belongs to the agent-binary repository, reject this
prompt and re-file the spec there instead of approving it.
-->

<summary>
- A failed run's own termination message is no longer thrown away when the executor records the failure
- Distinct crashed runs become distinguishable in the task record instead of collapsing into one generic reason
- The message is redacted with the same credential scrubber the log tail already uses, so a token in a termination message cannot leak into a task body
- A multi-line termination message is collapsed to one line, so it cannot inject a heading or a fenced block into the record
- Failures whose pod carries no termination message produce byte-identical output to today
- The failure reason vocabulary is unchanged, so the re-drive and escalation logic keeps parsing exactly the same values
- No new config field, feature flag, or threshold is introduced
- The change is covered by publisher tests that build a terminated pod and assert the composed failure body
- The change is recorded in the changelog
</summary>

<objective>
Stop discarding the failed container's termination message when the executor composes the `## Failure` section of a task record. The executor already reads the pod's exit code and terminated reason to build the published `reason`, but it never reads `ContainerStateTerminated.Message`; that message is the run's own account of why it stopped, and dropping it collapses distinct failures into one opaque record. Preserve it, redacted and single-lined, so a failed planning or execution run is diagnosable from the record alone.
</objective>

<context>
Read `CLAUDE.md` if present, and `README.md`, for project conventions.

Read these files in full before changing anything:

- `pkg/result_publisher.go` — the `ResultPublisher` interface (`PublishFailure`, `PublishStoreMissFailure`) and the private `publishFailure` it delegates to. `publishFailure` composes the `## Failure` section with `fmt.Sprintf("## Failure\n\n- **Timestamp:** %s\n- **Job:** %s\n- **Reason:** %s\n%s%s", now, jobName, reason, note, p.logTailBlock(ctx, task, jobName, pod))`. `logTailBlock` is the exemplar for a pod-derived, optional, redacted block: it returns `""` when there is nothing to write and never fails the publish. Follow that shape.
- `pkg/log_redactor.go` — `redactLog(text string) string`, package-private, already used by `logTailBlock` before the tail reaches the section. Reuse it; do not invent a second redactor.
- `pkg/result_publisher_log_tail_test.go` — `logTailFixture`, `newLogTailFixture`, `capturingSyncProducer`, `decodeUpdateFrontmatterCommand`, and `captureGlogStderr`. This file exists because `pkg/result_publisher_test.go` is at the revive file-length limit; put the new specs here, not in the main publisher test file.
- `pkg/job_watcher.go` — `publishSyntheticFailure`, `classifyPodFailure`, `terminalPodDetail`, and `backoffFailureReason`. These are the callers that pass a resolved `*corev1.Pod` to `PublishFailure`; they already read `cs.State.Terminated.ExitCode` and `.Reason`. `terminalPodDetail` shows the container-status loop shape to match.
- `pkg/zombie_reason.go` — the closed `ZombieReason` vocabulary and `ParseZombieReason`. The free-form message MUST NOT enter this vocabulary or the `reason` string; the vocabulary is parsed by the re-drive classifier and by `ParseZombieReason`.

Coding-plugin guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo v2/Gomega conventions.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — the `## Unreleased` bullet format and conventional prefix.

Verified facts (do not re-derive):
- `corev1.ContainerStateTerminated` has `Message string` (k8s.io/api, `json:"message,omitempty"`).
- `publishFailure` is `func (p *resultPublisher) publishFailure(ctx context.Context, task lib.Task, jobName string, reason string, note string, pod *corev1.Pod) error`.
- `PublishStoreMissFailure` calls `publishFailure` with `pod == nil`, and `pkg/zombie_sweeper.go` calls `PublishFailure` with `pod == nil`; both must keep producing no message line.
- The current `## Failure` body has no `- **Message:**` line and no existing test asserts exact equality of the whole section — existing assertions use `ContainSubstring`, so an added line is additive.
- The changelog's top heading is `## v0.67.0`; there is no `## Unreleased` section yet.
</context>

<requirements>
1. In `pkg/result_publisher.go`, add a package-private helper beside `logTailBlock`:

   ```go
   // terminationMessage returns the failed pod's container termination message,
   // redacted and collapsed to one line, or "" when the pod carries none.
   func terminationMessage(pod *corev1.Pod) string
   ```

   Behaviour:
   - Return `""` when `pod == nil`.
   - Walk `pod.Status.ContainerStatuses` in order. For the first status with `cs.State.Terminated != nil`, take `cs.State.Terminated.Message`.
   - Pass the raw message through `redactLog` before it can reach the section, exactly as `logTailBlock` does.
   - Trim trailing newlines, then replace every remaining `\r\n` / `\r` / `\n` with a single space so the value occupies one bullet line. Return `""` if the result is empty.
   - Do not read `.Reason` or `.ExitCode` here; those already flow through the published `reason`.

2. In `publishFailure`, compute `message := terminationMessage(pod)` once, before composing `section`. When `message` is non-empty, render it as a bullet line `- **Message:** <message>\n` and place it after the `- **Reason:**` line and before the existing `note` and log-tail block. When `message` is empty, render nothing for it, so the composed section for every pod without a termination message is byte-identical to today's output.

   The resulting order is: `## Failure`, blank line, `- **Timestamp:**`, `- **Job:**`, `- **Reason:**`, optional `- **Message:**`, optional `note`, optional `- **Log tail:**`.

   Keep the message in the body section only. Do not append it to `reason`, to the log line, or to any Sentry-facing string; the reason vocabulary and its parsers must not change.

3. In `pkg/result_publisher_log_tail_test.go`, add a new `Describe("PublishFailure termination message", ...)` block that reuses `newLogTailFixture()`. Add a small helper in the test file to set a terminated container on the fixture pod:

   ```go
   pod.Status.ContainerStatuses = []corev1.ContainerStatus{
       {State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
           ExitCode: 1, Reason: "Error", Message: msg,
       }}},
   }
   ```

   Cover, asserting on the decoded `taskcmd.UpdateFrontmatterCommand` via `decodeUpdateFrontmatterCommand`:
   - A non-empty message appears as `- **Message:** <msg>` in `Body.Section`, after the `- **Reason:**` line.
   - Credential shapes in the message are redacted: `ghp_0123456789abcdefghijABCDEFGHIJ012345`, `ANTHROPIC_AUTH_TOKEN=zz99887766554`, and a URL with `user:password@` all become `<redacted>` and the raw literals are absent from `Body.Section`.
   - A multi-line message is collapsed to one line: the section contains no raw `\n` between the message words, and a message containing a line `## Parked` does not create a second heading.
   - An empty message (`Message: ""`) produces no `- **Message:**` line.
   - A nil pod (`PublishFailure(ctx, task, job, reason, nil)`) produces no `- **Message:**` line.
   - Set `f.reader.TailReturns("", nil)` where the log tail would otherwise add noise, and keep the existing log-tail specs untouched.

4. Add a `## Unreleased` section at the top of `CHANGELOG.md` (above `## v0.67.0`) with one `- fix:` bullet. State that a failed Job's container termination message is now written into the `## Failure` section, redacted and single-lined, so distinct crashed runs no longer collapse into one opaque `pod_crash_no_stdout` record; note that the reason vocabulary and the empty-message output are unchanged. Name the changed files.

5. Before finishing, re-run `<verification>` and confirm it passes; walk each numbered requirement against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT add, rename, or reorder any `ZombieReason` constant. The vocabulary is a closed, documented on-disk contract parsed by `ParseZombieReason`; the message is free-form body text, never a reason value.
- Do NOT append the message to the `reason` string passed to `PublishFailure`; a suffixed reason would still parse, but the message is not a classification and must not reach the escalation logic.
- Do NOT change `PublishStoreMissFailure`, `pkg/zombie_sweeper.go`, or `PublishFailure`'s exported signature. The message is derived from the `pod` argument those paths already pass (or do not pass).
- Do NOT add a Config field, feature flag, threshold, or Prometheus metric. This is a body-text addition only.
- Keep the no-message output byte-identical. Every existing `ContainSubstring` assertion in `pkg/result_publisher_test.go` and `pkg/result_publisher_log_tail_test.go` must stay green unchanged.
- Do NOT run `go mod vendor`, and never use `-mod=vendor`.
- `pkg/result_publisher_test.go` is at the revive file-length limit — put new specs in `pkg/result_publisher_log_tail_test.go`.
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` — must pass.

Then, from the repo root:

- `grep -qF 'func terminationMessage(pod *corev1.Pod) string' pkg/result_publisher.go` — must succeed.
- `grep -qF '**Message:**' pkg/result_publisher.go` — must succeed.
- `test "$(grep -cF 'redactLog(' pkg/result_publisher.go)" -ge 2` — must succeed (the log tail and the termination message both redact).
- `grep -qF 'Describe("PublishFailure termination message"' pkg/result_publisher_log_tail_test.go` — must succeed.
- `grep -q '^## Unreleased' CHANGELOG.md` — must succeed.
- `grep -q 'termination message' CHANGELOG.md` — must succeed.
- `! grep -q 'Terminated.Message' pkg/job_watcher.go` — must succeed (the message is read in the publisher, not threaded through the watcher's existing reason path).
</verification>
