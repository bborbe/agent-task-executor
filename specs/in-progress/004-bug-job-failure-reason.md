---
status: verifying
approved: "2026-08-31T21:41:41Z"
generating: "2026-08-31T21:49:15Z"
prompted: "2026-08-31T21:55:41Z"
verifying: "2026-08-31T22:11:18Z"
branch: dark-factory/bug-job-failure-reason
---

## Summary

- agent-task-executor labels EVERY terminal Job failure as `deadline_exceeded`, even when the Job actually died of something else.
- The job-condition classifier treats two distinct Job conditions — "Job ran past its active deadline" (`DeadlineExceeded`) and "Job exhausted its retry backoff limit" (`BackoffLimitExceeded`) — as the same reason, and never reads the failed pod's actual termination reason.
- A pod OOM-killed at minute 6 of a 30-minute budget is therefore reported as having exhausted its deadline; `OOMKilled`/exit `137` is invisible without manually running `kubectl get pod -o json`.
- Real incident 2026-08-16: three genuinely different failures (OOM kill, missing GitHub App scope, quota-starved never-ran Job) all logged identically as `deadline_exceeded`, which misled a post-mortem into tallying 82 Jobs as quota-starved.
- Fix: `DeadlineExceeded` still maps to `deadline_exceeded`; `BackoffLimitExceeded` maps to a distinct pod-failure reason that surfaces the pod's terminated reason + exit code, with a dedicated reason value for OOM kills.

## Problem

The reason string is the executor's primary operator triage signal: it is written into the vault task's `## Failure` body and operators grep on these values to decide what went wrong. Because `BackoffLimitExceeded` and `DeadlineExceeded` collapse into one label, a confident but wrong `deadline_exceeded` terminates investigation at the wrong place. The evidence disagrees with the label every time it is checked: the Job condition says `BackoffLimitExceeded`, the pod says `OOMKilled`/`137`, the log says `deadline_exceeded`. On 2026-08-16 this produced a post-mortem that attributed 82 Jobs to quota starvation on the strength of the label alone; an unknown share may have been OOM kills or agent-side errors. The operator cannot trust the executor's failure reason until the label matches the Job condition and pod state.

## Goal

After this work, a failed Job's published reason describes what actually happened:

- a Job killed by its active deadline reports `deadline_exceeded` (unchanged);
- a Job that hit its backoff limit reports a distinct pod-failure reason — never `deadline_exceeded` — and the published failure text carries the pod's terminated reason and exit code (e.g. `OOMKilled`/`137`) so a reader sees the cause without a manual pod inspection;
- the executor log line and the vault task `## Failure` body show this same specific reason.

## Non-goals

- Raise the Job memory limit (the 4Gi→8Gi raise is a separate `quant/agent` values change).
- Fix the GitHub App missing-`workflows`-scope permission (operator action, unrelated to this bug).
- Touch the resync `not in task store` warning noise (separate sibling task).
- Fix the zombie sweeper's similar `deadline_exceeded` fallback (`zombie_sweeper.go` `classify`) — noted as a follow-up if the same mislabel surfaces from that path.

## Constraints

- Repo conventions are frozen: Ginkgo/Gomega v2 tests (no stdlib table tests), `github.com/bborbe/errors` wrapping (never `fmt.Errorf`), counterfeiter mocks for any new dependency (`//counterfeiter:generate`), glog `V(n)` gating.
- `ZombieReason` is a closed machine-readable set (`pkg/zombie_reason.go`). New value(s) are added deliberately and documented in the doc comment, which already states that adding a value requires updating the list and the documentation.
- Do NOT change `PublishFailure`'s interface — the reason string flows through as-is.
- Assumption: the failed Job's pod is usually still resolvable at `HandleJob` time — `TTLSecondsAfterFinished` keeps it until TTL expiry; when it is not (pod GC'd / lister not synced), the Job-condition path still publishes a distinct pod-failure reason without pod detail.
- The two existing `DeadlineExceeded` assertions in `pkg/job_watcher_test.go` stay; the two `BackoffLimitExceeded` assertions that pin the wrong behavior are updated.
- CHANGELOG: add an `## Unreleased` bullet for this fix (creating the section if absent — none exists at HEAD v0.8.0).

## Acceptance Criteria

- [ ] Failure reason is derived from the Job `.status.conditions[].reason`: a genuine `DeadlineExceeded` condition yields `deadline_exceeded`; a `BackoffLimitExceeded` condition yields a distinct pod-failure reason and NEVER `deadline_exceeded` — evidence: `make precommit` exits 0, the updated Ginkgo assertion (both the `HandleJob` path and the `JobFailureReason` direct mapping) asserts `BackoffLimitExceeded` no longer yields `deadline_exceeded` (test exit 0), and `grep -n 'BackoffLimitExceeded' pkg/job_watcher.go` returns ≥1 line not mapping to `ZombieReasonDeadlineExceeded`
- [ ] When the Job condition is `BackoffLimitExceeded`, the pod's `.state.terminated.reason` and `exitCode` are included in the published failure, so `OOMKilled`/`137` is visible without manual `kubectl get pod -o json` — evidence: (a) a unit test asserts the reason string passed to `PublishFailure` for an OOMKilled pod contains `OOMKilled` and `137`; (b) a second unit test with a non-OOMKilled terminated pod (e.g. `Error`/exit `1`) asserts the published reason surfaces that pod's own terminated reason + exit code and is neither `deadline_exceeded` nor `pod_oom_killed` (test exit 0)
- [ ] The synthetic failure published to the vault carries the same specific reason — evidence: unit test asserts `PublishFailure` was called with the specific reason string (e.g. `pod_oom_killed` for an OOMKilled pod) (test exit 0)
- [ ] A test asserts an OOMKilled job is NOT reported as `deadline_exceeded` — evidence: new unit test with an OOMKilled pod asserts a distinct reason is published (test exit 0)

No new scenario. Unit + integration tests in the implementation prompt reach the full behavior (pure in-process classification plus a fake publisher), so the four-condition scenario test is not met.

## Verification

### Container-executable (runs inside the dark-factory YOLO container at prompt time)

- `make precommit` — exits 0
- `go test ./pkg/...` — exits 0

### Operator-executable (runs on the host after PR merge, verification ladder)

Deploy is operator-driven (mirrored semver: bump `EXECUTOR_VERSION` in `quant/agent/Makefile`, deploy executor from `quant/agent`). Operator rung is intentionally minimal:

- Deterministic repro after deploy to dev: run the unit-level reproduction (this spec's `## Reproduction`) against the built artifact, then create a throwaway dev Job that always fails (container exits non-zero with a small `backoffLimit`; optionally OOMKilled) and confirm `kubectlnukedev -n dev logs <executor-pod> | grep 'failed (task'` shows the specific reason, never `deadline_exceeded`, and the vault task's `## Failure` body `- **Reason:**` line shows the specific reason plus the pod terminated reason/exit code.

## Desired Behavior

1. A failed Job whose condition reason is `DeadlineExceeded` reports `deadline_exceeded` — unchanged from today.
2. A failed Job whose condition reason is `BackoffLimitExceeded` reports a distinct pod-failure reason — never `deadline_exceeded` — regardless of whether the pod is still resolvable.
3. When the condition reason is `BackoffLimitExceeded`, the executor resolves the failed Job's pod (via the existing pod lister, matched by the Job owner reference or the `agent.benjamin-borbe.de/task-id` label) and reads the pod's `.state.terminated.reason` and `exitCode`.
4. A pod terminated with reason `OOMKilled` maps to a new dedicated `ZombieReason` value (`pod_oom_killed`); the value is added to the closed set in `pkg/zombie_reason.go` (with a matching assertion in `pkg/zombie_reason_test.go`) and to the doc comment. The dedicated value is chosen over reusing the generic pod-failure reason because operators grep for OOM kills specifically — a generic reason cannot be grepped for `OOMKilled`. Other non-zero terminated reasons fall back to the existing generic pod-failure reason — the terminated reason and exit code are surfaced either way.
5. The reason string published via `PublishFailure` (and therefore the log line and the vault `## Failure` body) includes the machine-readable reason AND the pod's terminated reason + exit code, so `pod_oom_killed` + `OOMKilled`/`137` is readable without `kubectl get pod -o json`. Exact formatting of the detail — agent decides at impl time.
6. Regression lock: the two existing tests that pin the wrong mapping are updated to assert the new behavior, and a new OOMKilled regression test is added (per Acceptance Criteria).

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection | Reversibility |
|---------|-------------------|----------|-----------|---------------|
| `BackoffLimitExceeded` Job whose pod is already GC'd / not in the pod-informer cache | Still publishes a distinct pod-failure reason, never `deadline_exceeded`; the reason carries no pod detail if none is resolvable | None needed — the Job condition alone already proves it was not a deadline kill | Published reason is distinct from `deadline_exceeded` | Irreversible (vault body already written) |
| Pod terminated reason is not `OOMKilled` (e.g. `Error`/`137`, `ImagePullBackOff`, `CrashLoopBackOff`) | Reason reflects the actual terminated reason, surfaced with its exit code; never `deadline_exceeded` | None | Reason string contains the terminated reason + exit code | Irreversible |
| Pod lister not yet synced / nil at `HandleJob` time | Job-condition path still classifies `BackoffLimitExceeded` as a distinct pod-failure reason without pod detail | None | Reason is distinct from `deadline_exceeded` | Irreversible |
| Job exhausts backoff across multiple pods with different terminated reasons | The terminal failure reflects the last pod's terminated state (the one that exhausted the backoff limit) | None | Published detail matches the last pod's `OOMKilled`/`137`-style state | Irreversible |
| Executor crashes mid-publish after classifying the failure | Publish is at-least-once via Kafka; existing `PublishFailure` dedupe (keyed on job name) prevents a second body write on re-delivery | None — dedupe absorbs re-delivery | No duplicate `## Failure` section in the vault task | Partial |
| Job condition flips from `BackoffLimitExceeded` to `DeadlineExceeded` (deadline fires after backoff exhausted) | Final reason reflects the terminal condition observed; either mapping is specific, never a silent merge | None | Reason matches the last observed condition | Irreversible |

## Reproduction

Worked example from the real incident, 2026-08-16 — job `github-update-go-agent-656ee21d-20260816092332` (repository `agent-task-executor` at release `v0.8.0`):

```
executor log : failed (task 656ee21d-...): deadline_exceeded
job condition: reason=BackoffLimitExceeded  "Job has reached the specified backoff limit"
pod          : reason=OOMKilled  exitCode=137  ran 09:23:33 -> 09:29:26  (~6 min of a 1800s budget)
```

The three artifacts disagree with each other: the pod died at 6/30 minutes from OOM, the Job condition says the backoff limit was reached, and the executor reports `deadline_exceeded`.

On the same day three genuinely different failures all logged identically as `deadline_exceeded`:

1. OOM at 4Gi during osv-scanner — pod `OOMKilled`/`137`
2. GitHub App missing workflows scope — task body `## Failure`
3. Job starved on `pods: 1` quota, never ran — Job condition `DeadlineExceeded` (the only genuine deadline)

Unit-level reproduction (runs against the current code):

```
job := Job{ status.conditions = [{ type: JobFailed, status: True, reason: "BackoffLimitExceeded" }] }
JobFailureReason(job)  ->  deadline_exceeded     # WRONG: must be a distinct pod-failure reason
```

```
pod := Pod{ status.containerStatuses[0].state.terminated = { reason: "OOMKilled", exitCode: 137 } }
job := Job{ status.conditions = [{ type: JobFailed, status: True, reason: "BackoffLimitExceeded" }] }
HandleJob(job) with pod resolvable  ->  PublishFailure called with reason "deadline_exceeded"  # WRONG
```

## Expected vs Actual

Expected:

- Reason derived from the Job condition: genuine `DeadlineExceeded` → `deadline_exceeded`; `BackoffLimitExceeded` → a distinct pod-failure reason (surfacing the pod's terminated reason + exit code), never `deadline_exceeded`.

Actual:

- Both `DeadlineExceeded` and `BackoffLimitExceeded` map to `ZombieReasonDeadlineExceeded` (`pkg/job_watcher.go` `JobFailureReason`, lines 261-262); the pod's `.state.terminated.reason` is never read on this path.

## Why this is a bug

The reason string is the primary operator triage signal: it is written into the vault task's `## Failure` body and operators grep on these values to triage. A confidently wrong label terminates investigation at the wrong place. The artifacts (Job condition, pod status) disagree with the log every time checked, and the 2026-08-16 incidents show real decision damage (82 Jobs tallied as quota-starved on the strength of the label). `ZombieReasonDeadlineExceeded`'s own doc contract ("machine-readable reason strings emitted in the ## Failure body... operators grep on these values") is broken when a `BackoffLimitExceeded`/OOM death is emitted as `deadline_exceeded`.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Classifier + new reason value + pod resolution + tests + CHANGELOG: split `BackoffLimitExceeded` from `DeadlineExceeded` in the Job-condition path, resolve the pod via the existing pod lister, add `pod_oom_killed` to the closed set + doc comment, surface terminated reason + exit code in the published reason, update the two wrong tests, add the OOMKilled regression test, add the `## Unreleased` CHANGELOG bullet. | 1-6 | 1-4 | — |

Rationale: prompt 1 is the whole required fix — classifier, closed-set value, pod resolution, tests, CHANGELOG — and satisfies every AC on its own. The zombie sweeper's similar fallback (`zombie_sweeper.go` `classify`) is out of scope for this spec — noted as a follow-up if the same mislabel surfaces from that path.

## Do-Nothing Option

The mislabel is not acceptable to keep: it corrupts the operator's primary triage signal (the `## Failure` reason operators grep on), already caused a wrong post-mortem (82 Jobs tallied as quota-starved on the strength of the label), and cost a wrong verdict in the 2026-08-30 recurrence. The fix is localized — one classifier split + one closed-set value + pod resolution + tests — so the cost of doing nothing (every future failure mislabeled, every count derived from `deadline_exceeded` unreliable) outweighs the fix cost.
