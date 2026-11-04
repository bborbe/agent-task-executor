---
status: prompted
approved: "2026-11-03T20:30:23Z"
generating: "2026-11-04T01:44:29Z"
prompted: "2026-11-04T01:44:29Z"
branch: dark-factory/capture-failed-job-log-in-failure-section
---

## Summary

- When a Job fails, the executor records only Timestamp, Job and Reason in the task's `## Failure` section.
- The pod's own log — the only evidence of *why* it failed — is never recorded anywhere.
- Pod logs survive 30 minutes after the Job finishes (`ttlSecondsAfterFinished: 1800`), so any diagnosis that lands later finds nothing.
- This spec adds a bounded, redacted log tail to `## Failure` at the moment the failure is published.
- Redaction is a hard requirement, not a nicety: the vault autocommits and pushes unattended, and agent pods carry `GITHUB_APP_PEM`, `ANTHROPIC_AUTH_TOKEN` and `SENTRY_DSN`.

## Problem

On 2026-11-03 a `service-analysis-agent` Job was killed `deadline_exceeded` at 1800 s. By the time anyone looked, the pod was gone and no log existed, so the failure could not be diagnosed — the `## Failure` section named the reason but nothing about the cause. The same is true of every failed Job: the record says *that* it failed, never *why*.

## Goal

Every failed Job's `## Failure` section carries the tail of its own pod log — redacted of credential-shaped material and bounded in size — written at the moment the failure is published, so diagnosis never depends on a pod outliving the failure.

## Non-goals

- Changing `JOB_TTL_SECONDS_AFTER_FINISHED` or any retention window.
- A platform-wide log-shipping pipeline (Loki, Vector, Fluent).
- Capturing logs for Jobs that succeed.
- Changing the `PublishFailure` interface signature (frozen by spec 004).
- The `pods/log` RBAC grant itself — see Assumptions; it is a separate change in `bborbe/agent`.

## Assumptions

- **The executor cannot currently read pod logs.** Its Role grants `pods` `get/list/watch` but **no** `pods/log` subresource — verified live on nuke dev 2026-11-03, and declared at `bborbe/agent` `helm/templates/executor-rbac.yaml:69` as `resources: [pods]`. Without the grant this feature returns 403 on every failure. The grant plus a chart bump in nuke is a required companion change tracked as its own subtask, and it must be merged and deployed **before** this spec's Rung-2 rung — no prompt in this spec can produce it, because the container mounts only this repo. Sequence: `bborbe/agent` RBAC PR → nuke chart bump → dev deploy → then this spec's Rung-2.
- `ttlSecondsAfterFinished` stays at 1800 s.
- The existing body-section writer accepts a fenced block inside `## Failure`.

## Acceptance Criteria

- [ ] A failure publish whose fake log contains a literal GitHub token, a PEM private-key block, a bearer token, a URL carrying userinfo, and a value of 13 or more characters assigned to a secret-named key produces a composed `## Failure` body containing **none** of those literal values — evidence: `go test ./pkg/ -run TestPkg -v -args -ginkgo.focus='redact|lookup key' | grep -qE 'Ran [1-9][0-9]+ of'` passes and the focused specs assert each value's absence against the **composed body**, not against the redactor's return value. The 13-character fixture is a value used in no other case, so a redactor hardcoded to the other fixtures fails here.
- [ ] The same composed body still contains a short lookup key verbatim, so the tail remains useful for triage — evidence: the same focused run asserts the literal is present in the composed body, from a spec under a `Describe` whose name contains `redact`.
- [ ] The log request carries both a line limit and a byte limit, and an over-limit input yields a tail within the byte limit — evidence: a focused spec asserts both fields on the request and that a 10× over-limit input yields at most the byte limit.
- [ ] A failed log read still publishes the failure, returns nil, adds no log block, and names the job on one V(1) line — evidence: a focused spec named `log read fails` asserts no error, the failure section present, `Log tail` absent, and the log line naming the job.
- [ ] A successful Job reads no log at all — evidence: a focused spec with a call-counting reader asserts zero calls and `Log tail` absent from the body.
- [ ] The tail is a fenced block, so the section stays parseable by the existing body-section writer — evidence: the composed body carries one opening fence immediately before the tail text.
- [ ] `PublishFailure`'s signature is unchanged — evidence: `git diff origin/master -U0 -- pkg/result_publisher.go | grep -cE '^[-+].*PublishFailure'` returns `0`.
- [ ] **Post-Deploy (Rung-2), gated on the RBAC prerequisite:** a forced deadline failure on dev leaves a redacted log tail in the task's `## Failure` section — evidence: `grep -n 'Log tail' "<task file>"` returns ≥1 line and `grep -cE '(BEGIN .*PRIVATE KEY|gh[pousr]_[A-Za-z0-9]{20,})' "<task file>"` returns `0`. Do not run this rung until the `pods/log` grant from Assumptions is deployed: against correct code it 403s and reads as a red AC.
  - `deploy_check:` `kubectlnukedev -n agent get deploy/agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `$(git describe --tags --abbrev=0)`

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — format, lint, vet, unit and integration tests
- `go test ./pkg/ -run TestPkg -v -args -ginkgo.focus='redact' 2>&1 | tee /tmp/spec-redact.out | grep -qE 'Ran [1-9][0-9]+ of' && grep -q 'SUCCESS!' /tmp/spec-redact.out` — redaction and composed-body assertions; the count assertion is what makes the command fail when the focus selects nothing, and the `SUCCESS!` assertion is what makes it fail when the selected specs fail (Ginkgo prints `Ran N of M` before the verdict)
- `go test ./pkg/ -run TestPkg -v -args -ginkgo.focus='failure' 2>&1 | tee /tmp/spec-failure.out | grep -qE 'Ran [1-9][0-9]* of' && grep -q 'SUCCESS!' /tmp/spec-failure.out` — failure-publish path
- `sed -n '/^\tPublishFailure($/,/^\t) error$/p' pkg/result_publisher.go | grep -cE '^\t\t[a-z]'` — prints `5`, one line per parameter, so an added or removed parameter changes the number

The suite entry point is `TestPkg` (`pkg/suite_test.go:18`), so a `-run 'Redact'` filter selects nothing and exits 0 — every Ginkgo selection in this spec uses `-run TestPkg` plus `-ginkgo.focus`.

Two ways to lose the focus silently, both measured on this repo with go1.27.3, and both why every selection above carries its `Ran` count:
- `-run 'Redact'` selects no suite, so nothing runs at all.
- `-- -ginkgo.focus='x'` does **not** forward the flag: `go test` runs the whole 247-spec suite, prints `Ran 247 of 247`, and exits 0. Use `-args -ginkgo.focus='x'` (or the bare `-ginkgo.focus='x'`), never `--`.

### Operator-executable (runs on the host after release and deploy)

- `kubectlnukedev -n agent get deploy/agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}'` — the dev executor runs the new tag
- Force one deadline failure on dev, then `grep -n 'Log tail' "<task file>"` — the tail is present
- `grep -cE '(BEGIN .*PRIVATE KEY|gh[pousr]_[A-Za-z0-9]{20,})' "<task file>"` — returns `0`

No scenario is added: the dev rung above exercises the real pod, real RBAC and real log path once, which is the only thing a scenario would add.

## Desired Behavior

1. When a Job fails and the executor publishes `## Failure`, the section carries Timestamp, Job and Reason exactly as today, plus one `- **Log tail:**` block.
2. The tail is the failed pod's own log, read at publish time, bounded by both a line count and a byte count so a runaway log cannot bloat the task file.
3. Every credential-shaped string in the tail is replaced before the section string is composed — redaction happens on the composing side, never downstream.
4. An assignment is redacted only when its key is secret-shaped and its value is 13 or more characters. A shorter value, and a value under an ordinary logfmt key such as `event=` or `job=`, stay readable — a lookup key is a reference, and a tail whose every field reads `<redacted>` is worthless for triage.
5. If the pod or its log cannot be read — already garbage-collected, lister not synced, API error, throttled — the failure publishes exactly as it does today: no log block, no error, one V(1) line naming the job.
6. A successful Job reads no log and gains no block.
7. The captured tail is plain text in a fenced block, so the markdown section stays parseable by the existing body-section writer.

## Constraints

- `PublishFailure(ctx, task, jobName, reason) error` keeps its signature — spec 004 freezes it, and the reason string still flows through unchanged.
- The redaction step must run before the section string is built; redacting the already-composed string is not acceptable.
- The log read must not depend on the job watcher's informer cache. The publisher and the watcher are constructed independently, so an informer-backed reader would couple them; the reader takes a client and a namespace instead.
- The redaction floor is a named constant, `redactionMinSecretLength = 13`, applied to the value of an assignment whose key is secret-shaped (`token`, `secret`, `passw`, `pem`, `dsn`, `credential`, `api_key`, `auth`, case-insensitive). A value of 13 or more characters under such a key is redacted; a shorter one survives as a lookup key, and a value under an ordinary logfmt key (`event`, `task`, `job`, `reason`) is never redacted. The key-shape requirement is load-bearing: the executor's own logs are logfmt (`pkg/result_publisher.go:549` — `event=… task=… job=… reason=…`), so a pure length floor replaces every field with `<redacted>` and ships a tail that cannot be triaged. The implementation and the test that pins the boundary both reference the constant, never a literal.
- No new cluster permission beyond reading pod logs in the namespaces the executor already watches.

## Failure Modes

| Trigger | Expected behavior | Detection | Recovery |
|---|---|---|---|
| Pod already garbage-collected when the failure publishes | Publish succeeds without a log block | One V(1) line naming the job and the miss | `kubectlnukedev -n agent logs job/<job> --tail=200` inside the TTL window |
| Log API returns an error | Publish succeeds and returns nil; the failure record is not lost to a logging failure | Same V(1) line | None needed — the failure still lands |
| k8s API throttles the log read | Empty or short tail, publish unaffected | Same V(1) line | Re-read the pod directly |
| Log contains a secret shape the table does not cover | The uncovered value is written — this is the spec's real risk | Operator grep of the written section for the known class | Add the class to the table and its test; treat as a bug, not a tuning change |
| Log is enormous | Bounded by the line and byte limits on the request | Tail visibly truncated at the byte limit | Raise the bound deliberately if a real case needs it |

## Security / Abuse

The captured log is untrusted container output written into a file that is committed and pushed without human review. Two properties bound that:

- **Redaction before composition.** The redaction step runs on the raw text before the section string exists, so an unredacted value never has a moment where it could be logged, returned or written. The end-to-end AC asserts this on the composed body, not on the redactor alone — a redactor that is never called must fail the spec.
- **Bounded size.** The line and byte limits are applied at the API, so a log designed to flood the task file cannot.

The residual risk is a secret shape the table does not recognise. That is why the redaction table ships with a test per class and why an uncovered shape is treated as a bug rather than a tuning change.

## Suggested Decomposition

Prompts should be generated in this order — redaction first, so no capture code can compose an unredacted section.

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Redaction helper, its floor constant, and the table test | 3, 4 | 1, 2, 3 | — |
| 2 | Bounded log reader against a fake client | 2, 5 | 4, 5 | — |
| 3 | `## Failure` wiring, composed-body assertions, success-path negative | 1, 6, 7 | 1, 2, 5, 6, 7, 8 | prompts 1, 2 |

Rationale: prompts 1 and 2 are independent and each testable alone; prompt 3 consumes both and owns the composed-body ACs that close the laziness gap.

## Do-Nothing Option

Every future Job failure stays undiagnosable past its 30-minute retention window. The 2026-11-03 incident cost a full investigation that ended in "the pod was already gone"; without this change the next one costs the same.
