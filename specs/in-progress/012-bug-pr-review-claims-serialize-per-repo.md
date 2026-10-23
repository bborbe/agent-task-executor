---
status: verifying
approved: "2026-10-23T12:13:49Z"
generating: "2026-10-23T12:23:27Z"
prompted: "2026-10-23T12:34:07Z"
verifying: "2026-10-23T13:07:31Z"
branch: dark-factory/bug-pr-review-claims-serialize-per-repo
---

## Summary

- Pull-request review tasks use one claim per repository. A review of one pull request therefore blocks reviews of every other pull request in that repository, on every cluster.
- Fix: review claims are keyed per pull request, so two tasks for the same pull request still exclude each other.
- The pull-request number comes from the PR's own link in the task text. When the link is missing, the claim falls back to the repository-wide claim with a warning and a counter, and the task still runs.
- The other repository-wide task types (dependency update, release, build fix) keep one claim per repository.
- Every successful claim and every claim removal now leaves one log line, so concurrency is observable at runtime.

## Problem

Unit-level reproduction on v0.30.7 (see Reproduction):

1. Two pr-review tasks for different pull requests of `bborbe/nuke` go through the spawn-claim path.
2. Both derive `claims/repo/bborbe-nuke`.
3. The second is refused with `ErrLeaseConflict` for as long as the first holds the key.

Context: on nuke prod (openclaw), 2026-10-23, pr-review task `ca0dc7eb` (bborbe/nuke#658) logged `event=claim_failed stage=takeover … error=lease conflict` for about 50 minutes and never ran. That is the bare `ErrLeaseConflict` sentinel from this executor's own held-claims map. A git-rest 409 would carry `PUT refs … returned 409`. At 11:39:21Z, `claims/repo/bborbe-nuke` was held by the nuke#662 review (task `4684419b`).

**Unverified:** whether 4684419b's Job was alive for the whole 50 minutes, because the kubectl log window starts only at 11:37Z. If it was not alive, an orphaned claim also played a part; the sibling spec `bug-held-claim-outlives-vanished-job` covers that. Either way, repo keying for pr-review serialises reviews of independent PRs on its own.

## Goal

- Reviews of different pull requests in one repository hold independent claims and run in parallel.
- Two tasks for the same pull request exclude each other.
- update-go, release and build-fix keep one claim per repository.
- Every claim acquisition and removal leaves one log line.

## Non-goals

- Do NOT add a `pr_number` frontmatter key written by the GitHub watcher. That is a possible follow-up spec.
- Do NOT change the concurrency cap, lease timing or the expired-lease takeover path.
- Do NOT change `DeriveClaimKey`'s signature or behaviour, or `claimOwnerRepo`.
- Do NOT fail closed when the PR number cannot be derived.
- Do NOT release orphaned claims. That is the sibling spec.

## Independence from the sibling spec

This spec does not depend on `bug-held-claim-outlives-vanished-job`. At rollout, startup recovery adopts this cluster's held `claims/repo/*` refs. Those with live Jobs are released by their Job's terminal event, exactly as today, and new pr-review claims use `claims/pr/*`. Only a `claims/repo/*` ref whose Job already vanished needs the sibling fix.

## Reproduction

Repo `bborbe/agent-task-executor` at `82d7453` (`release v0.30.7`). dark-factory `v0.198.7`.

**Deterministic (unit level, v0.30.7):**

1. Two pr-review tasks with `clone_url` `https://github.com/bborbe/nuke.git`. Their bodies have `https://github.com/bborbe/nuke/pull/658` and `…/pull/662` alone on line 4.
2. Run both through the spawn-claim path against one lease manager.
3. Expected on v0.30.7 (confirmed by the existing key table, spec 009 AC8): both derive `claims/repo/bborbe-nuke`. The second `Claim` returns `ErrLeaseConflict`, then `Takeover` returns `ErrLeaseConflict`.

**Runtime context (nuke prod openclaw, 2026-10-23):** the original log line is no longer retrievable, because the kubectl log window starts at 11:37Z. Reconstructed from the format string at `pkg/handler/claim_wiring.go:207` (`event=claim_failed stage=%s mode=%s task=%s error=%v`) and the reported fields:

```
event=claim_failed stage=takeover mode=on task=ca0dc7eb<full id not captured> error=lease conflict      # repeated ~50 min
```

## Expected vs Actual

| | Expected | Actual (v0.30.7) |
|---|---|---|
| Two pr-reviews, different PRs, same repo | Both claim and run | Second waits on `claims/repo/<owner>-<repo>` |
| Two pr-review tasks, same PR | Second gets `ErrLeaseConflict` | Same (unchanged) |
| update-go / release / build-fix | One claim per repo | Same (unchanged) |

Per-repo keying for pr-review is undocumented. Its only trace is spec 009 AC8 (`specs/in-progress/009-claim-lease-client-library.md:67`), which lists `pr-review` among the repo-keyed types in a key-derivation table and gives no rationale. `docs/claim-lease-protocol.md` has no key section at all. AC 7 adds that documentation.

## Why this is a bug

A review's critical section is one pull request, not the whole repository. Two reviews of different PRs share no working state, so a repo-wide lock adds latency (about 50 minutes for ca0dc7eb) without preventing any conflict. No document or spec gives a reason for the repo-wide lock.

## Acceptance Criteria

1. [ ] **Key derivation table.** A table-driven test over the new body-aware entry point. Evidence: `make test` exits 0 with every row passing.

   | Task | Body | Expected key |
   |---|---|---|
   | `repo: bborbe/nuke` | `https://github.com/bborbe/nuke/pull/658` on its own line | `claims/pr/bborbe-nuke-658` |
   | `clone_url`-only (`https://github.com/bborbe/nuke.git`) | the #658 URL alone on line 4, then a copied description containing a standalone `https://github.com/bborbe/nuke/pull/700` line | `claims/pr/bborbe-nuke-658` |
   | `repo: bborbe/nuke` | standalone `https://github.com/other/repo/pull/1` first, then the bborbe/nuke #658 URL | `claims/pr/bborbe-nuke-658` |
   | `repo: BBorbe/Nuke` | `https://github.com/BBorbe/Nuke/pull/658` | `claims/pr/bborbe-nuke-658` |
   | `repo: bborbe/nuke` | `   https://github.com/bborbe/nuke/pull/658  ` (leading and trailing whitespace) | `claims/pr/bborbe-nuke-658` |
   | `repo: bborbe/nuke` | the only matching URL sits mid-line: `see https://github.com/bborbe/nuke/pull/700 for context` | fallback `claims/repo/bborbe-nuke` |
   | `repo: bborbe/nuke` | `https://github.com/bborbe/nuke/pull/0` | fallback |
   | `repo: bborbe/nuke` | `https://github.com/bborbe/nuke/pull/12345678901` (11 digits) | fallback |
   | `repo: bborbe/nuke` | `https://github.com/bborbe/nuke/pull/658/files` | fallback |
   | `repo: bborbe/nuke` | `https://github.com/bborbe/nuke/pull/658/` | fallback |
2. [ ] **Different PRs in parallel, same PR conflicts.** Through the spawn-claim path against one lease manager and the fake git-rest:
   - pr-review tasks for nuke#658 and nuke#662 both acquire, and both keys are in `HeldClaims()`;
   - a third task id for nuke#658 is refused with `ErrLeaseConflict`, and the first claim stays held.

   Evidence: `make test` exits 0.
3. [ ] **Fallback, not fail-closed.** For each of three bodies, the task derives `claims/repo/bborbe-nuke`, its spawn claim is attempted, and `executor_claim_key_fallback_total` rises by exactly 1. The bodies are: empty, only another repo's PR URL, and `https://github.com/bborbe/nuke/pull/abc`. A registry gather before any task shows `executor_claim_key_fallback_total 0`. Evidence: `make test` exits 0, and `grep -c 'event=claim_key_fallback' pkg/handler/*.go` ≥ 1 in non-test code.
4. [ ] **Other types and frozen API unchanged.**
   - `update-go`, `release` and `build-fix` tasks whose body has a matching standalone PR URL still derive `claims/repo/<owner>-<repo>`.
   - Other types still derive `claims/task/<id>`.
   - `DeriveClaimKey` and `claimOwnerRepo` keep their signatures. Their v0.30.7 tests pass unmodified.

   Evidence: `make test` exits 0, and `git diff v0.30.7 -- pkg/claim/key_test.go pkg/handler/claim_owner_repo_internal_test.go` prints nothing.
5. [ ] **`claim_acquired` logged on both success branches.** One INFO line `event=claim_acquired key=<key> task=<id>` is emitted:
   - when `Claim` succeeds on the spawn path;
   - when `Takeover` succeeds after a conflict.

   It is not emitted when both fail. A test asserts all three cases; the log-capture mechanism is decided by the agent at impl time. Evidence: `make test` exits 0, and `grep -c 'event=claim_acquired' pkg/handler/*.go` (non-test files) = 1.
6. [ ] **One helper counts and logs every removal.** A single helper in `pkg/claim` counts `executor_claim_total{result=<label>}` and logs one INFO line `event=claim_released key=<key> task=<id> result=<label>`, for each label in `released`, `already_released`, `fenced` and `claim_lost`. Every removal path calls that helper. A test covers one path per label:
   - plain release → `released`;
   - release answered 409 → `already_released`;
   - fence → `fenced`;
   - renew 409 → `claim_lost`;
   - hold expiry → `released`.

   Evidence: `make test` exits 0, and both greps hold:
   - `grep -nE 'countClaim\("(released|already_released|fenced|claim_lost)"\)' pkg/claim/*.go | grep -v _test.go` returns 0 lines, because the helper passes the label as a variable;
   - `grep -c 'event=claim_released' pkg/claim/*.go` (non-test files) totals 1.
7. [ ] **Protocol doc.** `docs/claim-lease-protocol.md` gains a key section covering the three key shapes, the standalone-URL rule, the fallback, and the two log lines. Evidence: each of these six commands returns ≥ 1:
   - `grep -c 'claims/task/' docs/claim-lease-protocol.md`
   - `grep -c 'claims/repo/' docs/claim-lease-protocol.md`
   - `grep -c 'claims/pr/' docs/claim-lease-protocol.md`
   - `grep -c 'claim_key_fallback' docs/claim-lease-protocol.md`
   - `grep -c 'claim_acquired' docs/claim-lease-protocol.md`
   - `grep -c 'claim_released' docs/claim-lease-protocol.md`
8. [ ] **CHANGELOG.** Every line printed by `awk '/^## /{sec=$0} /claims\/pr\//{print "sits under: " sec}' CHANGELOG.md` is `sits under: ## Unreleased`, and at least one line is printed. Evidence: stdout match.
9. [ ] `make precommit` exits 0 at the repo root. Evidence: exit code.
10. [ ] **Post-Deploy (Rung-2):** on nuke dev, two pr-review claims of one repository overlap in time. Evidence: the log lines, quoted.
    - `kubectlnukedev -n dev logs deploy/agent-task-executor-openclaw --since=72h | grep -E 'event=claim_(acquired|released)'` shows an `event=claim_acquired` line for key `K1 = claims/pr/<o>-<r>-<n1>` at time t1.
    - It then shows an `event=claim_acquired` line for `K2 = claims/pr/<o>-<r>-<n2>`, with n1 ≠ n2, at t2 > t1.
    - No `event=claim_released key=K1` line has a timestamp in [t1, t2].
    - `logs deploy/…` reads one pod's logs, and only since that pod's last restart. If the pod restarted inside the window, earlier lines are missing and the check is re-run after the next pair.
    - If the window holds no such pair, the operator pushes commits to two non-draft PRs of one bborbe repository within one 5-minute window, and the check is re-run.
    - `deploy_check:` `kubectlnukedev -n dev get deploy/agent-task-executor-openclaw -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
    - `deploy_target:` `v0.30.8`. This is the predicted next patch tag. The operator updates it to the tag actually deployed, if that differs.

No new scenario: the fake git-rest and the spawn-claim path reach every behaviour, and AC 10 is the runtime proof.

## Verification

### Container-executable

```
make test
make precommit
git diff v0.30.7 -- pkg/claim/key_test.go pkg/handler/claim_owner_repo_internal_test.go   # empty
grep -nE 'countClaim\("(released|already_released|fenced|claim_lost)"\)' pkg/claim/*.go | grep -v _test.go   # empty
grep -c 'event=claim_acquired' pkg/handler/*.go
grep -c 'event=claim_released' pkg/claim/*.go
for t in 'claims/task/' 'claims/repo/' 'claims/pr/' claim_key_fallback claim_acquired claim_released; do grep -c "$t" docs/claim-lease-protocol.md; done
awk '/^## /{sec=$0} /claims\/pr\//{print "sits under: " sec}' CHANGELOG.md
```

### Operator-executable (host, after merge)

```
kubectlnukedev -n dev get deploy/agent-task-executor-openclaw -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'
kubectlnukedev -n dev logs deploy/agent-task-executor-openclaw --since=72h | grep -E 'event=claim_(acquired|released)'
```

## Desired Behavior

1. **Per-PR key for pr-review.** A new body-aware key entry point maps a `pr-review` task to `claims/pr/<owner>-<repo>-<n>`.
   - Owner and repo come from the existing source (`repo`, falling back to `clone_url`) and are lowercased.
   - `n` comes from the first body line that, trimmed of surrounding whitespace, is exactly `https://github.com/<o>/<r>/pull/<n>`. `<o>/<r>` must equal the task's owner/repo case-insensitively, and nothing may follow `<n>`.
   - `n` is 1–10 ASCII digits with a value of at least 1.
   - Every component passes the existing claim-component validation.

   The spawn-claim path uses this entry point. `DeriveClaimKey` stays as is and serves every other type.
2. **Fallback.** When no body line qualifies, a `pr-review` task:
   - derives `claims/repo/<owner>-<repo>`;
   - logs one warning line `event=claim_key_fallback task=<id> repo=<owner>/<repo>`;
   - increments `executor_claim_key_fallback_total`, which has no labels and is pre-initialised to 0.

   The spawn claim proceeds. When owner or repo cannot be derived, the existing `derive_key` failure is unchanged.
3. **`claim_acquired`.** A spawn claim that succeeds through `Claim`, or through `Takeover` after a conflict, logs one INFO line `event=claim_acquired key=<key> task=<id>`.
4. **`claim_released` through one helper.** One helper both counts and logs each of `released`, `already_released`, `fenced` and `claim_lost`, as `event=claim_released key=<key> task=<id> result=<label>`. Every path that removes a claim calls it. When the sibling spec lands, its jobless release reaches the helper through the same release code.
5. **Everything else as before.** update-go, release and build-fix keep `claims/repo/<owner>-<repo>`, and other types keep `claims/task/<id>`. Releases still find a task's claims by task id through `HeldClaims()`, never by re-deriving the key, so a key change cannot strand a held claim.

## Constraints

- `DeriveClaimKey`, `claimOwnerRepo`, `pkg/claim/key_test.go` and `pkg/handler/claim_owner_repo_internal_test.go` are unchanged.
- Key derivation is a pure function of task type, frontmatter and body, so every cluster derives the same key for the same task.
- Metric labels never carry keys or task ids. `executor_claim_key_fallback_total` has no labels, and the `ClaimTotal` label set is unchanged.
- Lease constants, fence, takeover, recovery and the success hold behave exactly as documented in `docs/claim-lease-protocol.md`.
- All existing tests pass unmodified.

## Assumptions

- The PR's own URL is the first `github.com/.../pull/<n>` line in a pr-review body. It sits alone on body line 4, ahead of the copied PR description (verified by coordinator, 2026-10-23).
- A pr-review body is retained in the task view while the task is non-terminal (`task_view.go` drops bodies only at terminal phases), so the spawn sees it.
- nuke dev runs every executor with `CLAIM_MODE=on` and receives pr-review traffic (confirmed by coordinator).
- The next release from `fix/claim-reaper` is a patch bump to `v0.30.8`.
- The key shape `<owner>-<repo>-<n>` keeps the ambiguity inherited from the repo key: owner `a` + repo `b-c` and owner `a-b` + repo `c` both yield `a-b-c`. This is accepted as the operator's chosen format. A collision only adds a needless conflict between two reviews.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection |
|---|---|---|---|
| Watcher output drifts (no standalone PR URL line) | Repo-key fallback, warning, counter; reviews serialise per repo as today | Watcher fix or the `pr_number` follow-up; evidence: `executor_claim_key_fallback_total` stops rising | `executor_claim_key_fallback_total` > 0 |
| Mixed-version rollout (one cluster on v0.30.7, one on the fix) | The two clusters derive different keys for the same pr-review task, so cross-cluster exclusion for that PR is lost during the window. The `current_job` foreign-Job guard still limits double runs. | Roll every cluster; evidence: the `deploy_check` image-tag query returns the same tag on every cluster running the executor | Two Jobs for one pr-review task id across clusters |
| Crash mid-action: executor restarts while holding a `claims/pr/*` ref | Startup recovery lists `claims/`, adopts the ref if its Job is running and releases it when the Job ends, or releases it immediately if the Job finished | automatic; evidence: `event=claim_released key=claims/pr/…` for that key | startup recovery log |
| A `claims/repo/*` ref held at rollout | Adopted by recovery and released by its Job's terminal event | automatic; evidence: `event=claim_released key=claims/repo/…` (an orphaned one needs the sibling spec) | that log line |
| Body edited after the claim | No effect on the held claim, because releases go by task id. A respawn derives the key from the current body. | n/a | n/a |
| git-rest unavailable at claim time | Unchanged: `claim_failed`, retried on the next pass | automatic; evidence: an `event=claim_acquired` line for the task on a later pass | `event=claim_failed` |
| Log volume | Two INFO lines per claim lifecycle, bounded by the spawn rate | n/a | n/a |

## Security / Abuse Cases

- PR authors influence the task body through the copied PR description. Only a line that is exactly a PR URL of the task's own repository qualifies, and the canonical URL sits at line 4, ahead of any copied text. An attacker's later URL line therefore never wins.
- Residual risk: if a body somehow lacks the canonical line and the copied description contains a standalone same-repo PR URL, the claim keys on that PR. The impact is limited to a needless conflict or a missing exclusion; it does not affect which code is reviewed.
- `n` is digits only (1–10, value ≥ 1), and every component passes the existing claim-component validation. No `/`, `..` or ref-unsafe character reaches a git ref name.
- The body scan is one linear pass over lines, bounded by the body size, with no backtracking regex on unbounded input.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Body-aware per-PR key entry point, fallback warning and counter, spawn-claim wiring | 1, 2, 5 | 1, 2, 3, 4, 9 | — |
| 2 | `claim_acquired` line, the single count-and-log release helper, protocol-doc key section, CHANGELOG bullet | 3, 4 | 5, 6, 7, 8, 9 | prompt 1 |

Rationale: prompt 1 is the behaviour fix in key derivation and wiring. Prompt 2 adds the observability that AC 10 reads, plus the docs and CHANGELOG that describe the shipped key format, so it goes second. AC 10 is run by the operator after release.

## Do-Nothing Option

Reviews of different PRs in one repository keep running strictly one at a time. ca0dc7eb waited about 50 minutes behind an unrelated PR's review, and the delay grows with the number of open PRs per repository. Not acceptable.
