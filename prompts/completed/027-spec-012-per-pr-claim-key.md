---
status: completed
spec: [012-bug-pr-review-claims-serialize-per-repo]
summary: Added body-aware per-pull-request claim keying for pr-review tasks with a repository-key fallback counter and warning, plus table and handler tests.
execution_id: agent-task-executor-claim-reaper-exec-023-spec-012-per-pr-claim-key
dark-factory-version: v0.198.7
created: "2026-10-23T12:26:59Z"
queued: "2026-10-23T12:36:10Z"
started: "2026-10-23T12:44:18Z"
completed: "2026-10-23T12:53:08Z"
branch: dark-factory/bug-pr-review-claims-serialize-per-repo
---

# Key pr-review claims by pull request

<summary>
- A review of one pull request no longer blocks reviews of every other pull request in the same repository.
- Two reviews of the same pull request still exclude each other, exactly as before.
- The pull-request number comes from the PR's own link on a line of the task body.
- The task body is accepted only when a line is exactly that repository's PR URL; copied descriptions with another repository's link, mid-line links, `/files` suffixes and malformed numbers are ignored.
- When no usable link is present, the task falls back to the repository-wide claim and still runs.
- Every fallback leaves one warning log line and increments one unlabelled counter, so operators can see that the watcher output drifted.
- Dependency-update, release and build-fix tasks keep one claim per repository; other task types keep one claim per task id.
- The existing key-derivation function and its tests are untouched.
- No new configuration knob, frontmatter key or lease-timing change is introduced.
</summary>

<objective>
Add a body-aware claim-key entry point in `pkg/claim` that keys a `pr-review` task by pull request (`claims/pr/<owner>-<repo>-<n>`) when the task body carries the repository's own standalone PR URL, and otherwise falls back to the existing repository key while the spawn path logs and counts the fallback. Wire the spawn-claim path to it so independent PR reviews of one repository run in parallel, while same-PR reviews and every other task type keep their current exclusion.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for the container agent's execution rules (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, external test packages, coverage ≥80% for new code, license header on every Go file). This repo has no project-level `CLAUDE.md`.

Read these files fully before changing anything:
- `pkg/claim/key.go` — `DeriveClaimKey`, the `repoKeyedTaskTypes` map (it still contains `pr-review`; do not remove it), and `validateClaimComponent` / `isSafeClaimRune`. `DeriveClaimKey` must not change.
- `pkg/claim/key_test.go` — the existing table. Do not modify this file (spec constraint: its v0.30.7 content is frozen).
- `pkg/handler/claim_wiring.go` — `acquireSpawnClaim` (the spawn-claim path), `claimOwnerRepo` (reads `repo`, falls back to `clone_url`), `claimFailureDecision`, and the `derive_key` stage. The new entry point replaces only the `claim.DeriveClaimKey(...)` call inside `acquireSpawnClaim`.
- `pkg/handler/claim_owner_repo_internal_test.go` — the frozen v0.30.7 test for `claimOwnerRepo`. Do not modify this file.
- `pkg/handler/claim_gate_internal_test.go` — `newGateHandler`, `gateTask`, `recordingClaimRefClient` (`putCalls`, `put`, `list`, `delete`), `snapshotGateClaims`, `expectGateClaimDelta`, `gateClaimResultLabels`. Reuse these helpers in the new tests; do not redefine them.
- `pkg/handler/claim_wiring_internal_test.go` — `noopJobControl`, used by `newGateHandler`.
- `pkg/handler/task_view_test.go` — `captureStderr`, the glog stderr-capture pattern (`flag.Set("logtostderr","true")`, swap `os.Stderr`, `glog.Flush()`). The internal test package needs its own copy because `captureStderr` lives in the external `handler_test` package.
- `pkg/metrics/metrics.go` — `ClaimTotal`, `ClaimMismatchTotal`, and the `init()` pre-initialisation blocks.
- `pkg/metrics/metrics_test.go` — `Describe("Metrics")`, the registry-name spec and `gatherLabels`.
- `pkg/spawner/job_spawner.go` — `renderTaskContent` reads `task.Content`, confirming the body is `lib.TaskContent` (underlying string) and is available at spawn time.

Verified facts (do not re-derive):
- `lib.Task` has `TaskIdentifier lib.TaskIdentifier`, `Frontmatter lib.TaskFrontmatter` and `Content lib.TaskContent`; `lib.TaskContent` converts to `string`.
- `task.Frontmatter.TaskType()` returns the frontmatter task type; `string(...)` is how `claim_wiring.go` already passes it.
- `claim.ClaimRef` has fields `Ref`, `Sha`, `Cluster`, `TaskID`, `LeaseUntil` (RFC3339 string).
- `claim.ClaimRefClient` is `List(ctx, prefix) ([]ClaimRef, error)`, `Put(ctx, key, expectedSha, cluster, taskID string, leaseUntil libtime.DateTime) (string, error)`, `Delete(ctx, key, expectedSha string) error`.
- `claim.ErrLeaseConflict` is the sentinel for a CAS conflict; `claim.ErrLeaseNotExpired` is the takeover refusal.
- `libtime.DateTime` exposes `.Time()`; format a lease with `leaseUntil.Time().UTC().Format(time.RFC3339)`.
- `metrics.ClaimTotal` is a `*prometheus.CounterVec` with label `result`; `prometheus/testutil.ToFloat64` reads a series.
- The task view retains `Content` for non-terminal tasks (`pkg/handler/task_view.go` clears it only at a terminal phase), so the spawn sees the body.

Relevant coding docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2, external `_test` package, coverage ≥80% for new code.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `github.com/bborbe/errors`, never `fmt.Errorf`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-doc-best-practices.md` — GoDoc comments start with the name and describe behaviour.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-precommit.md` — golines 100-char limit, funlen 80, nestif 4, license headers.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.

</context>

<requirements>
1. Add the unlabelled fallback counter to `pkg/metrics/metrics.go`, immediately after `ClaimMismatchTotal`:
   ```go
   // ClaimKeyFallbackTotal counts pr-review tasks whose body carried no usable
   // pull-request URL, so their claim fell back to the repository key.
   var ClaimKeyFallbackTotal = promauto.NewCounter(prometheus.CounterOpts{
       Name: "executor_claim_key_fallback_total",
       Help: "Total number of pr-review claim keys that fell back to the repository key.",
   })
   ```
   In `init()`, add `ClaimKeyFallbackTotal.Add(0)` so the series exists at 0 before any claim activity. Do not add labels to it and do not touch `ClaimTotal` or any other metric.

2. Add the body-aware entry point to `pkg/claim/key.go`. Keep `DeriveClaimKey`, `repoKeyedTaskTypes`, `validateClaimComponent` and `isSafeClaimRune` byte-for-byte unchanged. Add (new `strings`/`strconv` usage may be needed; `strings` is already imported):
   ```go
   // DeriveClaimKeyWithBody maps a task to the claim key it uses. It behaves like
   // DeriveClaimKey, except that a pr-review task whose body carries the task
   // repository's own pull-request URL is keyed per pull request. The second
   // return value reports that a pr-review task fell back to the repository key;
   // it is meaningful only when the error is nil.
   func DeriveClaimKeyWithBody(
       ctx context.Context,
       taskType string,
       taskID string,
       owner string,
       repo string,
       body string,
   ) (string, bool, error)
   ```
   Behaviour:
   - When `taskType != "pr-review"`, return `DeriveClaimKey(ctx, taskType, taskID, owner, repo)` with the fallback flag `false`. The error passes through unchanged.
   - For `pr-review`, lowercase `owner` and `repo`, then ask a helper for the pull-request number. When a number is found, validate `owner`, `repo` and the number with `validateClaimComponent` (names `"owner"`, `"repo"`, `"pull request number"`) and return `"claims/pr/" + owner + "-" + repo + "-" + n`, fallback `false`, nil. A validation error is returned as-is with fallback `false`.
   - When no number is found, call `DeriveClaimKey(ctx, taskType, taskID, owner, repo)` and return its key and error with fallback `true`. The caller checks the error first, so an empty owner or repo still fails loudly through `derive_key` and never increments the fallback counter.

3. Implement the body scan as two unexported pure helpers in `pkg/claim/key.go`, no regex:
   - `pullRequestNumberFromBody(owner, repo, body string) (string, bool)`: split the body on `"\n"`; for each line take `strings.TrimSpace(line)` and return the first number that `pullRequestNumberFromLine` accepts. Return `("", false)` when `owner == "" || repo == ""` or no line qualifies. Lines that do not qualify are skipped, so a different repository's URL before the task repository's URL does not win.
   - `pullRequestNumberFromLine(owner, repo, line string) (string, bool)`: accept only `line` exactly of the shape `https://github.com/<o>/<r>/pull/<n>`. Implement it as: require the literal prefix `https://github.com/`; split the remainder on `"/"`; require exactly four parts; require `parts[2] == "pull"`; require `strings.EqualFold(parts[0], owner)` and `strings.EqualFold(parts[1], repo)`; require `parts[3]` to be 1 to 10 bytes, every byte in `'0'`–`'9'`, and `strconv.Atoi(parts[3])` to succeed with a value `>= 1`. Return `strconv.Itoa(value)` (canonical, so `pull/0658` keys the same PR as `pull/658`) and true, else `""`, false.
   - This must reject, with no trailing characters allowed: a link in the middle of a line, a link for another repository, `pull/0`, an 11-digit number, `pull/<n>/files`, and `pull/<n>/` (the trailing slash produces a fifth empty part).

4. Change the spawn-claim path in `pkg/handler/claim_wiring.go`. Inside `acquireSpawnClaim`, after `owner, repo := claimOwnerRepo(task)`, replace the `claim.DeriveClaimKey(...)` call with:
   ```go
   key, fallback, err := claim.DeriveClaimKeyWithBody(
       ctx,
       string(task.Frontmatter.TaskType()),
       string(task.TaskIdentifier),
       owner,
       repo,
       string(task.Content),
   )
   ```
   On error, keep the existing `return h.claimFailureDecision(task, "derive_key", err)` (check the error before the flag). When `fallback` is true, increment `metrics.ClaimKeyFallbackTotal` exactly once and emit exactly one warning line:
   ```go
   glog.Warningf(
       "event=claim_key_fallback task=%s repo=%s/%s",
       task.TaskIdentifier, owner, repo,
   )
   ```
   Use `owner`/`repo` exactly as `claimOwnerRepo` returned them (raw case is acceptable in the log; the key is lowercased inside the claim package). Add the `pkg/metrics` import to `claim_wiring.go`. The rest of `acquireSpawnClaim` (Claim, one Takeover on conflict, `claimFailureDecision` on failure) is unchanged.

5. Add the key-derivation table test in a NEW file `pkg/claim/key_body_test.go` (`package claim_test`, Ginkgo `DescribeTable`, license header). Do not add these rows to `key_test.go`. Call `claim.DeriveClaimKeyWithBody` and assert key, fallback flag and error. Every row from spec AC 1:
   | owner/repo | body | expected |
   |---|---|---|
   | `bborbe`/`nuke` | `https://github.com/bborbe/nuke/pull/658` alone on a line | `claims/pr/bborbe-nuke-658`, fallback false |
   | `bborbe`/`nuke` (as resolved from `clone_url`) | the #658 URL alone on line 4, then a copied description with a standalone `https://github.com/bborbe/nuke/pull/700` line | `claims/pr/bborbe-nuke-658` |
   | `bborbe`/`nuke` | standalone `https://github.com/other/repo/pull/1` first, then the #658 URL | `claims/pr/bborbe-nuke-658` |
   | `BBorbe`/`Nuke` | `https://github.com/BBorbe/Nuke/pull/658` | `claims/pr/bborbe-nuke-658` |
   | `bborbe`/`nuke` | `   https://github.com/bborbe/nuke/pull/658  ` | `claims/pr/bborbe-nuke-658` |
   | `bborbe`/`nuke` | `see https://github.com/bborbe/nuke/pull/700 for context` | `claims/repo/bborbe-nuke`, fallback true |
   | `bborbe`/`nuke` | `https://github.com/bborbe/nuke/pull/0` | `claims/repo/bborbe-nuke`, fallback true |
   | `bborbe`/`nuke` | `https://github.com/bborbe/nuke/pull/12345678901` | `claims/repo/bborbe-nuke`, fallback true |
   | `bborbe`/`nuke` | `https://github.com/bborbe/nuke/pull/658/files` | `claims/repo/bborbe-nuke`, fallback true |
   | `bborbe`/`nuke` | `https://github.com/bborbe/nuke/pull/658/` | `claims/repo/bborbe-nuke`, fallback true |
   Add delegation rows: `update-go`, `release` and `build-fix` (one row each), each with a standalone `https://github.com/bborbe/nuke/pull/658` body → `claims/repo/bborbe-nuke`, fallback false (spec AC 4); `claude` with any body → `claims/task/<id>`, fallback false; `pr-review` with empty owner → an error. Give each row a `taskID` (e.g. `abc123`) so the non-pr-review row can assert the task-id key.
   Add a table row for the validation-error branch: `pr-review`, owner `bad..owner`, repo `nuke`, body `https://github.com/bad..owner/nuke/pull/1` → error, fallback false.
   Add a row: `pull/0658` (standalone `https://github.com/bborbe/nuke/pull/0658`) → `claims/pr/bborbe-nuke-658`, fallback false.

6. Add handler tests in a NEW file `pkg/handler/claim_key_body_internal_test.go` (`package handler`, plain Go tests, license header):
   - Define `captureGlogStderr(t *testing.T, fn func()) string` in this file, modelled on `pkg/handler/task_view_test.go`'s `captureStderr`: set `flag.Set("logtostderr","true")`, swap `os.Stderr` for a pipe, run `fn`, `glog.Flush()`, restore and read the pipe. It must `defer flag.Set("logtostderr","false")` so later tests are not affected. Prompt 2 reuses this helper; do not define a second one anywhere in `package handler`.
   - Define an in-memory compare-and-swap client:
     ```go
     type casClaimRefClient struct {
         mu   sync.Mutex
         refs map[string]claim.ClaimRef
         seq  int
     }
     ```
     `Put` returns `claim.ErrLeaseConflict` when `expectedSha == ""` and the key exists, and when `expectedSha != ""` and the key is absent or its sha differs; otherwise it stores a `claim.ClaimRef{Ref: "refs/"+key, Sha: <new sha>, Cluster: cluster, TaskID: taskID, LeaseUntil: leaseUntil.Time().UTC().Format(time.RFC3339)}` and returns the new sha. `List` returns every stored ref whose `Ref` equals `"refs/"+prefix` or starts with `"refs/"+prefix+"/"`. `Delete` returns `claim.ErrLeaseConflict` unless the key exists with a matching sha, then deletes it.
   - Define `prReviewClaimTask(id, body string) lib.Task` with `Frontmatter: lib.TaskFrontmatter{"task_type": "pr-review", "repo": "bborbe/nuke"}` and `Content: lib.TaskContent(body)`.
   - **AC 2 spec**: with one `casClaimRefClient` and `newGateHandler(pkg.ClaimModeOn, client)`, call `acquireSpawnClaim` for nuke#658 (`tid-658`) and nuke#662 (`tid-662`) with bodies whose PR URL sits alone on a line. Assert both return true. Get the manager via `h.ClaimLeaseManager(ctx)` and assert `HeldClaims()` contains exactly the keys `claims/pr/bborbe-nuke-658` and `claims/pr/bborbe-nuke-662` (one entry each, with the matching TaskID). Then wrap the third call in `before := snapshotGateClaims()` … `expectGateClaimDelta(t, before, snapshotGateClaims(), map[string]float64{"conflict": 2})`: call `acquireSpawnClaim` for a third task id `tid-658-third` with the #658 body; assert it returns false, that `HeldClaims()` still holds both keys with `claims/pr/bborbe-nuke-658` still owned by `tid-658`, and that the store saw no third PUT (the PUT count is unchanged by the refusal). Verify those helper names (`snapshotGateClaims`, `expectGateClaimDelta`) exist in `pkg/handler/claim_gate_internal_test.go`; otherwise use that file's equivalent.
   - **Handler-level clone_url case**: a task with frontmatter `{"task_type":"pr-review","clone_url":"https://github.com/bborbe/nuke.git"}` (no `repo` key) and a body with the #658 URL alone on line 4, followed by a later standalone #700 URL line; assert the first recorded Put key is `claims/pr/bborbe-nuke-658`.
   - **AC 3 spec**: a table over the bodies `""`, `"https://github.com/other/repo/pull/1"`, and `"https://github.com/bborbe/nuke/pull/abc"`. For each row build a fresh `recordingClaimRefClient` and `newGateHandler(pkg.ClaimModeOn, client)`, read `testutil.ToFloat64(metrics.ClaimKeyFallbackTotal)` before and after `acquireSpawnClaim`, and assert: the call returned true, the first recorded `Put` key is `claims/repo/bborbe-nuke`, the counter delta is exactly 1, and the captured stderr contains `event=claim_key_fallback task=<id> repo=bborbe/nuke`. Use a distinct task id per row.
   - The new file needs its own imports (imports are per file): `context`, `flag`, `fmt`, `io`, `os`, `strings`, `sync`, `testing`, `time`, `lib "github.com/bborbe/agent"`, `libtime "github.com/bborbe/time"`, `"github.com/golang/glog"`, `"github.com/prometheus/client_golang/prometheus/testutil"`, `pkg "github.com/bborbe/agent-task-executor/pkg"`, `"github.com/bborbe/agent-task-executor/pkg/claim"`, and `"github.com/bborbe/agent-task-executor/pkg/metrics"`. Import only what each test uses (`gofmt`/`goimports` reject unused imports).

7. Extend `pkg/metrics/metrics_test.go` (this file may be modified): add `Expect(names).To(HaveKey("executor_claim_key_fallback_total"))` to the registry-name spec, and add an `It` asserting `testutil`/gather reports the counter at 0 in this package's fresh test process (`prometheus.DefaultGatherer.Gather()` then find the family and assert one metric with value 0). This is AC 3's "before any task" evidence; the handler package's test process may already have incremented it, so do not assert 0 there.

8. Cover every new production branch: `DeriveClaimKeyWithBody` (non-pr-review delegation, PR-key success, validation error, fallback), both helpers (each rejection row), and the handler fallback branch. New code must reach ≥80% statement coverage.
</requirements>

<constraints>
- `DeriveClaimKey`, `claimOwnerRepo`, `pkg/claim/key_test.go` and `pkg/handler/claim_owner_repo_internal_test.go` are unchanged. Do not edit those two test files.
- Do NOT add a `pr_number` frontmatter key or read one.
- Do NOT change lease timing, the concurrency cap, the expired-lease takeover path, or the expired-lease recovery path.
- Do NOT fail closed when the pull-request number cannot be derived: fall back to the repository key, log, count, and let the spawn proceed.
- Do NOT release orphaned claims; that is a sibling spec.
- `update-go`, `release` and `build-fix` keep `claims/repo/<owner>-<repo>`; other types keep `claims/task/<id>`.
- Key derivation is a pure function of task type, task id, owner, repo and body: every cluster derives the same key for the same task.
- Metric labels never carry keys or task ids. `executor_claim_key_fallback_total` has no labels; the `ClaimTotal` label set is unchanged.
- The body scan is one linear pass over lines with no regex and no backtracking.
- No new flag, environment variable, CRD field, function option or struct field may be added.
- All existing tests pass unmodified, with the only permitted exception being the additive change to `pkg/metrics/metrics_test.go` in requirement 7.
- Do NOT commit — dark-factory handles git.
- Every Go file keeps the existing two-line BSD license header.
</constraints>

<verification>
Run from the repo root:

```
ROOTDIR=/workspace make test
ROOTDIR=/workspace make precommit
grep -c 'event=claim_key_fallback' pkg/handler/claim_wiring.go
grep -c 'executor_claim_key_fallback_total' pkg/metrics/metrics.go
grep -c 'DeriveClaimKeyWithBody' pkg/claim/key.go
! grep -q 'DeriveClaimKeyWithBody' pkg/claim/key_test.go
```

Expected: `make test` and `make precommit` exit 0; the first grep prints `1`; the second prints `1`; the third prints at least `1`; the `! grep -q` exits 0. The frozen-file `git diff v0.30.7` check (spec AC 4) is operator-side because `.git` is masked in the container: run no `git` command, and do not open-for-edit the two frozen test files.

Before finishing, re-run the block above and walk each acceptance criterion (AC 1, 2, 3, 4) against the change.
</verification>
