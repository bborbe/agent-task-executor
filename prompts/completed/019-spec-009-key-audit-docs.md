---
status: completed
spec: [009-claim-lease-client-library]
summary: Added claim key derivation, held-claim/running-Job mismatch audit, 13-series claim metric pre-initialisation, protocol doc and changelog entry, with no live wiring and make precommit green.
execution_id: agent-task-executor-claim-client-exec-019-spec-009-key-audit-docs
dark-factory-version: v0.198.7
created: "2026-10-21T18:52:26Z"
queued: "2026-10-21T19:14:22Z"
started: "2026-10-21T19:52:20Z"
completed: "2026-10-21T20:06:26Z"
branch: dark-factory/claim-lease-client-library
---

<summary>
- Adds the pure claim-key derivation function: repo-keyed task types map to a shared `claims/repo/<owner>-<repo>` key, every other type maps to `claims/task/<task id>`, and unsafe components are rejected.
- Adds a mismatch audit that compares the claims this manager holds against a given set of running Jobs and counts each "Job without a claim" and "claim without a Job" case.
- Pre-initialises all 13 `executor_claim_total{result}` series so the metric is visible before any claim activity, and exposes `executor_claim_mismatch_total`.
- Proves the library is not wired into the live dispatch path: no production code outside the claim package and its generated mocks imports it.
- Documents the constants, state machine and the fence / takeover / restart rules in `docs/claim-lease-protocol.md`.
- Adds the single `## Unreleased` changelog bullet for the feature.
- Finishes with `make precommit` green.
</summary>

<objective>
Complete the claim library with the pure key-derivation function, the held-claim vs running-Job mismatch audit, metric pre-initialisation and the protocol document, then prove no live wiring exists and the full precommit gate passes (AC8, AC9, AC11, AC13, AC14, AC15).
</objective>

<context>
Read the spec `/workspace/specs/in-progress/009-claim-lease-client-library.md` fully, especially Desired Behavior 8, AC8, AC9, AC11, AC13, AC14 and AC15.

Read these files fully before changing anything:
- `/workspace/prompts/1-spec-009-ref-client.md` and `/workspace/prompts/2-spec-009-lease-manager.md` — the two prompts that produced the client, the fake git-rest and the lease manager. This prompt builds on their exact identifiers (`claim.LeaseManager`, `claim.HeldClaim`, `claim.NewLeaseManager`, `claim.ClaimRefClient`, `claim.NewClaimRefClient`, `claim.ErrLeaseConflict`).
- `/workspace/pkg/claim/lease_manager.go` — the `LeaseManager` interface. Add `AuditMismatch` to it here.
- `/workspace/pkg/metrics/metrics.go` — the existing `promauto` counters and the single `init()` that pre-initialises label values. Add the mismatch counter and the 13-series pre-initialisation here.
- `/workspace/pkg/metrics/metrics_test.go` — the `gatherLabels` helper and the registry-assertion style.
- `/workspace/CHANGELOG.md` (first ~10 lines) — the current top section and the `## Unreleased` insertion point.
- `/workspace/docs/` — the existing project doc style; the new file is `docs/claim-lease-protocol.md`.

Verified external APIs (read from the module source; do not paraphrase):
- `"github.com/bborbe/errors"` → `Errorf(ctx, format, args...)`, `Wrapf`, `Is`.
- `"github.com/prometheus/client_golang/prometheus"` → `promauto.NewCounter`.
- `"github.com/prometheus/client_golang/prometheus/testutil"` → `ToFloat64(c prometheus.Collector) float64` (tests only).
- `libtime "github.com/bborbe/time"` → `DateTime`, `Duration`, `Second`.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md` — counter naming and pre-initialisation.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `bborbe/errors` wrapping.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` rules and the required `feat:` prefix.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo `DescribeTable`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-precommit.md` — linter limits.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules.
</context>

<requirements>
1. Create `/workspace/pkg/claim/key.go` (license header) with exactly this public function:

```go
// DeriveClaimKey maps a task to the claim key it uses. The four repo-keyed task
// types share one key per repository; every other type is keyed by task id.
// Every component is validated before it is used: empty, "/", "..", a leading
// ".", a trailing ".lock", or a character outside [A-Za-z0-9._-] is rejected.
func DeriveClaimKey(ctx context.Context, taskType, taskID, owner, repo string) (string, error)
```

Rules:
- Repo-keyed types: `update-go`, `release`, `pr-review`, `build-fix` (exact string match).
- For a repo-keyed type: lower-case `owner` and `repo` with `strings.ToLower`, validate both lower-cased values, then return `"claims/repo/" + owner + "-" + repo`. An empty, invalid or unsafe owner or repo is an error.
- For every other type (including `""`, `claude`, `backtest`): validate `taskID` as-is (do not lower-case it) and return `"claims/task/" + taskID`.
- Validation helper (unexported) `validateClaimComponent(ctx context.Context, name, value string) error`:
  - empty → `errors.Errorf(ctx, "claim %s must not be empty", name)`
  - `strings.Contains(value, "/")` → error
  - `strings.Contains(value, "..")` → error
  - `strings.HasPrefix(value, ".")` → error
  - `strings.HasSuffix(value, ".lock")` → error
  - any rune outside `[A-Za-z0-9._-]` → error
  - valid → nil
- Do NOT add a stage-pool key-selection function, a task-type enum, or a config knob. This is the pure derivation function only.

2. Create `/workspace/pkg/claim/key_test.go` in `package claim_test` as a Ginkgo `DescribeTable` over `(taskType, taskID, owner, repo)` covering AC8:
   - `update-go`, `release`, `pr-review`, `build-fix` with owner `bborbe`, repo `agent` → `claims/repo/bborbe-agent`.
   - owner `Bborbe`, repo `Agent` → `claims/repo/bborbe-agent` (lower-cased).
   - `claude`, `backtest` and `""` with task id `abc123` → `claims/task/abc123`.
   - A repo-keyed type with owner or repo empty, containing `/`, containing `..`, starting with `.`, ending in `.lock`, or containing a character outside `[A-Za-z0-9._-]` → non-nil error.
   - A non-repo type with a task id carrying the same defects → non-nil error.
   - A valid task id with uppercase letters and dots/dashes/underscores (for example `ABC-1.2_3`) is accepted unchanged.
   Assert the error is non-nil and the returned key is empty on every invalid row.

3. Add the mismatch audit. Claims in pending Job deletion or pending release count as held (they are in `HeldClaims()`). In `/workspace/pkg/claim/lease_manager.go`, add this method to the `LeaseManager` interface:

```go
	// AuditMismatch compares the claims this manager holds with the given set of
	// running Job task ids. It adds 1 to executor_claim_mismatch_total for every
	// running Job with no held claim and for every held claim with no running
	// Job, and logs which case each was. A task present in both is not counted.
	AuditMismatch(ctx context.Context, runningTaskIDs map[string]bool)
```

Create `/workspace/pkg/claim/audit.go` implementing it on `*leaseManager`. Take a consistent snapshot with the public `m.HeldClaims()` (which takes and releases the manager lock itself — do NOT call it while holding the lock, or it deadlocks), then iterate the snapshot without holding the lock:
- `heldTaskIDs := map[string]bool` from the held claims.
- For each task id in `runningTaskIDs` that is not in `heldTaskIDs`: `metrics.ClaimMismatchTotal.Inc()`, `glog.Warningf("claim_mismatch case=job_without_claim task=%s", taskID)`.
- For each held claim whose `TaskID` is not in `runningTaskIDs`: `metrics.ClaimMismatchTotal.Inc()`, `glog.Warningf("claim_mismatch case=claim_without_job task=%s", held.TaskID)`.
- A task present in both sets adds 0. Two held claims with the same task id count twice (per held claim).
- Do not use the task id as a metric label value.

4. Extend `/workspace/pkg/metrics/metrics.go`:
   a. Add:
```go
// ClaimMismatchTotal counts held-claim / running-Job mismatches, one per case.
var ClaimMismatchTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "executor_claim_mismatch_total",
		Help: "Total number of held-claim / running-Job mismatches.",
	},
)
```
   b. In the existing `init()`, add the closed 13-value pre-initialisation for `ClaimTotal`:
```go
	for _, result := range []string{
		"claimed",
		"conflict",
		"renewed",
		"recovered",
		"renew_failed",
		"claim_lost",
		"fenced",
		"taken_over",
		"takeover_too_early",
		"released",
		"already_released",
		"adopted",
		"error",
	} {
		ClaimTotal.WithLabelValues(result).Add(0)
	}
```
   Do NOT add any other metric, label, or result value.

5. Extend `/workspace/pkg/metrics/metrics_test.go`:
   - Assert the default registry contains `executor_claim_total` and `executor_claim_mismatch_total`.
   - Assert `gatherLabels(mfs, "executor_claim_total", "result")` contains all 13 result values before any claim activity (the pre-initialisation makes them present).
   - Assert the counter names and series are additive: the existing metric names in the current test still pass unchanged.

6. Create `/workspace/pkg/claim/audit_test.go` in `package claim_test` covering AC9's mismatch rows. Use the fake git-rest plus a real `claim.NewClaimRefClient`, a fake `JobControl`, a clock, and a manager. Claim keys to create held claims, then:
   - held `{t1}`, running `{t2}` → `executor_claim_mismatch_total` delta exactly 2.
   - held `{t1}`, running `{t1}` → delta exactly 0.
   - held `{}`, running `{t3}` → delta exactly 1.
   Read the value with `testutil.ToFloat64(metrics.ClaimMismatchTotal)` before and after each call. Do not call `AuditMismatch` concurrently in these rows.

7. Create `/workspace/docs/claim-lease-protocol.md` documenting the protocol exactly as implemented in prompts 1 and 2. It must contain at least these level-2 headings (the acceptance checks grep for `^## `, `fenceAt`, and a takeover/restart heading):
   - `## Constants`
   - `## Claim state machine`
   - `## Fence`
   - `## Takeover`
   - `## Restart recovery`
   - `## Lost-response recovery`

   Content requirements:
   - A constants table or list naming `300 s` lease, `10 s` tick interval, `60 s` renew interval, `fenceAt = lease_until − 60 s`, the fence firing when `now > fenceAt − 10 s`, `30 s` takeover grace after `lease_until`, and the `30 s` per-request cap. Put each of the strings `300 s`, `60 s`, `10 s` and `30 s` on its own line so the acceptance grep counts at least 4 matching lines.
   - The state machine: `claimed → held → renewed / recovered / lost / fenced / released`, plus the pending-Job-deletion and pending-release states.
   - The fence rule: a holder that cannot renew deletes its Job at or before `fenceAt`, sends one best-effort compare-and-swap `DELETE`, drops the claim, and never renews a fenced claim.
   - The takeover rule: a claim is takeable only at or after `lease_until + 30 s`, by one compare-and-swap `PUT` with the expired claim's sha; an unparseable `lease_until` refuses takeover.
   - The restart rule: re-adopt a running Job's claim at its listed sha, release a finished Job's claim, fence a claim inside the fence window, and never touch another cluster's claim.
   - The lost-response rule: a failed create/renew/takeover is read back; the writer recognises its own write by the exact `(cluster, task_id, lease_until)` triple on the entry whose `ref` equals `refs/<key>`, not by the sha and not by prefix.
   - State that the constants are invariants, not configuration.

8. Add the changelog entry. In `/workspace/CHANGELOG.md`, if `## Unreleased` already exists, add the bullet under it; otherwise insert one `## Unreleased` section immediately after the introductory paragraph (before the first `## v...` section), with one bullet whose line contains both `- feat:` and `refs/claims`, for example:

```markdown
## Unreleased

- feat: add a claim lease client library for git-rest `refs/claims` endpoints, with a compare-and-swap lease manager, fake-clock unit tests and `docs/claim-lease-protocol.md`.
```

   There must be exactly one `## Unreleased` in the file. Do not add a version heading.

9. Do NOT wire the library into `main.go`, the task event handler, the reconcile loop, the spawner or the Job watcher. No production file outside `pkg/claim/` and `mocks/` may import `github.com/bborbe/agent-task-executor/pkg/claim`.
</requirements>

<constraints>
- The executor's runtime behaviour is unchanged: nothing outside `pkg/claim` and its generated mock imports the package (AC11).
- `executor_claim_total` has exactly the 13 closed result values; keys and task ids are never label values.
- Existing metric names and series are unchanged; the two new counters are additive.
- The key derivation function rejects unsafe components before they reach a URL path: empty, `/`, `..`, leading `.`, trailing `.lock`, and anything outside `[A-Za-z0-9._-]`.
- The constants (lease length, tick interval, renew interval, fence margin, takeover grace, request timeout) are invariants. Do NOT add a knob or an opt-out flag.
- `## Unreleased` in `CHANGELOG.md` appears exactly once. The release tag is cut manually (`autoRelease: false` in `.dark-factory.yaml`); do not add a version heading.
- Do NOT add a scenario file. Every behaviour is reachable with the in-process fake server and the fake clock (spec Non-goal).
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `go test -mod=mod -race -count=1 ./pkg/claim/... ./pkg/metrics/...` iteratively after each change, then `make precommit` ONCE at the very end.

- `go test -mod=mod -race -count=1 ./pkg/claim/... ./pkg/metrics/...` — exits 0.
- `grep -rln 'pkg/claim"' --include='*.go' . | grep -vE '^./(pkg/claim/|mocks/)'` — returns 0 lines (AC11).
- `grep -l 'FakeClaimRefClient' mocks/*.go` — returns at least one file.
- `grep -l 'FakeClaimJobControl' mocks/*.go` — returns at least one file.
- `grep -c '^## ' docs/claim-lease-protocol.md` — returns at least 4.
- `grep -cE '300 s|60 s|10 s|30 s' docs/claim-lease-protocol.md` — returns at least 4.
- `grep -n 'fenceAt' docs/claim-lease-protocol.md` — returns at least 1 line.
- `grep -niE '^## .*(takeover|restart)' docs/claim-lease-protocol.md` — returns at least 1 line.
- `grep -c '^## Unreleased' CHANGELOG.md` — returns exactly 1.
- `awk '/^## /{sec=$0} /^- feat:/ && /refs\/claims/{print sec; exit}' CHANGELOG.md` — prints `## Unreleased`.
- `for r in claimed conflict renewed recovered renew_failed claim_lost fenced taken_over takeover_too_early released already_released adopted error; do grep -qE "\"$r\": *[1-9]" pkg/claim/*_test.go || echo "MISSING non-zero delta: $r"; done` — prints nothing (AC9: every series has at least one non-zero exact-delta assertion in an `expectClaimDelta` map; the `claimResults` name list alone does not count). If a name is reported, add the missing delta assertion to the relevant AC1–AC6 test.
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/claim/... ./pkg/metrics/... && go tool cover -func=/tmp/cover.out` — `DeriveClaimKey`, `validateClaimComponent`, `AuditMismatch` each exercised; ≥80% statement coverage for new code.
- `make precommit` — exits 0.

Do NOT run `docker`, `make build`, `make buca`, `kubectl`, `dark-factory`, `gh`, or `scripts/*.sh` commands in this prompt — those are operator-executable and belong on the spec's verification ladder.
</verification>

