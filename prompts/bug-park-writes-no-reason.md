---
spec: ["023-bug-park-writes-no-reason"]
status: draft
created: "2026-11-09T17:56:00Z"
---

# fix: record why a task was parked

<summary>
- A parked task now states why it was parked, inside its own body
- Both park paths carry the reason: a lost claim and a succeeded job whose phase never moved
- The lost-claim park also records the claim key the takeover addressed and the task id of the holder it displaced
- The phase change and the reason are published in one command, so a record never shows the phase without the reason
- A retried publish overwrites the section in place instead of appending a second copy
- The section is a fixed template of at most five lines with no timestamp and no task content, so the same inputs always produce the same bytes
- Every other publisher's behaviour is unchanged; exactly one existing assertion flips because it encoded this bug
- The new publisher specs go in a new test file, so the already-long publisher test file does not cross the repo's 2000-line limit
- The claim-lease protocol doc gains the section's shape, so the contract outlives the code comment
- The change is recorded in the changelog
- The post-deploy check against a real parked record is the spec's operator ladder, not this prompt
</summary>

<objective>
Every task the executor parks must explain itself. Today both park paths write only `phase: human_review`, so an operator cannot tell a deliberate hand-off from a stall and the queue scorer cannot excuse the record. Make the shared park write also publish a `## Parked` body section naming the reason and, on the lost-claim path, the claim key and the previous holder — in the same `UpdateFrontmatterCommand` that carries the phase, so the two can never land apart.
</objective>

<context>
Read `CLAUDE.md` if present, and `README.md`, for project conventions.

Read these files in full before changing anything:

- `pkg/result_publisher.go` (about 1182 lines on a clean baseline). The relevant parts are:
  - the `ResultPublisher` interface — the two park methods are declared near the end of the interface block;
  - `publishFailure` — the exemplar: it builds a `section` string, then publishes one `taskcmd.UpdateFrontmatterCommand` carrying both `Updates` and `Body: &taskcmd.BodySection{Heading: ..., Section: section}`. Follow this shape, do not invent another;
  - `PublishTriggerBudgetRefusal` — the same shape with a fixed heading and `- **Field:** value` bullet lines;
  - `PublishParkLostClaim` and `PublishParkSucceeded` — the two methods to change.
- `pkg/handler/claim_wiring.go` — `acquireSpawnClaim`, the `TakeoverPair` call (`_, takeoverErr := manager.TakeoverPair(...)`, result today discarded), the park branch, `taskShowsPriorRun`, and `parkAfterLostClaim`.
- `pkg/handler/park_succeeded.go` — `parkSucceededWithoutPhaseMove` and its `PublishParkSucceeded` call. This file needs no change.
- `pkg/claim/lease_manager.go` — `TakeoverResult` with field `PreviousTaskID string`, and `TakeoverPair`, which populates `PreviousTaskID` from the listed (displaced) claim, or `""` when the key was absent and a create was used.
- `pkg/claim/key.go` — `DeriveClaimKey` and `DeriveClaimKeysWithBody`. The latter returns `claims/pr/<owner>/<repo>/<n>` when the body has a usable PR URL, else `claims/task/<taskid>` with `fallback == true`.
- `pkg/result_publisher_test.go` — the `ResultPublisher` suite, `decodeUpdateFrontmatterCommand`, `capturingSyncProducer`, and `Describe("PublishParkSucceeded")` with the assertion to flip. **This file is about 1920 lines on a clean baseline and the repo's revive `file-length-limit` is 2000.** Do not add new specs to it; the new specs go in a new file (requirement 8).
- `pkg/handler/task_event_handler_claim_test.go` — `claimHarness`, `qualifyingClaimTask`, `listedClaimRef` (which sets `TaskID: "previous-task"`), `claimKeyFor`, and the `Describe("claim gate")` block containing `It("AC6: parks instead of spawning when rerunOnLostClaim is false")`.
- `pkg/handler/claim_wiring_internal_test.go` — `parkRecordingPublisher`. Plain `testing`, not Ginkgo; keep it that way.
- `pkg/handler/claim_spawn_order_internal_test.go` — `stubResultPublisher` (implements the park method).
- `pkg/job_watcher_internal_test.go` — `fakeResultPublisher` (implements the park method).
- `docs/claim-lease-protocol.md` — the `## Takeover` section, immediately followed by `## Restart recovery`. The new doc section goes between them.

Coding-plugin guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo v2/Gomega and Counterfeiter conventions.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `github.com/bborbe/errors` (`errors.Wrapf(ctx, err, ...)`), never `fmt.Errorf`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — the `## Unreleased` bullet format.

Verified facts (do not re-derive):
- `taskcmd.UpdateFrontmatterCommand` (`github.com/bborbe/agent` v0.113.0, `command/task`) is:
  ```go
  type UpdateFrontmatterCommand struct {
      TaskIdentifier lib.TaskIdentifier  `json:"taskIdentifier"`
      Updates        lib.TaskFrontmatter `json:"updates"`
      Body           *BodySection        `json:"body,omitempty"`
      TargetVault    string              `json:"targetVault,omitempty"`
  }
  ```
  and `BodySection` is:
  ```go
  type BodySection struct {
      Heading string `json:"heading"`
      Section string `json:"section"`
  }
  ```
  Its doc comment states: *"BodySection describes an idempotent body-section write for UpdateFrontmatterCommand. Heading MUST include the markdown prefix (e.g. \"## Failure\"). Section MUST include the heading as its first line and a trailing newline."*
- `UpdateFrontmatterCommand.Validate(ctx)` only checks a non-empty `TaskIdentifier`, a valid `TargetVault`, and that at least one of `Updates`/`Body` is set. It does NOT enforce the heading/section convention, so the specs must assert that convention explicitly.
- `mocks/result_publisher.go` is generated by the `//counterfeiter:generate` directive in `pkg/result_publisher.go`. `make precommit`'s `generate` target runs `rm -rf mocks avro` then `go generate -mod=mod ./...`; there is no `avro/` directory in this repo, so that target is safe to run.
- The `pkg` Ginkgo suite entry point is `TestPkg` in `pkg/suite_test.go`; the handler suite entry point is `TestHandler` in `pkg/handler/task_event_handler_test.go`. `-run` filters Go test functions, NOT spec names — to select specs use `-args -ginkgo.focus='...'`.
- `claim.DeriveClaimKeysWithBody(ctx, taskType, taskID, owner, repo, body)` requires a line in `body` that is **exactly** `https://github.com/<owner>/<repo>/pull/<n>` after trimming whitespace (`pullRequestNumberFromLine`). A URL embedded in a sentence does not match; put it on its own line.
- `qualifyingClaimTask()` carries neither `repo` nor `clone_url` and no `task_type`, so its derived claim key is `claims/task/tid-claim-off-1` — the same value `claimKeyFor` computes.
- In the AC6 fixture, `harness.client.ListReturns([]claim.ClaimRef{listedClaimRef(key, "expired-sha", ...)})` makes `TakeoverPair` return a non-nil `TakeoverResult` with `PreviousTaskID == "previous-task"`.
- `pkg/result_publisher_test.go` already imports `github.com/bborbe/agent-task-executor/pkg/claim`, `taskcmd`, `libtime`, `libtimetest`, `base`, and `pkg` — a new file in the same `pkg_test` package may use the same package-level helpers (`capturingSyncProducer`, `decodeUpdateFrontmatterCommand`).
- `fmt.Fprintf` is in the repo's `errcheck` `exclude-functions` list, so ignoring its two return values is allowed.
- On a clean baseline, four assertions require `cmd.Body` to be nil: three in `pkg/result_publisher_test.go` (one inside `Describe("PublishClearCurrentJob")`, one inside `Describe("PublishRestoreAssignee")`, one inside `Describe("PublishAbort")`) and one in `pkg/result_publisher_log_tail_test.go` (inside `PublishClearCurrentJob`). Only the `PublishParkSucceeded` one may change. Two of the four carry an explanatory message argument, so do not verify them with a single exact-pattern count.
</context>

<requirements>
1. **Add the park constants and the shared payload builder to `pkg/result_publisher.go`.** Add this const block immediately above `PublishParkLostClaim`:
   ```go
   // The park write's fixed vocabulary. The heading is constant so the
   // controller's heading-keyed write replaces the section in place on a retry
   // rather than appending a second copy.
   const (
       parkHeading         = "## Parked"
       parkReasonSucceeded = "parked_succeeded"
       parkReasonLostClaim = "parked_lost_claim"
   )
   ```
   Add the unexported builder:
   ```go
   // parkBodySection builds the park body section. It is a fixed template: no
   // timestamp, no job name, no echo of the task body, so the same inputs
   // always produce the same bytes. The claim-key and previous-holder lines are
   // written only when a claim key is supplied — the succeeded path has no claim
   // to name.
   func parkBodySection(reason string, claimKey string, previousHolder string) *taskcmd.BodySection {
       var b strings.Builder
       fmt.Fprintf(&b, "%s\n\n- **Reason:** %s\n", parkHeading, reason)
       if claimKey != "" {
           holder := previousHolder
           if holder == "" {
               holder = "unavailable"
           }
           fmt.Fprintf(
               &b,
               "- **Claim key:** %s\n- **Previous holder:** %s\n",
               claimKey,
               holder,
           )
       }
       return &taskcmd.BodySection{
           Heading: parkHeading,
           Section: b.String(),
       }
   }
   ```
   The lost-claim section is five lines (heading, blank, reason, claim key, previous holder); the succeeded section is three. Both end with `"\n"` and begin with `## Parked`.

2. **Replace the two park implementations with one shared write.** `PublishParkLostClaim` gains two parameters and `PublishParkSucceeded` keeps its signature. Both delegate to a new unexported `publishPark`:
   ```go
   func (p *resultPublisher) PublishParkLostClaim(
       ctx context.Context,
       task lib.Task,
       claimKey string,
       previousHolder string,
   ) error {
       return p.publishPark(ctx, task, parkReasonLostClaim, claimKey, previousHolder)
   }

   func (p *resultPublisher) PublishParkSucceeded(
       ctx context.Context,
       task lib.Task,
   ) error {
       return p.publishPark(ctx, task, parkReasonSucceeded, "", "")
   }

   func (p *resultPublisher) publishPark(
       ctx context.Context,
       task lib.Task,
       reason string,
       claimKey string,
       previousHolder string,
   ) error {
       cmd := taskcmd.UpdateFrontmatterCommand{
           TaskIdentifier: task.TaskIdentifier,
           TargetVault:    targetVaultFromTask(task),
           Updates: lib.TaskFrontmatter{
               "phase": string(domain.TaskPhaseHumanReview),
           },
           Body: parkBodySection(reason, claimKey, previousHolder),
       }
       if err := p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd); err != nil {
           return errors.Wrapf(ctx, err, "publish park update for task %s", task.TaskIdentifier)
       }
       return nil
   }
   ```
   `Updates` still carries the `phase` key and nothing else. The phase and the body travel in one command, so there is no second publish and no window in which a record reads `human_review` with no reason.

3. **Why the succeeded path no longer delegates to `PublishParkLostClaim` directly.** The two parks still share exactly one write — `publishPark` — which is the invariant the old doc comment protected. The reason token is now chosen by which exported method was called, so a caller cannot pass the wrong reason and the `ResultPublisher` interface does not gain a swap-prone reason string. Update the `PublishParkSucceeded` doc comment to say this in as many words, keeping its original point that duplicating the payload would let a future change drift between the paths.

4. **Update every doc comment that says the park write is frontmatter-only.** Rewrite the two interface declarations and the two implementation comments so each states: it writes phase `human_review` AND a `## Parked` body section in one command; the section names the reason; on the lost-claim path it also names the claim key and the previous holder (or `unavailable`). A stale contract comment here re-creates this bug class. Declare the widened interface method across multiple lines so no line exceeds 100 characters:
   ```go
   PublishParkLostClaim(
       ctx context.Context,
       task lib.Task,
       claimKey string,
       previousHolder string,
   ) error
   ```

5. **Capture the takeover result and widen the lost-claim call site in `pkg/handler/claim_wiring.go`.**
   - Change `_, takeoverErr := manager.TakeoverPair(` to `takeoverResult, takeoverErr := manager.TakeoverPair(`. `takeoverResult` is non-nil exactly when `takeoverErr == nil`.
   - In the park branch, pass the previous holder: `h.parkAfterLostClaim(ctx, primary, task, takeoverResult.PreviousTaskID)`.
   - Widen `parkAfterLostClaim` to `func (h *taskEventHandler) parkAfterLostClaim(ctx context.Context, key string, task lib.Task, previousHolder string)`. Inside the retry loop, publish with `h.resultPublisher.PublishParkLostClaim(ctx, task, key, previousHolder)`. `key` is the primary claim key already computed in `acquireSpawnClaim`; render it verbatim — it is already either the `claims/pr/<owner>/<repo>/<n>` shape or the `claims/task/<taskid>` fallback.
   - Keep every existing log line, metric increment, retry count, cancellation arm and failure arm exactly as they are. This change adds arguments; it does not alter when a park happens or what it logs.

6. **Regenerate the Counterfeiter mock and fix the three hand-written doubles.** After the interface change, run `go generate -mod=mod ./pkg/...` to refresh `mocks/result_publisher.go` (`make precommit` runs the same generation). Then update these three doubles so they compile, changing nothing they assert:
   - `pkg/handler/claim_spawn_order_internal_test.go` (`stubResultPublisher`) — add the two parameters, return `nil`.
   - `pkg/handler/claim_wiring_internal_test.go` (`parkRecordingPublisher`) — add the two parameters and keep recording only `task.TaskIdentifier` into `s.parked`, so the existing `len(publisher.parked)` assertions stay valid.
   - `pkg/job_watcher_internal_test.go` (`fakeResultPublisher`) — add the two parameters, return `nil`.
   Do not convert `claim_wiring_internal_test.go` from plain `testing` to Ginkgo.

7. **Flip the one assertion that encodes this bug, in `pkg/result_publisher_test.go`.** In `Describe("PublishParkSucceeded")`, rename the `It` from `"writes exactly the phase key with human_review and nothing else"` to `"writes the phase key and a ## Parked section naming the succeeded reason"`, and replace the `Expect(cmd.Body).To(BeNil())` line with:
   ```go
   Expect(cmd.Body).NotTo(BeNil())
   Expect(cmd.Body.Heading).To(Equal("## Parked"))
   Expect(cmd.Body.Section).To(HavePrefix("## Parked"))
   Expect(cmd.Body.Section).To(HaveSuffix("\n"))
   Expect(cmd.Body.Section).To(ContainSubstring("parked_succeeded"))
   ```
   Also update the stale comment above the fixture (currently *"The park's whole effect is this one key"*) so it describes the phase key plus the section. Leave `Expect(cmd.Updates).To(HaveLen(1))`, the `status`/`assignee`/`current_job` absence loop, and `Expect(cmd.Validate(ctx)).To(Succeed())` untouched.

8. **Add the lost-claim specs in a new file `pkg/result_publisher_park_test.go`** (`package pkg_test`, BSD license header matching the other files). Do not grow `pkg/result_publisher_test.go` past 2000 lines. The file declares a package-level helper and one Ginkgo tree with the specs nested under a `Describe("PublishParkLostClaim")` node, so a `-ginkgo.focus='PublishParkLostClaim'` run selects them:
   ```go
   var _ = Describe("Park publishers", func() {
       Describe("PublishParkLostClaim", func() {
           // BeforeEach + the four specs below
       })
   })
   ```
   The helper:
   ```go
   func newParkTestPublisher(producer *capturingSyncProducer) pkg.ResultPublisher {
       return pkg.NewResultPublisher(
           producer,
           base.TopicPrefix("develop"),
           libtime.NewCurrentDateTime(),
           nil,
           nil,
       )
   }
   ```
   (the park write reads no clock, so no `SetNow` is needed). Use a `BeforeEach` that builds `ctx`, `producer` and `publisher` from that helper. Add these specs:
   - **PR-shaped key, by value.** Fixture: `TaskIdentifier: "tid-park-pr"`, `Frontmatter: {"task_type": "pr-review", "repo": "bborbe/nuke", "target_vault": "work"}`, `Content` containing the line `https://github.com/bborbe/nuke/pull/42` on its own line. Derive the expected key in the spec:
     ```go
     key, _, fallback, err := claim.DeriveClaimKeysWithBody(
         ctx, "pr-review", "tid-park-pr", "bborbe", "nuke", string(task.Content),
     )
     Expect(err).NotTo(HaveOccurred())
     Expect(fallback).To(BeFalse())
     Expect(key).To(Equal("claims/pr/bborbe/nuke/42"))
     ```
     Call `publisher.PublishParkLostClaim(ctx, task, key, "previous-task")`. Assert on the decoded command: exactly one message; `operation == taskcmd.UpdateFrontmatterCommandOperation`; `cmd.TaskIdentifier`; `cmd.TargetVault == "work"`; `cmd.Updates` has length 1 and `cmd.Updates["phase"] == "human_review"`; `cmd.Body.Heading == "## Parked"`; `cmd.Body.Section` has prefix `## Parked`, suffix `"\n"`, contains `parked_lost_claim`, contains the derived `key` exactly, and contains `previous-task`; the section is at most 8 lines, counted as `len(strings.Split(strings.TrimSuffix(cmd.Body.Section, "\n"), "\n"))`; `cmd.Validate(ctx)` succeeds.
   - **Fallback key, by value.** Same shape but `Content` carries no PR URL. Derive with `claim.DeriveClaimKeysWithBody` and assert `fallback` is true and the key equals `"claims/task/tid-park-pr"`; then assert the published section contains that exact derived key.
   - **Empty previous holder.** Call `publisher.PublishParkLostClaim(ctx, task, key, "")` and assert the section contains `ContainSubstring("- **Previous holder:** unavailable")`.
   - **Same heading on both paths (idempotency by construction).** Publish once with `PublishParkLostClaim` and once with `PublishParkSucceeded` against two producers (or reset `producer.messages`), decode both, and assert `lostClaimCmd.Body.Heading == succeededCmd.Body.Heading == "## Parked"`. This is the heading-keyed write's idempotency condition; no dedupe code is added anywhere.

9. **Assert the arguments at the lost-claim call site, in the AC6 spec of `pkg/handler/task_event_handler_claim_test.go`.** The publisher specs cannot catch a swapped or wrong-field argument at `parkAfterLostClaim`. In `It("AC6: parks instead of spawning when rerunOnLostClaim is false")`, replace the two-value destructuring of `PublishParkLostClaimArgsForCall(0)` with the four-value one and assert by value:
   ```go
   _, parkedTask, parkedKey, parkedHolder := harness.publisher.PublishParkLostClaimArgsForCall(0)
   Expect(parkedTask.TaskIdentifier).To(Equal(task.TaskIdentifier))
   expectedKey, _, _, err := claim.DeriveClaimKeysWithBody(
       context.Background(),
       string(task.Frontmatter.TaskType()),
       string(task.TaskIdentifier),
       "",
       "",
       string(task.Content),
   )
   Expect(err).NotTo(HaveOccurred())
   Expect(parkedKey).To(Equal(expectedKey))
   Expect(parkedKey).To(Equal(claimKeyFor(task)))
   Expect(parkedHolder).To(Equal("previous-task"))
   ```
   The empty owner/repo are correct for this fixture: `qualifyingClaimTask()` carries neither `repo` nor `clone_url`. Do not change anything else in that spec — the spawn count, the claim deltas and the park call count stay as they are.

10. **Document the section in `docs/claim-lease-protocol.md`.** Insert a new `## Parking` section between `## Takeover` and `## Restart recovery`. It must state:
    - the two park paths: a lost claim (this executor won a takeover, the Config's `rerunOnLostClaim` forbids a second run, and the task shows prior-run evidence) and a succeeded Job whose phase never moved;
    - that both publish one `UpdateFrontmatterCommand` carrying `phase: human_review` and a `## Parked` body section in the same command, so a record never reads the phase without its reason;
    - the section shape: the `## Parked` heading, a `- **Reason:**` line carrying `parked_lost_claim` or `parked_succeeded`, and — on the lost-claim path only — a `- **Claim key:**` line (the key the takeover addressed, either `claims/pr/<owner>/<repo>/<n>` or the `claims/task/<task id>` fallback) and a `- **Previous holder:**` line carrying the displaced claim's task id, or `unavailable` when the takeover created the key instead of replacing one;
    - that the write is heading-keyed, so a retried publish replaces the section in place and a record never accumulates a second one;
    - why it exists: a park is a hand-off, and a bodyless park is indistinguishable from a stall.
    Do not edit any other part of the file.

11. **Add a changelog entry.** If `CHANGELOG.md` has no `## Unreleased` section, add one at the top (below the introductory paragraph) with a single `fix:` bullet that names the behaviour, not the files. Follow `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md`: one bullet per logical change, prefix required, be specific.

12. **Self-check before finishing.** Re-run every command in `<verification>` and confirm each passes. Walk requirements 1-11 against the diff and confirm each is satisfied. Confirm that no publisher other than the two park methods changed behaviour, and that the four non-park `cmd.Body`-is-nil assertions still pass unchanged.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT change `taskShowsPriorRun` in `pkg/handler/claim_wiring.go`. Whether its `job_started_at` evidence is too weak is a separate question; narrowing it here would change parking behaviour beyond this fix.
- Exactly one existing assertion may change: the `Expect(cmd.Body).To(BeNil())` inside `Describe("PublishParkSucceeded")` in `pkg/result_publisher_test.go`. The other four `cmd.Body`-is-nil assertions — three elsewhere in `pkg/result_publisher_test.go` and one in `pkg/result_publisher_log_tail_test.go` — belong to non-park publishers and must stay green unchanged. Compile fixes required by the widened signature (requirement 6) are not assertion changes.
- The two parks must keep sharing one payload builder. Do not split the section into two implementations to give the succeeded path a smaller one — `publishPark` plus `parkBodySection` is the single entry point, and a future park path inherits the section by using it.
- The section carries no timestamp, no job name, no log tail and no echo of the task body. It is a pure function of `(reason, claimKey, previousHolder)`.
- No new Config field, no new CRD knob, no feature flag, no new metric.
- Do NOT register the `## Parked` shape in any vault-side script (`pr-review-queue-sampler.py` and its `PARK_NOTICES` list). That work is out of scope for this repository.
- `pkg/handler/park_succeeded.go` and `taskShowsPriorRun` behaviour are unchanged.
- Do not convert `pkg/handler/claim_wiring_internal_test.go` from plain `testing` to Ginkgo.
- `pkg/result_publisher.go` must stay at or below 2000 lines (revive `file-length-limit`); it is about 1182 on a clean baseline, so this change fits. `pkg/result_publisher_test.go` is about 1920 lines on a clean baseline and must not grow — new publisher specs go in `pkg/result_publisher_park_test.go`.
- Wrap errors with `github.com/bborbe/errors` (`errors.Wrapf(ctx, err, ...)`), never `fmt.Errorf`. Respect the 100-character line limit (`golines --max-len=100`) and the `funlen` (80 lines / 50 statements), `gocognit` (20) and `nestif` (4) limits.
- Use `-mod=mod` as the Makefile does. Do not run `go mod vendor`, do not create a `vendor/` directory, and never use `-mod=vendor`.
- Existing tests must still pass.
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit`. It must exit 0. This also regenerates `mocks/result_publisher.go` and runs the full test suite.

Run `go test ./pkg/ -run TestPkg -v -args -ginkgo.focus='PublishParkLostClaim' > /tmp/park-pub.out 2>&1; grep -q 'SUCCESS!' /tmp/park-pub.out && grep -qE 'Ran [3-9] of' /tmp/park-pub.out && echo PARK_PUB_OK`. It must print `PARK_PUB_OK`. The count fails the command when fewer than the four lost-claim specs are discovered.

Run `go test ./pkg/handler/ -run 'Park' -v > /tmp/park-handler.out 2>&1; grep -q '^ok' /tmp/park-handler.out && grep -q 'TestAcquireSpawnClaimParksOnLostClaimWhenRerunDisabled' /tmp/park-handler.out && echo PARK_HANDLER_OK`. It must print `PARK_HANDLER_OK`.

Run `go test ./pkg/handler/ -run TestHandler -v -args -ginkgo.focus='parks instead of spawning' > /tmp/park-ac6.out 2>&1; grep -q 'SUCCESS!' /tmp/park-ac6.out && grep -q 'Ran 1 of' /tmp/park-ac6.out && echo PARK_AC6_OK`. It must print `PARK_AC6_OK`.

Run `test -f pkg/result_publisher_park_test.go && echo PARK_TEST_FILE_OK`. It must print `PARK_TEST_FILE_OK`.

Run `grep -c 'func parkBodySection' pkg/result_publisher.go`. It must print `1`.

Run `grep -c 'PublishParkLostClaim(' pkg/handler/claim_wiring.go`. It must print `1`.

Run `grep -q 'Describe("PublishParkSucceeded"' pkg/result_publisher_test.go && grep -q 'parked_succeeded' pkg/result_publisher_test.go && ! grep -q 'writes exactly the phase key with human_review and nothing else' pkg/result_publisher_test.go && echo SUCCEEDED_ASSERT_OK`. It must print `SUCCEEDED_ASSERT_OK`.

Run `grep -c 'cmd.Body).To(BeNil()' pkg/result_publisher_log_tail_test.go`. It must print `1`.

Run `! grep -q 'PublishParkLostClaim(context.Context, lib.Task) error' pkg/handler/*_internal_test.go && echo SIGNATURES_OK`. It must print `SIGNATURES_OK`.

Run `awk '/^## /{sec=$0} /^- fix: .*[Pp]ark/{print sec; exit}' CHANGELOG.md`. It must print `## Unreleased`.

Run `grep -c '^## Parking' docs/claim-lease-protocol.md`. It must print `1`.

Run `test "$(wc -l < pkg/result_publisher.go)" -le 2000 && test "$(wc -l < pkg/result_publisher_test.go)" -le 2000 && echo LEN_OK`. It must print `LEN_OK`.
</verification>
