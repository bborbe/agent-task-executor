---
status: completed
spec: [023-bug-park-writes-no-reason]
summary: 'Parked tasks now record a reason-bearing ## Parked section in the same frontmatter command, with claim key and previous holder on the lost-claim path'
execution_id: agent-task-executor-park-body-exec-063-bug-park-writes-no-reason
dark-factory-version: v0.198.17
created: "2026-11-09T18:36:00Z"
queued: "2026-11-09T17:54:51Z"
started: "2026-11-09T17:54:58Z"
completed: "2026-11-09T18:06:44Z"
---

# fix: record why a task was parked

<summary>
- A parked task records why it was parked, in its own body
- Both park paths carry the reason: a lost claim, and a succeeded job
- The lost-claim park also records the claim key and the previous holder
- The phase change and the reason are published in one command, so a record never shows the phase without the reason
- A retried publish overwrites the section rather than duplicating it
- Every other publisher's behaviour is unchanged
</summary>

<objective>
Every task the executor parks should explain itself. Today a park writes only `phase: human_review` and nothing else, so an operator cannot tell a deliberate hand-off from a stall and the queue scorer cannot excuse the record. Parked records must carry a `## Parked` section naming the reason and the evidence their path has.
</objective>

<context>
Read `pkg/result_publisher.go` — `PublishParkLostClaim`, `PublishParkSucceeded`, and `publishFailure`.
`publishFailure` is the exemplar: it already publishes `Updates` and `Body` in a single `taskcmd.UpdateFrontmatterCommand`. Follow that pattern rather than inventing one.
Read `pkg/handler/claim_wiring.go` — `acquireSpawnClaim` (the `TakeoverPair` call site) and `parkAfterLostClaim`.
Read `pkg/handler/park_succeeded.go` — `parkSucceededWithoutPhaseMove` and its `PublishParkSucceeded` call.
Read `pkg/claim/lease_manager.go` — `TakeoverResult` and `PreviousTaskID`.
Read `pkg/claim/key.go` — `DeriveClaimKeysWithBody` and the `claims/pr/...` / `claims/task/...` shapes.
Read `pkg/handler/task_event_handler_claim_test.go` — the takeover harness and its park assertion.
Read `docs/claim-lease-protocol.md` — the claim-key shapes and the park's place in the lifecycle.
</context>

<requirements>
1. In `pkg/result_publisher.go`, make `PublishParkLostClaim` publish one `taskcmd.UpdateFrontmatterCommand` carrying both `Updates` (the `phase` key as today) and `Body` (the new `## Parked` section). Use the same construction `publishFailure` uses in the same file. Do not add a second publish.

2. `PublishParkSucceeded` keeps delegating to `PublishParkLostClaim`, passing the succeeded path's own reason token (`parked_succeeded`) with an empty claim key and previous holder. The two paths share one write — the doc comment on `PublishParkSucceeded` explains why duplicating it is forbidden, and that reason still holds. The shared write therefore takes the reason as a parameter, and the succeeded path's section carries the reason line only: no claim-key line, no holder line.

3. The section's reason line carries the token the path already logs: `parked_succeeded` for the succeeded path, `parked_lost_claim` for the lost-claim path. The two paths must not share a constant reason.

4. On the lost-claim path the section also carries the claim key and the previous holder:
   - In `acquireSpawnClaim`, capture the `TakeoverResult` currently discarded at the `TakeoverPair` call site.
   - `parkAfterLostClaim` (`pkg/handler/claim_wiring.go:317`) already takes `key string` — the primary claim key computed in `acquireSpawnClaim` — so widen it only with the previous holder, and pass `key` and that holder through to the publisher.
   - Widen `PublishParkLostClaim` to `(ctx context.Context, task lib.Task, reason string, claimKey string, previousHolder string) error`, and widen the `ResultPublisher` interface declaration to match. `reason` is the path's token from requirement 3; `claimKey` and `previousHolder` are empty on the succeeded path.
   - The claim key is the primary claim key already computed at that call site; render it verbatim, whatever `DeriveClaimKeysWithBody` returns — `claims/pr/<owner>/<repo>/<n>`, `claims/task/<taskid>`, or `claims/repo/<owner>/<repo>`.
   - When the lost-claim path's `PreviousTaskID` is empty (key absent, create used), its section writes the literal `unavailable` rather than an empty field. The succeeded path has no holder at all: its section carries the reason line only, with no claim-key or holder line.

5. The section is a fixed template: heading `## Parked`, at most 8 lines, ending with a newline, no log tail and no echo of the task body. Count with `strings.Count(Body.Section, "\n") <= 8`, which matches the required trailing newline. The shape is the library's contract, not a new convention — `github.com/bborbe/agent/command/task/update-frontmatter-command.go:34-38`: *"Heading MUST include the markdown prefix … Section MUST include the heading as its first line and a trailing newline."*

6. Update every doc comment that currently states the park write is frontmatter-only — the `ResultPublisher` interface declarations at `pkg/result_publisher.go:176-180` and `:182-188`, and both park implementations at `:1027` and `:1042`. A stale contract comment here re-creates this bug class.

7. Regenerate the counterfeiter mock for `ResultPublisher` with the repo's existing generate target. `make precommit` depends on `generate` (`Makefile.precommit:5`), which runs `rm -rf mocks avro` and then `go generate -mod=mod ./...` — so `<verification>` deletes and rewrites the mock from the widened interface. Never hand-edit `mocks/result_publisher.go`; the generate target discards it. Then update the three hand-written doubles that implement the interface so they compile — `pkg/handler/claim_spawn_order_internal_test.go:125` (`stubResultPublisher`), `pkg/handler/claim_wiring_internal_test.go:319` (`parkRecordingPublisher`), `pkg/job_watcher_internal_test.go:557` (`fakeResultPublisher`). Change nothing those doubles assert. Also update the destructuring at `pkg/handler/task_event_handler_claim_test.go:520` (`_, parkedTask := harness.publisher.PublishParkLostClaimArgsForCall(0)`) for the widened return values — that is a compile fix, not an assertion change.

8. Tests:
   - Add a `Describe("PublishParkLostClaim")` block to `pkg/result_publisher_test.go` asserting, on the decoded `taskcmd.UpdateFrontmatterCommand`: `Body.Heading` is `## Parked`; `Body.Section` has prefix `## Parked` and suffix `"\n"`; the section contains the path's reason token; the section is at most 8 lines.
   - Assert the claim key **by value**: derive the expected key in the test with `claim.DeriveClaimKeysWithBody` over the same fixture body, pass that derived key to `PublishParkLostClaim`, and assert `Body.Section` contains that exact string — not a regex, and not a literal typed into the test. Cover both shapes: `claims/pr/<owner>/<repo>/<n>` from a body carrying a PR URL, and the `claims/task/<taskid>` fallback.
   - Cover both holder cases: `PreviousTaskID` set (the section carries it) and empty (the section carries `unavailable`).
   - Extend the takeover spec in `pkg/handler/task_event_handler_claim_test.go` (the one asserting `PublishParkLostClaimCallCount()` at ~line 519) to assert the park call's arguments: the reason is `parked_lost_claim`, the claim key equals the key derived for that fixture, and the previous holder equals the `TaskID` that `listedClaimRef` puts on the expired claim. The publisher test alone cannot catch a swapped or wrong-field argument at the `parkAfterLostClaim` call site.
   - Flip the one existing assertion at `pkg/result_publisher_test.go:1116` inside `Describe("PublishParkSucceeded")` from `Expect(cmd.Body).To(BeNil())` to assert the `## Parked` heading **and that its section carries no claim-key line and no `unavailable` holder line** (requirement 2). That assertion currently encodes this bug. Rename that `It` to match what it now asserts (e.g. `writes the phase key and a ## Parked section`); its current title would contradict its own body.

9. Document the two park paths and the `## Parked` section shape in `docs/claim-lease-protocol.md`, so the contract outlives the code comment.

10. Before finishing, re-run `<verification>` and confirm it passes; walk each numbered requirement against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT change `taskShowsPriorRun` in `pkg/handler/claim_wiring.go`. Whether its `job_started_at` evidence is too weak is a separate question; narrowing it here would change parking behaviour beyond this fix.
- Exactly one existing assertion may change: `pkg/result_publisher_test.go:1116`. The other four `Expect(cmd.Body).To(BeNil())` assertions — `pkg/result_publisher_test.go:936`, `:1170`, `:1448`, and `pkg/result_publisher_log_tail_test.go:182` — belong to non-park publishers and must stay green unchanged. Compile fixes required by the widened signature (requirement 7) are not assertion changes.
- No new Config field, no new CRD knob, no feature flag.
- Do NOT run `go mod vendor`, and never use `-mod=vendor`.
- `pkg/handler/claim_wiring_internal_test.go` is plain `testing`, not Ginkgo — internal-package Ginkgo specs are never discovered by the external suite in this repo.
- Do NOT register the section in any vault-side script; that work is out of scope for this repository.
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` — must pass.
</verification>
