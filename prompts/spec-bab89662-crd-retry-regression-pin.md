---
spec: ["bug-bab89662fe73a8bf4a0014d6f78791b3"]
status: draft
created: "2026-11-15T12:30:00Z"
---

# Pin the CRD conflict retry's re-Get contract

<summary>
- The CRD install path already retries a lost-update race, and that fix has been in the codebase and the changelog since the conflict was first observed — no production code changes here.
- A retry only helps because every attempt re-reads the CRD; today no test would notice if the read moved outside the retry loop and each attempt replayed a stale object.
- One existing test is extended so the read-write-read-write interleaving is asserted, not just the number of writes.
- A mutation that fetched once and retried only the write now fails a test instead of passing silently.
- A second test pins the other half of the contract: a write that fails for a reason a retry cannot fix surfaces after exactly one attempt, instead of being retried.
- Failure is still reported as a wrapped error naming the failed step, so the startup log keeps saying which operation broke.
- No behaviour changes, no new production symbol, no new test file, no cluster access.
- One changelog bullet records the added regression coverage.
</summary>

<objective>
Harden the regression coverage around the already-shipped `ensureCRD` conflict fix so that the property the fix actually depends on — each retry attempt re-reads the CRD and therefore writes with a fresh `resourceVersion` — is pinned by a test, and so that "retry conflicts only" is pinned against a retry-everything regression. Production code is not to change. The bug is the cluster-scoped CRD Get→Update race that made a losing executor crash on startup with a 409 during a concurrent rollout; the fix already prevents that, but only a test can keep a later refactor from silently undoing it.
</objective>

<context>
Read `pkg/k8s_connector.go` — in particular `ensureCRD` (it wraps a Get→Create/Get→Update sequence in `retry.RetryOnConflict(retry.DefaultRetry, ...)` from `k8s.io/client-go/util/retry`, re-`Get`s inside the closure on every attempt, and skips the `Update` entirely when `crdSpecEqual` reports the stored spec already matches), plus `desiredCRD` and `crdSpecEqual`. The doc comments on those functions state the two properties this prompt pins. `SetupCustomResourceDefinition` is the exported entry point the tests drive.

Read `pkg/k8s_connector_test.go` (package `pkg_test`, Ginkgo):
- `differingCRD()` — a stored CRD whose spec deliberately differs, so the update path is taken.
- `crdResource` — the `schema.GroupResource` used to construct an `apierrors.NewConflict`.
- `countVerb(actions, verb)` — the helper that counts actions by verb.
- the spec `"retries the update when the CRD was modified concurrently"` — the test to extend. It currently asserts only `conflicts == 1` and `countVerb(..., "update") == 2`, which a stale-object retry also satisfies.
- the spec `"returns a wrapped error when Get fails with a non-NotFound error"` — the existing precedent for a reactor returning a non-conflict API error and asserting the wrapped message.

Read `pkg/k8s_connector_internal_test.go` for the adjacent `crdSpecEqual` specs (do not duplicate them). The Ginkgo suite bootstrap lives in `pkg/suite_test.go` (`TestPkg`, package `pkg_test`) and serves both `package pkg` and `package pkg_test` test files in this directory — do not add another `RunSpecs`.

Read `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` for the external-test-package and Ginkgo conventions this repo follows, and `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` for the entry shape and conventional-prefix rule.

The fix itself is already recorded in `CHANGELOG.md` under the released heading `## v0.18.12` (the bullet beginning "a concurrent rollout of the executors no longer kills the losers…"). This prompt adds test coverage only; it must not restate or re-implement that fix. The current changelog has no `## Unreleased` section — the top heading is `## v0.62.2` — so requirement 3 creates one.
</context>

<requirements>
1. In `pkg/k8s_connector_test.go`, extend the existing spec `"retries the update when the CRD was modified concurrently"` (inside `Describe("SetupCustomResourceDefinition")`) with an assertion on the recorded action sequence, appended immediately after the existing `Expect(countVerb(cs.Actions(), "update")).To(Equal(2))` line. The fake clientset records an action even when a `PrependReactor` handles it, so the sequence is exactly `get`, `update`, `get`, `update`.

   Add, with a comment stating the mutation it catches (a `Get` hoisted out of the `RetryOnConflict` closure and retried only on the `Update` would keep the same two update actions and still pass without this):

   ```go
   // Each attempt must re-Get. RetryOnConflict alone does not make the retry
   // safe: a retry that replayed the object fetched before the loop would
   // re-send the same resourceVersion and lose the same race again. Pin the
   // interleaving, so hoisting the Get out of the closure fails here.
   Expect(countVerb(cs.Actions(), "get")).To(Equal(2))
   verbs := make([]string, 0, len(cs.Actions()))
   for _, action := range cs.Actions() {
       verbs = append(verbs, action.GetVerb())
   }
   Expect(verbs).To(Equal([]string{"get", "update", "get", "update"}))
   ```

   Do not change the reactor, the `conflicts` counter or any existing assertion in that spec.

2. Add a new spec `"does not retry an update that failed for a non-conflict reason"` in the same `Describe("SetupCustomResourceDefinition")` block, immediately after the extended conflict spec. It pins that `retry.RetryOnConflict` retries conflicts only: a retry that also re-ran on an internal error would repeat a write the API server has already rejected for a reason a retry cannot fix, and would hide the real failure behind a backoff.

   ```go
   It("does not retry an update that failed for a non-conflict reason", func() {
       cs := fake.NewSimpleClientset(differingCRD())

       updates := 0
       cs.PrependReactor(
           "update",
           "customresourcedefinitions",
           func(_ k8stesting.Action) (bool, runtime.Object, error) {
               updates++
               return true, nil, apierrors.NewInternalError(
                   stderrors.New("simulated internal error"),
               )
           },
       )

       connector := pkg.NewK8sConnector(&rest.Config{}, fakeBuilder(cs))
       err := connector.SetupCustomResourceDefinition(ctx)
       Expect(err).To(HaveOccurred())
       Expect(err).To(MatchError(ContainSubstring("update CRD")))
       Expect(updates).To(Equal(1))
   })
   ```

   `ctx` is set by the outer `Describe("K8sConnector")` `BeforeEach`. Every identifier used (`fake`, `rest`, `k8stesting`, `runtime`, `apierrors`, `stderrors`, `pkg`, `differingCRD`, `fakeBuilder`, `ctx`) is already imported or declared in this file — add no imports.

3. Add a `## Unreleased` section to `CHANGELOG.md` directly above the first `## v` heading (the preamble is `# Changelog`, a blank line, then the "All notable changes…" line; the first version heading is `## v0.62.2`). It does not exist yet — do not create a second one, and do not touch any released heading. Put exactly one bullet in it, in the file's existing voice (`- <prefix>: **bold summary.** prose`), using the `test:` prefix from the changelog guide:

   ```
   - test: **the CRD install path's conflict retry is pinned at the interleaving, not just the write count.** A retry only helps because each attempt re-reads `configs.agent.benjamin-borbe.de` and therefore writes a fresh `resourceVersion`; the previous assertion counted two `Update` actions, which a retry that replayed the object fetched before the loop would also satisfy. The spec now asserts `get, update, get, update`, and a second spec pins that an `Update` failing for a non-conflict reason surfaces as a wrapped `update CRD` error after exactly one attempt instead of being retried. No behaviour change.
   ```

4. Before finishing, re-run `<verification>` and confirm it passes; walk each numbered requirement against the diff.
</requirements>

<constraints>
- This prompt is test-and-changelog only. Do NOT modify `pkg/k8s_connector.go` or any other production file.
- Do NOT add handling for a concurrent `Create` (an `AlreadyExists` on the create path) and do NOT broaden the retry to non-conflict errors. Those would be behaviour changes outside this prompt's scope.
- Do NOT add a new test file and do NOT add another `RunSpecs`; the suite bootstrap is `pkg/suite_test.go`.
- Do NOT duplicate the `crdSpecEqual` specs in `pkg/k8s_connector_internal_test.go`.
- Do NOT attempt any cluster access or deployment. Nothing in this prompt is verifiable against a live cluster; deploying the already-shipped fix to a dev cluster is a separate operator action, not part of this change.
- Do NOT run `go mod vendor`, and never use `-mod=vendor`.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run from the repository root:

```bash
go test -mod=mod -count=1 ./pkg/
ROOTDIR=/workspace make precommit
grep -c '^## Unreleased$' CHANGELOG.md
awk '/^## /{sec=$0} /conflict retry is pinned at the interleaving/{print sec}' CHANGELOG.md
```

`make precommit` must pass. `grep -c '^## Unreleased$' CHANGELOG.md` must print `1`, and the `awk` line must print `## Unreleased` — together these prove the bullet landed in the unreleased section rather than under a released version.

Then confirm the pin is real: temporarily move the `crds.Get(...)` call in `ensureCRD` outside the `retry.RetryOnConflict` closure so the retried attempt reuses the first `existing` object, run `go test -mod=mod -count=1 ./pkg/`, and confirm the extended conflict spec fails. Revert the mutation and re-run `go test -mod=mod -count=1 ./pkg/` to green. The final `make precommit` must be run after the revert — a forgotten mutation makes the extended spec fail, so a green `make precommit` is itself the proof that the production file was restored.
</verification>
