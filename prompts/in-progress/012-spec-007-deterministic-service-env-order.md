---
status: executing
spec: [007-bug-service-env-order-rolls-pod-every-minute]
execution_id: agent-task-executor-envorder-exec-012-spec-007-deterministic-service-env-order
dark-factory-version: v0.196.0
created: "2026-10-01T21:40:01Z"
queued: "2026-10-01T21:55:14Z"
started: "2026-10-01T21:55:15Z"
branch: dark-factory/bug-service-env-order-rolls-pod-every-minute
---

<summary>
- An unchanged service Config now renders a byte-identical StatefulSet on every reconcile pass, so the pod stops rolling once a minute.
- A service agent's container `env` list is emitted in a deterministic, key-sorted order instead of whatever order Go's map iteration happens to produce.
- The `env` contents are unchanged: the same `AGENT_TYPE` stamp and the same Config-declared key/value pairs, only the ordering is fixed.
- A genuine Config change still rolls the pod — the fix stabilises the render, it does not suppress updates.
- A new spec builds the service env 50 times in one process and asserts exactly one distinct ordering is observed — and that it is the sorted one — so a future reintroduction of map-order churn fails the suite.
- A new spec asserts two consecutive reconciles produce `env` slices equal element-for-element in the same index order (not set-equal, which is what the existing `ContainElement` assertions check).
- A new spec covers an env map larger than one Go map bucket (20 entries), and another reconciles two Configs sharing a key set side by side, proving the ordering does not depend on the single-bucket case and that each Config renders independently of the other.
- New specs reconcile twice with a changed image, and twice with a changed env value, asserting each difference still reaches the rendered template while the key order stays stable — so the fix stabilises the render rather than suppressing updates.
- The Job path is explicitly left untouched — its identical latent pattern is out of scope and gets a separate follow-up.
- A `## Unreleased` changelog entry records the fix.
</summary>

<objective>
Make `buildServiceEnvBuilder` in `/workspace/pkg/spawner/service_reconciler.go` emit a service container's env in a deterministic, key-sorted order so that an unchanged Config produces a byte-identical StatefulSet on every reconcile pass. Today the function ranges over `resolved.Env` (a `map[string]string`), so Go randomises the iteration order and the rendered `env` slice differs every pass; the pod template therefore changes, `metadata.generation` advances, and the StatefulSet controller rolls the pod roughly once a minute on a Config nobody has edited. Lock the new behaviour with specs that fail if two orderings ever appear.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for the container agent's own execution rules (Ginkgo/Gomega v2, external `package spawner_test` test package, `github.com/bborbe/errors` wrapping, counterfeiter mocks, glog `V(n)` gating, coverage ≥80% for new code). This repo has no project-level `CLAUDE.md`.

Read these files fully before changing anything:
- `/workspace/pkg/spawner/service_reconciler.go` — the whole file. `buildServiceEnvBuilder` (line ~293) is the only function you change. Note its caller at line ~194 (`containerBuilder.SetEnvBuilder(buildServiceEnvBuilder(resolved))`) and the interface doc at lines ~69-71 that already promises "an unchanged Config is a no-op" — the fix makes the implementation honour that contract.
- `/workspace/pkg/spawner/service_reconciler_test.go` — the whole file. The existing `getStatefulSet` closure (line ~63) is the read-back helper; the existing `It("is idempotent — a second reconcile with the same config does not error")` (line ~173) only asserts no error and does NOT check ordering; the existing `It("rolls the pod when the image changes")` (line ~181) must keep passing.
- `/workspace/pkg/spawner/job_spawner.go` — read `buildJobEnvBuilder` (line ~545). It contains the same `for key, value := range config.Env` pattern. It is **out of scope** and must NOT be changed; you read it only to confirm you are not accidentally editing the Job path.
- `/workspace/pkg/agent_configuration.go` — `AgentConfiguration.Env` is `map[string]string` (line ~47); this is the map whose iteration order is being stabilised.
- `/workspace/pkg/spawner/spawner_suite_test.go` — the Ginkgo suite entry point (`RunSpecs(t, "Spawner Suite")`); your new specs run under it, no new suite file is needed.

Library source (pinned, do NOT modify) — read to confirm the ordering is entirely the caller's responsibility. These files exist only once the module is downloaded; if a path is absent, run `go mod download` first:
- `/home/node/go/pkg/mod/github.com/bborbe/k8s@v1.14.16/k8s_env-builder.go` — `EnvBuilder.Add(name, value string) EnvBuilder` appends in call order; `Build(ctx context.Context) ([]corev1.EnvVar, error)` returns the slice as-is. There is no sorting inside the library.
- `/home/node/go/pkg/mod/github.com/bborbe/k8s@v1.14.16/k8s_container-builder.go` — `Build` assigns `Env: envVars` directly from the env builder; no reordering.
- `/home/node/go/pkg/mod/github.com/bborbe/k8s@v1.14.16/k8s_statefulset-deployer.go` — the update path does `existing.Spec.Template = statefulSet.Spec.Template`, so the env order you build is the env order the fake client stores and returns.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2, external test packages, coverage ≥80%.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for changed code.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` format and prefix rules.
</context>

<requirements>
1. Fix `/workspace/pkg/spawner/service_reconciler.go` by adding `"sort"` to the stdlib import group and replacing the body of `buildServiceEnvBuilder`. Keep the `AGENT_TYPE` stamp first and emit the Config-declared entries in ascending key order. Do not introduce a helper function; use `sort.Strings` from the standard library (the repo has no existing `sort`/`slices`/`maps` usage to follow, so this is the chosen, committed idiom — do not present alternatives).

   OLD (the bug — map range order is randomised per iteration):
   ```go
   func buildServiceEnvBuilder(resolved pkg.AgentConfiguration) k8s.EnvBuilder {
   	envBuilder := k8s.NewEnvBuilder()
   	envBuilder.Add(agentTypeEnvKey, string(agentTypeOrDefault(resolved.Type)))
   	for key, value := range resolved.Env {
   		envBuilder.Add(key, value)
   	}
   	return envBuilder
   }
   ```

   NEW:
   ```go
   func buildServiceEnvBuilder(resolved pkg.AgentConfiguration) k8s.EnvBuilder {
   	envBuilder := k8s.NewEnvBuilder()
   	envBuilder.Add(agentTypeEnvKey, string(agentTypeOrDefault(resolved.Type)))
   	keys := make([]string, 0, len(resolved.Env))
   	for key := range resolved.Env {
   		keys = append(keys, key)
   	}
   	sort.Strings(keys)
   	for _, key := range keys {
   		envBuilder.Add(key, resolved.Env[key])
   	}
   	return envBuilder
   }
   ```

   Import block becomes:
   ```go
   import (
   	"context"
   	"sort"

   	"github.com/bborbe/errors"
   	k8s "github.com/bborbe/k8s"
   	...
   )
   ```

2. Update the doc comment on `buildServiceEnvBuilder` to state the ordering contract, replacing the existing three-line comment above the function:
   ```go
   // buildServiceEnvBuilder renders the env for a service agent's container. Unlike a
   // Job, a service agent has no task, so there is no TASK_CONTENT/TASK_ID/PHASE —
   // only the Config's own env plus the type stamp.
   //
   // The Config-declared keys are emitted in sorted order because the reconciler
   // re-renders this StatefulSet on every pass and the result is compared: ranging
   // over a Go map yields a different order each iteration, so an unchanged Config
   // produced a different pod template every minute and the StatefulSet controller
   // rolled the pod with it. AGENT_TYPE stays first; the sorted keys follow.
   ```

3. In `/workspace/pkg/spawner/service_reconciler_test.go` (external `package spawner_test`), add a local helper next to the existing `getStatefulSet` closure (line ~63), inside `Describe("ServiceReconciler", ...)`:
   ```go
   envNames := func(env []corev1.EnvVar) []string {
   	names := make([]string, 0, len(env))
   	for _, e := range env {
   		names = append(names, e.Name)
   	}
   	return names
   }
   ```
   Add `"fmt"` and `"strings"` to the stdlib import group of the test file.

4. Add the following six specs inside `Describe("ReconcileService", ...)` in `/workspace/pkg/spawner/service_reconciler_test.go`. Each uses `reconciler`, `ctx`, `serviceConf` and `serviceCfg` from the existing `BeforeEach`. Note `serviceCfg.Env` has only one entry by default (`ALLOWED_TOOLS`) — every new spec below overrides `Env` with two or more entries (four or more in (a)–(e); (f) uses two, which is still enough to expose map-order churn), because a one-entry map has only one possible ordering and would not detect the bug.

   (a) AC 1 — exactly one distinct ordering across 50 reconciles:
   ```go
   It("renders the container env in exactly one order across 50 reconciles", func() {
   	multiEnv := serviceCfg
   	multiEnv.Env = map[string]string{
   		"MODEL":           "claude-sonnet-4-5",
   		"PUSHGATEWAY_URL": "http://pushgateway:9091",
   		"ALLOWED_TOOLS":   "Read,Grep,Bash",
   		"LOG_LEVEL":       "debug",
   	}

   	orderings := map[string]struct{}{}
   	for i := 0; i < 50; i++ {
   		Expect(reconciler.ReconcileService(ctx, serviceConf, multiEnv)).To(Succeed())
   		env := getStatefulSet("identity").Spec.Template.Spec.Containers[0].Env
   		orderings[strings.Join(envNames(env), ",")] = struct{}{}
   	}

   	Expect(orderings).To(HaveLen(1),
   		"an unchanged Config must render one env ordering, not a distribution over several")

   	// The order must be the SORTED one, not merely *a* stable one: a deterministic
   	// but wrong order satisfies the count above while still being a defect, and
   	// this exact assertion is also what makes the ordering reproducible across
   	// process restarts rather than merely within one.
   	Expect(envNames(getStatefulSet("identity").Spec.Template.Spec.Containers[0].Env)).
   		To(Equal([]string{"AGENT_TYPE", "ALLOWED_TOOLS", "LOG_LEVEL", "MODEL", "PUSHGATEWAY_URL"}),
   			"AGENT_TYPE first, then the Config-declared keys in ascending order")
   })
   ```

   (b) Failure Mode row 4 — a map larger than one Go map bucket:
   ```go
   It("renders one env order for a map larger than one Go map bucket", func() {
   	bigEnv := serviceCfg
   	bigEnv.Env = map[string]string{}
   	for i := 0; i < 20; i++ {
   		bigEnv.Env[fmt.Sprintf("KEY_%02d", i)] = fmt.Sprintf("value-%d", i)
   	}

   	orderings := map[string]struct{}{}
   	for i := 0; i < 50; i++ {
   		Expect(reconciler.ReconcileService(ctx, serviceConf, bigEnv)).To(Succeed())
   		env := getStatefulSet("identity").Spec.Template.Spec.Containers[0].Env
   		orderings[strings.Join(envNames(env), ",")] = struct{}{}
   	}

   	Expect(orderings).To(HaveLen(1),
   		"ordering must not depend on the single-bucket case")
   })
   ```

   (c) AC 2 — two consecutive reconciles produce a byte-identical pod template, element-for-element in the same index order:
   ```go
   It("produces a byte-identical pod template across two consecutive reconciles", func() {
   	multiEnv := serviceCfg
   	multiEnv.Env = map[string]string{
   		"MODEL":           "claude-sonnet-4-5",
   		"PUSHGATEWAY_URL": "http://pushgateway:9091",
   		"ALLOWED_TOOLS":   "Read,Grep,Bash",
   		"LOG_LEVEL":       "debug",
   	}

   	Expect(reconciler.ReconcileService(ctx, serviceConf, multiEnv)).To(Succeed())
   	first := getStatefulSet("identity").Spec.Template

   	Expect(reconciler.ReconcileService(ctx, serviceConf, multiEnv)).To(Succeed())
   	second := getStatefulSet("identity").Spec.Template

   	// Element-for-element, same index order — NOT set-equal. ConsistOf/ContainElements
   	// would pass even while the order churns, which is exactly the bug this pins.
   	Expect(second.Spec.Containers[0].Env).To(Equal(first.Spec.Containers[0].Env))
   	Expect(second).To(Equal(first))
   })
   ```

   (d) AC 6 — a genuine Config change still reaches the cluster, and the env order is stable across it:
   ```go
   It("still rolls the pod when the image changes, leaving the env order stable", func() {
   	multiEnv := serviceCfg
   	multiEnv.Env = map[string]string{
   		"MODEL":           "claude-sonnet-4-5",
   		"PUSHGATEWAY_URL": "http://pushgateway:9091",
   		"ALLOWED_TOOLS":   "Read,Grep,Bash",
   		"LOG_LEVEL":       "debug",
   	}

   	Expect(reconciler.ReconcileService(ctx, serviceConf, multiEnv)).To(Succeed())
   	before := getStatefulSet("identity").Spec.Template.Spec.Containers[0]

   	updated := multiEnv
   	updated.Image = "docker.io/bborbe/agent-pi:v0.1.8"
   	Expect(reconciler.ReconcileService(ctx, serviceConf, updated)).To(Succeed())
   	after := getStatefulSet("identity").Spec.Template.Spec.Containers[0]

   	Expect(after.Image).To(Equal("docker.io/bborbe/agent-pi:v0.1.8"))
   	Expect(after.Image).NotTo(Equal(before.Image))
   	Expect(after.Env).To(Equal(before.Env))
   })
   ```

   (e) Failure Mode row 1 — two Configs share an env key set but differ in one value; both render deterministically and the value difference still reaches the pod template:
   ```go
   It("still rolls the pod when an env value changes, leaving the key order stable", func() {
   	multiEnv := serviceCfg
   	multiEnv.Env = map[string]string{
   		"MODEL":           "claude-sonnet-4-5",
   		"PUSHGATEWAY_URL": "http://pushgateway:9091",
   		"ALLOWED_TOOLS":   "Read,Grep,Bash",
   		"LOG_LEVEL":       "debug",
   	}

   	Expect(reconciler.ReconcileService(ctx, serviceConf, multiEnv)).To(Succeed())
   	before := getStatefulSet("identity").Spec.Template.Spec.Containers[0].Env

   	changed := multiEnv
   	changed.Env = map[string]string{
   		"MODEL":           "claude-opus-4-5",
   		"PUSHGATEWAY_URL": "http://pushgateway:9091",
   		"ALLOWED_TOOLS":   "Read,Grep,Bash",
   		"LOG_LEVEL":       "debug",
   	}
   	Expect(reconciler.ReconcileService(ctx, serviceConf, changed)).To(Succeed())
   	after := getStatefulSet("identity").Spec.Template.Spec.Containers[0].Env

   	Expect(envNames(after)).To(Equal(envNames(before)))
   	Expect(after).NotTo(Equal(before))
   	Expect(after).To(ContainElement(corev1.EnvVar{Name: "MODEL", Value: "claude-opus-4-5"}))
   })
   ```

   (f) Failure Mode row 1, the independence half — two Configs with the same key set render independently of each other:
   ```go
   It("renders two Configs with the same key set independently and deterministically", func() {
   	cfgA := serviceCfg
   	cfgA.Env = map[string]string{"MODEL": "claude-sonnet-4-5", "LOG_LEVEL": "debug"}
   	confA := serviceConf
   	confA.Name = "identity-a"

   	cfgB := serviceCfg
   	cfgB.Env = map[string]string{"MODEL": "claude-opus-4-1", "LOG_LEVEL": "info"}
   	confB := serviceConf
   	confB.Name = "identity-b"

   	for i := 0; i < 50; i++ {
   		Expect(reconciler.ReconcileService(ctx, confA, cfgA)).To(Succeed())
   		Expect(reconciler.ReconcileService(ctx, confB, cfgB)).To(Succeed())
   	}

   	Expect(envNames(getStatefulSet("identity-a").Spec.Template.Spec.Containers[0].Env)).
   		To(Equal([]string{"AGENT_TYPE", "LOG_LEVEL", "MODEL"}))
   	Expect(envNames(getStatefulSet("identity-b").Spec.Template.Spec.Containers[0].Env)).
   		To(Equal([]string{"AGENT_TYPE", "LOG_LEVEL", "MODEL"}))
   	Expect(getStatefulSet("identity-a").Spec.Template).
   		NotTo(Equal(getStatefulSet("identity-b").Spec.Template),
   			"a differing env value must still change the rendered template")
   })
   ```

5. Leave the existing `It("is idempotent — a second reconcile with the same config does not error")` (line ~173) and `It("rolls the pod when the image changes")` (line ~181) in place and passing. Do not weaken or delete any existing spec.

6. Add the changelog entry in `/workspace/CHANGELOG.md`. No `## Unreleased` section exists at HEAD (v0.18.3), so create one directly under the `# Changelog` heading and its intro paragraph, above `## v0.18.3`:
   ```markdown
   ## Unreleased

   - fix: render a service agent's container env in a deterministic key-sorted order, so an unchanged Config produces a byte-identical StatefulSet and the pod stops rolling once a minute
   ```

7. Before finishing, re-run every command in `<verification>` and confirm each passes, then walk each spec acceptance criterion (AC 1, 2, 6, 7) against the change and confirm the evidence each one names actually exists.
</requirements>

<constraints>
- **Do NOT modify `/workspace/pkg/spawner/job_spawner.go`.** Its `buildJobEnvBuilder` (line ~545) carries the same `for key, value := range config.Env` pattern but the Job path is explicitly out of scope: a Job is created once per task rather than re-updated, so it does not manifest as churn. A separate follow-up covers it.
- **Do NOT modify the pinned library `github.com/bborbe/k8s@v1.14.16`.** `EnvBuilder` is an append-only slice builder by design; ordering is the caller's responsibility, so the fix belongs in `buildServiceEnvBuilder`.
- **The env contents must not change.** The same Config must still yield the same `AGENT_TYPE` stamp and the same Config-declared key/value pairs. Only the ordering is at issue. `AGENT_TYPE` stays first; the Config-declared keys follow, sorted ascending with `sort.Strings`.
- **Do NOT touch the StatefulSet's immutable fields** (`selector`, `serviceName`, `volumeClaimTemplates`) — the API server rejects any change to them, and the deployer's update path deliberately merges only mutable spec fields.
- Use the standard library `sort.Strings`. Do not add a sorting helper, do not add a new dependency, do not use `slices`/`maps` (the repo has no precedent for either).
- Repo conventions are frozen: Ginkgo/Gomega v2 specs in the external `package spawner_test` test package, counterfeiter mocks for any new dependency (none needed here), `github.com/bborbe/errors` wrapping (never `fmt.Errorf`), glog `V(n)` gating. No new production logging is required.
- Coverage: the changed function `buildServiceEnvBuilder` must be exercised by the new specs (≥80% statement coverage for changed code).
- The fix must stabilise the render, not suppress updates — the "still rolls the pod when the image changes" spec is the guard. A second varying template field would mean this diagnosis was incomplete: do NOT guess at one and do NOT invent a second fix; report it and stop, and the spec stays open until that field is named (spec Failure Modes row 3). The dev-cluster observation that would reveal it is operator-executable and runs after merge, not here.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
- `make precommit` must pass.
</constraints>

<verification>
Run `make test` iteratively after each meaningful change (fast feedback loop), then `make precommit` ONCE at the very end.

- `go test -v ./pkg/spawner/... -count=1` — exits 0, with the six new spec names in the passing list. The `-v` matters: without it the runner prints only per-package `ok` lines and never the individual spec names, so "the new specs passed" could not be checked from this command.
- `make precommit` — exits 0 (format, generate, lint, unit + integration tests).
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/spawner/... && go tool cover -func=/tmp/cover.out` — confirm `buildServiceEnvBuilder` is exercised by the new specs (≥80% statement coverage for changed code).
- `grep -c 'for key, value := range config.Env' pkg/spawner/job_spawner.go` — prints `1` (the Job path still renders env by ranging its map, i.e. it was not given the service-path fix). A git-free form of the AC-7 assertion on purpose: the daemon is launched with `hideGit=true`, under which a `git diff` in the container fails and a non-zero exit can be misread as a failed prompt.
- `grep -n 'sort.Strings' pkg/spawner/service_reconciler.go` — returns the one sorted-keys call in `buildServiceEnvBuilder`.

Do NOT run `docker`, `make build`, `kubectl`, or `dark-factory` commands in this prompt — those are operator-executable and belong on the spec's verification ladder (spec `# Verification` → Operator-executable).
</verification>
