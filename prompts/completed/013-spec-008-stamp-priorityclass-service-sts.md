---
status: completed
spec: [008-bug-service-sts-ignores-priorityclass]
summary: Stamped the resolved priority class onto the service StatefulSet's pod template in buildStatefulSet, mirroring the Job path's guarded idiom, with three specs pinning the set, unset, and stable-render cases
execution_id: agent-task-executor-priorityclass-exec-013-spec-008-stamp-priorityclass-service-sts
dark-factory-version: v0.196.0
created: "2026-10-09T18:32:36Z"
queued: "2026-10-09T18:37:25Z"
started: "2026-10-09T18:37:26Z"
completed: "2026-10-09T18:46:53Z"
---

<summary>
- A `type: service` Config that declares a priority class now gets that class on the StatefulSet the executor creates for it, so the pod is admitted at the declared priority instead of 0.
- Before this change the class was read, resolved, and carried all the way to the service renderer, and then simply not written onto the pod template — the Job path stamped it, the service path did not.
- The stamp is guarded on the value being non-empty, exactly as the Job path guards it, so a service Config that declares no class renders exactly as it does today.
- A service agent that declares no class is not rolled by this change — the rendered pod template is byte-identical to the pre-fix render.
- A Config that declares a class still reconciles idempotently: two consecutive passes produce a byte-identical pod template, so the pod rolls once when the class is set or cleared and does not roll again on every reconcile.
- Three new specs pin the behaviour: the class is stamped when declared, the field stays unset (and absent from the marshalled template) when it is not, and the render is stable across repeated reconciles.
- The one-shot Job path is explicitly left untouched; its guard is the reference behaviour this fix mirrors.
- No new CRD field, no default class, no validation, and no resolver change — the value is already resolved onto the agent configuration.
</summary>

<objective>
Make the service reconciler honour the priority class the Config already declares. `ServiceReconciler.buildStatefulSet` in `/workspace/pkg/spawner/service_reconciler.go` renders a StatefulSet from the resolved `pkg.AgentConfiguration` and never writes `resolved.PriorityClassName` onto the pod template, so a `type: service` agent enters the cluster at pod priority 0 and is the first workload a higher-priority Job evicts. Mirror the guarded stamp the Job path already performs in `/workspace/pkg/spawner/job_spawner.go`, and lock the set-case, the unset-case, and the render's stability across repeated reconciles with specs.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for the container agent's own execution rules (Ginkgo/Gomega v2, external `package spawner_test` test package, `github.com/bborbe/errors` wrapping, counterfeiter mocks, glog `V(n)` gating, coverage ≥80% for changed code). This repo has no project-level `CLAUDE.md` or `AGENTS.md`.

Read these files fully before changing anything:
- `/workspace/pkg/spawner/service_reconciler.go` — the whole file. `buildStatefulSet` (the `func (r *serviceReconciler) buildStatefulSet` method) is the only function you change. Read its post-build mutation block, which runs after `statefulSet, err := statefulSetBuilder.Build(ctx)` and currently does, in this order: (1) the empty-storage-class repair that nils `statefulSet.Spec.VolumeClaimTemplates[0].Spec.StorageClassName`, (2) the `OwnerReferences` assignment, (3) the `PersistentVolumeClaimRetentionPolicy` assignment, (4) `applyServiceSecretEnvFrom`. Your new stamp goes into this block; the other four mutations must keep their relative order and behaviour.
- `/workspace/pkg/spawner/job_spawner.go` — read the priority stamp in the Job build path, which is the exact idiom to mirror:
  ```go
  if config.PriorityClassName != "" {
      job.Spec.Template.Spec.PriorityClassName = config.PriorityClassName
  }
  ```
  On the Job path the parameter is named `config`; on the service path the resolved configuration parameter is named `resolved` (both are `pkg.AgentConfiguration`). This file is **out of scope** — read it only to copy the guard's shape, never edit it.
- `/workspace/pkg/agent_configuration.go` — `AgentConfiguration.PriorityClassName` is declared `PriorityClassName string` (a plain string, no pointer). This is the field the service renderer receives as `resolved.PriorityClassName`.
- `/workspace/pkg/spawner/service_reconciler_test.go` — the whole file. Reuse the existing `BeforeEach` fixtures (`reconciler`, `ctx`, `serviceConf`, `serviceCfg`) and the existing `getStatefulSet` read-back closure. The existing specs that must keep passing include `"is idempotent — a second reconcile with the same config does not error"` and `"rolls the pod when the image changes"`; do not weaken or delete any of them.
- `/workspace/pkg/spawner/spawner_suite_test.go` — the Ginkgo suite entry point (`RunSpecs(t, "Spawner Suite")`); the new specs run under it, no new suite file is needed.

Library source (pinned, do NOT modify) — read to confirm the stamp survives the fake client's create/update round-trip:
- `/home/node/go/pkg/mod/github.com/bborbe/k8s@v1.14.16/k8s_statefulset-deployer.go` — `Deploy` sets `existing.Spec.Template = statefulSet.Spec.Template` on the update path, so anything you set on `Spec.Template.Spec` is what the fake client stores and returns.
- `/home/node/go/pkg/mod/k8s.io/api@v0.36.4/core/v1/types.go` — `PodSpec.PriorityClassName` is declared `PriorityClassName string \`json:"priorityClassName,omitempty"\``. The `omitempty` tag is why an empty value marshals to no key at all, which is the property the unset-case spec relies on.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2, external test packages, coverage ≥80%.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for changed code.
</context>

<requirements>
1. In `/workspace/pkg/spawner/service_reconciler.go`, add the guarded priority-class stamp to `buildStatefulSet`. Place it immediately after the empty-storage-class repair block (the `if r.storageClass == "" && len(statefulSet.Spec.VolumeClaimTemplates) > 0 { ... }` block) and before the `statefulSet.OwnerReferences = ...` assignment. Insert exactly:
   ```go
   // A Config that declares a priority class gets it on the pod template, stamped
   // after the StatefulSet is built — the same idiom as the Job path. Guarded on a
   // non-empty value so a Config that declares none renders exactly as before:
   // PriorityClassName is a string with omitempty, so assigning "" marshals the same
   // as leaving it unset, and an agent that does not use the field is not rolled.
   if resolved.PriorityClassName != "" {
       statefulSet.Spec.Template.Spec.PriorityClassName = resolved.PriorityClassName
   }
   ```
   Do not add a helper function, do not add a parameter, do not change the `ReconcileService` signature, and do not touch the other four post-build mutations.

2. Add `"encoding/json"` to the stdlib import group of `/workspace/pkg/spawner/service_reconciler_test.go`. `"strings"` is already imported and is used by the unset-case spec below.

3. Add three specs inside `Describe("ReconcileService", ...)` in `/workspace/pkg/spawner/service_reconciler_test.go`. Each uses `reconciler`, `ctx`, `serviceConf` and `serviceCfg` from the existing `BeforeEach`.

   (a) Acceptance Criterion 1 — the declared class reaches the pod template, verbatim and unvalidated (this also covers the spec's Failure Modes row 1: the reconciler must not look the class up or require it to exist — a class that does not exist is the API server's admission concern, not the reconciler's):
   ```go
   It("stamps the resolved priority class on the StatefulSet's pod template", func() {
       withPriority := serviceCfg
       withPriority.PriorityClassName = "claude-interactive"

       Expect(reconciler.ReconcileService(ctx, serviceConf, withPriority)).To(Succeed())

       sts := getStatefulSet("identity")
       Expect(sts.Spec.Template.Spec.PriorityClassName).To(Equal("claude-interactive"))
   })
   ```

   (b) Acceptance Criterion 2 — an empty class leaves the field unset AND absent from the marshalled pod template, so the render is unchanged for every agent that does not declare a class:
   ```go
   It("leaves the priority class unset when the Config declares none", func() {
       Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

       sts := getStatefulSet("identity")
       Expect(sts.Spec.Template.Spec.PriorityClassName).To(BeEmpty())

       marshalled, err := json.Marshal(sts.Spec.Template)
       Expect(err).NotTo(HaveOccurred())
       Expect(strings.Count(string(marshalled), `"priorityClassName"`)).To(Equal(0),
           "an unset priority class must not appear as a key in the marshalled pod template")
   })
   ```

   (c) The "must not produce a repeating roll" constraint (spec Constraints) — a class that is set renders one pod template across repeated reconciles, so the pod rolls once rather than once per pass (this mirrors the existing env-order stability spec "produces a byte-identical pod template across two consecutive reconciles"). Note: spec Failure Modes row 2 — the updated StatefulSet is written but the pod is not rolled — is cluster-controller behaviour observed post-deploy, so it is out of scope for this container prompt and is covered by the spec's rung-2 verification ladder instead:
   ```go
   It("renders a byte-identical pod template across two reconciles with a class set", func() {
       withPriority := serviceCfg
       withPriority.PriorityClassName = "claude-interactive"

       Expect(reconciler.ReconcileService(ctx, serviceConf, withPriority)).To(Succeed())
       first := getStatefulSet("identity").Spec.Template

       Expect(reconciler.ReconcileService(ctx, serviceConf, withPriority)).To(Succeed())
       second := getStatefulSet("identity").Spec.Template

       Expect(second).To(Equal(first))
   })
   ```

4. Leave every existing spec in the file in place and passing. In particular, do not touch the idempotency spec, the image-roll spec, the env-order specs, or the storage-class specs.
</requirements>

<constraints>
- **Do NOT modify `/workspace/pkg/spawner/job_spawner.go`.** The Job path (`type: job`) already stamps the class correctly; its guard is the reference behaviour this fix mirrors, and any change there is a separate spec. This is a hard boundary — the Job path's regression criterion pins it.
- **Do NOT add a CRD field, a default priority class, a `globalDefault`, or a resolver change.** The value is already resolved onto `pkg.AgentConfiguration.PriorityClassName` and reaches `buildStatefulSet` as its `resolved` argument. No new field, no defaulting, no validation, and no lookup of whether the class exists.
- **The stamp must be guarded on the value being non-empty**, mirroring the Job path exactly. This is a code-shape invariant rather than an acceptance criterion: `PriorityClassName` is a plain `string` with `omitempty`, so assigning `""` marshals identically to leaving the field unset, and no spec can discriminate the guard — code review is what enforces it. Do not write the assignment unconditionally.
- **Apply the stamp after the StatefulSet is built**, by mutating the built object — the same place and the same idiom as the existing storage-class repair in `buildStatefulSet`. Do not teach the builder about the class.
- **The service renderer's existing post-build mutations must all still be applied, in the same order relative to each other**: the storage-class repair, the Config owner reference, the PVC retention policy, and the secret `envFrom`.
- **Immutable StatefulSet fields** (`selector`, `serviceName`, `volumeClaimTemplates`) must not be sent on update — the API server rejects any change to them. This change touches only the pod template's `priorityClassName`, which is mutable.
- **Removing a class from a Config is a real change**: the pod template changes once and the pod rolls once. It must not produce a repeating roll — that is the sibling defect in `bug-service-env-order-rolls-pod-every-minute`, and this fix must not reintroduce it for either the set or the unset case. The idempotency spec in requirement 3(c) is the guard.
- **Do NOT hand-edit or regenerate anything under `/workspace/k8s/`.** In particular, do NOT run `make generatek8s`: the repo's `hack/update-codegen.sh` pins the wrong module path and regenerating rewrites the whole `k8s/client` tree and breaks the build. A separate prompt owns the documentation comment.
- **Do NOT touch `CHANGELOG.md`, `docs/`, or the CRD type comment in this prompt.** They are handled by the sibling prompt that follows.
- Repo conventions are frozen: Ginkgo/Gomega v2 specs in the external `package spawner_test` test package, `github.com/bborbe/errors` wrapping (never `fmt.Errorf`), glog `V(n)` gating. No new production logging is required.
- Coverage: the new guard in `buildStatefulSet` must be exercised by the new specs (≥80% statement coverage for changed code).
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
- `make precommit` must pass.
</constraints>

<verification>
Run `make test` iteratively after each meaningful change (fast feedback loop), then `make precommit` ONCE at the very end.

- `go test -v ./pkg/spawner/... -count=1` — exits 0, with the three new spec names in the passing list. The `-v` matters: without it the runner prints only per-package `ok` lines and the individual spec names cannot be checked.
- `grep -n 'PriorityClassName' pkg/spawner/service_reconciler.go` — returns ≥1 line (the new guard and its assignment).
- `grep -c 'PriorityClassName' pkg/spawner/job_spawner.go` — still returns exactly `2` (the guard condition and the assignment in the Job path), i.e. the Job path was not modified. A git-free form of Acceptance Criterion 3 on purpose: the daemon runs with `hideGit=true`, under which a `git diff` in the container fails and a non-zero exit can be misread as a failed prompt.
- `make precommit` — exits 0 (format, generate, test, check, addlicense).
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/spawner/... && go tool cover -func=/tmp/cover.out` — confirm `buildStatefulSet` is exercised by the new specs (≥80% statement coverage for changed code).

Do NOT run `docker`, `make build`, `make buca`, `kubectl`, or `dark-factory` commands in this prompt — those are operator-executable and belong on the spec's verification ladder. Do NOT run `git` commands either: the container's `.git` is masked, so a `git` command fails and a non-zero exit can be misread as a failed prompt.
</verification>
