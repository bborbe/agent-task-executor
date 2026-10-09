---
status: prompted
tags:
    - dark-factory
    - spec
approved: "2026-10-09T18:17:40Z"
generating: "2026-10-09T18:18:55Z"
prompted: "2026-10-09T18:38:05Z"
branch: dark-factory/bug-service-sts-ignores-priorityclass
---

## Summary

- `agent-task-executor` stamps a Config's declared priority class onto the pods it creates for one-shot **Job** tasks, and never onto a **service** agent's StatefulSet — so every `type: service` agent enters the cluster at pod priority 0 and is the first workload a higher-priority Job evicts.
- The value is already read, resolved, and carried all the way to the service path; nothing upstream is missing — the class is simply not applied there.
- Live on nuke dev: `Config/claude-interactive` declares priority class `claude-interactive`, yet `sts/claude-interactive`'s pod template carries none and `pod/claude-interactive-0` reports `.spec.priority` = 0. It had been preempted by trading-agent Jobs at priority 500. `pi-interactive` is in the identical state.
- The field's own documentation scopes it to Jobs, which is why the gap survived review: the doc and the Job-path code agree with each other and both are silent about the service path.
- A service Config that declares no class must keep rendering exactly as it does today — the field stays unset and no pod rolls — so the change is inert for every agent that does not use the field.

## Problem

The executor derives a Kubernetes priority class for an agent from its Config and is supposed to stamp it onto the pods that Config creates, so an agent that declares a class competes for node capacity on its own declared terms. It does that for one-shot task Jobs and does not do it for service agents: the service path renders a StatefulSet from the same resolved configuration and never writes the class onto the pod template. A service agent therefore enters the cluster at priority 0 — the lowest value — while the Jobs sharing its node can carry classes in the hundreds. The declared value is silently dropped on one of the two spawn paths, which is the failure shape this repo has hit before: a field that is read, resolved, carried to the renderer, and then not applied, with a successful reconcile logged and no trace except the object that is missing it.

## Goal

The end state: a `type: service` Config that declares a priority class produces a StatefulSet whose pod template carries that class, so its pod is admitted at the declared priority and is no longer the first thing evicted when a higher-priority Job arrives. A service Config that declares no class renders exactly as it does today — the field stays unset and no pod is rolled by this change. The one-shot Job path keeps its current behaviour. The CRD field's documentation describes both spawn paths rather than Jobs alone, and the fix is recorded in the changelog with a `fix:` prefix so the version bump is correct.

## Non-goals

- Do NOT change the Job path (`type: job`) — it already stamps the class correctly, and the regression criterion pins it. Any change there is a separate spec.
- Do NOT add a CRD field, a default priority class, a `globalDefault`, or a resolver change — the value is already resolved onto the agent configuration and reaches the service renderer. No new field, no defaulting, no validation.
- Do NOT change who declares the class. The Helm chart derives it from the agent name and writes it onto the Config; this fix only makes the executor honour what the Config already says.

## Reproduction

Repo: `bborbe/agent-task-executor` at `4ea042b` (the merge that landed after `release v0.18.4`, with one unreleased `fix:` bullet already in flight), branch `fix/stamp-priorityclass-on-service-sts`, deployed to nuke dev as `EXECUTOR_VERSION=v0.18.4`. Dark-factory: `v0.196.0`.

The smallest config that exhibits the bug is any `type: service` Config that sets `spec.priorityClassName`:

```yaml
apiVersion: agent.benjamin-borbe.de/v1
kind: Config
metadata:
  name: claude-interactive
  namespace: dev
spec:
  type: service
  assignee: claude-interactive
  priorityClassName: claude-interactive
  # (other fields omitted)
```

Three field-scoped reads (`-o jsonpath` only — never a whole-object dump, whose `last-applied-configuration` annotation embeds the container `env` in plaintext):

```
$ kubectlnukedev -n dev get config claude-interactive -o jsonpath='{.spec.priorityClassName}'
claude-interactive

$ kubectlnukedev -n dev get sts claude-interactive -o jsonpath='{.spec.template.spec.priorityClassName}'

                      <-- empty

$ kubectlnukedev -n dev get pod claude-interactive-0 -o jsonpath='{.spec.priority}'
0
```

The Config declares the class; the StatefulSet built from it carries nothing; the pod is admitted at priority 0. `pi-interactive` is in the identical state. The pod had been preempted by trading-agent Jobs carrying priority 500.

The one-shot path, for contrast, is correct — it guards the stamp on a non-empty value:

```
$ kubectlnukedev -n dev get pod <a-trading-agent-job-pod> -o jsonpath='{.spec.priorityClassName}'
trading-agent
```

## Expected vs Actual

**Expected.** Per the CRD field's own documentation (`docs/agent-crd-specification.md`, `spec.priorityClassName` row) the field is *"the Kubernetes PriorityClass name to stamp onto spawned Job PodTemplates. When set, a matching `ResourceQuota` scoped to this class enforces the concurrent pod cap."* The same document's *Future Extensions* section builds the whole concurrency story on it: *"Concurrency is now enforced K8s-natively: set `spec.priorityClassName` on a Config CR and apply a `ResourceQuota` with a `scopeSelector` matching that PriorityClass."* A Config that declares a class must therefore have that class on every pod it causes to exist — Job or service — and a service agent is admitted at its declared priority.

**Actual.** A `type: service` Config's declared class never reaches the StatefulSet. The pod template's priority class field is empty and the pod reports `.spec.priority` = 0. The executor logs a successful reconcile for the StatefulSet and reports no error, so the only observable is the missing field on the object the Config owns.

## Why this is a bug

The CRD documents `spec.priorityClassName` as the field that gives an agent a Kubernetes PriorityClass, and the executor reads it, resolves it onto the agent configuration, and applies it on one spawn path only. So the CRD's contract — declare a class, get that class on the pods this Config creates — holds for Jobs and not for services, silently: the resolve succeeds, the reconcile reports success, and the only trace is an empty field on the StatefulSet. The documentation is complicit: the type comment and the CRD doc both scope the field to Job pod templates, so a reviewer comparing code against docs finds the two in agreement and the service path invisible. And the consequence is precisely the one the `service` shape exists to prevent — a service agent holds an identity and a session across time, and a workload admitted at priority 0 is the first thing evicted whenever batch work arrives.

## Acceptance Criteria

- [ ] **A service Config that declares a priority class gets it on the StatefulSet's pod template.** Evidence: a Ginkgo spec in `pkg/spawner/service_reconciler_test.go` reconciles a service Config whose resolved configuration carries a priority class and asserts the StatefulSet fetched from the fake client has that exact value in its pod template's priority class field; observed by `make test` exiting 0 with that spec's name in the passing list.
- [ ] **A service Config that declares no priority class leaves the field unset, and its render is unchanged.** Evidence: a Ginkgo spec in the same file reconciles a service Config whose resolved priority class is empty and asserts the field on the fetched StatefulSet's pod template is the empty string, and that the marshalled pod template contains no `priorityClassName` key (`grep -c '"priorityClassName"'` over the marshalled template returns 0); observed by `make test` exiting 0 with that spec's name in the passing list.
- [ ] **The Job path is unchanged.** Evidence: `git diff --stat -- pkg/spawner/job_spawner.go` prints no diff for that file, and the existing job-spawner specs pass — `make test` exits 0.
- [ ] **The CRD type comment names both spawn paths, in the source and in its generated copy.** Evidence: `grep -n 'PriorityClassName is' k8s/apis/agent.benjamin-borbe.de/v1/types.go` returns ≥1 line containing both `Job` and `StatefulSet`; and after `make generate`, `grep -n 'PriorityClassName is' k8s/client/applyconfiguration/agent.benjamin-borbe.de/v1/configspec.go` returns the same corrected sentence.
- [ ] **`docs/agent-crd-specification.md` names the service path everywhere it describes the field.** Evidence: three greps against the doc — the field's own row (`grep -n 'spec.priorityClassName' docs/agent-crd-specification.md`) returns a line containing both `Job` and `StatefulSet`; the "Who Uses the CRD" table region (`awk '/^## Who Uses the CRD/,/^## Fields/' docs/agent-crd-specification.md | grep -c 'StatefulSet'`) returns ≥1; and the "Future Extensions" region (`awk '/^## Future Extensions/,0' docs/agent-crd-specification.md | grep -c 'StatefulSet'`) returns ≥1.
- [ ] **`CHANGELOG.md` keeps exactly one `## Unreleased` heading and gains a `fix:` bullet naming the service path under it.** Evidence: `grep -c '^## Unreleased' CHANGELOG.md` returns 1 (a second heading makes it 2 and fails this criterion); and `awk '/^## Unreleased/{f=1;next} /^## /{f=0} f && /^- fix:/ && /StatefulSet/{print; exit}' CHANGELOG.md` prints the new bullet — the `fix:` bullet already in that section does not contain `StatefulSet`, so the match is discriminating.
- [ ] **`make precommit` exits 0** in the repo root, run from the worktree `/Users/bborbe/Documents/workspaces/agent-task-executor-priorityclass`. Evidence: exit code 0 (format + generate + test + check + addlicense).
- [ ] **Post-Deploy (Rung-2):** on nuke dev, the live service agent's StatefulSet carries the class and its pod is admitted at that priority. Evidence: after the executor rolls and one reconcile interval passes, `kubectlnukedev -n dev get sts claude-interactive -o jsonpath='{.spec.template.spec.priorityClassName}'` returns `claude-interactive`, and `kubectlnukedev -n dev get pod claude-interactive-0 -o jsonpath='{.spec.priority}'` returns the `value` declared by PriorityClass `claude-interactive` — non-zero, no longer 0. The same two reads for `pi-interactive`; all four quoted in the PR body.
  - `deploy_check:` `kubectlnukedev -n dev get deploy/agent-task-executor -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `v0.18.5`

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — exits 0 (format, generate, test, check, addlicense)
- `go test ./pkg/spawner/... -count=1` — the priority-class specs pass
- `grep -n 'PriorityClassName' pkg/spawner/service_reconciler.go` — returns ≥1 line
- `grep -n 'PriorityClassName is' k8s/apis/agent.benjamin-borbe.de/v1/types.go` — returns a line naming both spawn paths
- `grep -n 'spec.priorityClassName' docs/agent-crd-specification.md` — returns the row, naming both spawn paths
- `awk '/^## Who Uses the CRD/,/^## Fields/' docs/agent-crd-specification.md | grep -c 'StatefulSet'` — returns ≥1
- `grep -c '^## Unreleased' CHANGELOG.md` — returns 1
- `awk '/^## Unreleased/{f=1;next} /^## /{f=0} f && /^- fix:/ && /StatefulSet/{print; exit}' CHANGELOG.md` — prints the new bullet

### Operator-executable (runs on the host after PR merge, spec verification ladder)

- Open the PR by hand with `gh pr create` — this repo sets `pr: false` in `.dark-factory.yaml`, so dark-factory commits on the feature branch but does not open the PR.
- `/github-release-repo-trigger` after merge, then hand-build and push the released image (CI runs `make precommit` only and does not build it).
- Deploy dev per [[Deploy Mirrored Agent Service]]: bump `EXECUTOR_VERSION` in `nuke/agent/Makefile`, then `cd ~/Documents/workspaces/nuke/agent && BRANCH=dev make apply`.
- `kubectlnukedev -n dev rollout status deploy/agent-task-executor --timeout=120s`, then allow one reconcile interval for the StatefulSet to be re-rendered.
- `kubectlnukedev -n dev get sts claude-interactive -o jsonpath='{.spec.template.spec.priorityClassName}'` → `claude-interactive`
- `kubectlnukedev -n dev get pod claude-interactive-0 -o jsonpath='{.spec.priority}'` → non-zero
- Repeat both reads for `pi-interactive`.

## Desired Behavior

1. When a service Config's resolved configuration carries a non-empty priority class, the StatefulSet rendered for it has that class name on its pod template.
2. When the resolved priority class is empty, the rendered pod template leaves the field unset — the object is identical to the render produced before this fix, so a service agent that declares no class is not rolled by this change.
3. The CRD type comment and `docs/agent-crd-specification.md` describe the field as stamped onto both the Job pod template and the service StatefulSet pod template, and the doc's "Who Uses the CRD" table and *Future Extensions* paragraph no longer leave the service path unnamed.
4. `CHANGELOG.md` records the fix as a bullet appended under its existing `## Unreleased` heading, beginning with `- fix:` and naming the service StatefulSet path.

## Constraints

- The Job path (`pkg/spawner/job_spawner.go`) must not change; its guard at `job_spawner.go:162-163` is the reference behaviour this fix mirrors.
- No CRD, type, or resolver change: the value is already resolved onto `pkg.AgentConfiguration.PriorityClassName` and reaches the service renderer as its `resolved` argument. No new field, no default, no validation.
- **The stamp must be guarded on the value being non-empty**, mirroring the Job path exactly, so both spawn paths treat the field identically. This is a code-shape invariant rather than an acceptance criterion: `PriorityClassName` is declared `string` with `omitempty`, so assigning `""` unconditionally marshals identically to leaving the field unset — no acceptance criterion can discriminate the guard, so code review is what enforces it.
- **The stamp is applied after the StatefulSet is built**, by mutating the built object — the same place and the same idiom as the existing storage-class repair in that function, which also mutates the object after `Build`.
- The service renderer's existing post-build mutations — the storage-class repair, the Config owner reference, the PVC retention policy, and the secret `envFrom` — must all still be applied, in the same order relative to each other.
- **Removing a class from a Config is a real change**: the pod template changes once and the pod rolls once. It must not produce a repeating roll — that is the sibling defect in `bug-service-env-order-rolls-pod-every-minute`, and this fix must not reintroduce it for either the set or the unset case.
- The generated applyconfiguration file (`k8s/client/applyconfiguration/agent.benjamin-borbe.de/v1/configspec.go`) must not be hand-edited — its copy of the field comment is produced by `make generate` from the type comment in `k8s/apis/agent.benjamin-borbe.de/v1/types.go`; edit the source and let generate propagate it. A hand-edit here is silently reverted by the next `make precommit`.
- Immutable StatefulSet fields (`selector`, `serviceName`, `volumeClaimTemplates`) must not be sent on update — the API server rejects any change to them.
- Tests follow the file's existing Ginkgo/Gomega `BeforeEach`/`JustBeforeEach`/`It` structure; the storage-class specs in `pkg/spawner/service_reconciler_test.go` are the closest model, including their use of a second reconciler constructed with a different option value.
- `make precommit` must pass, run from the worktree `/Users/bborbe/Documents/workspaces/agent-task-executor-priorityclass` — never from the main checkout `/Users/bborbe/Documents/workspaces/agent-task-executor`, which is on master and bypasses the PR gate.
- The repo has no `CLAUDE.md`/`AGENTS.md`. `.dark-factory.yaml` sets `workflow: direct`, `pr: false`, `autoRelease: false`, `autoGeneratePrompts: true` — the PR is opened by hand and no release is cut automatically.
- **CHANGELOG: the `## Unreleased` heading already exists.** At HEAD `4ea042b` it holds the `fix:` bullet for the x/net + osv-scanner prerequisite work. The new bullet must be **appended under that existing heading**, not placed under a second `## Unreleased` heading — `grep -c '^## Unreleased' CHANGELOG.md` must stay 1. The bullet must begin with `- fix:` (a wrong or missing prefix breaks the auto version bump) and must name the service StatefulSet path.
- The dev deploy's image tag is the release semver, not a commit SHA: `Makefile.docker`'s `check-version-tag` target refuses to build `vX.Y.Z` from a tree whose HEAD is not tagged `vX.Y.Z`, and `nuke/agent/Makefile:100` deploys `EXECUTOR_VERSION` (currently `v0.18.4`). So the post-deploy criterion's `deploy_target:` is the next release tag — expected `v0.18.5`, a patch bump because `## Unreleased` carries only `fix:` bullets — and it is updated at prompt time to whatever tag `EXECUTOR_VERSION` is bumped to. Do not substitute `$(git rev-parse --short HEAD)`: the deploy check reads the image tag and would never match a SHA.

## Failure Modes

| Trigger | Expected behavior | Recovery |
|---|---|---|
| The Config names a PriorityClass that does not exist in the namespace | The StatefulSet is written; the pod fails the Priority admission plugin and does not start | Operator creates the PriorityClass (the Helm chart derives it from the agent name) or clears `spec.priorityClassName`; `kubectl describe pod` names the missing class |
| The reconciler writes the updated StatefulSet but the pod is not rolled | The StatefulSet controller applies the template change and recreates the pod | `kubectlnukedev -n dev rollout restart sts/<name>`, then re-read `.spec.priority` on the new pod |

## Security / Abuse Cases

- The priority class name comes from a Config CR that the operator owns and that is already the authority for the agent's image, resources and secret; this fix does not widen who can set it and adds no new input surface.
- An empty value must stay unset, so no agent is silently granted a priority it did not declare — a class is applied only where the Config names one.
- A service agent admitted at its declared priority competes with, and may preempt, lower-priority pods on the node. That is the field's declared intent and is unchanged by this fix: the class's own `value` and `preemptionPolicy` govern, exactly as they already do for Jobs.

## Suggested Decomposition

Prompts should be generated in this order — each row is a single prompt with a clear scope.

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Stamp the resolved priority class onto the service StatefulSet's pod template, guarded on non-empty, plus the set-case and unset-case specs | 1, 2 | 1, 2, 3, 7 | — |
| 2 | Correct the field's documentation (type comment + CRD spec doc) and append the changelog bullet under the existing `## Unreleased` | 3, 4 | 4, 5, 6 | prompt 1 (shares its wording) |

Rationale: prompt 1 is the behavioural change and its regression lock, confined to one function in one file plus its test file. Prompt 2 is doc-only and depends on prompt 1 only for the phrasing of what the field now does. AC 8 is observed on nuke dev, which the YOLO container cannot reach — it is operator-executable (see `# Verification`) and generates no prompt.

## Do-Nothing Option

Leaving it means every `type: service` agent runs at priority 0 while the Config that declares its class is silently ignored on that path. Observed cost on nuke dev 2026-10-09: `claude-interactive` had been preempted by trading-agent Jobs at priority 500, and `pi-interactive` is in the same state. A service agent exists to hold an identity and a session across time; being the first workload evicted whenever batch work arrives defeats exactly that, and the failure is silent — the Config looks correct, the executor logs a successful reconcile, and only the pod's `.spec.priority` shows the field was dropped. The fix is a small change to a value already in hand.
