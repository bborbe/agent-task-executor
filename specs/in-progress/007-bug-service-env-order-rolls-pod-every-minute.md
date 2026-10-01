---
status: prompted
approved: "2026-10-01T21:34:51Z"
generating: "2026-10-01T21:36:38Z"
prompted: "2026-10-01T21:47:23Z"
branch: dark-factory/bug-service-env-order-rolls-pod-every-minute
---

## Summary

- A service agent's StatefulSet is re-rendered roughly once a minute, and **every re-render rolls the pod**: a fresh pod UID appears while `restarts` stays 0, so this is a rollout and not a crash loop.
- The Config CR that owns the StatefulSet is never edited (`generation=1`), so nothing upstream accounts for the churn.
- Between two consecutive generations, the **only** thing that differs is the order of the container's `env` list. The env *contents* are byte-identical — the same entries hashed **sorted** produce the same digest across every generation observed.
- Observed on nuke dev 2026-10-01 against both service agents, `pi-interactive` and `claude-interactive`; each advanced one generation per minute, 59-65s apart.
- Consequence: a `port-forward` to the pod dies with `network namespace for sandbox ... is closed` on every roll, so a long-running service agent cannot be exercised by curl for longer than about a minute, and the identity the service exists to preserve is disturbed on every roll even though the session transcript survives on the PVC.

## Problem

`agent-task-executor` reconciles one StatefulSet per Config whose `spec.type` is `service`, and its reconcile loop runs about once a minute. Each pass re-renders the StatefulSet from the Config and writes it back. The rendered object is not stable across passes: the container's `env` list comes out in a different order each time, so the pod template differs, the API server bumps `metadata.generation`, and the StatefulSet controller rolls the pod. A Config nobody has touched therefore produces a permanent, self-inflicted rollout loop — and because the loop is the reconciler's normal steady state, it never converges and never errors.

## Goal

An unchanged Config CR produces a byte-identical StatefulSet on every reconcile pass, so the StatefulSet's `generation` and its pod's UID stay constant for as long as the Config is unchanged. A genuine Config change still rolls the pod.

## Reproduction

Repo: `bborbe/agent-task-executor` at `f12f37a` (`release v0.18.3`), deployed to nuke dev. Dark-factory: `v0.196.0`.

Two service agents exist on nuke dev (`pi-interactive`, `claude-interactive`); both reproduce. `claude-interactive` is shown below because it was sampled at 5s resolution.

The smallest config that exhibits the bug is any `type: service` Config carrying **more than one** `env` entry. `pi-interactive` is exactly that shape — three Config-declared keys alongside the `AGENT_TYPE` stamp the executor adds itself:

```yaml
apiVersion: agent.benjamin-borbe.de/v1
kind: Config
metadata:
  name: pi-interactive
  namespace: dev
spec:
  type: service
  assignee: pi-interactive
  image: docker.prod.nuke.benjamin-borbe.de:443/bborbe/agent-pi:v0.5.1
  env:
    MODEL: <model id>
    PUSHGATEWAY_URL: <pushgateway url>
    ALLOWED_TOOLS: <comma-separated tool names>
```

The trigger is the entry count, not the values: a one-entry map has only one possible ordering, so the churn needs two or more entries to appear at all.

The executor reconciles both, once a minute:

```
$ kubectlnukedev -n dev logs deploy/agent-task-executor --since=5m | grep -c "reconciled service statefulset"
10
$ kubectlnukedev -n dev logs deploy/agent-task-executor --since=5m | grep "reconciled service statefulset" | tail -2
I1001 21:12:59.946047       1 service_reconciler.go:135] reconciled service statefulset pi-interactive for assignee pi-interactive with image docker.prod.nuke.benjamin-borbe.de:443/bborbe/agent-pi:v0.5.1
I1001 21:13:00.936141       1 service_reconciler.go:135] reconciled service statefulset claude-interactive for assignee claude-interactive with image docker.prod.nuke.benjamin-borbe.de:443/bborbe/agent-claude:v0.3.0
```

Three consecutive generations of `claude-interactive`, projected with field-scoped reads (`-o jsonpath` only — never a whole-object dump, whose `last-applied-configuration` annotation embeds the container `env` in plaintext). Each column is a sha256 prefix:

```
21:17:21 gen=16 tpl=2efc0c78 envORDER=6e9b07bc envSORTED=4700af9d img=af28b5e4 probe=b61d2a4f rest=882a84bc
21:18:02 gen=17 tpl=8f4a0d4a envORDER=33217bc4 envSORTED=4700af9d img=af28b5e4 probe=b61d2a4f rest=882a84bc
21:19:04 gen=18 tpl=21c92e8c envORDER=b387d37c envSORTED=4700af9d img=af28b5e4 probe=b61d2a4f rest=882a84bc
```

The template hash and the rendered-order env hash change on every generation. The **sorted** env hash is identical across all three, as are the image, the readiness probe and the rest of the container spec. So the env contents never change — only their order.

Cadence, from a 5s-resolution series:

```
21:17:05  claude:g15 ... pod93f6c0a4
21:18:04  claude:g17 ... podb7c39a68
21:19:03  claude:g18 ... pod6d603caa
```

Each generation advance carries a new pod UID, with `restarts=0` throughout. The same cadence was measured on `pi-interactive` (`g818 -> g819 -> g820` at 21:18:04 and 21:19:03). Every non-template spec field — `replicas`, `updateStrategy`, `revisionHistoryLimit`, `minReadySeconds`, `persistentVolumeClaimRetentionPolicy`, `podManagementPolicy`, `serviceName` — hashed **unchanged** across the same window.

## Expected vs Actual

**Expected.** The reconciler's own interface contract states that an unchanged Config is a no-op: `ServiceReconciler.ReconcileService` is documented at `pkg/spawner/service_reconciler.go:69-71` as *"It is idempotent: an existing StatefulSet has only its mutable fields merged, so an image change rolls the pod and **an unchanged Config is a no-op**."* Kubernetes agrees: a StatefulSet's `metadata.generation` advances only when its spec actually changes, and the controller rolls a pod only when the pod template changes. So a Config that has not been edited should leave `generation` and the pod UID untouched indefinitely.

**Actual.** `generation` advances once a minute and the pod is recreated with it, on a Config whose CR sits at `generation=1` and has never been edited. The delta is confined to one field: the pod template's env list is reordered on each pass while its contents stay identical.

## Why this is a bug

The behaviour contradicts the reconciler's own documented contract (above), and it contradicts the purpose of the service agent shape. The `type: service` variant was introduced specifically so an agent's identity and session outlive any single task; a workload that is destroyed and recreated every minute cannot hold a connection, cannot be addressed by a caller for longer than the rollout interval, and makes any long-running interaction — a curl against `POST /prompt`, a held `port-forward` — fail mid-flight. The rollout is not a symptom of load or of a failing container: `restarts` stays 0 and the Config is inert, so the reconciler is manufacturing the churn from its own output.

## Desired Behavior

1. Reconciling the same Config twice in a row produces two byte-identical StatefulSets, including the container's env list in the same order.
2. Reconciling the same Config repeatedly — at least 50 times in one process — produces exactly one distinct env ordering, not a distribution over several.
3. The ordering is stable across process restarts, not merely within one.
4. `metadata.generation` on a service StatefulSet does not advance while its Config is unchanged, over a window of at least 10 minutes.
5. The pod is not recreated while its Config is unchanged — the pod UID is constant across the same window.
6. A genuine Config change still reaches the cluster: changing the Config's image rolls the pod, so the reconciler has not been reduced to a no-op.
7. A `port-forward` to the pod, held across a window in which no Config changes, survives and completes a `POST /prompt` round-trip.

## Constraints

- The env list's **contents** must not change: the same Config must still yield the same `AGENT_TYPE` stamp and the same Config-declared entries. Only the ordering is at issue.
- The Job path (`type: job`, `pkg/spawner/job_spawner.go`) is out of scope for this fix and must not regress. It renders env from the same Config map shape, and carries the same latent ordering instability, but a Job is created once per task rather than re-updated, so it does not manifest as churn. A separate follow-up covers it.
- Immutable StatefulSet fields (`selector`, `serviceName`, `volumeClaimTemplates`) must not be sent on update — the API server rejects any change to them.
- The library `github.com/bborbe/k8s` is pinned at `v1.14.16` in `go.mod:12` and is not modified by this fix. Its `statefulSetDeployer.Deploy` calls `Update` unconditionally on the existing object; that is an amplifier, not the trigger, since a genuinely no-op update does not bump `generation`.
- `make precommit` must pass.

## Acceptance Criteria

- [ ] **Rendering the same Config repeatedly yields one distinct env ordering.** Evidence: a spec in `pkg/spawner/` that builds the service env for one Config at least 50 times in a single process and asserts exactly one distinct ordering is observed; the suite fails if two orderings appear. Observed by `make test` exiting 0 with that spec's name in the passing list.
- [ ] **Two consecutive reconciles of one Config produce byte-identical pod templates.** Evidence: a spec asserting the `env` slices of the two rendered templates are equal element-for-element and in the same index order — not merely set-equal, which is what the existing `ContainElement` assertions check. Observed by `make test` exiting 0 with that spec's name in the passing list.
- [ ] **Post-Deploy (Rung-2):** `generation` on `statefulset/pi-interactive` is unchanged across a >=10-minute window on nuke dev. Evidence: two `kubectlnukedev -n dev get sts pi-interactive -o jsonpath='{.metadata.generation}'` samples, >=10 minutes apart, returning the same integer, both quoted in the PR body.
  - `deploy_check:` `kubectlnukedev -n dev get deploy/agent-task-executor -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` to be filled with the exact tag at prompt time when `EXECUTOR_VERSION` is bumped
- [ ] **Post-Deploy (Rung-2):** the pod UID is unchanged across that same window. Evidence: two `kubectlnukedev -n dev get pod pi-interactive-0 -o jsonpath='{.metadata.uid}'` samples matching, both quoted.
  - `deploy_check:` `kubectlnukedev -n dev get deploy/agent-task-executor -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` to be filled with the exact tag at prompt time when `EXECUTOR_VERSION` is bumped
- [ ] **Post-Deploy (Rung-2):** a `port-forward` to `pi-interactive-0` held across that window completes a `POST /prompt` round-trip and is never killed by `network namespace for sandbox ... is closed`. Evidence: the curl output and the absence of that string in the port-forward's stderr, both quoted.
  - `deploy_check:` `kubectlnukedev -n dev get deploy/agent-task-executor -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` to be filled with the exact tag at prompt time when `EXECUTOR_VERSION` is bumped
- [ ] **A Config change still rolls the pod** — the fix must not achieve stability by ceasing to write. Evidence: a spec that reconciles the same Config twice with a changed `Image` between the calls and asserts the rendered image differs; plus the existing `rolls the pod when the image changes` spec still passing. Observed by `make test` exiting 0 with both spec names in the passing list.
- [ ] **The Job path is untouched.** Evidence: `git diff --stat` shows no change under `pkg/spawner/job_spawner.go`, and the existing job-spawner specs pass.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — format, generate, lint, unit + integration tests all clean.
- `go test ./pkg/spawner/... -count=1` — the ordering specs pass.

### Operator-executable (runs on the host after PR merge, spec verification ladder)

- `/github-release-repo-trigger` after merge, then hand-build + push the released image (CI runs `make precommit` only and does not build it).
- `cd ~/Documents/workspaces/nuke/agent && BRANCH=dev make apply` — mirror + helm upgrade on dev, per the *Deploy Mirrored Agent Service* runbook. The PR itself is opened by hand with `gh pr create`: this repo sets `pr: false` in `.dark-factory.yaml`, so dark-factory commits on the feature branch but does not open the PR.
- Sample `generation` and the pod UID at the start and end of a >=10-minute window and compare.
- Hold a `port-forward` across that window and complete a `POST /prompt`.

## Suggested Decomposition

Prompts should be generated in this order — each row is a single prompt with a clear scope.

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Deterministic env render, plus the specs that lock it | 1, 2, 3, 6 | 1, 2, 6, 7 | — |

Rationale: the code change is one function in one file, so it is a single prompt. DBs 4, 5 and 7 are observed on the deployed dev cluster, which the YOLO container cannot reach — they are operator-executable (see `# Verification`) and generate no prompt; they run on the post-merge ladder.

## Failure Modes

| Trigger | Expected behavior | Recovery |
|---|---|---|
| Two Configs share an env key set but differ in one value | Both render deterministically; the value difference still rolls the respective pod | Run the repeated-render spec once per Config and confirm each yields one distinct ordering, and that neither ordering depends on the other Config |
| The fix suppresses all updates rather than stabilising the render | Caught by the "a Config change still rolls the pod" criterion | `git revert` the change and re-run the image-change spec; the reconciler must keep writing when the Config genuinely changes |
| `generation` still advances after deploy | The diagnosis was incomplete — some other template field also varies | Re-run the field-scoped projection from `## Reproduction` against two fresh generations and diff; the spec stays open until a second varying field is named |
| A Config's env map grows past one Go map bucket | Ordering stays stable — the fix must not rely on the single-bucket case | Extend the repeated-render spec with a map of more than 8 entries and confirm it still yields one distinct ordering |
