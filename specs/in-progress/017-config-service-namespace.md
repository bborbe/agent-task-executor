---
status: approved
approved: "2026-10-26T06:52:56Z"
branch: dark-factory/config-service-namespace
---

## Summary

- `ConfigSpec` gains a `ServiceNamespace` field so a `spec.type: service` agent's long-running StatefulSet can be deployed in a namespace other than the executor's
- An empty `ServiceNamespace` keeps today's behaviour exactly: the StatefulSet, and by construction its session PVC, land in the executor's own namespace
- A non-empty `ServiceNamespace` places the StatefulSet in that namespace; setting it explicitly to the executor's own namespace is rejected, mirroring `JobNamespace`
- `ResolveServiceNamespace` shares `ResolveJobNamespace`'s rule through one helper, so the executor has one namespace-resolution idiom rather than two that can drift
- The executor-side mechanism only. The target namespace, its RBAC, its **Service**, its ingress and its ingress-admitting policy are `bborbe/agent`'s problem
- ⚠️ The executor creates **no** Service object — the chart does. Moving the StatefulSet therefore does **not** move the Service, and the ingress repointing is real work, not a consequence

## Desired Behavior

1. A `spec.type: service` Config with `serviceNamespace: agent-sandbox` produces its StatefulSet in `agent-sandbox`, and one with the field unset produces it in the executor's own namespace, unchanged from today.
2. A Config naming the executor's own namespace is **refused at reconcile time** — no StatefulSet is created and the reconcile returns an error naming the Config. The refusal is not merely a property of the resolver function; it is observed on the path that would otherwise create the object.
3. `spec.serviceNamespace` survives the whole path from API object to `AgentConfiguration`: `convert()` carries it, `ConfigSpec.Equal` detects a change to it alone, and `TaggedConfigurations` preserves it.
4. The binary's own CRD schema declares the field with the same `Pattern` and `MaxLength` as `jobNamespace`, so an invalid namespace is refused at admission rather than at StatefulSet create time.
5. When a Config's resolved namespace **changes**, the executor's StatefulSet in the previous namespace is removed — otherwise the rollout produces two running agents, the old one still holding the Config's secrets.
6. `UndeployService` deletes in the Config's resolved namespace, not the executor's.
7. With the field unset everywhere, every object the reconciler builds is byte-identical to what `origin/master` builds today.

## Problem

`ServiceReconciler` builds every object with a single namespace captured at construction — `objectMetaBuilder.SetNamespace(r.namespace)`, `r.deployer.Undeploy(ctx, r.namespace, …)` — and its `StatefulSetInterface` is bound to that namespace at factory time (`kubeClient.AppsV1().StatefulSets(namespace.String())`). A `spec.type: service` agent therefore cannot leave the executor's namespace, however the Config is written.

That is the last blocker on `bborbe/agent`'s per-domain sandbox partition: every job-type Config moves its Jobs by `jobNamespace`, and the two service agents (`claude-interactive`, `pi-interactive`) stay beside the control plane because no equivalent field exists. The partition task's SC7 requires them in `agent-sandbox` and forbids reading the criterion as satisfied by any Config edit, so the mechanism must exist here first.

There is a second, quieter reason. A Job-pod-only egress policy is default-deny on **ingress** as well. A service agent moved without a policy that admits traffic reads healthy by every Job-side probe and still cannot be reached — which is why this is a prerequisite rather than the whole change.

## Goal

A `spec.type: service` Config can name the namespace its StatefulSet lives in, every create and delete for that Config lands in the resolved namespace, a namespace change does not leave the previous copy running, and with the field unset nothing about the executor's behaviour changes.

## Non-goals

- Creating the target namespace, its NetworkPolicy, or the RBAC that lets the executor manage StatefulSets and PVCs there — `bborbe/agent` owns all manifests
- Creating or moving the **Service**. The executor creates none; the chart renders it in the control namespace, so a moved StatefulSet needs a Service in the new namespace and an ingress repointed at it. That is chart work, and it is what makes the live ingress proof non-trivial
- Migrating session data. A StatefulSet moved to a new namespace creates a **fresh, empty** claim from its `volumeClaimTemplates`; it reads `Bound` and attached whether or not a byte survived, so the data decision belongs to the operator
- Cross-namespace ingress wiring and the ingress-admitting policy
- Watching `Config` resources outside the executor's namespace — Configs stay where they are
- Any change to the Job path

## Constraints

- Existing `Config` CRs must remain valid unedited — the field is additive and optional.
- **Unset must be byte-identical to today.** Existing service-reconciler tests may change only for the added `UndeployService` parameter; no assertion may be weakened or deleted.
- **One resolution rule.** `ResolveServiceNamespace` must not copy `ResolveJobNamespace`'s body; the rule is extracted into one unexported helper taking the field name, and `ResolveJobNamespace`'s existing error text and tests stay untouched.
- **The namespace-bound client is a named ripple.** `k8s.StatefulSetInterface` wraps a namespaced handle, so a namespace parameter alone cannot change where a lookup lands. The reconciler must hold something cluster-wide (a `kubernetes.Interface`, or a factory) and derive the namespaced handle per call. `NewServiceReconciler`'s signature and its binding in `pkg/factory/factory.go` change with it.
- **The counterfeiter mock is a named ripple.** `//counterfeiter:generate` above `ServiceReconciler` means the `UndeployService` signature change requires `mocks/service_reconciler.go` regeneration, and `service_reconcile_loop_test.go`'s `UndeployServiceArgsForCall` assertion changes with it.
- **Rollout assumption, stated because it is silent when violated:** before any Config sets the field, the target namespace must exist, already permit StatefulSets/PVCs for the executor's service account, and already hold that Config's `secretName` Secret. Without all three the reconciler fails on every pass and the agent is down.
- Do NOT add a namespace, NetworkPolicy, RBAC grant, ingress, Service object or `securityContext` change.
- Do NOT make the reconciler create a Service or a PVC. It creates only the StatefulSet, and this change must not widen that.
- Do NOT change the Job path: `job_spawner.go`, the Job lookups, or the claim fence.

## Failure Modes

| Failure | Symptom | Reversibility |
| --- | --- | --- |
| Target namespace does not exist | `create` returns 404 on every reconcile pass; the agent never starts | Full — create the namespace, then `kubectl -n <target> get ns` reads `Active` |
| Executor lacks RBAC in the target namespace | 403 on every pass; the loop logs and retries | Full — grant the Role; verify with `kubectl auth can-i create statefulsets -n <target> --as=<executor-sa>` |
| `ResourceQuota` exceeded in the target namespace | StatefulSet created, pod never schedules | Full — raise the quota or shrink the request |
| Namespace changed on a live Config, old copy not removed | **Two agents run at once**; the old one still holds the Config's Secret env and its retained PVC | **Partial** — the old pod has already run with the secrets; deleting the old StatefulSet stops it, but any session state written there is on a claim that will not be reused |
| Field removed while a sandbox StatefulSet exists | The sandbox copy is orphaned and unreachable by a delete that resolves to the executor namespace | Partial — the same cluster-wide sweep that catches a namespace change must also run on the undeploy path, or the orphan is found only by hand |
| **Executor lacks cluster-scoped `list` on `statefulsets`** | The previous-namespace sweep cannot see the old copy, so the move silently leaves two agents running. A fake clientset enforces no authorization, so **no unit test can catch this** | Full — `bborbe/agent` grants cluster-wide `list`/`delete` on `statefulsets`; the sweep must log the failure and **not** fail a reconcile that already created its StatefulSet, since a 403 there would take every service agent down |
| Moved agent starts with an empty session volume | The claim reads `Bound` and attached, so nothing looks wrong; the agent simply has no history | None for the old data unless copied first — this is the operator decision the Non-goal defers |

## Do-Nothing Option

Leave the field out. The partition stays blocked at SC7: `claude-interactive` and `pi-interactive` remain in the control namespace, which is default-allow egress, so the two workloads that hold the longest-lived credentials keep the widest network reach in the fleet. Nothing breaks immediately — that is precisely why it has survived — and every Job-side probe stays green while the gap remains.

## Security / Abuse Cases

- A Config author can still name a **third**, unprotected namespace; the guard rejects only the executor's own namespace. Accepted: Configs are operator-authored and version-controlled, and the chart's namespace set is what defines the sanctioned targets. Widening the guard to an allowlist would put the target list in two repos.
- The moved workload holds `ANTHROPIC_AUTH_TOKEN` and a session volume. Moving it into a default-deny namespace **reduces** reach; the risk introduced is ingress loss, which fails closed.
- A malformed namespace value is refused at admission by the CRD `Pattern`, not at create time — the schema-parity AC exists for this reason.

## Acceptance Criteria

- [ ] `make precommit` exits 0
- [ ] `ConfigSpec` has a `ServiceNamespace` field — `grep -n 'ServiceNamespace' k8s/apis/agent.benjamin-borbe.de/v1/types.go` returns ≥1 line
- [ ] The binary's own CRD schema carries the field — `grep -c 'serviceNamespace' pkg/k8s_connector.go` returns ≥1. A field missing here is pruned from every Config within seconds of the next restart, silently
- [ ] **The schema entry is as strict as its sibling** — `TestServiceNamespaceSchemaParity` (internal, `pkg/k8s_connector_internal_test.go`) asserts `configSpecProperties()["serviceNamespace"]` has the same `Pattern` and `MaxLength` as `["jobNamespace"]`, not merely that the key exists. `grep -c 'serviceNamespace' ≥1` passes on a bare `{Type: "string"}`, which would admit `Dev_Sandbox` at admission and fail at create time instead
- [ ] Unset resolves to the executor's own namespace — table-driven `TestResolveServiceNamespace`: `agent` + empty → `agent`; `dev` + empty → `dev`; `agent-sandbox` → `agent-sandbox`; `agent` + `agent` → non-nil error containing `may not equal the executor namespace`. `go test ./... -run TestResolveServiceNamespace -v` stdout contains `--- PASS: TestResolveServiceNamespace`
- [ ] **The refusal is observed on the reconcile path, not only in the resolver** — `TestServiceReconcileRejectsExecutorNamespace` drives a Config naming the executor's namespace and asserts the recorded `create` action count for `statefulsets` is **0** and the loop returns an error naming the Config. Without this the rejection rule is dead code and a Config can deploy beside the control plane while every other AC stays green
- [ ] **`Equal` detects a change to this field alone** — a `types_test.go` case asserting two Configs differing only in `ServiceNamespace` are not equal, plus a JSON round-trip, copying the `priorityClassName` shape. Omitted from `Equal`, an edit that changes only this field is invisible and an existing service Config never moves
- [ ] **`TaggedConfigurations` preserves it** — `TestTaggedConfigurationsCarriesServiceNamespace` asserts the value survives the branch-tagged copy, which rebuilds every field explicitly
- [ ] **`convert()` carries it** — a `v1.Config` with `spec.serviceNamespace: agent-sandbox` yields an `AgentConfiguration` with that value; unset yields empty. `go test ./... -run TestConvertCarriesServiceNamespace -v` stdout contains `--- PASS: TestConvertCarriesServiceNamespace`
- [ ] **The StatefulSet is created in the resolved namespace** — `TestServiceReconcileTargetNamespace` asserts the recorded `create` action namespace equals `agent-sandbox` when named, and the executor's namespace when unset. `go test ./... -run TestServiceReconcileTargetNamespace -v` stdout contains `--- PASS: TestServiceReconcileTargetNamespace`
- [ ] **A namespace change removes the previous copy** — `TestServiceReconcileRemovesPreviousNamespace` seeds an executor-owned StatefulSet for the Config in the executor namespace, reconciles with `serviceNamespace: agent-sandbox`, and asserts a `delete` action against the executor namespace and a `create` in `agent-sandbox`. A create-only implementation leaves two agents running, which is the failure this rollout would hit first
- [ ] **Undeploy targets the resolved namespace** — `TestServiceUndeployTargetNamespace` asserts the recorded `delete` action namespace is `agent-sandbox` for a Config naming it. `go test ./... -run TestServiceUndeployTargetNamespace -v` stdout contains `--- PASS: TestServiceUndeployTargetNamespace`
- [ ] **A type change does not orphan the sandbox copy** — `TestServiceUndeployRemovesPreviousNamespace` drives the loop for a job-type Config with an empty `serviceNamespace` while an owned StatefulSet sits in `agent-sandbox`, and asserts the delete lands there. The undeploy path resolves to the executor namespace, so without the sweep the sandbox workload runs forever with the Config's Secret
- [ ] **A failed sweep does not fail the reconcile** — a fake-clientset reactor returns 403 on the cluster-wide list; `ReconcileService` still succeeds and records no delete. A 403 here would otherwise take every service agent down; a fake clientset enforces no authorization, so a reactor is the only way to exercise the path
- [ ] **Unset is unchanged** — `TestServiceReconcileUnsetNamespace` asserts the recorded `create` namespace is the executor's for an empty field, and `make precommit` proves every pre-existing service-reconciler assertion still passes untouched. This replaces an earlier AC that asked for a comparison against `origin/master`'s code path, which a Go test cannot make
- [ ] The CRD doc states the field's default and its rejection rule **on its own row** — `grep -n 'spec.serviceNamespace' docs/agent-crd-specification.md | grep -ci 'executor namespace'` returns ≥1. A bare `grep -ci 'executor namespace'` over the whole file already passes today because the `jobNamespace` row contains it, so it guards nothing
- [ ] `CHANGELOG.md` carries the change under `## Unreleased` — `awk '/^## /{sec=$0} /[Ss]ervice[Nn]amespace/{print sec}' CHANGELOG.md` prints `## Unreleased`
- [ ] `TestResolveJobNamespace` still passes — `go test ./... -run TestResolveJobNamespace -v` stdout contains `--- PASS: TestResolveJobNamespace`. This is what proves the shared-helper extraction did not change the job-side rule or its message

## Verification

```bash
go test ./... -run 'TestResolveServiceNamespace|TestServiceReconcileRejectsExecutorNamespace|TestServiceReconcileTargetNamespace|TestServiceReconcileRemovesPreviousNamespace|TestServiceUndeployTargetNamespace|TestConvertCarriesServiceNamespace|TestTaggedConfigurationsCarriesServiceNamespace|TestResolveJobNamespace|TestServiceNamespaceSchemaParity' -v
```

```bash
grep -n 'ServiceNamespace' k8s/apis/agent.benjamin-borbe.de/v1/types.go
grep -c 'serviceNamespace' pkg/k8s_connector.go
grep -n 'spec.serviceNamespace' docs/agent-crd-specification.md | grep -ci 'executor namespace'
ROOTDIR=/workspace make precommit
```

Every AC is container-executable, so there is **no operator rung** here — deliberately omitted rather than invented. The live proof (HTTP `200` plus a successful `attention-controller` call on `:18081` after the move) belongs to the `bborbe/agent` change, which also owns the Service and the ingress.

## Suggested Decomposition

The three layers are named because they *are* separable, but this ships as **one prompt** deliberately: layer 1 alone is inert and unobservable, and layers 2 and 3 are the same edit to the same function — the audit of the first prompt draft found that every hazard (the namespace-bound client, the orphan on move) lives exactly at the seam between them, so splitting would ship a reconciler that creates in the new namespace and cannot delete from the old one, between two PRs, with nothing catching it. The split stays recorded so that if this prompt proves too large to review, the seam is already named.

1. **Inert field**: `ConfigSpec.ServiceNamespace` + `Equal` + `AgentConfiguration` + `TaggedConfigurations` + `convert()` + CRD schema with parity + `ResolveServiceNamespace` (shared helper) + tests. No behaviour change; the executor deploy is a no-op.
2. **Reconciler follows it**: the cluster-wide client ripple, resolved namespace on create, `UndeployService` signature + mock regeneration + loop caller, refusal AC, target-namespace AC. Still no Config sets the field.
3. **Move safety**: the previous-namespace removal, its AC, the unset-namespace substitution, docs and CHANGELOG.

## Follow-ups

- `bborbe/agent`: `agents[].serviceNamespace` passthrough, the sandbox Role verbs for StatefulSets and PVCs, a **Service in the target namespace**, the ingress repointed at it, and a policy that admits **ingress** — with the live proof
- The two service agents' data disposition is an operator decision recorded before the move, never reconstructed after: the moved StatefulSet's claim is fresh and empty by construction
