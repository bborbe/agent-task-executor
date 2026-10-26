---
status: draft
branch: feat/config-service-namespace
---

## Summary

- `ConfigSpec` gains a `ServiceNamespace` field so a `spec.type: service` agent's long-running StatefulSet can be deployed in a namespace other than the executor's
- An empty `ServiceNamespace` keeps today's behaviour exactly: the StatefulSet, its Service and its session PVC land in the executor's own namespace
- A non-empty `ServiceNamespace` places all three in that namespace; setting it explicitly to the executor's own namespace is rejected, mirroring `JobNamespace`, so no Config can opt *in* to co-location
- `ResolveServiceNamespace` is a sibling of `ResolveJobNamespace` and carries the same rule set, so there is one namespace-resolution idiom in the executor rather than two
- The executor-side mechanism only. The target namespace, its RBAC and its ingress wiring are `bborbe/agent`'s problem (chart spec, separate change)

## Problem

`ServiceReconciler` builds every object with a single namespace captured at construction — `objectMetaBuilder.SetNamespace(r.namespace)`, `r.deployer.Undeploy(ctx, r.namespace, …)`. A `spec.type: service` agent therefore cannot leave the executor's namespace, however the Config is written.

That is now the last blocker on `bborbe/agent`'s per-domain sandbox partition: every job-type Config moves its Jobs by `jobNamespace`, and the two service agents (`claude-interactive`, `pi-interactive`) stay beside the control plane because no equivalent field exists. The partition task's SC7 requires them in `agent-sandbox`, and explicitly forbids reading the criterion as satisfied by any Config edit — so the mechanism has to exist in the executor first.

There is a second, quieter reason. A Job-pod-only egress policy is default-deny on **ingress** as well. A service agent moved without a policy that admits traffic reads healthy by every Job-side probe and still cannot be reached, which is why this spec is a prerequisite rather than the whole change.

## Goal

A `spec.type: service` Config can name the namespace its StatefulSet lives in, and every object the reconciler creates, updates or deletes for that Config lands in the resolved namespace. With the field unset, nothing about the executor's behaviour changes.

## Non-goals

- Creating the target namespace, its NetworkPolicy, or the RBAC that lets the executor manage StatefulSets, Services and PVCs there — `bborbe/agent` owns all manifests
- Migrating session data. A StatefulSet moved to a new namespace creates a **fresh, empty** claim from its `volumeClaimTemplates`; it reads `Bound` and attached whether or not a byte survived, so the data decision belongs to the operator, not to this mechanism
- Cross-namespace ingress wiring. Moving the StatefulSet moves its Service with it, so the Ingress in `agent` can no longer name it by short name; that repointing is part of the chart change and must be proved by the live ingress probe, never by DNS resolution alone
- Watching `Config` resources outside the executor's namespace — Configs stay where they are
- Any change to the Job path

## Acceptance Criteria

- [ ] `make precommit` exits 0
- [ ] `ConfigSpec` has a `ServiceNamespace` field — `grep -n 'ServiceNamespace' k8s/apis/agent.benjamin-borbe.de/v1/types.go` returns ≥1 line
- [ ] The binary's own CRD schema carries the field — `grep -c 'serviceNamespace' pkg/k8s_connector.go` returns ≥1. This is the schema `SetupCustomResourceDefinition` writes on every process start; a field missing here is pruned from every Config within seconds of the next restart, and the failure is silent
- [ ] Unset `ServiceNamespace` resolves to the executor's own namespace — table-driven `TestResolveServiceNamespace` asserts: executor ns `agent` + empty → `agent`; executor ns `dev` + empty → `dev`; `"agent-sandbox"` → `"agent-sandbox"`. `go test ./... -run TestResolveServiceNamespace -v` stdout contains `--- PASS: TestResolveServiceNamespace`
- [ ] A `ServiceNamespace` equal to the executor's own namespace is rejected — `TestResolveServiceNamespace` includes a case asserting a non-nil error whose message contains `may not equal the executor namespace`. Negative evidence of the rule's purpose: without it a Config can express "run beside the control plane" as a deliberate choice, which is the state this change exists to leave behind
- [ ] **The Config value reaches the reconciler.** A `v1.Config` with `spec.serviceNamespace: agent-sandbox` passed through `convert()` (`pkg/config_resolver.go`) yields an `AgentConfiguration` whose `ServiceNamespace` is `agent-sandbox`, and one with the field unset yields empty. Without this the field can exist in the CRD and be dropped by the resolver while every AC above still passes against a hand-built `AgentConfiguration`. Test `TestConvertCarriesServiceNamespace`
- [ ] **The StatefulSet is created in the resolved namespace** — test `TestServiceReconcileTargetNamespace` asserts the fake clientset's recorded `create` action namespace for the StatefulSet equals `agent-sandbox` for a Config naming it, and equals the executor's namespace when unset
- [ ] **The session volume follows by construction, and is not asserted as a separate object** — `ServiceReconciler` builds only the StatefulSet; its `volumeClaimTemplates` create the session PVC in the StatefulSet's own namespace, and the Service is rendered by the chart, not by the executor. So the PVC assertion is the StatefulSet-namespace assertion above, and there is deliberately **no** `create` action for a PVC or a Service to assert. An AC demanding one would be unfinishable against correct code — this was wrong in the first draft of this spec and is corrected here
- [ ] **Undeploy follows it too** — `UndeployService` today takes only `(ctx, name)` and therefore cannot know a Config's namespace; it gains the resolved namespace as a parameter, and `TestServiceUndeployTargetNamespace` seeds a StatefulSet in `agent-sandbox`, drives the loop for a Config naming that namespace, and asserts the recorded `delete` action targets `agent-sandbox`. A delete aimed at the executor's namespace leaves an orphan running and reports success
- [ ] **Unset is byte-identical to today** — every existing service-reconciler test passes unchanged, and a render-level assertion compares the object produced for a Config without the field against the same object built by `origin/master`'s code path. `go test ./... -run TestServiceReconcile` stdout contains `--- PASS`
- [ ] The CRD doc states the field's default and its rejection rule, not merely the field name — `grep -c 'serviceNamespace' docs/agent-crd-specification.md` ≥1 **and** `grep -ci 'executor namespace' docs/agent-crd-specification.md` ≥1
- [ ] `CHANGELOG.md` carries the change under `## Unreleased` — `awk '/^## /{sec=$0} /[Ss]ervice[Nn]amespace/{print sec}' CHANGELOG.md` prints `## Unreleased`

## Verification

```bash
make precommit
go test ./... -run 'TestResolveServiceNamespace|TestConvertCarriesServiceNamespace|TestServiceReconcileTargetNamespace|TestServiceUndeployTargetNamespace' -v
```

## Follow-ups

- `bborbe/agent`: `agents[].serviceNamespace` passthrough, the sandbox Role verbs for StatefulSets/Services/PVCs, the ingress repointing, and a policy that admits **ingress** to the interactive service. The live proof — HTTP `200` plus a successful `attention-controller` call on `:18081` after the move — belongs there
- The two service agents' data disposition is an operator decision recorded before the move, never reconstructed after: the moved StatefulSet's claim is fresh and empty by construction
