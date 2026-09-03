---
status: prompted
approved: "2026-08-08T20:38:48Z"
generating: "2026-09-03T17:46:06Z"
prompted: "2026-09-03T17:57:12Z"
branch: dark-factory/config-job-namespace
---

## Summary

- `ConfigSpec` gains a `JobNamespace` field so an agent's Jobs can be spawned in a namespace other than the executor's
- An unset `JobNamespace` resolves to `<executor-namespace>-agents-sandbox` — the safe value is the default, and it is derived per environment so dev and prod never share a sandbox
- Setting `JobNamespace` to the executor's own namespace is rejected, so no Config can opt out of isolation
- Spawned Job pods always carry `app: agent`, so a NetworkPolicy selector can match them
- Manifests (namespace, NetworkPolicy, RBAC) are out of scope here — they live in the `bborbe/agent` helm chart

## Problem

The executor spawns agent Jobs into its own namespace, so Jobs inherit the executor's network access and sit beside the executor Deployment, its Secrets, and Kafka credentials. There is no way to place a Job anywhere else.

Network isolation for agent Jobs (tracked in `bborbe/agent`) requires the Jobs to land in a separate, policy-restricted namespace and to carry a label the policy can select. Neither is possible today. This spec supplies the executor-side mechanism only; the namespace and policies that give it meaning are specified and deployed from `bborbe/agent`.

Background on the threat model and target topology: `bborbe/agent:docs/agent-network-security.md`.

## Goal

The executor resolves a target namespace per Config, defaulting to the sandbox, refusing the executor's own namespace, and stamps every spawned Job pod with `app: agent`. A Config author cannot produce an unisolated Job — not by omission, and not by explicit override.

## Non-goals

- Creating the sandbox namespace, NetworkPolicy, or RBAC — `bborbe/agent` owns all manifests
- Watching `Config` resources across multiple namespaces — Configs stay in the executor's namespace; cross-namespace *spawning* does not require cross-namespace *watching*
- Migrating PVCs or re-seeding Secrets into the sandbox namespace (operational work)
- Per-agent network policy or egress rules

## Acceptance Criteria

- [ ] `make precommit` exits 0
- [ ] `ConfigSpec` has a `JobNamespace` field — `grep -n 'JobNamespace' k8s/apis/agent.benjamin-borbe.de/v1/types.go` returns ≥1 line
- [ ] Unset `JobNamespace` resolves to the per-environment sandbox — table-driven test `TestResolveJobNamespace` asserts: executor ns `dev` + empty → `dev-agents-sandbox`; executor ns `prod` + empty → `prod-agents-sandbox`; `"other-ns"` → `"other-ns"`. `go test ./... -run TestResolveJobNamespace -v` stdout contains `--- PASS: TestResolveJobNamespace`
- [ ] A `JobNamespace` equal to the executor's own namespace is rejected — `TestResolveJobNamespace` includes a case asserting a non-nil error whose message contains `may not equal the executor namespace`; `go test ./... -run TestResolveJobNamespace -v` stdout contains `--- PASS`
- [ ] A rejected Config does not spawn a Job — test `TestSpawnRejectsExecutorNamespace` asserts the fake clientset recorded zero `create` actions on `jobs`. Negative evidence: recorded action count for verb `create`, resource `jobs` is 0. `go test ./... -run TestSpawnRejectsExecutorNamespace -v` stdout contains `--- PASS`
- [ ] Spawned Job pods carry `app: agent` unconditionally — test `TestJobPodLabels` asserts the generated `batchv1.Job` pod template labels include `app: agent`, and that a conflicting `app` label set directly on the built pod template is overwritten by the spawner. This locks existing unconditional behaviour (`pkg/spawner/job_spawner.go`) as a regression guard rather than introducing it. `go test ./... -run TestJobPodLabels -v` stdout contains `--- PASS`
- [ ] The Job is created against the resolved namespace, not the executor's — test asserts the fake clientset's recorded `create` action namespace equals `dev-agents-sandbox` for an executor in `dev` with `JobNamespace` unset; `go test ./... -run TestSpawnTargetNamespace -v` stdout contains `--- PASS`
- [ ] The CRD doc states the field's default and its rejection rule, not merely the field name — `grep -c 'jobNamespace' docs/agent-crd-specification.md` ≥1 **and** `grep -c 'agents-sandbox' docs/agent-crd-specification.md` ≥1 **and** `grep -ci 'executor namespace' docs/agent-crd-specification.md` ≥1 (an appended line naming only the field would pass a bare name grep while documenting nothing)

## Verification

```bash
make precommit
go test ./... -run 'TestResolveJobNamespace|TestSpawnRejectsExecutorNamespace|TestJobPodLabels|TestSpawnTargetNamespace' -v
```

All ACs are container-executable. No deployed system is observed by this spec — the Post-Deploy checks that prove isolation actually works live in the `bborbe/agent` specs that ship the manifests.

## Desired Behavior

1. `ConfigSpec` carries `JobNamespace string` with json tag `jobNamespace,omitempty`.
2. Namespace resolution: empty → `<executorNamespace>-agents-sandbox` (derived, so `dev` → `dev-agents-sandbox`); any other value → that value; a value equal to the executor's own namespace → error, and the Job is not spawned. The default is derived rather than a literal because dev and prod run in shared namespaces on one cluster and must not share a sandbox.
3. The spawner creates the Job in the resolved namespace.
4. Every spawned Job's pod template carries the label `app: agent`, set by the spawner and not overridable by Config-supplied labels.
5. A resolution error is logged at ERROR with the Config name and the rejected namespace, and leaves the task untouched so a human notices (consistent with `docs/agent-crd-specification.md` failure handling).

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | `JobNamespace` field on `ConfigSpec` + `ResolveJobNamespace` derivation/rejection + unit tests | 1, 2 | 2, 3, 4 | — |
| 2 | Spawner wiring — resolved-namespace target, refuse-on-error, label lock + unit tests | 3, 4, 5 | 5, 6, 7 | 1 |
| 3 | CRD doc row covering default and rejection rule | — | 8 | 1 |

Rationale: the field and its resolution function are self-contained; the spawner consumes them; the doc row describes the settled semantics and lands last.

## Constraints

- Existing `Config` CRs must remain valid unedited — `JobNamespace` is additive and optional
- The default must be the *safe* value: omitting the field may never place a Job in the executor's namespace
- The Kafka result-publishing path must not change; the agent still publishes its own result
- No new capability, securityContext change, or RBAC grant is introduced by this spec
- `TaskType`/`TaskTypes` routing behavior is untouched

## Failure Modes

| Trigger | Expected behavior | Recovery |
|---|---|---|
| `JobNamespace` equals the executor namespace | Resolution error, no Job created, ERROR log naming the Config | Fix the Config; the guard is intentional and not overridable |
| Sandbox namespace does not exist yet | Job creation fails with a 404 from the API server, logged | Deploy the `bborbe/agent` manifests first; this spec is inert without them |
| Executor lacks RBAC in the sandbox namespace | Job creation fails with 403, logged | Apply the cross-namespace RBAC from `bborbe/agent` |
| Executor restarts mid-rollout, some Configs migrated | Each Config resolves independently; already-running Jobs are untouched | No coordinated cutover needed — resolution is per-spawn, not global state |
| Config supplies a conflicting `app` label | Spawner's `app: agent` wins | Intentional; DB 4 makes the label unconditional |

## Security / Abuse Cases

- **A Config author places Jobs beside the executor's Secrets.** Rejected by DB 2's guard — this is the specific hole the guard exists to close.
- **A Config author places Jobs in a third, unprotected namespace.** Still possible: the guard rejects only the executor's namespace. Residual risk, accepted here because the Configs are operator-authored and version-controlled in the helm chart; tightening to an allowlist is a follow-up if Configs ever become externally authored.
- **A Job escapes the NetworkPolicy by lacking the selector label.** Prevented by DB 4 — the label is stamped by the spawner, not supplied by the Config.

## Do-Nothing Option

Agent Jobs stay in the executor's namespace permanently, which means the network-isolation work in `bborbe/agent` cannot roll out at all — there is no other mechanism to place a Job somewhere a policy applies. Doing nothing keeps agents carrying an `ANTHROPIC_AUTH_TOKEN` and a GitHub App private key on the same flat network as the trading stack.
