---
status: verifying
approved: "2026-10-23T10:58:11Z"
generating: "2026-10-23T11:00:14Z"
prompted: "2026-10-23T11:14:17Z"
verifying: "2026-10-23T11:18:52Z"
branch: dark-factory/config-job-namespace
---

## Summary

- `ConfigSpec` gains a `JobNamespace` field so an agent's Jobs can be spawned in a namespace other than the executor's
- An empty `JobNamespace` keeps today's behaviour exactly: the Job lands in the executor's own namespace
- A non-empty `JobNamespace` places the Job in that namespace; setting it explicitly to the executor's own namespace is rejected, so no Config can opt *in* to unisolated spawning
- Every Job lookup (`IsJobActive`, `JobExists`, `CountActiveJobs`, `ListActiveJobs`) resolves the same namespace the Job was created in, so the concurrency cap and the duplicate-spawn guard keep working across namespaces
- Spawned Job pods always carry `app: agent`, so a NetworkPolicy selector can match them
- Manifests (namespace, NetworkPolicy, RBAC) are out of scope here — they live in the `bborbe/agent` helm chart

## Amendment to the 2026-08-08 draft

This spec was first written on 2026-08-08 with the opposite default: an unset `JobNamespace` resolved to `<executor-namespace>-agents-sandbox`, making the sandbox the safe default. It was never implemented, and the file was dropped from the tree by `8884802`.

The ordering constraint that forced the flip: `bborbe/agent` spec 047 deliberately creates **neither** the PVCs nor the Secrets the Job Configs reference inside the sandbox namespace, and the fleet is rolled one Config at a time. A derived sandbox default would move every agent into the sandbox on the first executor restart — a big-bang cutover that hangs every Job carrying a `volumeMountPath` or a `secretEnv` on an unbound PVC or a missing Secret, with no per-Config rollback.

So the default becomes "empty means today's behaviour", and isolation is opted into per Config once the namespace exists. The executor deploy is a no-op until a Config sets the field. The `app: agent` label and the explicit-equal-to-executor rejection are unchanged from the original draft.

## Problem

The executor spawns agent Jobs into its own namespace, so Jobs inherit the executor's network access and sit beside the executor Deployment, its Secrets, and Kafka credentials. There is no way to place a Job anywhere else.

Network isolation for agent Jobs (tracked in `bborbe/agent`) requires the Jobs to land in a separate, policy-restricted namespace and to carry a label the policy can select. Neither is possible today. This spec supplies the executor-side mechanism only; the namespace and policies that give it meaning are specified and deployed from `bborbe/agent`.

Background on the threat model and target topology: `bborbe/agent:docs/agent-network-security.md`.

## Goal

The executor resolves a target namespace per Config, defaulting to its own namespace and refusing an explicit opt-out, and stamps every spawned Job pod with `app: agent`. Once an operator sets `JobNamespace` on a Config, that agent's Jobs land in the named namespace, and every lookup the executor performs about those Jobs follows them there.

## Non-goals

- Creating the sandbox namespace, NetworkPolicy, or RBAC — `bborbe/agent` owns all manifests
- Watching `Config` resources across multiple namespaces — Configs stay in the executor's namespace; cross-namespace *spawning* does not require cross-namespace *watching*
- Watching `Job`/`Pod` objects in the target namespace — see Follow-ups; the Job informer stays scoped to the executor's namespace in this spec
- Migrating PVCs or re-seeding Secrets into the sandbox namespace (operational work)
- Per-agent network policy or egress rules

## Acceptance Criteria

- [ ] `make precommit` exits 0
- [ ] `ConfigSpec` has a `JobNamespace` field — `grep -n 'JobNamespace' k8s/apis/agent.benjamin-borbe.de/v1/types.go` returns ≥1 line
- [ ] The binary's own CRD schema carries the field — `grep -c 'jobNamespace' pkg/k8s_connector.go` returns ≥1. This is the schema `SetupCustomResourceDefinition` writes into the cluster on every process start; a field missing here is pruned from every Config within seconds of the next restart, and the failure is silent
- [ ] Unset `JobNamespace` resolves to the executor's own namespace — table-driven test `TestResolveJobNamespace` asserts: executor ns `dev` + empty → `dev`; executor ns `prod` + empty → `prod`; `"dev-agents-sandbox"` → `"dev-agents-sandbox"`. `go test ./... -run TestResolveJobNamespace -v` stdout contains `--- PASS: TestResolveJobNamespace`
- [ ] A `JobNamespace` equal to the executor's own namespace is rejected — `TestResolveJobNamespace` includes a case asserting a non-nil error whose message contains `may not equal the executor namespace`; `go test ./... -run TestResolveJobNamespace -v` stdout contains `--- PASS`
- [ ] A rejected Config does not spawn a Job — test `TestSpawnRejectsExecutorNamespace` asserts the fake clientset recorded zero `create` actions on `jobs`. Negative evidence: recorded action count for verb `create`, resource `jobs` is 0. `go test ./... -run TestSpawnRejectsExecutorNamespace -v` stdout contains `--- PASS`
- [ ] Spawned Job pods carry `app: agent` unconditionally — test `TestJobPodLabels` asserts the generated `batchv1.Job` pod template labels include `app: agent`. This locks existing unconditional behaviour (`pkg/spawner/job_spawner.go`, `jobBuilder.SetApp("agent")`) as a regression guard rather than introducing it; there is no Config-supplied pod-label seam to conflict with it. `go test ./... -run TestJobPodLabels -v` stdout contains `--- PASS: TestJobPodLabels`
- [ ] **The Config value reaches the spawner.** A `v1.Config` with `spec.jobNamespace: dev-agents-sandbox` passed through `convert()` (`pkg/config_resolver.go`) yields an `AgentConfiguration` whose `JobNamespace` is `dev-agents-sandbox`, and one with the field unset yields an empty `AgentConfiguration.JobNamespace`. Without this the field can be added to the CRD and to the spawner while the resolver drops it, and every AC below still passes against a hand-built `AgentConfiguration`. Test `TestConvertCarriesJobNamespace` asserts both cases. `go test ./... -run TestConvertCarriesJobNamespace -v` stdout contains `--- PASS: TestConvertCarriesJobNamespace`
- [ ] The Job is created against the resolved namespace, not unconditionally the executor's — test `TestSpawnTargetNamespace` asserts the fake clientset's recorded `create` action namespace equals `dev-agents-sandbox` for an executor in `dev` with `JobNamespace` set, and equals `dev` with it unset. `go test ./... -run TestSpawnTargetNamespace -v` stdout contains `--- PASS: TestSpawnTargetNamespace`
- [ ] **Every Job lookup follows the Job.** For a Config whose `JobNamespace` is set, `IsJobActive`, `JobExists`, `CountActiveJobs` and `ListActiveJobs` issue their list/get against the resolved namespace, not the executor's. Negative evidence of the bug this closes: with the lookups left on the executor namespace, a Job in the sandbox is invisible, so `IsJobActive` returns false and the executor spawns a duplicate on every pass, while `CountActiveJobs` reads 0 and the concurrency cap never fires. Test `TestJobLookupsUseResolvedNamespace` asserts each of the four recorded list/get actions carries the resolved namespace. `go test ./... -run TestJobLookupsUseResolvedNamespace -v` stdout contains `--- PASS: TestJobLookupsUseResolvedNamespace`
- [ ] The CRD doc states the field's default and its rejection rule, not merely the field name — `grep -c 'jobNamespace' docs/agent-crd-specification.md` ≥1 **and** `grep -ci 'executor namespace' docs/agent-crd-specification.md` ≥1 (an appended line naming only the field would pass a bare name grep while documenting nothing)
- [ ] `CHANGELOG.md` carries the change under `## Unreleased` — `awk '/^## /{sec=$0} /[Jj]ob[Nn]amespace/{print sec}' CHANGELOG.md` prints `## Unreleased`. The file's top section is a released version today, so this AC requires creating the `## Unreleased` heading above it. Negative evidence: the printed section is never `## vX.Y.Z`

## Verification

```bash
make precommit
go test ./... -run 'TestResolveJobNamespace|TestSpawnRejectsExecutorNamespace|TestJobPodLabels|TestSpawnTargetNamespace|TestJobLookupsUseResolvedNamespace' -v
```

All ACs are container-executable. No deployed system is observed by this spec — the Post-Deploy checks that prove isolation actually works live in the `bborbe/agent` specs that ship the manifests.

## Desired Behavior

1. `ConfigSpec` carries `JobNamespace string` with json tag `jobNamespace,omitempty`.
2. Namespace resolution: empty → the executor's own namespace; any other value → that value; a value equal to the executor's own namespace → error, and the Job is not spawned. The empty default is a transitional rollout state: it keeps every existing Config behaving exactly as before, so the executor deploy is a no-op until an operator sets the field per Config. The explicit-equal rejection survives the default flip so that a Config author can never *write* an opt-out into a Config; it does not, and cannot, cover the transitional default, which is removed once the fleet carries a sandbox value.
3. The spawner creates the Job in the resolved namespace.
4. Every spawned Job's pod template carries the label `app: agent`, set by the spawner and not overridable by Config-supplied labels.
5. All Job lookups — `IsJobActive`, `JobExists`, `CountActiveJobs`, `ListActiveJobs` — resolve the same namespace the Job was created in. The namespace is resolved at the call site, where the `AgentConfiguration` is in scope, and passed to the spawner: a single spawner instance serves every Config and cannot infer it.
6. A resolution error is logged at ERROR with the Config name and the rejected namespace, and leaves the task untouched so a human notices — the same leave-it-for-a-human posture the executor already takes on a spawn it cannot complete.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | `JobNamespace` field on `ConfigSpec` and `AgentConfiguration`, the binary's CRD schema, `convert()` carrying the value, `ResolveJobNamespace` derivation/rejection + unit tests | 1, 2 | 2, 3, 4, 5, 7 | — |
| 2 | Spawner create path — resolved-namespace target, refuse-on-error, label lock + unit tests | 3, 4 | 6, 8 | 1 |
| 3 | The four Job lookups — resolved-namespace list/get, interface and test-double ripple + unit tests | 5 | 9 | 1 |
| 4 | CRD doc row covering default and rejection rule, plus the CHANGELOG entry | — | 10, 11 | 1 |

Rationale: the field, its schema entry, the resolver and the resolution function are self-contained; the spawner's create path and its lookup path are separate surfaces and split cleanly once the resolver is in place; the doc row describes the settled semantics and lands last. AC 1 (`make precommit`) applies to every prompt, not to one row.

## Constraints

- Existing `Config` CRs must remain valid unedited — `JobNamespace` is additive and optional
- The empty default must preserve today's behaviour *exactly*, so the executor can be deployed before any Config sets the field
- **Assumption the operator carries:** before any Config sets the field, the target namespace must already exist and already hold the PVCs and Secrets that Config's Jobs reference. Spec 047's chart creates neither, so setting the field first hangs every Job that mounts a volume or a secret. This is the reason the default is empty rather than derived.
- The Kafka result-publishing path must not change; the agent still publishes its own result
- No new capability, securityContext change, or RBAC grant is introduced by this spec
- `TaskType`/`TaskTypes` routing behavior is untouched
- **Named ripple:** passing the resolved namespace to the lookups changes the `JobSpawner` interface — `IsJobActive`, `JobExists`, `CountActiveJobs` and `ListActiveJobs` each gain a namespace argument, which ripples through the counterfeiter fakes in `mocks/` and the test doubles in `pkg/handler/*_test.go`. The rejected alternative was resolving inside the spawner: it would keep the signatures, but `SpawnJob` is the only method that receives the `AgentConfiguration`, so the lookups would have no way to know which Config a Job belonged to.

## Failure Modes

| Trigger | Expected behavior | Recovery | Reversibility |
|---|---|---|---|
| `JobNamespace` equals the executor namespace | Resolution error, no Job created, ERROR log naming the Config | Correct the Config; `kubectl -n <executor-ns> get jobs` shows no new Job for that assignee, which confirms nothing was created | Full — nothing was created |
| `JobNamespace` empty (every Config today) | Job created in the executor namespace, exactly as before | None needed; set the field per Config once its namespace exists | Full |
| Sandbox namespace does not exist yet | Job creation fails with a 404 from the API server, logged | Deploy the `bborbe/agent` manifests first, then confirm with `kubectl -n <target> get ns` returning `Active` | Full — the create failed |
| Executor lacks RBAC in the sandbox namespace | Job creation fails with 403, logged | Apply the cross-namespace RBAC from `bborbe/agent`, then confirm with `kubectl auth can-i create jobs -n <target> --as=<executor-sa>` | Full — the create failed |
| Target namespace has a `ResourceQuota` the Job exceeds | Job creation fails with 403 `exceeded quota`, logged | Raise the quota or lower the Job's requests | Full — the create failed |
| Executor restarts mid-rollout, some Configs migrated | Each Config resolves independently; already-running Jobs are untouched | No coordinated cutover needed — resolution is per-spawn, not global state | Full — per-Config |
| Config supplies a conflicting `app` label | Spawner's `app: agent` wins | Intentional; DB 4 makes the label unconditional | Full |
| Jobs spawn into a namespace the Job informer does not watch | Terminal Job events for those Jobs are unseen: no synthetic failure result, no store cleanup | Follow-up below; until it lands, a sandboxed agent's failures are detected by the deadline sweeper rather than the informer | Full |
| A Config is pointed at the wrong namespace | Jobs run there with the executor's credentials | Delete the Job (`kubectl -n <wrong> delete job <name>`) and correct the Config | **Partial** — the Job may already be running, and its pod carries the agent's secrets; deletion stops it but does not undo what it did |

## Follow-ups

- **The Job informer does not follow `JobNamespace`.** `pkg/job_watcher.go` builds its shared informer with `WithNamespace(string(w.namespace))`, so Jobs created in the sandbox are invisible to it. Terminal-state handling — the synthetic failure result and the `DeleteIfCurrentJob` cleanup — therefore degrades for sandboxed agents. Fixing it needs either a cluster-scoped informer (a new RBAC grant, which this spec's Constraints forbid) or an explicit extra-namespace list rendered by the `bborbe/agent` chart. Filed as its own task rather than widened into this spec.
- **The claim fence does not follow `JobNamespace` either.** `pkg/handler/claim_job_control.go` (`kubeJobControl`) lists and deletes Jobs only in `c.namespace`. Once a Config sets the field, `IsJobRunning` returns false for a Job running in the sandbox and `DeleteJob` deletes nothing, so a claim that cannot renew leaves its Job running unclaimed — the duplicate-execution class the claims exist to prevent, failing silently. This is the same class as the informer gap above and is deliberately not fixed here: the `claim.JobControl` interface receives only a `taskID`, so a correct fix needs either a cluster-scoped list (a new RBAC grant, forbidden by Constraints) or a persistent task→namespace map that survives a restart. Both are larger than this spec.
- `bborbe/agent:helm/crds/config-crd.yaml` is kept in sync with `configSpecProperties()` by hand. The executor binary overwrites the cluster CRD on every start, so the chart file is documentation; it should still gain `jobNamespace` in the chart PR that sets the field on a Config.

## Security / Abuse Cases

- **A Config author places Jobs beside the executor's Secrets.** Rejected by DB 2's guard for an explicit value — this is the specific hole the guard exists to close. The empty default is the one exception and is a transitional state, not an authorable configuration.
- **A Config author places Jobs in a third, unprotected namespace.** Still possible: the guard rejects only the executor's namespace. Residual risk, accepted here because the Configs are operator-authored and version-controlled in the helm chart; tightening to an allowlist is a follow-up if Configs ever become externally authored.
- **A Job escapes the NetworkPolicy by lacking the selector label.** Prevented by DB 4 — the label is stamped by the spawner, not supplied by the Config.

## Do-Nothing Option

Agent Jobs stay in the executor's namespace permanently, which means the network-isolation work in `bborbe/agent` cannot roll out at all — there is no other mechanism to place a Job somewhere a policy applies. Doing nothing keeps agents carrying an `ANTHROPIC_AUTH_TOKEN` and a GitHub App private key on the same flat network as the trading stack.
