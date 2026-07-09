---
status: verifying
tags:
    - dark-factory
    - spec
approved: "2026-07-09T10:35:28Z"
generating: "2026-07-09T10:37:35Z"
prompted: "2026-07-09T10:44:31Z"
verifying: "2026-07-09T10:49:24Z"
branch: dark-factory/mount-kafka-mtls-certs-into-agent-jobs
---

## Summary

- On the octopus cluster, agent Job pods spawned by agent-task-executor must speak Kafka over mTLS, but the job-spawner has no way to mount client/CA certs into them, so the Jobs crash on startup.
- Add two global executor config values naming the pre-existing Kafka client-cert secret and CA-cert secret; when both are set, mount three cert files at the fixed paths the Kafka client library expects.
- Default (neither set) is byte-identical to today's behavior, so plaintext-Kafka (quant) deployments are unaffected.
- Both-or-neither: if only one of the two is configured, treat as OFF — never half-configure a pod.
- Scope is the agent-task-executor Go binary only; the Helm chart and config-repo values are separate, out-of-scope changes.

## Problem

agent-task-executor spawns a per-task Kubernetes Job for each agent run (pr-reviewer, github-releaser). On the octopus cluster these agents connect to Kafka over mTLS and the Kafka client library auto-enables TLS on the `tls://` broker scheme, expecting the client cert, client key, and server (CA) cert at three fixed filesystem paths. The job-spawner today can attach only a PVC volume and an `envFrom` secret — it has no way to project secret files into the spawned Job. As a result the spawned Jobs start with no certs and crash immediately with `open /client-cert/file: no such file or directory`. Quant Kafka is plaintext so this gap was never exercised; octopus mTLS exposes it and blocks the first pr-reviewer verdict on the Seibert-Data project.

## Goal

The executor can be configured with the names of two existing Kubernetes secrets (a Strimzi-issued Kafka client secret and the cluster CA secret). When both are configured, every Job the executor spawns has the three Kafka cert files projected into its first container at the fixed paths the Kafka client library reads, so the agent completes mTLS Kafka startup and runs to completion. When neither is configured, spawned Jobs are unchanged from today.

## Non-goals

- Do NOT wire the Helm chart's executor-deployment env for these values — that is a separate, direct chart change.
- Do NOT change the sm-octopus config-repo agent values — separate change.
- Do NOT add any per-agent CRD / Config-CR schema field — this is deliberately GLOBAL executor config, not per-agent; if a future consumer needs per-agent cert selection, that is a separate spec.
- Do NOT create, sync, or manage the referenced secrets — the executor references existing secrets by name only.
- Do NOT touch the watcher / controller / recurring components — they already receive certs via the chart.

## Desired Behavior

1. The executor accepts a config value naming the Kafka client-cert secret (arg `job-kafka-client-cert-secret`, env `JOB_KAFKA_CLIENT_CERT_SECRET`, not required, default empty).
2. The executor accepts a config value naming the Kafka CA-cert secret (arg `job-kafka-ca-cert-secret`, env `JOB_KAFKA_CA_CERT_SECRET`, not required, default empty).
3. When BOTH secret names are non-empty, each spawned Job's first container gets three secret-backed cert files: client cert at `/client-cert/file` (from the client-cert secret, key `user.crt`), client key at `/client-key/file` (from the client-cert secret, key `user.key`), and server cert at `/server-cert/file` (from the CA-cert secret, key `ca.crt`). Each secret volume uses defaultMode `0440` (decimal 288).
4. When NEITHER secret name is set, spawned Jobs carry none of these cert volumes or mounts — the produced Job spec is byte-identical to today's.
5. When exactly ONE of the two secret names is set, the behavior is identical to the neither-set case (no cert volumes or mounts) — the executor never half-configures a pod.

## Constraints

- The three mount paths (`/client-cert/file`, `/client-key/file`, `/server-cert/file`), the three secret item keys (`user.crt`, `user.key`, `ca.crt`), each projecting to file name `file`, and the secret defaultMode `0440` are FROZEN — they are dictated by the Kafka client library and must mirror the executor-deployment's own cert mounts and the `agent.kafkaCertVolumes` chart helper.
- The default render (neither secret set) must not change: existing job-spawner tests and any golden Job output must still pass unchanged.
- The two new config values are non-required with empty defaults so existing deployments (quant plaintext Kafka) keep starting with no new flags.
- The new mounts apply only to the spawned agent container (the Job's first container), consistent with `applySecretEnvFrom` / `applyVolumeMount`.
- CHANGELOG: add the change under a `## Unreleased` heading (additive minor version bump); the maintainer bot cuts the tag from it per `.maintainer.yaml` (autoRelease).

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection |
|---------|-------------------|----------|-----------|
| Only one of the two secret names set | No cert volumes/mounts added; Job spawned as in default case | Operator sets the missing name; next spawn mounts certs | Spawned Job pod spec has no `client-cert`/`client-key`/`server-cert` volumes |
| Named secret does not exist in the namespace at pod-schedule time | Executor still creates the Job (it only references by name); Kubernetes fails to start the pod (secret not found) | Operator creates/syncs the secret; pod schedules on retry/next spawn | Pod stuck with `MountVolume.SetUp failed ... secrets "<name>" not found` in `kubectl describe pod` |
| Named secret exists but lacks expected key (`user.crt`/`user.key`/`ca.crt`) | Kubernetes projects only present keys; missing file absent; agent crashes reading the missing cert path | Operator fixes the secret keys | Agent container logs `open /<path>/file: no such file or directory` |
| Both set on a plaintext-Kafka (quant) deployment by mistake | Certs mounted but unused (broker scheme is not `tls://`); no crash from mounts | Operator unsets the two values | Cert files present in pod but agent connects plaintext |

## Security / Abuse Cases

- The two config values are operator-supplied secret NAMES, not cert material — no cert bytes pass through the executor or its logs.
- The executor only references secrets by name in the same namespace it already spawns Jobs into; it grants no new cross-namespace access and creates/reads no secret contents.
- No user/task-controlled input reaches these values — they are global process config, not per-task data, so a task cannot influence which secret is mounted.
- Secret files are projected with defaultMode `0440` (owner+group read only), not world-readable.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|--------------|------------|------------|------------|
| 1 | Add global executor config values in `main.go`: `JobKafkaClientCertSecret` (arg `job-kafka-client-cert-secret`, env `JOB_KAFKA_CLIENT_CERT_SECRET`) and `JobKafkaCaCertSecret` (arg `job-kafka-ca-cert-secret`, env `JOB_KAFKA_CA_CERT_SECRET`) struct fields, both `required:"false"` default empty; thread them to the spawner factory. | 1, 2 | Config-exposure AC (`grep -n 'job-kafka-client-cert-secret\|job-kafka-ca-cert-secret' main.go`) | — |
| 2 | Implement `applyKafkaCertVolumes` in `pkg/spawner/job_spawner.go` (both-or-neither gate; three secret volumes `client-cert`/`client-key`/`server-cert`, keys `user.crt`/`user.key`/`ca.crt` → path `file`, defaultMode `0440`/288; mounts on first container at the three frozen paths) plus factory wiring and the Ginkgo table test covering both-set / neither-set / exactly-one-set cases; add `## Unreleased` CHANGELOG entry. | 3, 4, 5 | Both-set volumes AC, both-set mounts AC, neither-set negative AC, one-set negative AC, `make precommit` AC, CHANGELOG AC | Prompt 1 |

## Acceptance Criteria

- [ ] `make precommit` exits 0 — evidence: exit code
- [ ] Config exposes `job-kafka-client-cert-secret` / `JOB_KAFKA_CLIENT_CERT_SECRET` and `job-kafka-ca-cert-secret` / `JOB_KAFKA_CA_CERT_SECRET`, both non-required default empty — evidence: `grep -n 'job-kafka-client-cert-secret\|job-kafka-ca-cert-secret' main.go` returns two lines with `required:"false"`
- [ ] Unit test: SpawnJob with BOTH secret names set → built Job pod spec has exactly three secret volumes named `client-cert`, `client-key`, `server-cert`, with items key `user.crt`/`user.key`/`ca.crt` each projecting to path `file`, and defaultMode 288 — evidence: Ginkgo assertions pass (test exit 0)
- [ ] Unit test: same case → the first container has three volumeMounts at `/client-cert/file`, `/client-key/file`, `/server-cert/file` — evidence: Ginkgo assertions pass (test exit 0)
- [ ] Unit test: with BOTH secret names empty → built Job pod spec has NONE of the three named volumes and NONE of the three cert volumeMounts (matching current output) — evidence: Ginkgo assertions pass (test exit 0)
- [ ] Unit test: with exactly ONE secret name set (either one) → built Job pod spec has NONE of the three named volumes/mounts — evidence: Ginkgo assertions pass (test exit 0)
- [ ] CHANGELOG has a `## Unreleased` section describing the new cert-mount capability — evidence: `grep -n 'Unreleased' CHANGELOG.md` returns a line

## Verification

```
make precommit
```

Expected: exit 0, all Ginkgo specs green including the new job-spawner cert-mount table cases.

Runtime confirmation (after the later, out-of-scope chart + config deploy on octopus dev): the pr-reviewer Job for the test-dev PR starts without the `/client-cert/file: no such file or directory` error and runs to completion — evidence: `kubectl logs` for that Job shows no cert-open error and the Job reaches Completed.

## Do-Nothing Option

Not acceptable. Without this, every agent Job on any mTLS Kafka cluster (octopus) crashes on startup and no agent result event is ever emitted, blocking the first pr-reviewer verdict on Seibert-Data. The plaintext-only workaround (quant) does not generalize to the company cluster.
