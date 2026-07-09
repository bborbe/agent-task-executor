---
status: completed
spec: [001-mount-kafka-mtls-certs-into-agent-jobs]
summary: Implemented applyKafkaCertVolumes helper on jobSpawner that mounts three Kafka mTLS secret volumes (client-cert, client-key, server-cert) into spawned Jobs when both jobKafkaClientCertSecret and jobKafkaCaCertSecret are set; added table-driven tests covering both-set, neither-set, only-client-set, and only-CA-set cases; recorded feat in CHANGELOG Unreleased.
execution_id: agent-task-executor-jobcerts-exec-002-spec-001-mount-kafka-certs
dark-factory-version: v0.191.0
created: "2026-07-09T12:45:00Z"
queued: "2026-07-09T10:50:36Z"
started: "2026-07-09T10:50:38Z"
completed: "2026-07-09T11:00:11Z"
branch: dark-factory/mount-kafka-mtls-certs-into-agent-jobs
---

<summary>
- When both Kafka cert secret names are configured, every spawned agent Job gets three cert files projected into its container at the fixed paths the Kafka client library expects.
- The client cert, client key, and CA (server) cert are mounted read-only (owner+group read) from the two named secrets.
- Both-or-neither: if only one secret name is set, or neither, no cert volumes or mounts are added — the Job is identical to today's output, so plaintext-Kafka deployments are unaffected.
- The executor only references the secrets by name; it never reads or logs cert bytes and manages no secret material.
- Adds table-driven tests covering both-set, neither-set, and exactly-one-set cases.
- Records the new capability in the changelog as an additive (minor) change.
</summary>

<objective>
Mount the three Kafka mTLS cert files into every spawned Job's first container when BOTH Kafka cert secret names are configured, using the frozen paths, keys, and permissions the Kafka client library and the executor-deployment's own mounts require. When neither or only one secret name is set, spawned Jobs are byte-identical to today's output. Prompt 1 already exposed the two config values and stored them on the spawner; this prompt implements the mounting behavior plus tests and the changelog entry.
</objective>

<context>
Read CLAUDE.md for project conventions.

Read these files fully before changing anything:
- `/workspace/pkg/spawner/job_spawner.go` — after prompt 1, `jobSpawner` has fields `jobKafkaClientCertSecret string` and `jobKafkaCaCertSecret string`. Study the post-build helper pattern: `applySecretEnvFrom(config, job)` (mutates `job.Spec.Template.Spec.Containers[0].EnvFrom`) and `applyEphemeralStorage(config, job)` — both take the already-built `*batchv1.Job` and mutate its first container / pod spec directly. Your new helper follows the same post-build style. Also study `applyVolumeMount(...)` for the volume + volumeMount shape (Name, VolumeSource, MountPath).
- `/workspace/pkg/spawner/job_spawner_test.go` — the PVC-volume test (around lines 460-515) shows how to assert on `job.Spec.Template.Spec.Volumes` and `container.VolumeMounts` from the fake clientset. Mirror that style for the new cert tests. Confirm the `NewJobSpawner(...)` call sites now take two trailing string args (from prompt 1).
- `/workspace/CHANGELOG.md` — top of file; add a `## Unreleased` section.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega, `DescribeTable`/`Entry`, coverage ≥80%, error-path tests.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased`, `feat:` prefix, be specific.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md` — error wrapping / general conventions.

Verified k8s corev1 types (import `corev1 "k8s.io/api/core/v1"`, already imported in job_spawner.go):
```go
type Volume struct {
    Name         string
    VolumeSource // embedded
}
type VolumeSource struct {
    Secret *SecretVolumeSource
    // ...
}
type SecretVolumeSource struct {
    SecretName  string
    Items       []KeyToPath
    DefaultMode *int32   // mode bits, e.g. 0440 == decimal 288
    Optional    *bool
}
type KeyToPath struct {
    Key  string
    Path string
    Mode *int32
}
type VolumeMount struct {
    Name      string
    MountPath string
    // ...
}
```

FROZEN contract (dictated by the Kafka client library — do NOT deviate):
| Volume name   | Secret            | Item key   | Item path | Container mount path  |
|---------------|-------------------|------------|-----------|-----------------------|
| `client-cert` | client-cert secret| `user.crt` | `file`    | `/client-cert/file`   |
| `client-key`  | client-cert secret| `user.key` | `file`    | `/client-key/file`    |
| `server-cert` | CA-cert secret    | `ca.crt`   | `file`    | `/server-cert/file`   |

- Client cert AND client key both come from `jobKafkaClientCertSecret` (two volumes, same secret, different keys).
- Server cert comes from `jobKafkaCaCertSecret`.
- Every secret volume uses `DefaultMode` = `0440` (decimal 288).
</context>

<requirements>
1. In `/workspace/pkg/spawner/job_spawner.go`, add a post-build helper method with this exact signature:
   ```go
   func (s *jobSpawner) applyKafkaCertVolumes(job *batchv1.Job)
   ```
   Behavior:
   - Both-or-neither gate FIRST: if `s.jobKafkaClientCertSecret == "" || s.jobKafkaCaCertSecret == ""`, `return` immediately (adds nothing — covers both the neither-set and exactly-one-set cases).
   - Define the mode as a `*int32` pointing at `0440`. Follow the existing local-var-then-address pattern used by `applyActiveDeadlineSeconds` (`deadline := int64(...); ... = &deadline`). For example:
     ```go
     mode := int32(0o440) // decimal 288; owner+group read only
     ```
     Reuse `&mode` for the `DefaultMode` of all three volumes (a shared pointer is fine — the value is never mutated).
   - Append three volumes to `job.Spec.Template.Spec.Volumes` (append, do NOT overwrite — a PVC volume named `agent-data` may already be present):
     - Volume `client-cert`: `SecretVolumeSource{SecretName: s.jobKafkaClientCertSecret, Items: []corev1.KeyToPath{{Key: "user.crt", Path: "file"}}, DefaultMode: &mode}`
     - Volume `client-key`: `SecretVolumeSource{SecretName: s.jobKafkaClientCertSecret, Items: []corev1.KeyToPath{{Key: "user.key", Path: "file"}}, DefaultMode: &mode}`
     - Volume `server-cert`: `SecretVolumeSource{SecretName: s.jobKafkaCaCertSecret, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "file"}}, DefaultMode: &mode}`
   - Append three volumeMounts to `job.Spec.Template.Spec.Containers[0].VolumeMounts` (append, do NOT overwrite):
     - `{Name: "client-cert", MountPath: "/client-cert/file"}`
     - `{Name: "client-key", MountPath: "/client-key/file"}`
     - `{Name: "server-cert", MountPath: "/server-cert/file"}`
   - Add a GoDoc comment starting with the method name describing the both-or-neither behavior and the frozen contract. Keep the function under the 80-line funlen limit; if it approaches the limit, factor the volume construction into a small unexported helper in the same file.

2. In `SpawnJob(...)`, call `s.applyKafkaCertVolumes(job)` on the built `*batchv1.Job`, in the same block as the other post-build mutators (right after `applySecretEnvFrom(config, job)` and `applyEphemeralStorage(config, job)`, before the `s.kubeClient.BatchV1().Jobs(...).Create(...)` call). Because the method self-gates on empty secret names, calling it unconditionally is correct and keeps `SpawnJob` clean.

3. Add tests in `/workspace/pkg/spawner/job_spawner_test.go`. Prefer a `DescribeTable` with `Entry` rows, mirroring the existing PVC-volume assertion style (spawn via `jobSpawner.SpawnJob`, then read the Job back from `fakeClient.BatchV1().Jobs("test-ns").List(...)`). To vary the two secret names per case, construct a fresh `jobSpawner` inside the table function via `spawner.NewJobSpawner(fakeClient, "test-ns", "kafka:9092", "develop", "test-prefix", currentDateTime, 1800, clientCertSecret, caCertSecret)`. Cover these cases:
   - **both set** (`clientCertSecret="kafka-client-cert"`, `caCertSecret="kafka-ca-cert"`): assert the pod spec `Volumes` contains exactly the three cert volumes named `client-cert`, `client-key`, `server-cert` (there is no PVC in these cases, so `Volumes` has length 3); each has a non-nil `.Secret`; assert `client-cert.Secret.SecretName == "kafka-client-cert"` with `Items[0].Key == "user.crt"` and `Items[0].Path == "file"`; `client-key.Secret.SecretName == "kafka-client-cert"` with `Items[0].Key == "user.key"`, `Path == "file"`; `server-cert.Secret.SecretName == "kafka-ca-cert"` with `Items[0].Key == "ca.crt"`, `Path == "file"`; and for all three assert `*Secret.DefaultMode == int32(288)`. Also assert the first container has three volumeMounts at `/client-cert/file`, `/client-key/file`, `/server-cert/file` mapped to the matching volume names.
   - **neither set** (`"", ""`): assert the pod spec has NONE of the volumes named `client-cert`/`client-key`/`server-cert` and the first container has NONE of the three cert mount paths. (Use a helper that checks absence by name/path so a future PVC volume would not false-fail; in these test cases there is no PVC so `Volumes` is empty.)
   - **only client set** (`"kafka-client-cert", ""`): same negative assertions as neither-set.
   - **only CA set** (`"", "kafka-ca-cert"`): same negative assertions as neither-set.
   Prefer indexing by name/path (build a `map[string]corev1.Volume` keyed by `Name` and a `map[string]corev1.VolumeMount` keyed by `MountPath`) rather than asserting exact slice order/length — the by-name approach survives a future PVC `agent-data` volume coexisting and does not depend on the table fixture leaving `VolumeClaim` empty. Assert presence + fields of the three cert volumes/mounts by name; for the negative cases assert absence by name/path.

4. In `/workspace/CHANGELOG.md`, add a `## Unreleased` section at the top (immediately under the intro lines, above `## v0.3.3`) with a `feat:` entry, for example:
   ```
   ## Unreleased

   - feat: Mount Kafka mTLS client cert/key and CA cert into spawned agent Jobs when the new
     `job-kafka-client-cert-secret` and `job-kafka-ca-cert-secret` executor config values are both
     set — projects three read-only (0440) secret files at `/client-cert/file`, `/client-key/file`,
     `/server-cert/file`. When neither or only one is set, spawned Jobs are unchanged (plaintext-Kafka
     deployments unaffected).
   ```
   If a `## Unreleased` section already exists, append the bullet instead of adding a second heading.
</requirements>

<constraints>
- The three mount paths (`/client-cert/file`, `/client-key/file`, `/server-cert/file`), the three secret item keys (`user.crt`, `user.key`, `ca.crt`), the projected file name `file`, and the secret `DefaultMode` `0440` (decimal 288) are FROZEN — they mirror the executor-deployment's own cert mounts and the `agent.kafkaCertVolumes` chart helper. Do NOT change them.
- Both-or-neither: if only one of the two secret names is set, add NO cert volumes or mounts — identical to the neither-set case. Never half-configure a pod.
- The default render (neither secret set) must not change: existing job-spawner tests and any golden Job output must still pass unchanged. Append to `Volumes` / `VolumeMounts`, never overwrite (a PVC `agent-data` volume may coexist).
- The new mounts apply only to the spawned agent container (the Job's first container, `Containers[0]`), consistent with `applySecretEnvFrom` / `applyVolumeMount`.
- The executor references secrets by NAME only — do NOT read, create, sync, or log secret contents. No cert bytes pass through the executor.
- Do NOT add a per-agent CRD / Config-CR field and do NOT touch `pkg.AgentConfiguration` — these are global executor config (already wired in prompt 1).
- Do NOT wire the Helm chart or config-repo values — out of scope.
- CHANGELOG: additive minor version bump under `## Unreleased`; the maintainer bot cuts the tag per `.maintainer.yaml`.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run in `/workspace`:
```
make precommit
grep -n 'Unreleased' CHANGELOG.md
go test -coverprofile=/tmp/cover.out -mod=mod ./pkg/spawner/... && go tool cover -func=/tmp/cover.out | tail -1
```
Expected:
- `make precommit` exits 0 — all Ginkgo specs green including the new both-set / neither-set / only-client-set / only-CA-set cert-mount cases.
- `grep 'Unreleased' CHANGELOG.md` returns a line.
- Coverage for `pkg/spawner` stays at or above 80%.

If `make precommit` exits non-zero, the prompt is NOT done — fix and re-run only the failing target until it passes, then run `make precommit` once more.
</verification>

## Definition of Done

- [ ] `applyKafkaCertVolumes(job *batchv1.Job)` implemented on `*jobSpawner` with the both-or-neither empty-name gate returning early
- [ ] Called from `SpawnJob` alongside the other post-build mutators, unconditionally (self-gated)
- [ ] Both-set case: three secret volumes `client-cert`/`client-key`/`server-cert` with keys `user.crt`/`user.key`/`ca.crt` → path `file`, `DefaultMode` 288, and three container mounts at the three frozen paths
- [ ] Neither-set and each exactly-one-set case: no cert volumes and no cert mounts (Job unchanged)
- [ ] Table tests cover both-set, neither-set, only-client-set, only-CA-set; all green
- [ ] `## Unreleased` CHANGELOG entry with a `feat:` bullet describing the cert-mount capability
- [ ] `make precommit` exits 0
- [ ] `pkg/spawner` coverage ≥ 80%
