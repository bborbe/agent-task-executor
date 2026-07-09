---
status: completed
spec: [001-mount-kafka-mtls-certs-into-agent-jobs]
summary: Added job-kafka-client-cert-secret and job-kafka-ca-cert-secret global config fields, threaded through factory into jobSpawner struct
execution_id: agent-task-executor-jobcerts-exec-001-spec-001-add-kafka-cert-config
dark-factory-version: v0.191.0
created: "2026-07-09T12:45:00Z"
queued: "2026-07-09T10:45:16Z"
started: "2026-07-09T10:45:17Z"
completed: "2026-07-09T10:49:23Z"
branch: dark-factory/mount-kafka-mtls-certs-into-agent-jobs
---

<summary>
- Adds two new optional global executor settings that name a Kafka client-cert secret and a Kafka CA-cert secret.
- Both settings default to empty, so existing deployments (plaintext-Kafka quant) start with no new flags and behave identically to today.
- Threads both values from process startup down to the component that spawns per-task Kubernetes Jobs, so the next prompt can use them.
- No behavior change yet: this prompt only exposes and wires the config; the actual cert mounting happens in prompt 2.
- Existing tests continue to pass unchanged (empty values behave exactly as before).
</summary>

<objective>
Expose two new non-required global executor config values — `job-kafka-client-cert-secret` / `JOB_KAFKA_CLIENT_CERT_SECRET` and `job-kafka-ca-cert-secret` / `JOB_KAFKA_CA_CERT_SECRET` (both default empty) — and thread them through the factory into the job spawner so a later prompt can mount Kafka mTLS certs into spawned Jobs. This prompt only adds and wires the config; it must NOT change any spawned-Job output (empty values behave identically to today).
</objective>

<context>
Read CLAUDE.md for project conventions.

Read these files fully before changing anything:
- `/workspace/main.go` — the `application` struct (config fields with `arg:`/`env:`/`required:` tags) and its `Run` method, which calls `factory.CreateConsumer(...)`.
- `/workspace/pkg/factory/factory.go` — `CreateConsumer(...)` (around line 95), which constructs the spawner via `spawner.NewJobSpawner(...)` (around line 109).
- `/workspace/pkg/spawner/job_spawner.go` — `NewJobSpawner(...)` constructor and the `jobSpawner` struct.
- `/workspace/pkg/spawner/job_spawner_test.go` — every `spawner.NewJobSpawner(...)` call site (9 of them: around lines 51, 121, 159, 430, 872, 899, 927, 952, 975). All must keep compiling.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-factory-pattern.md` — factory has zero business logic, `Create*` prefix.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md` — constructor/interface conventions.

Current `NewJobSpawner` signature (verified verbatim):
```go
func NewJobSpawner(
	kubeClient kubernetes.Interface,
	namespace k8s.Namespace,
	kafkaBrokers string,
	branch string,
	topicPrefix string,
	currentDateTimeGetter libtime.CurrentDateTimeGetter,
	jobTTLSecondsAfterFinished int32,
) JobSpawner
```

Current `application` config field pattern (verified verbatim, note the struct-tag alignment style):
```go
JobTTLSecondsAfterFinished int32 `required:"false" arg:"job-ttl-seconds-after-finished" env:"JOB_TTL_SECONDS_AFTER_FINISHED" usage:"..." default:"1800"`
```
</context>

<requirements>
1. In `/workspace/main.go`, add two fields to the `application` struct, immediately after `JobTTLSecondsAfterFinished`:
   ```go
   JobKafkaClientCertSecret string `required:"false" arg:"job-kafka-client-cert-secret" env:"JOB_KAFKA_CLIENT_CERT_SECRET" usage:"Name of the existing K8s secret holding the Kafka client cert/key (keys user.crt/user.key) to mount into spawned Jobs; empty disables cert mounting"`
   JobKafkaCaCertSecret     string `required:"false" arg:"job-kafka-ca-cert-secret"     env:"JOB_KAFKA_CA_CERT_SECRET"     usage:"Name of the existing K8s secret holding the Kafka CA cert (key ca.crt) to mount into spawned Jobs; empty disables cert mounting"`
   ```
   Keep the struct-tag alignment consistent with the surrounding fields (gofmt/golines will normalize; alignment is not load-bearing but keep it tidy). Both fields are plain `string`, `required:"false"`, no `default:` tag (empty string is the zero value).

2. In `/workspace/main.go`, in `(a *application) Run(...)`, pass the two new fields to `factory.CreateConsumer(...)`. Append them as the final two arguments of the existing call (after `a.JobTTLSecondsAfterFinished`):
   ```go
   consumer, taskEventHandler := factory.CreateConsumer(
       saramaClient,
       a.Branch,
       a.TopicPrefix,
       kubeClient,
       a.Namespace,
       a.KafkaBrokers,
       resolver,
       log.DefaultSamplerFactory,
       currentDateTimeGetter,
       resultPublisher,
       taskStore,
       a.JobTTLSecondsAfterFinished,
       a.JobKafkaClientCertSecret,
       a.JobKafkaCaCertSecret,
   )
   ```

3. In `/workspace/pkg/factory/factory.go`, update `CreateConsumer(...)` to accept the two new values as the final two parameters (append after `jobTTLSecondsAfterFinished int32`):
   ```go
   func CreateConsumer(
       saramaClient sarama.Client,
       branch base.Branch,
       topicPrefix base.TopicPrefix,
       kubeClient kubernetes.Interface,
       namespace libk8s.Namespace,
       kafkaBrokers string,
       resolver pkg.ConfigResolver,
       logSamplerFactory log.SamplerFactory,
       currentDateTimeGetter libtime.CurrentDateTimeGetter,
       resultPublisher pkg.ResultPublisher,
       taskStore *pkg.TaskStore,
       jobTTLSecondsAfterFinished int32,
       jobKafkaClientCertSecret string,
       jobKafkaCaCertSecret string,
   ) (libkafka.Consumer, handler.TaskEventHandler) {
   ```
   Then pass both new values as the final two arguments of the `spawner.NewJobSpawner(...)` call inside this function (after `jobTTLSecondsAfterFinished`). The factory must remain zero-logic (no conditionals/loops) — just forward the values.

4. In `/workspace/pkg/spawner/job_spawner.go`:
   - Add two `string` fields to the `jobSpawner` struct, after `jobTTLSecondsAfterFinished int32`:
     ```go
     jobKafkaClientCertSecret string
     jobKafkaCaCertSecret     string
     ```
   - Extend `NewJobSpawner(...)` to accept these two as the final two parameters (after `jobTTLSecondsAfterFinished int32`), and set them in the returned `&jobSpawner{...}` literal:
     ```go
     func NewJobSpawner(
         kubeClient kubernetes.Interface,
         namespace k8s.Namespace,
         kafkaBrokers string,
         branch string,
         topicPrefix string,
         currentDateTimeGetter libtime.CurrentDateTimeGetter,
         jobTTLSecondsAfterFinished int32,
         jobKafkaClientCertSecret string,
         jobKafkaCaCertSecret string,
     ) JobSpawner {
         return &jobSpawner{
             kubeClient:                 kubeClient,
             namespace:                  namespace,
             kafkaBrokers:               kafkaBrokers,
             branch:                     branch,
             topicPrefix:                topicPrefix,
             currentDateTimeGetter:      currentDateTimeGetter,
             jobTTLSecondsAfterFinished: jobTTLSecondsAfterFinished,
             jobKafkaClientCertSecret:   jobKafkaClientCertSecret,
             jobKafkaCaCertSecret:       jobKafkaCaCertSecret,
         }
     }
     ```
   - Update the constructor GoDoc comment to briefly mention the two new params (empty = no cert mounting). Do NOT add any mounting logic in this prompt — the fields are stored but unused for now.

5. Update ALL 9 `spawner.NewJobSpawner(...)` call sites in `/workspace/pkg/spawner/job_spawner_test.go` to append two empty-string arguments (`"", ""`) after the existing final `int32` argument. Go has no default parameters — every call site must compile. Example transform:
   ```go
   // before
   jobSpawner = spawner.NewJobSpawner(
       fakeClient, "test-ns", "kafka:9092", "develop", "test-prefix", currentDateTime, 1800,
   )
   // after
   jobSpawner = spawner.NewJobSpawner(
       fakeClient, "test-ns", "kafka:9092", "develop", "test-prefix", currentDateTime, 1800, "", "",
   )
   ```
   Do this for every one of the 9 call sites (find them with `grep -n 'NewJobSpawner' /workspace/pkg/spawner/job_spawner_test.go`). This prompt adds NO new test assertions — cert-mount tests come in prompt 2.

6. Do NOT change `pkg.AgentConfiguration` — these values are GLOBAL process config, not per-agent. Do NOT add a CRD/Config-CR field.
</requirements>

<constraints>
- Both new config values are non-required with empty string defaults so existing deployments (quant plaintext Kafka) keep starting with no new flags.
- Do NOT change any spawned-Job output in this prompt: with empty values the produced Job spec must be byte-identical to today's. This prompt only stores the values on the struct; it does not read them.
- These are deliberately GLOBAL executor config, NOT per-agent — do NOT add a per-agent CRD / Config-CR schema field, and do NOT touch `pkg.AgentConfiguration`.
- Do NOT wire the Helm chart or config-repo values — out of scope.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
- The factory must stay zero-logic: forward values only, no conditionals.
</constraints>

<verification>
Run in `/workspace`:
```
make test
grep -n 'job-kafka-client-cert-secret\|job-kafka-ca-cert-secret' main.go
```
Expected:
- `make test` exits 0 (all existing Ginkgo specs green, everything still compiles including the 9 updated test call sites).
- The grep returns exactly two lines, both containing `required:"false"`.

Do NOT run `make precommit` at the end of this prompt — the CHANGELOG entry and full validation land in prompt 2. Running `make test` is sufficient here.
</verification>

## Definition of Done

- [ ] `main.go` exposes `job-kafka-client-cert-secret` / `JOB_KAFKA_CLIENT_CERT_SECRET` and `job-kafka-ca-cert-secret` / `JOB_KAFKA_CA_CERT_SECRET`, both `required:"false"` with empty defaults
- [ ] `factory.CreateConsumer` accepts and forwards both values to `spawner.NewJobSpawner`
- [ ] `spawner.NewJobSpawner` accepts both values and stores them on the `jobSpawner` struct (unused for now)
- [ ] All 9 `NewJobSpawner` call sites in the test file updated with two trailing `""` args and compile
- [ ] `make test` exits 0
- [ ] No change to `pkg.AgentConfiguration` and no CRD field added
