---
status: completed
spec: [bug-nuke-dev-gm]
summary: Added kafkaStartupError helper naming configured brokers and the deployment runbook in the executor's Kafka startup error, pinned by a plain Go test, and updated the runbook failure-mode section plus an Unreleased changelog entry.
execution_id: 549add33-f0e7-5cf9-993b-295ae5687c8c-exec-068-spec-bug-nuke-dev-gm-kafka-startup-diagnostics
dark-factory-version: dev
created: "2026-11-19T03:05:00Z"
queued: "2026-11-19T02:53:16Z"
started: "2026-11-19T02:53:17Z"
completed: "2026-11-19T03:04:20Z"
---

# fix: name the configured Kafka brokers in the executor's startup error

<!--
OPEN QUESTIONS FOR THE REVIEWER (non-interactive run; resolved best-effort from the spec):

1. The spec's "Recommended Fix" is entirely operator-side: set `KAFKA_BROKERS` in
   the nuke-dev executor Helm values/overlay, redeploy, verify startup, then remove
   the dangling Strimzi Service. This repository is publish-only — it contains no
   Helm chart, no values file and no Deployment manifest (`docs/deployment-kafka-brokers.md`
   says so explicitly, and the only env-shaped files are `tools.env` and the Makefiles).
   There is no file in this worktree that can carry that change, and a YOLO container
   has no cluster access. That remediation belongs on the spec's Verification ladder
   as an operator step, not in this prompt.
2. The sibling Sentry issue `NUKE-DEV-FM` was the same bug (same dangling ClusterIP,
   same `ErrOutOfBrokers`, `main.go:315`) and was resolved by
   `prompts/completed/067-spec-bug-nuke-dev-fm-kafka-brokers-runbook.md`, which created
   `docs/deployment-kafka-brokers.md`. That doc is landed at HEAD (commit 2d82d60) and
   is linked from `README.md` line 15. The GM re-file four days later is evidence that
   the runbook alone did not make the next occurrence cheaper to triage.
3. Prompt 067 explicitly deferred one follow-up: "adding the broker list to the startup
   error message in `main.go` ... if diagnosability is wanted, it is a separate
   follow-up." This spec is that separate follow-up. The in-container deliverable is
   therefore: make the crash-loop self-diagnosing by naming the configured broker list
   (non-secret) and the runbook in the startup error, plus keeping the runbook accurate.
4. If the reviewer instead intends this spec to be a no-op in this repository (the fix
   lives entirely in `bborbe/trading`), reject this prompt rather than approve it — the
   change here does not apply the deployment fix and must not be read as having done so.
-->

<summary>
- A pod that cannot reach its Kafka brokers now says which broker addresses it tried.
- The startup error also points at the deployment runbook, so the next operator is not left debugging the image.
- The change is error text only: no retry, no reachability probe, no startup gate, no new configuration.
- Broker addresses are treated as non-secret, which the deployment runbook already states; client certificates stay out of the message.
- A unit test pins both startup failure points to the enriched message, so a later refactor cannot silently drop the addresses.
- The deployment runbook's failure-mode section is updated so it matches the error the executor now emits.
- A changelog entry records the diagnostic improvement.
- The actual broker-list change and the Strimzi Service removal stay operator-side in the external config repository; nothing here claims to apply them.
</summary>

<objective>
Make the executor's Kafka startup failure self-diagnosing. Today a pod that crash-loops on Sarama `ErrOutOfBrokers` reports only `create sarama client` or `create kafka sync producer`, so a Sentry issue cannot tell an operator which broker address was configured or where the remediation lives. Append the configured (non-secret) broker list and a pointer to `docs/deployment-kafka-brokers.md` to the wrapped error, and pin that behaviour with a unit test.
</objective>

<context>
Read these files in full before changing anything:

- `main.go` (565 lines). The relevant parts are:
  - the `application` struct field `KafkaBrokers` (`required:"true"`, env `KAFKA_BROKERS`, usage "comma-separated Kafka broker addresses");
  - `func (a *application) Run(ctx context.Context, sentryClient libsentry.Client) error`. Inside it, the Kafka block is the two consecutive calls:
    ```go
    saramaClient, err := libkafka.CreateSaramaClient(
        ctx,
        libkafka.ParseBrokersFromString(a.KafkaBrokers),
    )
    if err != nil {
        return errors.Wrapf(ctx, err, "create sarama client")
    }
    defer saramaClient.Close()
    currentDateTimeGetter := libtime.NewCurrentDateTime()
    syncProducer, err := libkafka.NewSyncProducerFromSaramaClient(ctx, saramaClient)
    if err != nil {
        return errors.Wrapf(ctx, err, "create kafka sync producer")
    }
    defer syncProducer.Close()
    ```
  - `a.KafkaBrokers` is also forwarded to the consumer later in `Run`. Do not touch that forwarding site.
  - `github.com/bborbe/errors` is already imported as `errors`; `github.com/bborbe/kafka` as `libkafka`.
- `main_internal_test.go` (303 lines, `package main`). It already imports `context`, `strings` and `testing`, and declares plain `func TestXxx(t *testing.T)` tests such as `TestParseClaimModeFromEnv` and `TestValidateClaimTransport`. This is the file for the new test — plain `testing`, not Ginkgo.
- `main_test.go` — the single Ginkgo `TestSuite` entry point. Do not add a second suite.
- `docs/deployment-kafka-brokers.md` (69 lines) — the `## Failure mode: an unreachable broker set crash-loops the pod` section is the paragraph to update. The `## nuke-dev` and `## Remediation` sections are operator-side and must not change.
- `CHANGELOG.md` — the top is `# Changelog`, a blank line, the `All notable changes...` line, then `## v0.67.0`. There is no `## Unreleased` section.
- `README.md` line 15 already points at the runbook; do not change `README.md`.
- `prompts/completed/067-spec-bug-nuke-dev-fm-kafka-brokers-runbook.md` — the prior runbook prompt, for voice and for the deferred follow-up it names.

Coding-plugin guides (in-container paths):

- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `github.com/bborbe/errors` (`errors.Wrapf(ctx, err, ...)`), never `fmt.Errorf`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — plain `testing` vs Ginkgo conventions in this repo.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` placement and the required conventional prefix.
- `/home/node/.claude/plugins/marketplaces/coding/docs/documentation-guide.md` — `docs/` reference style.

Verified facts (do not re-derive):

- `libkafka.CreateSaramaClient(ctx context.Context, brokers Brokers, opts ...SaramaConfigOptions) (SaramaClient, error)` and `libkafka.NewSyncProducerFromSaramaClient(ctx context.Context, saramaClient SaramaClient) (SyncProducer, error)` both return a non-nil error on `ErrOutOfBrokers`. `libkafka.ParseBrokersFromString(value string) Brokers` does not return an error.
- `github.com/bborbe/errors` `Wrapf(ctx context.Context, err error, format string, args ...interface{}) error` returns `nil` when `err` is `nil`, otherwise wraps the cause with `pkg/errors.Wrapf` and attaches context data. The cause's message remains visible in `err.Error()`.
- Broker strings are `schema://host:port` (or bare `host:port`, defaulted to `plain://`); `libkafka` parses no userinfo, so the broker list carries no credentials. `docs/deployment-kafka-brokers.md` already states the value "is not secret; the Kafka client certificates are configured separately."
- `main_internal_test.go` follows the repo convention `stderrors "errors"` for the stdlib package (see `pkg/handler/task_event_handler_permanent_skip_test.go`). The bborbe `errors` package is not imported in this test file.
- The revive `file-length-limit` is 2000; both files have ample headroom. `golines --max-len=100` is enforced by `make precommit`.
</context>

<requirements>
1. **Add one unexported helper to `main.go`, immediately above `func (a *application) Run`.** It has no new imports:
   ```go
   // kafkaStartupError wraps a Kafka startup failure so the error names the
   // configured broker list and points at the deployment runbook. Broker
   // addresses are not secret — the Kafka client certificates are configured
   // separately — so naming them is safe, and a crash-looping pod's Sentry event
   // then shows which address was tried instead of repeating the stage alone.
   func kafkaStartupError(ctx context.Context, err error, stage string, brokers string) error {
       return errors.Wrapf(
           ctx,
           err,
           "%s: kafka brokers %q (see docs/deployment-kafka-brokers.md)",
           stage,
           brokers,
       )
   }
   ```
   The format string must keep `stage` as the leading text so the existing prefixes `create sarama client` and `create kafka sync producer` remain substrings of the message.

2. **Replace both Kafka startup error returns in `Run` with calls to the helper.** The two call sites become:
   ```go
   saramaClient, err := libkafka.CreateSaramaClient(
       ctx,
       libkafka.ParseBrokersFromString(a.KafkaBrokers),
   )
   if err != nil {
       return kafkaStartupError(ctx, err, "create sarama client", a.KafkaBrokers)
   }
   defer saramaClient.Close()
   currentDateTimeGetter := libtime.NewCurrentDateTime()
   syncProducer, err := libkafka.NewSyncProducerFromSaramaClient(ctx, saramaClient)
   if err != nil {
       return kafkaStartupError(ctx, err, "create kafka sync producer", a.KafkaBrokers)
   }
   defer syncProducer.Close()
   ```
   Change nothing else in `Run`. In particular, do not move the Kafka block, do not add a reachability probe or retry, and do not change the `a.KafkaBrokers` forwarding to the consumer.

3. **Add a plain Go test to `main_internal_test.go`.** Add `stderrors "errors"` to the import block (keep the existing imports; `context`, `strings`, `testing` are already there). Append this test at the end of the file:
   ```go
   // TestKafkaStartupErrorNamesBrokersAndRunbook pins the crash-loop diagnostic:
   // an unreachable broker set fails startup with an error that names the
   // configured brokers and points at the runbook, so the next Sentry issue is
   // self-diagnosing instead of repeating "create kafka sync producer" alone.
   func TestKafkaStartupErrorNamesBrokersAndRunbook(t *testing.T) {
       cause := stderrors.New("kafka: client has run out of available brokers")
       for _, stage := range []string{"create sarama client", "create kafka sync producer"} {
           err := kafkaStartupError(context.Background(), cause, stage, "10.43.21.228:9092")
           if err == nil {
               t.Fatalf("%s: expected an error", stage)
           }
           for _, want := range []string{
               stage,
               "10.43.21.228:9092",
               "docs/deployment-kafka-brokers.md",
               "out of available brokers",
           } {
               if !strings.Contains(err.Error(), want) {
                   t.Fatalf("%s: expected error to contain %q, got %q", stage, want, err.Error())
               }
           }
       }
       if err := kafkaStartupError(context.Background(), nil, "create sarama client", "x"); err != nil {
           t.Fatalf("a nil cause must stay nil, got %v", err)
       }
   }
   ```
   Both stage strings are exercised so a swapped or dropped argument at either call site is caught by the message assertions, and the nil case pins `Wrapf`'s contract.

4. **Update the failure-mode paragraph in `docs/deployment-kafka-brokers.md`.** Replace exactly this paragraph:
   ```
   If the configured brokers have no reachable endpoints,
   `CreateSaramaClient` and `NewSyncProducerFromSaramaClient` fail with Sarama
   `ErrOutOfBrokers`. The executor wraps the error as `create kafka sync
   producer` (or `create sarama client`) and exits non-zero, so the pod restarts
   and crash-loops. A startup failure of this shape means the broker list is
   wrong or unreachable — not that the image is broken.
   ```
   with:
   ```
   If the configured brokers have no reachable endpoints,
   `CreateSaramaClient` and `NewSyncProducerFromSaramaClient` fail with Sarama
   `ErrOutOfBrokers`. The executor wraps the error as `create kafka sync
   producer` (or `create sarama client`), appends the configured broker list,
   and points at this runbook, then exits non-zero, so the pod restarts and
   crash-loops. A startup failure of this shape means the broker list is wrong
   or unreachable — not that the image is broken. The broker list is printed
   because it is not secret; the Kafka client certificates are configured
   separately.
   ```
   Do not change any other section of the file.

5. **Add a changelog entry.** Insert a new `## Unreleased` heading directly above `## v0.67.0`, one blank line between sections, without moving or editing the `# Changelog` title, the `All notable changes...` line, or any released heading. Put exactly one bullet in it:
   ```
   - fix: **name the configured Kafka brokers in the startup error, so an unreachable broker set is self-diagnosing.** A pod that crash-looped on Sarama `ErrOutOfBrokers` used to report only `create sarama client` / `create kafka sync producer`, so the Sentry issue did not say which address was tried. The error now appends the configured `KAFKA_BROKERS` value — not secret, the client certificates are separate — and points at `docs/deployment-kafka-brokers.md`. No behaviour change beyond the error text.
   ```

6. **Self-check before finishing.** Re-run every command in `<verification>` and confirm each passes. Walk requirements 1-5 against the diff and confirm each is satisfied, and confirm no other error message, log line, metric, or startup ordering changed.
</requirements>

<constraints>
- This prompt changes the error text only. Do NOT add a broker reachability probe, a retry, a startup gate, a new configuration field, a feature flag, or a new metric.
- Do NOT apply the deployment fix. The nuke-dev `KAFKA_BROKERS` value and the dangling Strimzi Service are managed in `bborbe/trading` and on the cluster; no file in this repository can change them, and this prompt must not imply otherwise.
- Do NOT attempt any cluster access, `kubectl`, `docker`, `gh`, or dark-factory command. Nothing here is verifiable against a live cluster.
- Do NOT change the `## nuke-dev` or `## Remediation` sections of `docs/deployment-kafka-brokers.md`, and do not change `README.md`.
- Do NOT put secrets in the message. Broker addresses are non-secret; never add certificate material, passwords, or gateway secrets.
- Do NOT change the `a.KafkaBrokers` forwarding site to the consumer, and do not reorder the Kafka block relative to the surrounding startup steps.
- Do NOT add a second Ginkgo `RunSpecs`; the new test is plain `testing` in `main_internal_test.go`.
- Wrap errors with `github.com/bborbe/errors` (`errors.Wrapf(ctx, err, ...)`), never `fmt.Errorf`. Respect the 100-character line limit (`golines --max-len=100`) and the `funlen`, `gocognit` and `nestif` limits.
- Use `-mod=mod` as the Makefile does. Do not run `go mod vendor`, do not create a `vendor/` directory, and never use `-mod=vendor`.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run from the repository root:

```bash
go test -mod=mod -count=1 -v . > /tmp/main-test.out 2>&1; grep -q -- '--- PASS: TestKafkaStartupErrorNamesBrokersAndRunbook' /tmp/main-test.out && echo KAFKA_ERR_TEST_OK
grep -c 'func kafkaStartupError' main.go
grep -c 'kafkaStartupError(ctx, err, "create sarama client", a.KafkaBrokers)' main.go
grep -c 'kafkaStartupError(ctx, err, "create kafka sync producer", a.KafkaBrokers)' main.go
! grep -q 'errors.Wrapf(ctx, err, "create sarama client")' main.go && echo OLD_SARAMA_WRAP_GONE
! grep -q 'errors.Wrapf(ctx, err, "create kafka sync producer")' main.go && echo OLD_PRODUCER_WRAP_GONE
grep -c 'see docs/deployment-kafka-brokers.md' main.go
grep -c 'appends the configured broker list' docs/deployment-kafka-brokers.md
grep -c '^## Unreleased$' CHANGELOG.md
awk '/^## /{sec=$0} /name the configured Kafka brokers in the startup error/{print sec; exit}' CHANGELOG.md
make precommit
```

Expected: the first line prints `KAFKA_ERR_TEST_OK`; the three `grep -c` for `kafkaStartupError` / its call sites print `1` each; both `! grep -q` lines print `OLD_SARAMA_WRAP_GONE` and `OLD_PRODUCER_WRAP_GONE` (the old wrap forms were replaced, not duplicated); `grep -c 'see docs/deployment-kafka-brokers.md' main.go` prints `1`; `grep -c 'appends the configured broker list' docs/deployment-kafka-brokers.md` prints `1`; `grep -c '^## Unreleased$' CHANGELOG.md` prints `1`; the `awk` line prints `## Unreleased` (proving the bullet landed in the unreleased section, not under a released version); `make precommit` must pass.
</verification>
