---
status: completed
spec: [bug-nuke-dev-fm]
summary: 'Added docs/deployment-kafka-brokers.md recording the nuke-dev Kafka broker source of truth and crash-loop remediation, linked it from README.md, and added a docs: Unreleased changelog entry; make precommit passes.'
execution_id: 635cfd1e-b03a-5034-b27e-2960aa4d3fd8-exec-067-spec-bug-nuke-dev-fm-kafka-brokers-runbook
dark-factory-version: dev
created: "2026-11-15T12:45:00Z"
queued: "2026-11-15T11:28:57Z"
started: "2026-11-15T11:28:58Z"
completed: "2026-11-15T11:37:03Z"
---

# Record the nuke-dev executor's Kafka broker source of truth and crash-loop remediation

<!--
OPEN QUESTIONS FOR THE REVIEWER (non-interactive run; resolved best-effort from the spec):

1. The spec asserts the nuke-dev broker list's source of truth is
   `bborbe/trading/nuke-dev.env`. That repository is not readable from this
   worktree, so this prompt documents it as the spec states it and does not
   verify the path. If the config repo or file name differs, correct the doc
   before approving.
2. The dangling Strimzi Service is not named in the spec. The runbook therefore
   tells the operator to identify it by its ClusterIP (`10.43.21.228`) instead
   of hard-coding a name that could be wrong.
3. The actionable change — editing the Deployment's `KAFKA_BROKERS` and
   redeploying — lives in the external config repo and is operator-side. No code
   or manifest in this repository can apply it; this prompt records the
   reproducible runbook only.
4. Considered and deliberately excluded: adding the broker list to the startup
   error message in `main.go` (line 315). That is a code change the spec's
   operational fix does not require; if diagnosability is wanted, it is a
   separate follow-up.
-->

<summary>
- The executor's Deployment and its Kafka broker list are not managed in this repository; they live in the external config repo and are rendered by the Helm chart.
- A new runbook records where the nuke-dev broker list comes from and how to fix a Deployment that points at an unreachable in-cluster address.
- It names the external nodeport brokers nuke-dev should use and the config file that is their source of truth.
- It explains the crash-loop symptom — startup failing with an out-of-brokers error — so the next operator recognizes it instead of debugging the image.
- It gives an ordered remediation: confirm the current value, change the source of truth rather than only the live Deployment, redeploy, verify, and remove the dangling Strimzi Service last.
- The README points at the runbook so the deployment ownership is visible from the project's front page.
- A changelog entry records the documentation.
- No code, manifest, or chart change is made in this repository; the actual Deployment edit remains operator-side.
</summary>

<objective>
Record the operational fix for the nuke-dev executor's wrong Kafka broker list in this repository's documentation, so the next operator can recognize the crash-loop, find the correct broker set and its source of truth, and apply the remediation in a safe order. The Deployment itself lives in the external config repository; no code or manifest in this repository can apply the fix.
</objective>

<context>
This repository is publish-only for the image. The `Makefile` header states it explicitly: runtime config (Kafka brokers, Sentry DSN, stage vars) and the full deployment moved to the quant config repo. `README.md` line 13 says the executor is deployed via the `agent` Helm chart. The `k8s/` directory holds generated CRD client code, not Deployment manifests — do not add manifests there.

Read before editing:

- `README.md` — the intro paragraphs (through line 13, ending `published image \`docker.io/bborbe/agent-task-executor\`.`) and the `## Cluster routing` heading that follows. The new pointer belongs between them, as its own paragraph.
- `main.go` lines 50-53 and 306-318 — for accuracy only. `KafkaBrokers` is a required field on the `application` struct with env `KAFKA_BROKERS`; startup calls `libkafka.CreateSaramaClient(ctx, libkafka.ParseBrokersFromString(a.KafkaBrokers))` and `libkafka.NewSyncProducerFromSaramaClient(ctx, saramaClient)`, wrapping failures as `create sarama client` and `create kafka sync producer`. `a.KafkaBrokers` is also forwarded to the consumer at line 415. Do not modify `main.go`.
- `CHANGELOG.md` — the top is `# Changelog`, a blank line, the `All notable changes...` line, then `## v0.62.2`. There is no `## Unreleased` section; the new heading goes directly above `## v0.62.2`.
- `docs/agent-crd-specification.md` — an example of the `docs/` long-form reference style (plain markdown, `#` title, `##` sections).

Reference guides (in-container paths):

- `/home/node/.claude/plugins/marketplaces/coding/docs/documentation-guide.md` — `docs/` holds the full env-var/config reference; README stays short.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — a documentation-only change uses the `docs:` prefix; `## Unreleased` goes directly above the highest `## vX.Y.Z`.

The external facts in this prompt come from the spec `specs/in-progress/bug-nuke-dev-fm.md`: the nuke-dev executor used `10.43.21.228:9092` (an in-cluster k3s ClusterIP with no ready endpoints) instead of the external nodeport brokers `192.168.178.41:32159,192.168.178.42:32159,192.168.178.43:32159`, which are recorded in `bborbe/trading/nuke-dev.env`. The Strimzi Service is dangling and should be removed after confirming the current env value.
</context>

<requirements>
1. Create `docs/deployment-kafka-brokers.md` with exactly this content:

   ````markdown
   # Deployment: Kafka brokers for the executor

   This repository publishes the `bborbe/agent-task-executor` image. It does not
   own the executor Deployment: runtime configuration and the Deployment itself
   live in the quant config repository (`bborbe/trading`) and are rendered by the
   `agent` Helm chart. This document records where the executor's Kafka broker
   list comes from and how to remediate a Deployment that points at an
   unreachable broker set.

   ## `KAFKA_BROKERS`

   `KAFKA_BROKERS` is a required environment variable. `main.go` declares it on
   the `application` struct (`KafkaBrokers`, env `KAFKA_BROKERS`) and passes the
   value through `libkafka.ParseBrokersFromString` to
   `libkafka.CreateSaramaClient` and `libkafka.NewSyncProducerFromSaramaClient`
   during startup. The value is a comma-separated list of `host:port` broker
   addresses. It is not secret; the Kafka client certificates are configured
   separately.

   ## Failure mode: an unreachable broker set crash-loops the pod

   If the configured brokers have no reachable endpoints,
   `CreateSaramaClient` and `NewSyncProducerFromSaramaClient` fail with Sarama
   `ErrOutOfBrokers`. The executor wraps the error as `create kafka sync
   producer` (or `create sarama client`) and exits non-zero, so the pod restarts
   and crash-loops. A startup failure of this shape means the broker list is
   wrong or unreachable — not that the image is broken.

   ## nuke-dev

   The nuke-dev executor was observed pointing at the in-cluster Kafka ClusterIP
   `10.43.21.228:9092`, a k3s ClusterIP with no ready endpoints, and
   crash-looping on `ErrOutOfBrokers`. The correct value for nuke-dev is the
   external nodeport broker list, whose source of truth is
   `bborbe/trading/nuke-dev.env`:

   ```
   192.168.178.41:32159,192.168.178.42:32159,192.168.178.43:32159
   ```

   ## Remediation

   1. Confirm the current value before changing anything:

      ```bash
      kubectl -n <namespace> get deployment <executor> \
        -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="KAFKA_BROKERS")].value}'
      ```

   2. Change the value in its source of truth (`bborbe/trading`
      `nuke-dev.env`), not only on the live Deployment. A `kubectl set env` is
      overwritten by the next Helm render, so a live-only edit reappears as the
      same crash-loop after the next deploy.

   3. Redeploy or roll the executor Deployment and confirm the pod reaches
      Ready.

   4. Confirm the startup log no longer reports `ErrOutOfBrokers` or
      `create kafka sync producer`.

   5. Remove the dangling Strimzi Service only after confirming no remaining
      consumer uses it. Identify it by its ClusterIP (`10.43.21.228`) rather
      than assuming a name, and remove it last, so a wrong guess cannot break
      another workload.

   ## Related

   - `README.md` — the executor's runtime environment surface.
   - `docs/agent-crd-specification.md` — the Config CRD.
   ````

2. In `README.md`, add one paragraph immediately after the intro paragraph that ends `published image \`docker.io/bborbe/agent-task-executor\`.` and before the `## Cluster routing` heading, separated by blank lines:

   ```markdown
   Runtime configuration (including `KAFKA_BROKERS`) and the Deployment itself live outside this repository; see [`docs/deployment-kafka-brokers.md`](docs/deployment-kafka-brokers.md) for the broker source of truth and the crash-loop remediation.
   ```

   Do not change any other README content.

3. In `CHANGELOG.md`, insert a new `## Unreleased` heading directly above `## v0.62.2` (one blank line between sections; do not move or edit the preamble or any released heading) with exactly one bullet:

   ```markdown
   - docs: **record where the executor's `KAFKA_BROKERS` comes from and how to remediate a crash-looping Deployment.** The nuke-dev executor pointed at an in-cluster ClusterIP with no ready endpoints, so startup failed with Sarama `ErrOutOfBrokers`. The runbook names the external nodeport broker list, the `bborbe/trading` source of truth, and the remediation order: confirm the current value, update the source of truth, redeploy, and remove the dangling Strimzi Service last. No code change — the Deployment lives outside this repository.
   ```

4. Before finishing, re-run `<verification>` and confirm every check passes.
</requirements>

<constraints>
- Documentation only. Do NOT modify any Go file, test, Makefile, Dockerfile, CRD, `k8s/` file, or manifest.
- Do NOT invent a Strimzi Service name or namespace. The spec does not name it; the runbook tells the operator to identify it by ClusterIP.
- Do NOT state that the fix is applied. The Deployment lives in `bborbe/trading` and is edited there by an operator.
- Do NOT include secret material. Broker addresses are not credentials; the Kafka client certificates are separate and are out of scope here.
- Do NOT add a new deployment manifest or a script that needs cluster access.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run from the repository root:

```bash
test -f docs/deployment-kafka-brokers.md
grep -c 'KAFKA_BROKERS' docs/deployment-kafka-brokers.md
grep -c 'ErrOutOfBrokers' docs/deployment-kafka-brokers.md
grep -c 'bborbe/trading' docs/deployment-kafka-brokers.md
grep -Fc '192.168.178.41:32159,192.168.178.42:32159,192.168.178.43:32159' docs/deployment-kafka-brokers.md
grep -c '10.43.21.228' docs/deployment-kafka-brokers.md
grep -c 'docs/deployment-kafka-brokers.md' README.md
grep -c '^## Unreleased$' CHANGELOG.md
awk '/^## /{sec=$0} /record where the executor/{print sec}' CHANGELOG.md
make precommit
```

Every `grep -c` must print a value of at least `1`. `grep -c '^## Unreleased$' CHANGELOG.md` must print `1`, and the `awk` line must print `## Unreleased` — together they prove the bullet landed in the unreleased section rather than under a released version. `make precommit` must pass.
</verification>
