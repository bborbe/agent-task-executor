---
status: completed
spec: [bug-nuke-dev-fm]
summary: Refreshed the nuke-dev Kafka broker runbook to name bborbe/trading-lib/nuke-dev.env plus its nuke/agent/values-dev.yaml mirror, documented the committed value is correct and the re-render/roll step is the recurring gap, and added a docs changelog entry.
execution_id: 5ea286a9-691b-5eb1-80ff-968e522074f0-exec-070-spec-bug-nuke-dev-fm-runbook-source-of-truth
dark-factory-version: dev
created: "2026-11-27T07:26:03Z"
queued: "2026-11-27T07:31:23Z"
started: "2026-11-27T07:31:24Z"
completed: "2026-11-27T07:44:13Z"
---

# Refresh the nuke-dev Kafka broker runbook to the current source of truth

<!--
OPEN QUESTIONS FOR THE REVIEWER (non-interactive run; resolved best-effort from the spec):

1. Repository name. The runbook created by the previous generation of this spec
   (completed prompt 067) names the broker list's source of truth as
   `bborbe/trading/nuke-dev.env`. The current spec — re-filed 2026-11-27 from a
   fresh deep verdict — names it as `bborbe/trading-lib/nuke-dev.env` (mirrored by
   `nuke/agent/values-dev.yaml`). Neither external repository is readable from this
   worktree, so this prompt follows the current spec. If `bborbe/trading` is still
   the live source of truth (i.e. `trading-lib` is a spec typo), correct the doc
   before approving rather than shipping a rename that points at the wrong repo.

2. Scope. The spec's Recommended Fix is entirely operator-side: edit an external
   config repo, re-render/roll the Deployment, remove the dangling Strimzi Service.
   No code or manifest in this repository can apply it, and no test in this
   repository can observe it. This prompt therefore only refreshes the in-repo
   runbook so the operator who applies it edits the right files and performs the
   re-render/roll step that has repeatedly gone unapplied.

3. Considered and deliberately excluded: enriching the startup error at `main.go`
   (the `create kafka sync producer` / `create sarama client` wrap) to name the
   configured broker addresses, so a crash log self-diagnoses the bad value. The
   previous generation of this spec considered and excluded it as a separate
   follow-up, and this spec's Recommended Fix does not require it. If diagnosability
   is wanted, it is a separate prompt.
-->

<summary>
- The executor's Deployment and its Kafka broker list are managed outside this repository, in external config repos and on the cluster.
- The runbook that explains how to fix a crash-looping Deployment now names the current config source of truth.
- It also names the second config file that mirrors the broker list, and states that both must carry the same value.
- It records that the committed broker list is already correct, so the recurring failure is a Deployment that was never re-rendered.
- It calls out the re-render/roll step as the one that keeps getting skipped, so the next operator does not stop at the config edit.
- The ordered remediation keeps its shape: confirm the current value, edit both config files, re-render and roll, verify Ready with no out-of-brokers error, and remove the dangling Service last.
- A changelog entry records the documentation update.
- No code, manifest, or chart change is made here; the actual Deployment edit stays operator-side.
</summary>

<objective>
Keep the in-repo runbook for the nuke-dev executor's Kafka broker crash-loop accurate against the current spec, so the operator who follows it edits the correct config files and performs the re-render/roll step that has repeatedly gone unapplied. The Deployment itself lives in external config repositories and on the cluster; no code or manifest in this repository can apply the fix, so this change is documentation only.
</objective>

<context>
This repository publishes the `bborbe/agent-task-executor` image; it does not own the executor Deployment. The `Makefile` header and `README.md` both say runtime configuration and the Deployment live in an external config repo and are rendered by the `agent` Helm chart. The `k8s/` directory holds generated CRD client code, not Deployment manifests — do not add manifests there.

Read before editing:

- `docs/deployment-kafka-brokers.md` — the whole file. This is the runbook you are correcting. It currently names `bborbe/trading/nuke-dev.env` as the source of truth and has no mention of the `nuke/agent/values-dev.yaml` mirror.
- `README.md` — the pointer paragraph (line 15 on a clean baseline): *"Runtime configuration (including `KAFKA_BROKERS`) and the Deployment itself live outside this repository; see [`docs/deployment-kafka-brokers.md`](docs/deployment-kafka-brokers.md) for the broker source of truth and the crash-loop remediation."* It does not name the config repo, so it needs no change. Do not edit it.
- `CHANGELOG.md` — the top is `# Changelog`, a blank line, the `All notable changes...` line, then `## v0.71.2`. There is no `## Unreleased` section; the new heading goes directly above `## v0.71.2`.
- `main.go` — the `application` struct field `KafkaBrokers` (env `KAFKA_BROKERS`) and the startup block that calls `libkafka.CreateSaramaClient(ctx, libkafka.ParseBrokersFromString(a.KafkaBrokers))` and `libkafka.NewSyncProducerFromSaramaClient(ctx, saramaClient)`, wrapping failures as `create sarama client` and `create kafka sync producer`. Read for accuracy only; do not modify `main.go`.

Coding-plugin guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/documentation-guide.md` — `docs/` holds the full reference; README stays short.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — a documentation-only change uses the `docs:` prefix; `## Unreleased` goes directly above the highest `## vX.Y.Z`.

Boundary: the source-of-truth config file (`bborbe/trading-lib/nuke-dev.env`), its mirror (`nuke/agent/values-dev.yaml`), the live Deployment, and the dangling Strimzi Service all live outside this repository. The container agent has only this repo. Do not attempt to edit, render, deploy, or verify any of them; the runbook is the deliverable.

The external facts in this prompt come from the current spec `specs/in-progress/bug-nuke-dev-fm.md`: the nuke-dev executor used `10.43.21.228:9092` (an in-cluster k3s ClusterIP with no ready endpoints) instead of the external nodeport brokers `192.168.178.41:32159,192.168.178.42:32159,192.168.178.43:32159`, which live in `bborbe/trading-lib/nuke-dev.env` and are mirrored into `nuke/agent/values-dev.yaml`. The committed value is already correct; the live Deployment still crash-loops on Sarama `ErrOutOfBrokers` because it was never re-rendered after the config was corrected. The dangling Strimzi Service is removed last.
</context>

<requirements>
1. **Correct the source-of-truth repository name in the opening paragraph of `docs/deployment-kafka-brokers.md`.** The paragraph currently reads:
   ```
   live in the quant config repository (`bborbe/trading`) and are rendered by the
   `agent` Helm chart.
   ```
   Change it to:
   ```
   live in the quant config repository (`bborbe/trading-lib`) and are rendered by
   the `agent` Helm chart.
   ```
   Change nothing else in that paragraph.

2. **Correct the `## nuke-dev` section and record the mirror and the current state.** Replace the paragraph that currently reads:
   ```
   The nuke-dev executor was observed pointing at the in-cluster Kafka ClusterIP
   `10.43.21.228:9092`, a k3s ClusterIP with no ready endpoints, and
   crash-looping on `ErrOutOfBrokers`. The correct value for nuke-dev is the
   external nodeport broker list, whose source of truth is
   `bborbe/trading/nuke-dev.env`:
   ```
   with:
   ```
   The nuke-dev executor was observed pointing at the in-cluster Kafka ClusterIP
   `10.43.21.228:9092`, a k3s ClusterIP with no ready endpoints, and
   crash-looping on `ErrOutOfBrokers`. The correct value for nuke-dev is the
   external nodeport broker list, whose source of truth is
   `bborbe/trading-lib/nuke-dev.env` and which is mirrored into
   `nuke/agent/values-dev.yaml`. The committed value is already correct; the
   recurring failure is that the live Deployment was never re-rendered after the
   config was corrected, so it keeps the old in-cluster address until the next
   deploy.
   ```
   Leave the fenced broker-list block that follows (`192.168.178.41:32159,192.168.178.42:32159,192.168.178.43:32159`) unchanged.

3. **Name both config files in remediation step 2 and state the both-must-match invariant.** Replace the step that currently reads:
   ```
   2. Change the value in its source of truth (`bborbe/trading`
      `nuke-dev.env`), not only on the live Deployment. A `kubectl set env` is
      overwritten by the next Helm render, so a live-only edit reappears as the
      same crash-loop after the next deploy.
   ```
   with:
   ```
   2. Change the value in its source of truth — `bborbe/trading-lib/nuke-dev.env`
      and its mirror `nuke/agent/values-dev.yaml` — not only on the live
      Deployment. The two files must carry the same value: a change made in only
      one is reverted by the next render, and a `kubectl set env` is overwritten
      by the next Helm render, so a live-only edit reappears as the same
      crash-loop after the next deploy.
   ```

4. **Mark the re-render/roll step as the one that recurringly goes unapplied.** Replace remediation step 3, which currently reads:
   ```
   3. Redeploy or roll the executor Deployment and confirm the pod reaches
      Ready.
   ```
   with:
   ```
   3. Re-render and roll the executor Deployment, then confirm the pod reaches
      Ready. This is the step that recurringly goes unapplied: correcting the
      committed config without re-rendering leaves the live Deployment on the old
      value and the pod keeps crash-looping.
   ```
   Leave steps 1, 4 and 5, the `## Remediation` heading, and the `## Related` section otherwise unchanged.

5. **Add a changelog entry.** Insert a new `## Unreleased` heading directly above `## v0.71.2` in `CHANGELOG.md` (one blank line between sections; do not move or edit the preamble or any released heading) with exactly one bullet:
   ```
   - docs: **the nuke-dev Kafka broker runbook now names the source of truth as `bborbe/trading-lib/nuke-dev.env`, mirrored by `nuke/agent/values-dev.yaml`, and calls out the re-render/roll step that recurringly goes unapplied.** The committed broker list is already the external nodeport set; the live Deployment still crash-loops on `ErrOutOfBrokers` because it was never re-rendered after the config was corrected. The runbook now requires both config files to carry the same value and keeps the dangling Strimzi Service removal last. No code change — the Deployment lives outside this repository.
   ```

6. **Self-check before finishing.** Re-run every command in `<verification>` and confirm each passes; walk requirements 1-5 against the diff and confirm each is satisfied.
</requirements>

<constraints>
- Documentation only. Do NOT modify any Go file, test, Makefile, Dockerfile, CRD, `k8s/` file, or manifest.
- Do NOT invent a Strimzi Service name or namespace. The spec does not name it; the runbook identifies it by its ClusterIP (`10.43.21.228`).
- Do NOT state that the fix has been applied. The Deployment lives in external config repositories and on the cluster, and is edited there by an operator.
- Do NOT include secret material. Broker addresses are not credentials; the Kafka client certificates are separate and out of scope here.
- Do NOT add a deployment manifest or a script that needs cluster access.
- Leave the `README.md` pointer paragraph and the `## Related` section of the runbook unchanged.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run from the repository root:

```bash
test -f docs/deployment-kafka-brokers.md && echo DOC_OK
grep -q 'bborbe/trading-lib/nuke-dev.env' docs/deployment-kafka-brokers.md && echo SOT_OK
grep -q 'nuke/agent/values-dev.yaml' docs/deployment-kafka-brokers.md && echo MIRROR_OK
! grep -qE 'bborbe/trading([^-]|$)' docs/deployment-kafka-brokers.md && echo NO_STALE_TRADING
grep -Fq '192.168.178.41:32159,192.168.178.42:32159,192.168.178.43:32159' docs/deployment-kafka-brokers.md && echo BROKERS_OK
grep -q '10.43.21.228' docs/deployment-kafka-brokers.md && echo OLD_IP_OK
grep -c '^## Unreleased$' CHANGELOG.md
awk '/^## /{sec=$0} /runbook now names the source of truth/{print sec; exit}' CHANGELOG.md
make precommit
```

- `DOC_OK`, `SOT_OK`, `MIRROR_OK`, `NO_STALE_TRADING`, `BROKERS_OK`, `OLD_IP_OK` must each print.
- `grep -c '^## Unreleased$' CHANGELOG.md` must print `1`.
- The `awk` line must print `## Unreleased` — together with the previous check this proves the bullet landed in the unreleased section rather than under a released version.
- `make precommit` must pass.
</verification>
