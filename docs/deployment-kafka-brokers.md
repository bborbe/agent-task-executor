# Deployment: Kafka brokers for the executor

This repository publishes the `bborbe/agent-task-executor` image. It does not
own the executor Deployment: runtime configuration and the Deployment itself
live in the quant config repository (`bborbe/trading-lib`) and are rendered by
the `agent` Helm chart. This document records where the executor's Kafka broker
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
`bborbe/trading-lib/nuke-dev.env` and which is mirrored into
`nuke/agent/values-dev.yaml`. The committed value is already correct; the
recurring failure is that the live Deployment was never re-rendered after the
config was corrected, so it keeps the old in-cluster address until the next
deploy.

```
192.168.178.41:32159,192.168.178.42:32159,192.168.178.43:32159
```

## Remediation

1. Confirm the current value before changing anything:

   ```bash
   kubectl -n <namespace> get deployment <executor> \
     -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="KAFKA_BROKERS")].value}'
   ```

2. Change the value in its source of truth — `bborbe/trading-lib/nuke-dev.env`
   and its mirror `nuke/agent/values-dev.yaml` — not only on the live
   Deployment. The two files must carry the same value: a change made in only
   one is reverted by the next render, and a `kubectl set env` is overwritten
   by the next Helm render, so a live-only edit reappears as the same
   crash-loop after the next deploy.

3. Re-render and roll the executor Deployment, then confirm the pod reaches
   Ready. This is the step that recurringly goes unapplied: correcting the
   committed config without re-rendering leaves the live Deployment on the old
   value and the pod keeps crash-looping.

4. Confirm the startup log no longer reports `ErrOutOfBrokers` or
   `create kafka sync producer`.

5. Remove the dangling Strimzi Service only after confirming no remaining
   consumer uses it. Identify it by its ClusterIP (`10.43.21.228`) rather
   than assuming a name, and remove it last, so a wrong guess cannot break
   another workload.

## Related

- `README.md` — the executor's runtime environment surface.
- `docs/agent-crd-specification.md` — the Config CRD.
