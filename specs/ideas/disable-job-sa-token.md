---
status: idea
---

## Summary

- Agent Job pods currently mount a Kubernetes ServiceAccount token they never use
- Set `automountServiceAccountToken: false` on the spawned Job's pod template
- One line in the spawner; no behaviour change for any existing agent

## Problem

The spawner sets neither `ServiceAccountName` nor `AutomountServiceAccountToken` on the Job pod template, so Kubernetes applies its default: the namespace's `default` ServiceAccount, with its token projected into every agent container.

Agents do not talk to the Kubernetes API. They read a task, do work, and publish a result to Kafka — the executor spawns and forgets (`docs/agent-job-interface.md`). The token is dead weight.

**Today it grants almost nothing.** Verified 2026-08-08 on quant:

```
kubectlquant auth can-i --list --as=system:serviceaccount:dev:default
→ selfsubjectreviews, selfsubjectaccessreviews, /api discovery. Nothing else.
```

So this is not an open hole — it is an unnecessary credential sitting in a pod that executes LLM output derived from untrusted input (PR titles, bodies, diffs). Two ways it becomes one later, both silent:

1. Someone binds a Role to `default` in that namespace for an unrelated reason. Every agent gains it at once, with no change to any agent's config and nothing in review to catch it.
2. A cluster or K3s upgrade changes what `default` can reach.

Removing the token now means neither can happen. It also costs nothing: no agent loses a capability it uses.

## Goal

No agent Job pod carries a Kubernetes API token. An agent that tries to reach the API server has no credential to present, regardless of what RBAC exists in its namespace.

## Non-goals

- Giving agents a dedicated ServiceAccount — they need no API access at all, so the right amount is none rather than a scoped one
- Changing executor RBAC (it needs its own permissions to spawn Jobs)
- Namespace or NetworkPolicy work — see `bborbe/agent` specs 047 and 048

## Sketch

Set `AutomountServiceAccountToken: ptr.To(false)` on the Job's `PodSpec` in the spawner, unconditionally — not a Config knob. An agent needing API access would be a design change worth its own discussion, not a per-agent toggle.

Verification is a unit test asserting the field on the generated `batchv1.Job`, plus a post-deploy check that `/var/run/secrets/kubernetes.io/serviceaccount` is absent inside a running agent pod.

## Context

Raised 2026-08-08 while deciding whether each agent type needs its own namespace (answer: no — see `bborbe/agent:docs/agent-network-security.md` and the vault goal [[Enable NetworkPolicy enforcement on K3s cluster]]). Filed as an idea rather than folded into approved spec `002-config-job-namespace` — that spec is approved and re-opening it would mean unapprove → edit → re-audit → re-approve for a one-line change that is independent of it.

## Related

- `specs/in-progress/002-config-job-namespace.md` — the other spawner-side change; could ship together if 002 is ever reopened
- `bborbe/agent:docs/agent-network-security.md` — threat model and the two-direction blast radius
- `docs/agent-job-interface.md` — confirms the executor does not watch Jobs or read stdout, so agents need no API access
