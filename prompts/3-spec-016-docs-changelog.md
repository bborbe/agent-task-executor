---
status: draft
spec: [016-bug-claim-only-until-quota]
created: "2026-10-24T19:34:00Z"
branch: dark-factory/bug-claim-only-until-quota
---

<summary>
- The claim lease protocol document now states the admission rule: the bound is the smaller of the declared cap and the pod quota, and a task over the bound is never claimed.
- The README states the same rule for operators, next to the cluster-routing and reconcile configuration sections.
- Both documents name the decision-line fields (`running`, `cap`, `max`, `quota`) an operator greps to see which bound fired.
- The CHANGELOG gains an `Unreleased` entry describing the behaviour change, placed so the release tooling reads it as unreleased.
- No Go code, tests, manifests, or configuration values change in this prompt.
- The full repository precommit suite still passes.
</summary>

<objective>
Document the new admission rule and record the change in the changelog, so an operator reading the protocol or the README understands why a task over the effective bound is never claimed and how to tell which bound bound it.
</objective>

<context>
Read `CLAUDE.md` for project conventions.

Read these files before editing:
- `docs/claim-lease-protocol.md` — in particular `## Claim state machine`, which defines the held state and already says the fence and takeover rules exist to bound a running holder. The new section must sit directly after `## Claim state machine` and before `## Fence`.
- `README.md` — `## Cluster routing` and `## Reconcile loop configuration` are the surrounding operator-facing sections. The new section goes directly after `## Cluster routing`, before `## Reconcile loop configuration`.
- `CHANGELOG.md` — read the top of the file: there is currently no `## Unreleased` section; the newest section is `## v0.36.1`. Entry style is `- <prefix>: <what> [context]`, one bullet per logical change.
- `pkg/handler/concurrency_capacity.go` and `pkg/handler/task_event_handler.go` (`deferIfAtConcurrencyCap`) — the exact decision-line field names the documentation must name.
- `pkg/spawner/pod_quota_limiter.go` — the quota selection rule (priority-class scope selector, `status.hard` then `spec.hard`, 30 s cache, failed read falls back to the declared cap).

The post-deploy verification for this change (`kubectlnukedev` reads, a live `event=concurrency_admit` line) is operator-executable and lives on the spec's Verification ladder, not in this prompt. Do not attempt any cluster command here. What this prompt owes the operator is prose that names the fields that check greps.
</context>

<requirements>
1. In `docs/claim-lease-protocol.md`, insert a new `## Admission capacity` section directly after `## Claim state machine` and before `## Fence`. It must state, in prose:
   - a cluster admits a task only when the assignee has a free slot, and the number of slots is **the smaller of** the Config's declared `maxConcurrentJobs` and the pod quota that scopes the agent's `priorityClassName`;
   - the quota is the `hard.pods` of the `ResourceQuota` whose `scopeSelector` matches `PriorityClass In [<priorityClassName>]`, preferring `status.hard` and falling back to `spec.hard`, and it is read at most once per namespace and priority class per 30 seconds;
   - a zero `maxConcurrentJobs` means "no declared cap" and does not disable the quota bound; zero for both means uncapped; a failed quota read, a Config with no priority class, or no quota scoping that class leaves the declared cap as the only bound (with one warning on a failed read);
   - because admission is decided before the claim is taken, a task over the bound is **never claimed** — it is deferred and a peer cluster can take it immediately — which is why the fence and takeover rules only ever have to bound a holder that is actually running;
   - the decision is visible in one line: `event=concurrency_admit` and `event=concurrency_cap` both carry `running=`, `cap=`, `max=` and `quota=`, where `cap` is the effective bound, `max` the declared value and `quota` the value read from the quota (0 when no quota bound applies).
2. In `README.md`, insert a new `## Admission capacity` section directly after `## Cluster routing` and before `## Reconcile loop configuration`. Cover the same rule in operator-facing terms, and include the literal phrases **smaller of** and **never claim** (e.g. "a task over the bound is never claimed"). Name the two counters that record a deferral (`agent_executor_task_events_total{result="deferred_concurrency_cap"}` and `agent_executor_deferred_concurrency_cap_total{assignee=...}`) and the decision-line fields an operator greps. Note that a denied quota read (for example before the executor's Role gains the `resourcequotas` grant) degrades to the declared cap and logs `event=pod_quota_read_failed`, so shipping the executor before the RBAC change is safe.
3. In `CHANGELOG.md`, add a `## Unreleased` section immediately below the file's introductory paragraph and above the current newest `## v...` section. Add exactly one bullet, prefixed `fix:`, in the existing style, containing the phrase "pod quota". The bullet must describe the behaviour change: admission is bounded by the smaller of the declared concurrency cap and the pod quota of the agent's priority class, so a cluster no longer claims a task whose Job its quota would refuse, and the peer pool can take the task immediately. Mention the 30 s cache, the fallback on a failed read, and that the decision line now carries `running`, `cap`, `max` and `quota`. Do not add a second bullet mentioning "pod quota".
4. Run the three documentation checks and the changelog check below; all must pass. `grep -ciE` must print a count of at least 1 for each of the four phrase greps.
5. Before finishing, re-run `<verification>` and confirm every command exits 0.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT change any Go file, test, manifest, `ResourceQuota` object, `maxConcurrentJobs` value, Helm chart, or RBAC rule in this prompt.
- Do NOT restate or alter the lease constants; the existing `## Constants` section is unchanged.
- Do NOT add a `NOTE FOR OPERATOR` block or any operator-instruction block to the documents beyond the prose required above; the post-deploy rung belongs to the spec.
- Do NOT duplicate the whole protocol into the README — the README section is operator-facing and should stay short (roughly 8-15 lines).
- The CHANGELOG bullet must sit under `## Unreleased`. No line containing "pod quota" may sit under a `## v...` heading.
- Existing documentation structure and headings must otherwise be preserved.
</constraints>

<verification>
```
ROOTDIR=/workspace make precommit
grep -ciE 'smaller of' docs/claim-lease-protocol.md
grep -ciE 'never claim' docs/claim-lease-protocol.md
grep -ciE 'smaller of' README.md
grep -ciE 'never claim' README.md
awk '/^## /{sec=$0} /pod quota/{print "sits under: " sec}' CHANGELOG.md
```
`make precommit` must exit 0. Each of the four `grep -ciE` commands must print a number >= 1. The `awk` command must print at least one `sits under: ## Unreleased` line and no `sits under: ## v` line.
</verification>
