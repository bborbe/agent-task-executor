# Claim lease protocol

A claim is a ref on the shared vault origin written through git-rest
(`refs/claims/...`). Exactly one executor may run a task at a time: the holder
of the task's claim. This document describes the protocol implemented by
`pkg/claim`. The constants below are invariants, not configuration — there is
no knob, flag or opt-out for any of them.

## The spawn guard reads a claim, it never writes one

A task's frontmatter carries a `current_job`, and it is shared through the
vault, so it can name a Job another cluster created. `IsJobActive` lists only
this cluster's Jobs, so "no active Job here" does not mean the Job is over.

When this executor's own store has no entry for the task — a restart, or a task
it never spawned — the guard reads the task's claim (`GET` on the claim route)
and treats a live claim as proof that some cluster is still running the task:
the spawn is suppressed and re-deferred. `claim_free` lets the respawn proceed,
a claim past its lease **by the takeover grace** counts as free — not one merely
past its lease, because inside that grace the protocol still treats the holder as
possibly running — an unreadable lease counts as held, and a failed read
suppresses and retries on the next pass. The read is
one request per dispatch pass on a task whose store entry is empty, and none
otherwise. Only claim mode `on` probes: `shadow` takes claims but never blocks a
dispatch, so a held claim must not suppress a spawn there, and treating shadow as
unavailable is what keeps the probe from quietly turning it into `on`.

⚠️ **Two residuals, both real.** Claim mode `off` keeps no claim at all, so the
guard falls back to local evidence and says so in
`event=foreign_job_unprobeable`. And a claim's absence only proves that no
*claim-taking* cluster holds the task: a peer running claim mode off, or an image
older than `v0.26.2`, writes no claim, so a task it is running stays invisible
here and the respawn can still duplicate it. That gap closes when every cluster
takes claims, not by anything this side can read.

This is why the success hold matters beyond the peer's `Claim` being refused:
the held claim is also what stops a cold peer from respawning the task inside
the success grace.

## Constants

| Constant | Value | Meaning |
|---|---|---|
| Lease length | 300 s | How long a claim stays valid without a renew. |
| Tick interval | 10 s | How often the lease loop evaluates the fence and renews. |
| Renew interval | 60 s | How often a held claim is renewed. |
| Fence margin | 60 s | `fenceAt` is this long before `lease_until`. |
| Fence lead | 10 s | The fence fires when `now > fenceAt − 10 s`, so a 10 s tick lands at or before `fenceAt`. |
| Takeover grace | 30 s | A claim may be taken over no earlier than `lease_until + 30 s`. |
| Jobless grace | 300 s | A held claim whose Job has been continuously not running for this long is released. |
| Jobless check interval | 60 s | How often the lease loop asks whether a held claim's Job is still running. |
| Jobless pass budget | 10 s | One jobless pass starts no new Job check after this long; due claims are visited oldest-check-first, so claims a pass did not reach are checked first on the following ticks. A check already started may still run up to the request timeout. |
| Request timeout cap | 30 s | Hard cap on every git-rest request; a tick also caps each request at the earliest `fenceAt` among held claims. |

`fenceAt = lease_until − 60 s`. The fence fires when `now > fenceAt − 10 s`.

## Claim keys

Every claim lives at one of three key shapes. Owner and repo are lowercased in
every key, and keys and task ids are never metric label values.

| Key shape | Used by |
|---|---|
| `claims/task/<task id>` | task-id-keyed types, and `pr-review` with no usable PR link |
| `claims/repo/<owner>-<repo>` | `update-go`, `release`, `build-fix` |
| `claims/pr/<owner>-<repo>-<n>` | `pr-review` with a usable PR link |

A `pr-review` key is per pull request when the task body carries a standalone
pull-request URL: the first body line that, after trimming surrounding
whitespace, is exactly `https://github.com/<o>/<r>/pull/<n>`, where `<o>/<r>`
matches the task repository case-insensitively, `<n>` is 1–10 digits with a
value of at least 1, and nothing follows `<n>`.

When no line qualifies, the key falls back to `claims/task/<task id>` — never to
the per-repository key, which every review of that repository would then share.
The fallback logs one `event=claim_key_fallback task=<id> repo=<owner>/<repo>`
warning and increments the unlabelled `executor_claim_key_fallback_total`
counter once.

### Single ref per claim

Every claim is one ref. No task type writes a pair.

**During a rolling deploy the fleet is mixed**, in two distinct ways.

The *URL branch* is safe: both images derive `claims/pr/<owner>-<repo>-<n>`, so
old and new contend on the same ref exactly as before.

The *fallback branch* is not. An executor on the previous image keys a
URL-less pr-review by `claims/repo/<owner>-<repo>`; this one keys it by
`claims/task/<task id>`. The two shapes share no ref, so for the duration of the
rollout an old and a new executor can each claim the same URL-less task and each
spawn a Job for it — the duplicate run the pair existed to prevent. Only a
uniform fleet closes that window; keeping the fallback on the repository key
until the rollout finished would have avoided it, at the cost of leaving the
whole fallback population serialised.

The measured population at the 2026-10-25 change made it harmless in practice.
Two different figures are quoted for it and they answer different questions:
across **all** repositories the store holds 10,510 pr-review records, 333 of them
URL-less, of which **30 were open** — 25 `planning` untrusted-author records that
cannot spawn and 5 `human_review`, which is not driven. Within
`bborbe/homebrew-tap` alone, **25 of its 32** records are URL-less. No runnable
URL-less review was open, which is why the window was accepted. A URL-less
review that becomes runnable *during* a rollout can still run twice.

The *recovery* side has a smaller transient: an old image re-derives and writes
the per-repository partner when its `Recover` meets a per-PR-only ref,
re-creating the pair until that pod is replaced. That one is self-clearing — the
old pod releases its pair, or the lease expires and is fenced on the next
recovery — so it cannot permanently block a repo-keyed task.

A `pr-review` task with a usable pull-request URL is keyed **only** by
`claims/pr/<owner>-<repo>-<n>`; one without takes `claims/task/<task id>`. It
never takes the shared per-repository key, on either branch. That is deliberate:
while the per-repository key was the primary of a pair, the first review of a
repository held it, and every sibling review of the same repository was refused
at takeover with a bare `lease conflict` for as long as the holder renewed — so
no second review of that repository could start. Measured 2026-10-25 on nuke
prod: one acquired `claims/repo/bborbe-nuke` refused all eleven other
`bborbe/nuke` reviews. The fallback branch matters as much as the URL branch:
25 of the 32 `bborbe/homebrew-tap` records carry no pull-request URL at all —
they are untrusted-author records parked pending an allowlist decision — so
keying the fallback by repository would keep that whole queue contending for one
key.

The consequence is that a `pr-review` and a repo-keyed task (`update-go`,
`release`, `build-fix`) may now run against the same repository concurrently.
Those types keep the per-repository key among themselves, which is the
serialization they need; reviews do not participate in it.

The recovery path adopts a listed ref exactly as it is found. It never derives a
partner key, so a restart cannot re-create a pair that the claim path no longer
writes. A pair still present in the store from an earlier release is recovered
as a pair by `storeRecoveredPair`, which is the only path that handles one;
`ClaimPair` and `TakeoverPair` keep their secondary parameter but every caller
now passes it empty.

Every key passes `validateClaimComponent`, so a hostile body cannot inject a
path segment into a ref.

These log lines make claims observable at runtime:

- `event=claim_acquired key=<key> task=<id>` — one per successful `Claim` or
  `Takeover`.
- `event=claim_released key=<key> task=<id> result=<label> sha=<sha>` — one per
  removal, with `result` one of `released`, `already_released`, `fenced` or
  `claim_lost`. The `claim_lost` line is logged at warning level.
- `event=claim_fence_pending key=<key> task=<id>` — warning, logged when the
  fence cannot delete the claim's Job. `fenced` is counted then and the claim
  stays held; the `event=claim_released … result=fenced` line follows once a
  later tick deletes the Job and the ref, without counting again.
- `event=claim_jobless_released key=<key> task=<id> jobless_since=<RFC3339>` —
  warning, logged when a held claim's Job has been continuously not running for
  the jobless grace. It counts the `jobless_released` result once; the
  `event=claim_released` line for the same removal follows from the release
  itself.

## Claim state machine

A claim moves through these states:

```
claimed → held → renewed / recovered / lost / fenced / released
```

- `claimed`: a create with empty `expected_sha` succeeded and the claim is held.
- `held`: the manager owns the claim at a known sha and `lease_until`.
- `renewed`: a renew compare-and-swap succeeded and a new sha was adopted.
- `recovered`: a write that failed on the wire was read back and found to be
  the manager's own `(cluster, task_id, lease_until)` triple.
- `lost`: a renew returned `409`, or the read-back showed another writer, or the
  ref was absent. The task's Job is deleted and the claim is dropped without
  contacting git-rest.
- `fenced`: the holder could not keep the lease alive and stopped its own Job at
  or before `fenceAt`.
- `released`: the Job ended and the claim was deleted with the held sha.
- Jobless release: a held claim whose Job has been continuously not running for
  `JoblessGrace` is deleted with the held sha, and the task becomes eligible for
  respawn. The success hold, a pending release, a pending Job deletion and an
  unanswered adopted running check are excluded and keep their own handling.
- Pending Job deletion: deleting the Job failed on the lost or fence path. The
  claim stays held, no renew and no ref `DELETE` are sent, and the Job deletion
  is retried at every tick.
- Pending release: a release `DELETE` failed with a non-`409` error. The claim
  stays held and later ticks retry the delete instead of renewing, until the
  fence point.

A claim's refs are held and released as a unit: a claim is not held while only
one of its refs is held, and a release that removes one ref but not the other
stays pending-release.

`executor_claim_total{result}` counts every outcome above; all 14 result series
are pre-initialised to 0. Keys and task ids are never label values.

## Admission capacity

A cluster admits a task only when the assignee has a free slot. The number of
slots is the **smaller of** the Config's declared `maxConcurrentJobs` and the pod
quota that scopes the agent's `priorityClassName`.

The quota is the `hard.pods` of the `ResourceQuota` whose `scopeSelector`
matches `PriorityClass In [<priorityClassName>]`, preferring `status.hard` and
falling back to `spec.hard`. It is read at most once per namespace and priority
class per 30 seconds.

A zero `maxConcurrentJobs` means "no declared cap" and does not disable the
quota bound. A failed quota read, a Config with no priority class, or no quota
scoping that class leaves the declared cap as the only bound; a failed read also
logs one `event=pod_quota_read_failed` warning.

A matching quota whose `hard.pods` is `0` is a **hard zero**, not an absent
quota: the class may run nothing, so every spawn for it is deferred. The two
states are distinct and must stay so — treating a zero limit as "no quota" would
fall back to the declared cap and claim work the quota can never start, which is
the defect this bound exists to remove.

Because admission is decided before the claim is taken, a task over the bound is
**never claimed**: it is deferred and a peer cluster can take it immediately.
That is why the fence and takeover rules only ever have to bound a holder that
is actually running.

The decision is visible in one line: `event=concurrency_admit` and
`event=concurrency_cap` both carry `running=`, `cap=`, `max=` and `quota=`,
where `cap` is the effective bound, `max` the declared value and `quota` the
value read from the quota. `quota_bound=` names why the quota did or did not
bound the decision — `ok` when the read succeeded (`quota` is then authoritative,
including a genuine `hard.pods: 0`), `failed` when the read was refused or
errored (`cap` fell back to `max`), or `none` when no limiter, no priority class,
or no quota scoping the class applied. It exists because `quota=0` alone cannot
distinguish those three, and the failed read is exactly the state an operator
must be able to see without inferring it from `cap`.

## Fence

A held claim is fenced when `now > fenceAt − 10 s`. The holder:

1. deletes the task's Job (skipped when the claim is pending release because its
   Job already ended);
2. sends one best-effort compare-and-swap `DELETE` with the held sha, ignoring
   `409` and any error;
3. drops the claim.

A fenced claim is never renewed. If the Job deletion fails, the claim enters the
pending-deletion state and the deletion is retried every tick until it succeeds;
then the best-effort ref `DELETE` runs and the claim is dropped.

A fenced dual claim deletes the pair — both the per-repository and the
per-pull-request refs — best-effort, after deleting the Job once. A failed Job
deletion keeps both refs in pending deletion; the retry deletes them together
once the Job is gone.

## Takeover

A claim is takeable only at or after `lease_until + 30 s`. Before that, or when
`lease_until` cannot be parsed as RFC3339, the manager refuses with
`ErrLeaseNotExpired` and sends no write. A takeover is one compare-and-swap
`PUT` carrying the expired claim's sha as `expected_sha`. Exactly one of two
racing takers wins; the loser gets `ErrLeaseConflict` and sends no further
request. A takeover of an absent key falls back to a create with an empty
`expected_sha`.

A takeover must win the pair: a live secondary refuses the takeover before any
write, and a secondary conflict after the primary was taken releases the primary
again so the pair is never half-held.

## Restart recovery

On start the manager lists `claims/` and keeps only entries whose `cluster`
equals its own name. The listed refs are grouped by task id, so a repository key
and a per-pull-request key belonging to the same task are recovered together.
For each group:

- A claim inside the fence window (`now > lease_until − 70 s`) is fenced: the
  task's Job is deleted if it is running, then one best-effort `DELETE` with the
  listed sha is sent and the claim is not adopted.
- A running Job's pair is re-adopted as one claim with both refs at their listed
  shas and the earlier of their two `lease_until` values, with no create; ticks
  renew it from there.
- A per-pull-request-only claim is adopted as the single ref it is. No partner
  key is derived or written, so a restart cannot re-create a pair the claim path
  no longer writes. A pair still present in the store from an earlier release is
  recovered as a pair, above.
- A per-repository-only claim is adopted as a single claim, because a repository
  key carries neither the task type nor the pull-request number, so its per-PR
  partner is not derivable.
- A finished Job's claim is released with the listed sha.
- A failed running check keeps the claim held and renewed, and the check is
  re-asked at every tick until it answers.
- A claim of another cluster is never touched.
- Keys under `claims/redrive/` are skipped before the cluster comparison, so a
  streak ref is never adopted, renewed, fenced or released regardless of its
  `cluster` value.

## Streak refs

A streak ref records how many consecutive times a task has been spawned at one
phase. It lives in the same git-rest ref store as claims but is not a claim, and
no lease-manager path treats it as one.

- Key shape: `claims/redrive/<taskid>/<phase>/<n>`, where `n` is an integer
  greater than or equal to 1. A task with no phase uses the phase component
  `none`.
- `cluster` is always the sentinel `redrive-streak`. It must never equal a real
  executor cluster name.
- `lease_until` is create time + `RedriveStreakLeaseDuration` (6 h) and is a real
  sliding window. Nothing renews a ref, so expiry is the only release path the
  executor controls: a ref whose `lease_until` has passed is ignored by `Count`
  and by `Snapshot` and pruned by `Record`, so a task capped by a past outage is
  released once the window has passed. Three live refs still refuse a fourth
  spawn inside one window, and a ref with an absent or unparseable `lease_until`
  counts as live, so the cap never weakens.
- No lease-manager path acts on a streak ref. Restart recovery is the only path
  that lists the whole `claims/` prefix, and it skips `claims/redrive/` keys
  explicitly; renew, fence, takeover, release and audit act only on the
  `claims/task/` and `claims/repo/` keys the executor derives itself, so they
  never reach a streak ref. A streak ref therefore never triggers a fence and
  never stops a task's Job.
- The count is the number of consecutive spawns at one phase. Recording a spawn
  at a new phase resets the streak by deleting the old phase's refs.
- To remove a task's stored streak, an operator deletes its streak refs
  through git-rest: list them with
  `GET /api/v1/refs?prefix=claims/redrive/<taskid>` and delete each entry with
  `DELETE /api/v1/refs/claims/redrive/<taskid>/<phase>/<n>?expected_sha=<sha>`.
- Nothing reaps streak refs on its own. A task whose refs are never cleared —
  its file deleted, or a clear whose deletes failed (logged as
  `event=redrive_streak_write_failed`) — leaves them behind. They are inert only
  while no executor drives that task id: refs are keyed by task id alone, so a
  task reassigned to another vault, or re-opened after a failed clear, inherits
  the same stored count. Remove them with the list-and-delete
  above; `GET /api/v1/refs?prefix=claims/redrive` lists every remaining streak
  ref.
- The executor clears a task's streak refs when it handles the task's terminal
  state, and deletes the old phase's refs when it records a spawn at a new
  phase.
- The executor's cap is the larger of the stored count and its own in-memory
  count at the task's current phase. Deleting the refs therefore un-caps a task
  everywhere except on an executor that has itself counted 3 spawns at that
  phase, which stays capped until it restarts or the task moves phase.
- A task capped by the shared streak but not by this executor's memory is
  reported as `capped` in the reconcile pass summary, like one this executor
  counted itself: the spawn funnel carries its refusal back to the pass.
  `executor_redrive_capped_total{task_type, phase}` counts every cap regardless,
  broken down by the task's declared task type and the phase it was capped at.
  The task type says which kind of work is being stranded; the phase says
  whether the record is stuck at one phase or advanced and re-capped. Example:
  `sum by (task_type, phase) (increase(executor_redrive_capped_total[1h]))`.
  The `event=redrive_capped` line carries `class=` (`broken`, `transient` or
  `unknown`) and `reasons=` (the recorded reasons, comma-joined, or `-`) after
  its existing fields.

### Reason refs

A reason ref records why one spawn attempt failed, next to the streak, so the
cap can tell a repeated broken signature from a transient failure instead of
treating every three-times-failed record the same way.

- Key shape: `claims/redrive-reason/<taskid>/<phase>/<reason>/<n>` — **six**
  segments under `refs/`. `<n>` is one more than the highest existing number at
  that (task, phase) **across every reason**, so the numbers are chronological
  and the newest attempt sorts last even when the reason changed. A per-reason
  counter would restart a new reason at 1 and let a stale reason dominate the
  classifier's window. An empty reason is stored as `unknown`.
- `<reason>` is a bare `ZombieReason` value or the literal `unknown`. The
  formatted `backoffFailureReason` form (`pod_oom_killed (OOMKilled/137)`) is
  normalised to its vocabulary value before it is written; it is never stored,
  because its spaces and parentheses are not valid key segments.
- The refs live in a separate prefix rather than as a sixth segment on the
  streak ref because the streak prefix and its five-segment parser are frozen:
  refs written by the deployed build must keep parsing.
- An older binary is blind to them because they carry the `redrive-streak`
  cluster sentinel, which its lease-manager recovery skips — not because of the
  prefix alone.
- The reason refs always describe the current streak. They are cleared on the
  task's terminal path and again when a streak's first spawn is recorded, so
  deleting only the streak refs still un-caps a task once it restarts or moves
  phase (the carve-out above), and no reason left by an earlier streak can reach
  the classifier's window.

### The cap's decision

At `maxConsecutiveRedrives` the executor reads the reasons for that (task, phase)
and classifies the record. The rules are total and are evaluated in the order
unknown, broken, transient — the first match wins.

The window the classifier reads is the most recent `min(count, len(reasons))`
reasons, where `count` is the streak. The streak counts every successful spawn
while a reason is recorded only from `PublishFailure`, so a spawn that publishes
no failure — a reconcile re-drive whose job vanished — advances the streak
without adding a reason. Sizing the window by the streak alone would refuse such
a record forever: the spawn is refused, so it can never earn the missing reason,
and nothing is ever closed.

- **broken** — the window holds at least `maxConsecutiveRedrives` reasons and they
  are all one value from `pod_crash_no_stdout`, `pod_oom_killed`,
  `type_mismatch`. The record is closed and the spawn refused.
- **transient** — everything else: the reasons differ between attempts, one is a
  value outside the broken set, or fewer reasons were recorded than the cap. The
  spawn is allowed up to `maxConsecutiveRedrives + 3` (a bounded extension);
  reaching that bound closes the record and refuses the spawn.
- **unknown** — no reasons were recorded, or every reason in the window is the
  literal `unknown`. The spawn is refused and nothing is closed: closing a record
  because its history could not be read is not the safe direction.

Fewer reasons than the cap is deliberately not a close: partial evidence must
never classify a record as broken. It is transient instead, which retries under
the bounded extension and ends the record either way — so a record that reached
the cap *with* recorded evidence is never abandoned there.

The **unknown** branch above is the one case that still is. A record with no
reasons at all, or a window that is entirely the literal `unknown`, is refused
without being closed, and a refused spawn can never earn the missing reason — so
it stays at the cap indefinitely. That is deliberate rather than an oversight:
it is the population capped before this classification existed, and closing on
unread history is not the safe direction. Draining it is separate work.

`type_mismatch` stays in the broken set because the taxonomy puts it there, but
it cannot in practice reach the store: a type-mismatched task is filtered before
the spawn and its failure is published by `PublishTypeMismatchFailure`, not
`PublishFailure`, so its streak never grows.

Closing a record writes `status: aborted`, `aborted_reason`,
`gate_successor: "none"` and `current_job: ""` — the close-out pair vault-cli
requires before it will accept `aborted`. The two reasons are
`capped:broken-signature` and `capped:transient-retries-exhausted`. The write is
deduped per task and skipped when the task is already `aborted`.

`executor_redrive_aborted_total{reason}` counts records closed this way. Unlike
`executor_redrive_capped_total` it **is** pre-initialised at 0 for both reasons:
its label domain is closed at compile time, which is the bounded side of the
counter-pre-initialization rule. Do not copy the omission from its neighbour.

## Lost-response recovery

A create, renew or takeover that fails without a `409` is read back. The writer
recognises its own write by the exact `(cluster, task_id, lease_until)` triple
on the listing entry whose `ref` equals `refs/<key>` — never by the sha, and
never by a prefix match (a sibling `refs/<key>/x` is not the key). A matching
triple adopts the listed sha (`recovered`). A renew read-back still at the held
sha keeps the claim held (`renew_failed`); a read-back that shows another write
or an absent ref loses the claim. A failed read-back keeps the claim held.
