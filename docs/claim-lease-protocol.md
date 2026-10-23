# Claim lease protocol

A claim is a ref on the shared vault origin written through git-rest
(`refs/claims/...`). Exactly one executor may run a task at a time: the holder
of the task's claim. This document describes the protocol implemented by
`pkg/claim`. The constants below are invariants, not configuration — there is
no knob, flag or opt-out for any of them.

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
| Request timeout cap | 30 s | Hard cap on every git-rest request; a tick also caps each request at the earliest `fenceAt` among held claims. |

`fenceAt = lease_until − 60 s`. The fence fires when `now > fenceAt − 10 s`.

## Claim keys

Every claim lives at one of three key shapes. Owner and repo are lowercased in
every key, and keys and task ids are never metric label values.

| Key shape | Used by |
|---|---|
| `claims/task/<task id>` | task-id-keyed types |
| `claims/repo/<owner>-<repo>` | `update-go`, `release`, `build-fix`, and the `pr-review` fallback |
| `claims/pr/<owner>-<repo>-<n>` | `pr-review` with a usable PR link |

A `pr-review` key is per pull request when the task body carries a standalone
pull-request URL: the first body line that, after trimming surrounding
whitespace, is exactly `https://github.com/<o>/<r>/pull/<n>`, where `<o>/<r>`
matches the task repository case-insensitively, `<n>` is 1–10 digits with a
value of at least 1, and nothing follows `<n>`.

When no line qualifies, the key falls back to `claims/repo/<owner>-<repo>`. The
fallback logs one `event=claim_key_fallback task=<id> repo=<owner>/<repo>`
warning and increments the unlabelled `executor_claim_key_fallback_total`
counter once.

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

`executor_claim_total{result}` counts every outcome above; all 14 result series
are pre-initialised to 0. Keys and task ids are never label values.

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

## Takeover

A claim is takeable only at or after `lease_until + 30 s`. Before that, or when
`lease_until` cannot be parsed as RFC3339, the manager refuses with
`ErrLeaseNotExpired` and sends no write. A takeover is one compare-and-swap
`PUT` carrying the expired claim's sha as `expected_sha`. Exactly one of two
racing takers wins; the loser gets `ErrLeaseConflict` and sends no further
request. A takeover of an absent key falls back to a create with an empty
`expected_sha`.

## Restart recovery

On start the manager lists `claims/` and keeps only entries whose `cluster`
equals its own name. For each entry:

- A claim inside the fence window (`now > lease_until − 70 s`) is fenced: the
  task's Job is deleted if it is running, then one best-effort `DELETE` with the
  listed sha is sent and the claim is not adopted.
- A running Job's claim is re-adopted at its listed sha and `lease_until` with no
  create; ticks renew it from there.
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
- `lease_until` is create time + 24 h. It is informational only: nothing renews
  it and nothing expires a streak ref.
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
  reported as `deferred`, not `capped`, in the reconcile pass summary;
  `executor_redrive_capped_total` counts every cap regardless.

## Lost-response recovery

A create, renew or takeover that fails without a `409` is read back. The writer
recognises its own write by the exact `(cluster, task_id, lease_until)` triple
on the listing entry whose `ref` equals `refs/<key>` — never by the sha, and
never by a prefix match (a sibling `refs/<key>/x` is not the key). A matching
triple adopts the listed sha (`recovered`). A renew read-back still at the held
sha keeps the claim held (`renew_failed`); a read-back that shows another write
or an absent ref loses the claim. A failed read-back keeps the claim held.
