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
| Request timeout cap | 30 s | Hard cap on every git-rest request; a tick also caps each request at the earliest `fenceAt` among held claims. |

`fenceAt = lease_until − 60 s`. The fence fires when `now > fenceAt − 10 s`.

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
- Pending Job deletion: deleting the Job failed on the lost or fence path. The
  claim stays held, no renew and no ref `DELETE` are sent, and the Job deletion
  is retried at every tick.
- Pending release: a release `DELETE` failed with a non-`409` error. The claim
  stays held and later ticks retry the delete instead of renewing, until the
  fence point.

`executor_claim_total{result}` counts every outcome above; all 13 result series
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

## Lost-response recovery

A create, renew or takeover that fails without a `409` is read back. The writer
recognises its own write by the exact `(cluster, task_id, lease_until)` triple
on the listing entry whose `ref` equals `refs/<key>` — never by the sha, and
never by a prefix match (a sibling `refs/<key>/x` is not the key). A matching
triple adopts the listed sha (`recovered`). A renew read-back still at the held
sha keeps the claim held (`renew_failed`); a read-back that shows another write
or an absent ref loses the claim. A failed read-back keeps the claim held.
