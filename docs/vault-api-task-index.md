# Vault-api task index

The executor's divergence check reads every task's `status` and `phase` from
vault-api in one call. This document pins the endpoint, the auth header pair,
the response envelope and the bounds the client in
`pkg/claim/vault_api_index_client.go` applies. Treat it as the contract: a
change to the client that is not reflected here is a change without a source of
truth.

## Endpoint

`GET /api/v1/tasks` — no query parameters.

The request is deliberately **unfiltered**. See *Do not filter* below.

## Auth

The request carries the same gateway header pair as every other vault-api
request:

| Header | Value |
|---|---|
| `X-Gateway-Secret` | the gateway secret |
| `X-Gateway-Initator` | the configured initiator |

`X-Gateway-Initator` is **misspelled exactly as shown**. The server compares
that exact name; "correcting" it to `X-Gateway-Initiator` makes every request a
`401` and leaves the check silently dead. Both vault-api clients share
`setVaultAPIAuthHeaders` so the pair cannot drift between them.

The secret is referenced by name from `VAULT_API_GATEWAY_SECRET`, read once at
startup through `pkg.ReadVaultAPIGatewaySecret`, and held in memory. It is never
logged, never written to the image, and never embedded in an error string.

## Response envelope

```json
{
  "items": [
    {"id": "b75217de-ffd7-5167-a7fb-09bdaacf8484", "status": "in_progress", "phase": "execution"},
    {"id": "c8432e1d-9c3a-575c-9757-3f25eca5df88", "status": "completed", "phase": "done"}
  ],
  "total": 2,
  "warnings": 0,
  "index_age_seconds": 17
}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `items[].id` | string | yes, per item | The task identifier the view joins on. |
| `items[].status` | string | no | The task's `status` frontmatter value. |
| `items[].phase` | string | no | The task's `phase` frontmatter value. |
| `total` | int | no (default 0) | The server's item count; a coverage signal. |
| `warnings` | int | no (default 0) | Global count of files vault-api could not index. |
| `index_age_seconds` | int | yes | Age of the server's index, in seconds. Server-computed. |

## Decode strictness

The client decodes into a wire struct so key presence is observable:

- `items` is **required**. An absent key is a `read_error`
  (`decode vault-api task index response: missing items`), never an empty index.
  A renamed or nested `items` is therefore visible instead of reading as
  `compared=0`.
- `items: null` is a **legitimate empty index**. A Go server marshalling a nil
  slice writes `null`; it yields zero items and no error.
- `index_age_seconds` is **required**. A dropped age is schema drift, and the
  summary cannot report the age without it.
- `total` and `warnings` are **optional**, defaulting to `0`. A server that
  omits a zero value must not turn every healthy pass into a `read_error`.
- A body that is not JSON at all — a proxy's HTML error page, a truncated
  stream — is a `read_error`, never a silent empty index.

## Body bound

The body is read through `io.LimitReader(resp.Body, 32 MiB + 1)`. A response
larger than **32 MiB** is a `read_error`; it is not truncated and parsed. The
bound is larger than the claim client's 10 MiB cap because the index carries
every task in the vault (9,002 files on the personal vault).

A non-`200` status is a `read_error` naming the status code only. The response
body is not echoed: a server body can carry a credential or a holder.

## Do not filter

Do **not** add a `cluster=` filter (or any other) to this request. Files that
carry no `cluster` frontmatter are not in the cluster set, so a filtered read
omits them entirely — and those are exactly the tasks most likely to be stale.
A filtered read would manufacture `absent` divergences for them.

## Index age

`index_age_seconds` is computed by vault-api and passed through unchanged. The
executor never compares two clocks and never derives the age locally. A
client-side timestamp would be a second clock, and the age's whole purpose is to
report the freshness of the server's own index.

## Warnings

`warnings` is a **global** count of files vault-api could not index. It is not
attributable to a specific id from this response, so the executor never reports
it per id; it is carried in the pass summary as a coverage signal only.

An operator distinguishes the two per-id causes with the vault-api proxy probe:

- `422 invalid_frontmatter` — the file exists but was excluded from the index.
- `404 not_found` — no file exists for that id.

## Divergence check

Once per reconcile pass that reaches the drive loop, the executor reads this
index once and joins it to the tasks the pass owns.

- **Join key:** each owned view task's `task_identifier` to `items[].id`.
- **Compared fields:** `status` and `phase`. A difference in **either** is a
  divergence; a matching status with a different phase is still reported.
- **Per-id events**, at warning level, for each finding:
  - `event=view_divergence task=<id> view_status=<s> vault_status=<s> view_phase=<p> vault_phase=<p>`
  - `event=view_divergence_absent task=<id> view_status=<s> view_phase=<p>` —
    the id is owned but the index does not contain it. It is a separate event
    name so a grep for `event=view_divergence task=` never matches an absent id.
- **Summary event**, once per pass, at info level:
  `event=view_divergence_summary compared=<n> divergent=<n> index_age_seconds=<n> warnings=<n>`.
  `warnings` is the global index warning count, not a per-id attribution.
- **`compared` counts ids actually joined**, not owned ids. A failed or empty
  index therefore reads as `compared=0`, never as agreement.
- **The same read now gates spawn eligibility on every path.** The reconcile
  candidate gather refuses an id whose index entry is terminal, the deferred
  evaluation refuses a due entry whose index entry is terminal, and the spawn
  funnel itself — `spawnIfNeededClaimed`, which the Kafka task-event path
  reaches directly — refuses a task whose index entry is terminal immediately
  before it takes the lease claim. That last position is deliberate: it is the
  only gate that costs a network read, so every cheaper refusal (parked, active
  Job, capped assignee, invalid namespace) returns above it and pays nothing.
  A task that genuinely re-opens is non-terminal in the index and still spawns.
  The comparison still only reports; it never writes to the vault.
- **One read per reconcile pass, shared.** The reconcile pass performs exactly
  one read and shares it between the candidate gate and the divergence check.
  No read happens before the vault-classification early returns
  (`view_not_ready`, `task_vault_unstamped`, `task_vault_mismatch`). The
  deferred evaluation runs on its own ticker, performs its own single read when
  at least one entry is due and none when nothing is due; its spawn calls reuse
  that pass cache, so three due entries still cost one read. A Kafka task event
  is not a pass and carries no cache: it pays one read, and only when it
  reaches the spawn point.
- **A failed read fails open.** Nothing is suppressed, the reconcile pass
  reports the failure once as `event=view_divergence_read_error`, and the
  deferred path is silent because it runs no divergence check.
- **Terminal on the index side** means `status` `completed` or `aborted`, or a
  terminal `phase` (`human_review`, `done`). A non-terminal entry that merely
  differs from the view suppresses nothing.
- **A deployment without a vault-api URL** builds no client, makes no call and
  emits nothing.
- **Canonical values on both sides.** The view normalizes legacy aliases
  (`todo`→`next`, `done`→`completed`, `deferred`→`hold`, and the phase
  equivalents). Vault-api reads the same `vault-cli` domain values, so the index
  side is assumed canonical too. This is an assumption, not a contract: if the
  serving vault-api ever returns a raw alias, the check reports a permanent
  false divergence for that id.

## Metric contract

`executor_view_divergence_total` counts the check's outcomes, labelled `result`
with a closed set: `ok`, `mismatch`, `absent`, `read_error`.

| Result | Increment unit |
|---|---|
| `ok` | once per pass that reaches the check and reads the index successfully |
| `read_error` | once per pass whose read fails |
| `mismatch` | once per divergent task id |
| `absent` | once per owned id the index does not contain |

- **`ok` means the read succeeded, not "no divergence".** A pass that finds
  three divergences still increments `ok` once. An alert written against `ok`
  as agreement is wrong under this unit.
- **All four series are pre-initialised at 0**, so `rate(...) == 0` can fire on
  a check that never ran. The label domain is closed at compile time, which is
  what makes that seeding honest.
- **No task id, claim key or timestamp is ever a label value.** Cardinality is
  fixed at four.
- **A deployment with no vault-api URL increments nothing** — no client, no
  check, no series movement.
- **Why a new name.** `executor_reconcile_passes_total{result="task_vault_mismatch"}`
  already exists, but it describes vault-ownership classification over the
  in-memory snapshot, not a per-id file comparison. Reusing it as a label value
  would let a dashboard read one failure as the other, so the two stay separate
  counters.

### Refusal gate: `index_terminal`

The read also drives a refusal gate, counted separately from the divergence
outcomes above.

- **Counter:** `executor_reconcile_refused_total{gate="index_terminal"}`.
- **Unit:** one per suppressed id, **uncapped**, on the reconcile candidate
  gather, the deferred-respawn evaluation and the spawn funnel the Kafka
  task-event path reaches. The counter is the exact signal and is never
  trimmed.
- **Log contract:** one `event=index_terminal_suppressed task=<id>` line per
  suppressed id per pass at V(2), sorted ascending by id and capped at 20 names
  per pass. The cap bounds the log line and never the counter.
- **Fail-open rule:** a missing client, a failed read or a spent pass budget
  suppresses nothing and increments nothing.
- **Label hygiene:** `gate` is the only label, its value comes from the fixed
  gate vocabulary, and no task id, cluster, vault or assignee is ever a label
  value.

## Source

The envelope above is pinned from the served response of `GET /api/v1/tasks`
against the vault-api deployment named in the `VAULT_API_URL` env var, read on
2026-10-26. Re-read that endpoint before changing a field name here — this
document is the contract the executor decodes against, and it goes stale the
moment vault-api's `ListResponse` moves.

**The serving vault-api version was not recorded in this repository.** vault-api
is not a Go dependency here and no manifest pins its image, so there is no
version against which this document can be said to be current. This is an
explicit open question, not an omission: before changing the client, re-read the
live endpoint, compare it against this envelope, and update this section (with
the version you observed) in the same change.
