---
status: completed
spec: [009-claim-lease-client-library]
summary: Added the pkg/claim git-rest claim-ref client (List/Put/Delete with 409 sentinel), generated FakeClaimRefClient, and a reusable in-process fake git-rest with self-tests.
execution_id: agent-task-executor-claim-client-exec-017-spec-009-ref-client
dark-factory-version: v0.198.7
created: "2026-10-21T18:52:26Z"
queued: "2026-10-21T19:14:22Z"
started: "2026-10-21T19:14:23Z"
completed: "2026-10-21T19:23:01Z"
branch: dark-factory/claim-lease-client-library
---

<summary>
- The executor gains a new self-contained `pkg/claim` package containing an HTTP client for git-rest's leased claim-ref endpoints (`GET /api/v1/refs`, `PUT /api/v1/refs/<key>`, `DELETE /api/v1/refs/<key>`), so a later lease manager can create, renew and delete claims on the shared vault origin.
- The client sends the same gateway auth headers as the existing file client, caps every request at 30 seconds, issues exactly one request per call (never retries), and never logs or returns the gateway secret.
- A `409` from git-rest becomes a sentinel `ErrLeaseConflict` that callers can match with `errors.Is`; every other failure returns an error that names the HTTP status code.
- A generated counterfeiter fake (`FakeClaimRefClient`) lets later prompts test lease logic without a live server.
- A reusable in-process fake git-rest (test-only, `httptest`) implements compare-and-swap semantics, request recording, a PUT barrier for race tests, per-request fault injection and sibling-ref fixtures; its own self-test proves the fake actually rejects stale writes.
- Nothing outside the new package and its generated mock imports it; the executor's runtime behaviour is unchanged.
</summary>

<objective>
Establish the wire contract for git-rest's leased claim-ref API (git-rest spec 018) inside a new `pkg/claim` package: a three-method compare-and-swap HTTP client with sentinel conflict errors, a generated fake, and a reusable in-process fake server with a self-test. This is prompt 1 of 3; the lease manager (prompt 2) and key derivation / docs / changelog (prompt 3) build on it.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for project conventions (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, counterfeiter mocks, glog `V(n)` gating, coverage ≥80% for new code).

Read these files fully before changing anything:
- `/workspace/pkg/gitrestclient/git_rest_client.go` — the existing git-rest file client. Copy its `setAuthHeaders` behaviour and its `fileURL` segment-escaping idiom exactly. The claim client is a sibling client, not a modification of this file.
- `/workspace/pkg/gitrestclient/git_rest_client_test.go` — the httptest assertion style (assert `r.Method`, `r.URL.Path`, `r.URL.Query()` per request).
- `/workspace/pkg/gitrestclient/gitrestclient_suite_test.go` — the suite-file pattern for a new package.
- `/workspace/pkg/metrics/metrics_suite_test.go` — the `time.Local = time.UTC` + `RunSpecs` suite pattern.
- `/workspace/pkg/handler/task_event_handler_test.go` (first ~10 lines) — the exact `//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6@v6.12.2 -generate` directive to copy.
- `/workspace/tools.env` — counterfeiter is pinned to `v6.12.2`; do not use another version.

Server contract (git-rest spec 018 — do not invent extra fields or endpoints):
- `GET /api/v1/refs?prefix=<claims/...>` → JSON array of `{"ref":"refs/claims/...","sha":"...","cluster":"...","task_id":"...","lease_until":"<RFC3339>"}`. An empty result is `[]`, never `null`.
- `PUT /api/v1/refs/<key>` with body `{"expected_sha":"...","cluster":"...","task_id":"...","lease_until":"<RFC3339 UTC>"}` → `200 {"ref":"refs/<key>","sha":"<new sha>"}`. Empty `expected_sha` means "must not exist". A stale or empty-on-existing expectation returns `409`.
- `DELETE /api/v1/refs/<key>?expected_sha=<sha>` → `200 {"ok":true}`; absent ref or stale sha returns `409`.
- `409` means lease mismatch and is final — never retried.
- A claim key includes its `claims/` prefix (for example `claims/task/abc123`), so the request path is `/api/v1/refs/claims/task/abc123` and a listed `ref` is `refs/claims/task/abc123`.
- Auth headers match the existing file client: `X-Gateway-Secret` and `X-Gateway-Initator` (deliberate misspelling), both omitted when the secret is empty.

Verified external APIs (do not paraphrase these; they were read from the module source):
- `libtime "github.com/bborbe/time"` → `type DateTime stdtime.Time`; `func (d DateTime) Time() stdtime.Time`; `func (d DateTime) Add(duration HasDuration) DateTime`; `type Duration stdtime.Duration`; `const Second Duration = 1000 * Millisecond`; `func (d Duration) Duration() stdtime.Duration`.
- `libtimetest "github.com/bborbe/time/test"` → `func ParseDateTime(value interface{}) libtime.DateTime` (tests only).
- `"github.com/bborbe/errors"` → `New(ctx, message)`, `Errorf(ctx, format, args...)`, `Wrapf(ctx, err, format, args...)`, `Is(err, target)`.
- `stderrors "errors"` for package-level sentinels (project rule).

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega, external test packages, coverage ≥80%.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `bborbe/errors` wrapping and the `stderrors` sentinel convention.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-security-linting.md` — request timeouts, gosec.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-precommit.md` — golines 100 columns, funlen 80 lines / 50 statements.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.
</context>

<requirements>
1. Create `/workspace/pkg/claim/claim_suite_test.go` in `package claim_test`, mirroring `/workspace/pkg/metrics/metrics_suite_test.go` and `/workspace/pkg/spawner/spawner_suite_test.go`. It must contain the counterfeiter go:generate directive verbatim:

```go
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6@v6.12.2 -generate
```

and `func TestClaim(t *testing.T)` that sets `time.Local = time.UTC`, calls `RegisterFailHandler(Fail)` and `RunSpecs(t, "Claim Suite")`.

2. Create `/workspace/pkg/claim/ref_client.go` (license header: `// Copyright (c) 2026 Benjamin Borbe All rights reserved.` plus the BSD boilerplate used by every file in this repo). It must contain exactly this public surface (add unexported helpers freely):

```go
package claim

import (
	"context"
	stderrors "errors"

	libtime "github.com/bborbe/time"
)

// RequestTimeout is the hard per-request cap for every git-rest claim call.
const RequestTimeout = 30 * libtime.Second

// ErrLeaseConflict is returned when git-rest answers 409 on a PUT or DELETE.
// It is final: the claim client never retries.
var ErrLeaseConflict = stderrors.New("lease conflict")

// ClaimRef is one entry of GET /api/v1/refs. Ref is the full ref name
// ("refs/claims/..."), not the key. LeaseUntil is the raw RFC3339 string the
// server returned; callers parse it and never trust it without validation.
type ClaimRef struct {
	Ref        string `json:"ref"`
	Sha        string `json:"sha"`
	Cluster    string `json:"cluster"`
	TaskID     string `json:"task_id"`
	LeaseUntil string `json:"lease_until"`
}

//counterfeiter:generate -o ../../mocks/claim_ref_client.go --fake-name FakeClaimRefClient . ClaimRefClient

// ClaimRefClient is the git-rest leased claim-ref API (git-rest spec 018).
type ClaimRefClient interface {
	// List returns every claim ref whose path is prefix or lies under it as a
	// whole path component. prefix is passed as the ?prefix= query value.
	List(ctx context.Context, prefix string) ([]ClaimRef, error)
	// Put creates or replaces the claim at key by compare-and-swap. An empty
	// expectedSha means "must not exist". It returns the new sha from the
	// server response. A 409 is ErrLeaseConflict.
	Put(ctx context.Context, key string, expectedSha string, cluster string, taskID string, leaseUntil libtime.DateTime) (string, error)
	// Delete removes the claim at key by compare-and-swap. A 409 is
	// ErrLeaseConflict.
	Delete(ctx context.Context, key string, expectedSha string) error
}

// NewClaimRefClient creates a ClaimRefClient for the git-rest instance at
// baseURL. gatewaySecret empty means no auth headers are sent. The secret is
// held in memory only and must never appear in a log line or an error string.
func NewClaimRefClient(baseURL, gatewaySecret, gatewayInitiator string) ClaimRefClient
```

3. Implement the client in the same file:
   a. `NewClaimRefClient` returns `&claimRefClient{...}` — the unexported struct is named `claimRefClient` and every method uses receiver `c` (e.g. `func (c *claimRefClient) List(...)`); the verification grep depends on these names. It stores `strings.TrimRight(baseURL, "/")`, an `*http.Client{Timeout: 30 * time.Second}`, the secret and the initiator.
   b. Every method derives `ctx, cancel := context.WithTimeout(ctx, RequestTimeout.Duration())` and `defer cancel()`. This is a cap only: a shorter caller deadline wins because `context.WithTimeout` keeps the earlier deadline.
   c. `setAuthHeaders(req)` is byte-for-byte the behaviour of `/workspace/pkg/gitrestclient/git_rest_client.go`: set both headers when the secret is non-empty, set neither when it is empty.
   d. `escapedKey(key string) string` splits `key` on `/`, applies `url.PathEscape` to each segment, and rejoins with `/`. Use it for PUT and DELETE paths. Never `url.PathEscape` the whole key (that would escape the separators).
   e. `List`: `GET <base>/api/v1/refs` with `prefix` set through `url.Values`. Non-200 → wrapped error whose message contains the status code. 200 with a body that does not unmarshal into `[]ClaimRef` → wrapped error whose message contains the status code `200`. Return the decoded slice as-is (an empty `[]` decodes to a non-nil empty slice only if the server sent `[]`; do not synthesise entries).
   f. `Put`: JSON body struct with exactly four keys, no `omitempty`: `expected_sha`, `cluster`, `task_id`, `lease_until`. `lease_until` is `leaseUntil.Time().UTC().Format(time.RFC3339)` — RFC3339, UTC, no fractional seconds. 200 → decode `{"ref":"...","sha":"..."}` and return `sha`; an unparseable or empty-`sha` 200 body is an error containing the status code `200`. 409 → return an error for which `errors.Is(err, ErrLeaseConflict)` is true. Any other status → wrapped error containing the status code.
   g. `Delete`: `DELETE <base>/api/v1/refs/<escapedKey>?expected_sha=<url-encoded sha>` via `url.Values`. 200 → nil. 409 → `ErrLeaseConflict` (matchable with `errors.Is`). Any other status → wrapped error containing the status code.
   h. No retry loop, no backoff, no metrics, no logging of headers or the secret. Error messages include the method, key and status code only — never the response body (a server could echo the secret back) and never a request header. `glog.V(3)` request-line logging is allowed (method, key, status) but must not include the secret.
   i. Add `func formatLeaseUntil(leaseUntil libtime.DateTime) string` returning `leaseUntil.Time().UTC().Format(time.RFC3339)` and use it in `Put`. The lease manager in prompt 2 will reuse this exact helper for read-back triple matching, so there must be exactly one formatting implementation.

4. Regenerate the mock with `go generate -mod=mod ./pkg/claim/...` so `/workspace/mocks/claim_ref_client.go` contains `FakeClaimRefClient`. Do not hand-write it. The generated file must compile against `claim.ClaimRefClient`.

5. Create `/workspace/pkg/claim/ref_client_test.go` in `package claim_test` covering AC7. Use `net/http/httptest` servers and assert the exact request on every row:
   a. `List`: handler asserts method GET, path `/api/v1/refs`, query `prefix=claims/`; returns `[]`; client returns a zero-length slice and nil error.
   b. `List` returns two entries: client returns both with every field decoded.
   c. `Put`: handler asserts method PUT, path `/api/v1/refs/claims/task/abc123`, and that the decoded JSON object has exactly the key set `{expected_sha, cluster, task_id, lease_until}` (decode into `map[string]json.RawMessage` and assert `len == 4` plus each key present). Assert `lease_until` parses with `time.Parse(time.RFC3339, ...)` and equals the UTC formatted value of the input `libtime.DateTime`.
   d. `Put` path escaping: key `claims/repo/a b` (a space in a segment) produces a request whose `r.URL.EscapedPath()` contains `%20` and whose decoded `r.URL.Path` is `/api/v1/refs/claims/repo/a b`.
   e. `Delete`: handler asserts method DELETE, path `/api/v1/refs/claims/task/abc123`, query `expected_sha=S0`; returns `{"ok":true}`; client returns nil.
   f. Auth headers present: with secret `s3cret` and initiator `agent-task-executor`, both `X-Gateway-Secret: s3cret` and `X-Gateway-Initator: agent-task-executor` are present on List, Put and Delete (use a table over the three calls).
   g. Auth headers absent: with empty secret, neither header is present on all three calls.
   h. Conflict mapping: PUT 409 → `errors.Is(err, claim.ErrLeaseConflict)` true; DELETE 409 → same.
   i. Non-conflict failures: PUT 400, PUT 500, DELETE 500, and a PUT 200 with body `not json` each return a non-nil error, `errors.Is(err, claim.ErrLeaseConflict)` false, and the error message contains the status code (`400`, `500`, `200` respectively).
   j. No retry: the server counts requests; every successful and every failing row records exactly one request.
   k. Secret never leaks: with secret `s3cret`, force a PUT 500 whose body is `s3cret` echoed back; assert the returned error string does not contain `s3cret`.
   l. Caller deadline wins: pass a context with a 1 ms deadline to a handler that blocks on `<-r.Context().Done()` (never `time.Sleep` — tests never sleep); the client returns an error for which `errors.Is(err, context.DeadlineExceeded)` is true, and the server records exactly one request.

6. Create `/workspace/pkg/claim/fake_git_rest_test.go` in `package claim_test` with a reusable in-process fake git-rest. It is test-only and must not be referenced from production code. Provide at least this API (names are load-bearing — prompt 2 uses them):

```go
type fakeRef struct {
	Sha        string
	Cluster    string
	TaskID     string
	LeaseUntil string
}

type fakeRequest struct {
	Method   string
	Path     string
	RawQuery string
	Header   http.Header
	Body     []byte
	Status   int // response status actually written
}

type fakeFault struct {
	Status int
	Body   string
	Apply  bool // PUT only: apply the CAS write (when it would succeed) before answering Status
}

type fakeGitRest struct { /* unexported fields */ }

func newFakeGitRest() *fakeGitRest
func (f *fakeGitRest) Close()
func (f *fakeGitRest) URL() string
func (f *fakeGitRest) Seed(ref, sha, cluster, taskID, leaseUntil string)
func (f *fakeGitRest) Ref(ref string) (fakeRef, bool)
func (f *fakeGitRest) DeleteRef(ref string)
func (f *fakeGitRest) Requests() []fakeRequest
func (f *fakeGitRest) ResetRequests()
func (f *fakeGitRest) SetFault(fn func(fakeRequest) *fakeFault)
func (f *fakeGitRest) SetPutBarrier(n int)
```

Semantics (these must be real, not stubs — AC1–AC6 depend on them):
   a. `newFakeGitRest` starts `httptest.NewServer` with one handler for paths under `/api/v1/refs`. `Close` closes the server. `URL` returns `server.URL`.
   b. Storage is `map[string]fakeRef` keyed by the full ref (`refs/claims/...`). `Seed` inserts or replaces. `Ref` returns a copy. `DeleteRef` removes.
   c. Request recording: append one `fakeRequest` per request, including the final response `Status`. `Requests` returns a copy; `ResetRequests` clears. The recording must be mutex-protected (race detector runs).
   d. `GET /api/v1/refs?prefix=P`: if `P` ends with `/`, return every stored ref with `strings.HasPrefix(ref, "refs/"+P)` (this is the `Recover` call with `P == "claims/"`). Otherwise return every stored ref where `ref == "refs/"+P` or `strings.HasPrefix(ref, "refs/"+P+"/")`. This is whole-segment prefix matching: prefix `claims/task/1` must not match `refs/claims/task/10`. Encode a non-nil `[]ClaimRef` so an empty result is `[]`, never `null`.
   e. `PUT /api/v1/refs/<key>`: decode `{expected_sha, cluster, task_id, lease_until}`. If a fault matches and `Apply` is false, answer `fault.Status` with `fault.Body` without touching storage. If `Apply` is true, perform the CAS write when it would succeed (see next clause), then answer `fault.Status`. Normal CAS: `expected_sha == ""` succeeds only when the ref is absent; a non-empty `expected_sha` succeeds only when the ref exists and its sha equals it; otherwise `409`. On success generate a fresh sha (a monotonic counter formatted as 40 hex chars is fine) and store `{sha, cluster, taskID, leaseUntil}`. Respond `200 {"ref":"refs/<key>","sha":"<new sha>"}`.
   f. `DELETE /api/v1/refs/<key>?expected_sha=S`: absent ref → `409`; sha mismatch → `409`; otherwise remove and respond `200 {"ok":true}`.
   g. Faults are consulted for every method before normal handling and can return any status. `SetFault(nil)` clears.
   h. `SetPutBarrier(n)`: when `n > 0`, every PUT handler blocks until `n` PUT handlers are waiting, then all proceed; the barrier is one-shot (reset after release). CAS application happens after the barrier and under the storage mutex, so two contenders both send `expected_sha == ""` and exactly one gets `200`. `SetPutBarrier(0)` disables.
   i. `newFakeGitRest` must be safe for concurrent use (`-race`).

7. Create `/workspace/pkg/claim/fake_git_rest_test.go` self-tests (AC12) in the same file or a sibling `_test.go`, exercising the fake **directly over HTTP** with `net/http` (not through the claim client):
   a. `PUT` with `expected_sha == ""` on an existing ref → `409`.
   b. `PUT` with a stale `expected_sha` → `409` and `Ref` shows the original sha unchanged.
   c. `DELETE` of an absent ref → `409`.
   d. Two successful `PUT`s on two different keys with byte-identical bodies (same `expected_sha == ""`, same cluster, same task id, same `lease_until`) return different `sha` values.
   e. `GET ?prefix=claims/task/1` returns `refs/claims/task/1` and never `refs/claims/task/10` when both are seeded.
   f. `GET ?prefix=claims/absent` returns body `[]`, not `null`.
   g. Fault injection: a fault returning `500` for PUT leaves the ref unchanged; a fault with `Apply: true` returning `500` leaves the ref at the new write (this is the AC5 read-back fixture).
   h. Barrier: two concurrent PUTs with `expected_sha == ""` through `SetPutBarrier(2)` produce exactly one `200` and one `409`, and the stored ref holds the winner's sha.

8. Do NOT add a lease manager, a key-derivation function, metrics, a protocol doc, or a CHANGELOG entry in this prompt. Do NOT import `pkg/claim` anywhere outside `pkg/claim/` and `mocks/` (AC11 is checked in prompt 3).
</requirements>

<constraints>
- The executor's runtime behaviour is unchanged: nothing outside `pkg/claim` and its generated mock imports the package.
- Gateway header names `X-Gateway-Secret` and `X-Gateway-Initator` (deliberate misspelling) and their omission when the secret is empty match the existing file client exactly; the secret is never logged, never returned in an error, never placed in a URL or metric label.
- `409` is final. Do NOT add retries, backoff, or a retry loop inside the client.
- Every request has a 30 s cap; a shorter caller context deadline wins.
- No knobs for the 30 s cap. It is an invariant constant.
- Keys and task ids never become metric label values (there are no metrics in this prompt).
- Do NOT add `github.com/bborbe/agent-task-controller` to `go.mod`.
- Do NOT add a CHANGELOG entry in this prompt — the feature-level `feat:` bullet lands in prompt 3.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `go test -mod=mod -race -count=1 ./pkg/claim/...` iteratively after each change, then `make precommit` ONCE at the very end.

- `go test -mod=mod -race -count=1 ./pkg/claim/...` — exits 0.
- `grep -l 'FakeClaimRefClient' mocks/*.go` — returns at least one file.
- `grep -rn 'func NewClaimRefClient\|func (c \*claimRefClient) List\|func (c \*claimRefClient) Put\|func (c \*claimRefClient) Delete' pkg/claim/` — all present.
- `grep -rnE 'time\.(Now|Sleep|After|AfterFunc|NewTimer|NewTicker|Since|Until|Tick)\(' pkg/claim --include='*.go' | grep -v '_test.go'` — returns 0 lines.
- `grep -rn 'go func' pkg/claim --include='*.go' | grep -v '_test.go'` — returns 0 lines.
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/claim/... && go tool cover -func=/tmp/cover.out` — `NewClaimRefClient`, `List`, `Put`, `Delete`, `setAuthHeaders`, `escapedKey`, `formatLeaseUntil` each exercised; ≥80% statement coverage for new code.
- `make precommit` — exits 0.

Do NOT run `docker`, `make build`, `make buca`, `kubectl`, `dark-factory`, `gh`, or `scripts/*.sh` commands in this prompt — those are operator-executable and belong on the spec's verification ladder.
</verification>

