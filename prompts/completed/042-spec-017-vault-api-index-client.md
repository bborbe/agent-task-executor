---
status: completed
spec: [017-view-divergence-detector]
execution_id: agent-task-executor-view-divergence-exec-042-spec-017-vault-api-index-client
dark-factory-version: v0.198.7
created: "2026-10-27T00:00:00Z"
queued: "2026-10-26T23:15:33Z"
started: "2026-10-26T23:15:34Z"
completed: "2026-10-27T19:16:11Z"
---

# Read vault-api's task index once, bounded and authenticated

<summary>
- The executor gains a read-only client for vault-api's task index
- The client authenticates with the deployment's existing gateway header pair, not a second auth path
- A response larger than 32 MiB, unparseable, or missing a required envelope field is an error, never an empty index
- An empty index is a valid answer and reads as zero items, not as a failure
- The index is requested unfiltered, so tasks without a cluster are not manufactured as absent
- The index's age is passed through from the server, never computed locally
- A written contract records the endpoint, envelope, auth, body cap and the unfiltered-request trap
- No new credential is introduced and no task content leaves the executor
</summary>

<objective>
Add the typed, bounded vault-api task-index client and its contract document, so one reconcile pass can read the vault's `status` and `phase` for every task in a single call. This prompt ships the client only; nothing consumes it yet.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for the container agent's execution rules: `github.com/bborbe/errors` wrapping, license headers on every Go file, Ginkgo/Gomega for external test packages, coverage ≥80% for new code. This repo has no project-level `CLAUDE.md`; `README.md` documents routing and admission only, not conventions.

Then read these, in this order:

- `pkg/claim/vault_api_client.go` — the exemplar this client mirrors. Note `setAuthHeaders` (the `X-Gateway-Secret` / `X-Gateway-Initator` pair, the latter misspelled on purpose), `doPath`'s `io.LimitReader(resp.Body, maxResponseBytes+1)` bound, the "read the body before the caller sees it" rule, `StatusError`, and the `errors.Wrapf` style.
- `pkg/claim/ref_client.go` — `RequestTimeout = 30 * libtime.Second`, the per-request cap both clients use.
- `pkg/vault_api_gateway_secret.go` — `ReadVaultAPIGatewaySecret`. Do not re-read the Secret here; the caller supplies the value.
- `pkg/claim/vault_api_client_test.go` — the `httptest` fixture and header assertions you will mirror.
- `pkg/claim/claim_suite_test.go` — `TestClaim` is the package suite; a new `_test.go` in `package claim_test` is discovered by it.
- `docs/claim-lease-protocol.md` — surrounding vault-api conventions, including "keys and task ids are never metric label values".

The repo has no `docs/vault-api-task-index.md` yet. Coding docs to read:
`/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/go-licensing-guide.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/documentation-guide.md`.
</context>

<requirements>
1. **Share the auth header pair instead of duplicating it.** In `pkg/claim/vault_api_client.go`, replace the body of `(*vaultAPIClaimClient).setAuthHeaders` with a call to a new package-private function, and add that function next to it:

   ```go
   // setVaultAPIAuthHeaders applies the gateway header pair every vault-api
   // request carries. The initiator header is misspelled "X-Gateway-Initator"
   // on purpose: the server compares that exact name, and "correcting" it makes
   // every request a 401. Both vault-api clients share this function so the pair
   // cannot drift.
   func setVaultAPIAuthHeaders(req *http.Request, secret, initiator string) {
       if secret == "" {
           return
       }
       req.Header.Set("X-Gateway-Secret", secret)
       req.Header.Set("X-Gateway-Initator", initiator)
   }
   ```

   Old:
   ```go
   func (c *vaultAPIClaimClient) setAuthHeaders(req *http.Request) {
       if c.gatewaySecret == "" {
           return
       }
       req.Header.Set("X-Gateway-Secret", c.gatewaySecret)
       req.Header.Set("X-Gateway-Initator", c.gatewayInitiator)
   }
   ```
   New:
   ```go
   func (c *vaultAPIClaimClient) setAuthHeaders(req *http.Request) {
       setVaultAPIAuthHeaders(req, c.gatewaySecret, c.gatewayInitiator)
   }
   ```
   The existing `pkg/claim/vault_api_client_test.go` already asserts both headers and both empty-header cases; it must stay green without edits.

2. **Add `pkg/claim/vault_api_index_client.go`** (package `claim`) with:

   ```go
   // TaskIndexItem is one task in vault-api's index: its id and the two fields
   // the divergence check compares.
   type TaskIndexItem struct {
       ID     string `json:"id"`
       Status string `json:"status"`
       Phase  string `json:"phase"`
   }

   // TaskIndex is vault-api's task index response. Total and Warnings are
   // coverage signals; IndexAgeSeconds is server-computed and must never be
   // derived locally.
   type TaskIndex struct {
       Items           []TaskIndexItem
       Total           int
       Warnings        int
       IndexAgeSeconds int
   }

   // VaultAPIIndexClient reads vault-api's task index. It is read-only: the
   // divergence check never writes to the vault through it.
   type VaultAPIIndexClient interface {
       Read(ctx context.Context) (*TaskIndex, error)
   }
   ```

   Constructor and concrete type:
   ```go
   func NewVaultAPIIndexClient(baseURL, gatewaySecret, gatewayInitiator string) VaultAPIIndexClient {
       return &vaultAPIIndexClient{
           baseURL:          strings.TrimRight(baseURL, "/"),
           httpClient:       &http.Client{Timeout: RequestTimeout.Duration()},
           gatewaySecret:    gatewaySecret,
           gatewayInitiator: gatewayInitiator,
       }
   }

   type vaultAPIIndexClient struct {
       baseURL          string
       httpClient       *http.Client
       gatewaySecret    string
       gatewayInitiator string
   }
   ```

3. **Bound the response at 32 MiB.** Add `const maxIndexResponseBytes = 32 * 1024 * 1024` in the new file. Do not reuse the claim client's 10 MiB `maxResponseBytes`: the index carries every task in the vault (9,002 files on the personal vault) and 10 MiB would reject a legitimate response.

4. **`Read` behavior**, in this order:
   - Empty `baseURL` → `errors.Errorf(ctx, "vault-api task index: base URL is empty")`.
   - `ctx, cancel := context.WithTimeout(ctx, RequestTimeout.Duration())`, `defer cancel()`.
   - `GET` to `c.baseURL + "/api/v1/tasks"` with **no query string**. The unfiltered read is deliberate: a `cluster=` filter omits files with no `cluster` frontmatter, which manufactures `absent` divergences for exactly the tasks most likely to be stale.
   - `setVaultAPIAuthHeaders(req, c.gatewaySecret, c.gatewayInitiator)`.
   - `c.httpClient.Do(req)`; transport error → `errors.Wrapf(ctx, err, "read vault-api task index")`.
   - `defer resp.Body.Close()`; read through `io.ReadAll(io.LimitReader(resp.Body, maxIndexResponseBytes+1))`. If `len(raw) > maxIndexResponseBytes`, return `errors.Errorf(ctx, "vault-api task index response exceeded %d bytes", maxIndexResponseBytes)`. Do not truncate and parse.
   - Non-`200` status → `errors.Errorf(ctx, "vault-api task index answered %d", resp.StatusCode)`. Do not echo the body: a server body can carry a credential or a holder.
   - Decode with `decodeTaskIndex(ctx, raw)`.

5. **Envelope strictness in `decodeTaskIndex`.** Use a wire struct, not the domain struct, so key presence is observable:
   ```go
   type taskIndexResponseBody struct {
       Items           json.RawMessage `json:"items"`
       Total           *int            `json:"total"`
       Warnings        *int            `json:"warnings"`
       IndexAgeSeconds *int            `json:"index_age_seconds"`
   }
   ```
   Rules, each with the stated reason:
   - `json.Unmarshal(raw, &parsed)` fails → `errors.Wrapf(ctx, err, "decode vault-api task index response")`. A body that is not JSON at all (a proxy's HTML error page, a truncated stream) is a `read_error`, never a silent empty index.
   - `parsed.Items == nil` (key absent) → `errors.Errorf(ctx, "decode vault-api task index response: missing items")`. A renamed or nested `items` must be a `read_error`, never a silent empty index.
   - `string(parsed.Items) == "null"` → an empty `[]TaskIndexItem{}`, **no error**. A Go server marshalling a nil slice writes `null`; that is a legitimate empty index and must read as `compared=0`, not as a failure.
   - Otherwise `json.Unmarshal(parsed.Items, &items)`; a decode error → `errors.Wrapf(ctx, err, "decode vault-api task index items")`.
   - `parsed.IndexAgeSeconds == nil` → `errors.Errorf(ctx, "decode vault-api task index response: missing index_age_seconds")`. The summary cannot report the age without it, and a dropped age is the schema drift the spec names.
   - `parsed.Total` and `parsed.Warnings` are **optional**: absent means `0`. They are decoded so the contract is pinned, but a server that omits a zero value must not turn every healthy pass into a `read_error`.
   - Return `&TaskIndex{Items: items, Total: ..., Warnings: ..., IndexAgeSeconds: *parsed.IndexAgeSeconds}`.

6. **Error and style conventions.** Use `github.com/bborbe/errors` (`errors.Wrapf`, `errors.Errorf`), never `fmt.Errorf`, never `context.Background()`. Start every exported GoDoc comment with the name. Add the standard license header to the new Go file. Keep `golines` lines ≤100. `.golangci.yml` sets `funlen` at 80 lines / 50 statements; `Read` delegating to `decodeTaskIndex` stays under both.

7. **Write `docs/vault-api-task-index.md`** with these sections:
   - **Endpoint** — `GET /api/v1/tasks`, no query parameters.
   - **Auth** — the request carries `X-Gateway-Secret` and `X-Gateway-Initator` (misspelled exactly; correcting it is a 401 on every pass). The secret is referenced by name from `VAULT_API_GATEWAY_SECRET`, read once through `pkg.ReadVaultAPIGatewaySecret`, held in memory, never logged.
   - **Response envelope** — a JSON example and a field table for `items[].id`, `items[].status`, `items[].phase`, `total`, `warnings`, `index_age_seconds`.
   - **Decode strictness** — `items` and `index_age_seconds` required; `total`/`warnings` optional defaulting to 0; `items: null` is an empty index; a missing required field is a `read_error`, never an empty index.
   - **Body bound** — 32 MiB via `io.LimitReader`; over-limit is a `read_error`.
   - **Do not filter** — the `cluster=` filter trap and why a filtered read manufactures `absent` divergences.
   - **Index age** — server-computed; the executor never compares two clocks and never derives it.
   - **Warnings** — a global count of files vault-api could not index, not attributable to one id from this response; the operator distinguishes `422 invalid_frontmatter` from `404 not_found` with the vault-api proxy probe.
   - **Source** — state that the envelope is pinned from `specs/in-progress/017-view-divergence-detector.md` and that **the serving vault-api version was not recorded in this repository** (vault-api is not a Go dependency and no manifest pins its image). Tell the reader to re-read the live endpoint and update this section before changing the client. This is an explicit open question, not an omission.

8. **Tests in `pkg/claim/vault_api_index_client_test.go`** (`package claim_test`, Ginkgo, discovered by `TestClaim`). Mirror the `httptest` fixture in `vault_api_client_test.go`; capture the request's method, path, raw query and headers. Cover:
   - one `GET` to `/api/v1/tasks` with `RawQuery == ""`, `X-Gateway-Secret == "secret-value"` and `X-Gateway-Initator == "agent-task-executor"`.
   - all four envelope fields decode (three items with distinct ids/statuses/phases, `total`, `warnings`, `index_age_seconds`).
   - `{"items": []}` and `{"items": null}` both yield `len(Items) == 0` and no error.
   - `{"index_age_seconds": 0}` with no `items` key → error.
   - `{"items": []}` with no `index_age_seconds` key → error.
   - `{"items": [], "index_age_seconds": 0}` with no `total`/`warnings` → `Total == 0`, `Warnings == 0`, no error.
   - a `429` response → error.
   - a body of `{"items": [` (truncated JSON) → error.
   - a server whose listener is closed before the call (`server.Close()`, keep the captured `URL`) → error naming `read vault-api task index`. This is the transport-error path.
   - `{"items": [{"id": 1}], "index_age_seconds": 0}` (a non-string `id`) → error naming `decode vault-api task index items`.
   - a body of `32*1024*1024+1` bytes → error. Stream it with a zero-filled reader and `io.CopyN`; do not allocate a 32 MiB string.
   - an empty `baseURL` → error before any request (assert the server saw zero requests).
   - a trailing-slash `baseURL` still requests `/api/v1/tasks` once (no `//`).
   - an empty secret sends neither header.
   Assert request counts exactly; a retry would make a "one call" contract false.
</requirements>

<constraints>
- Do NOT add a query filter to the index request.
- Do NOT compute `index_age_seconds` locally; pass the server value through.
- Do NOT introduce a second auth path or a new credential. Reuse `setVaultAPIAuthHeaders` and the deployment's existing gateway secret.
- Do NOT log the gateway secret, the response body, or any task content.
- Do NOT reuse the 10 MiB claim-body cap; the index needs 32 MiB.
- Do NOT add a counterfeiter fake for `VaultAPIIndexClient`. The client is exercised through `httptest`, and an unused generated fake is dead code. If a later prompt needs one, it can add it then.
- Do NOT change the existing claim client's behaviour. Extract shared helpers rather than copying blocks: the header pair is one, and a bounded-body read helper is a permitted second if `dupl` flags the copy — behaviour must stay identical either way.
- Do NOT touch `pkg/claim/ref_client.go`. Its `(*claimRefClient).setAuthHeaders` sets the same pair, but it addresses git-rest while this client addresses vault-api (see `pkg/claim_client_factory.go`), so it is deliberately outside this change.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
ROOTDIR=/workspace make precommit
go test -mod=mod -count=1 ./pkg/claim/
grep -n 'setVaultAPIAuthHeaders' pkg/claim/vault_api_index_client.go
grep -n 'X-Gateway-Initator' pkg/claim/vault_api_client.go
grep -n 'maxIndexResponseBytes' pkg/claim/vault_api_index_client.go
</verification>
