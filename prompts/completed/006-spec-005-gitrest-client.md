---
status: completed
spec: [005-bug-executor-restart-drops-deferred-respawn-queue]
summary: Ported the read-only git-rest client (Get/List/IsReady with gateway-secret auth) into pkg/gitrestclient, added the ReadGitRestGatewaySecret helper, declared the GITREST_URL/GITREST_GATEWAY_SECRET/TASK_GLOB env config surface in main.go, and generated the FakeGitRestClient mock with full httptest and fake-clientset coverage.
execution_id: agent-task-executor-exec-006-spec-005-gitrest-client
dark-factory-version: dev
created: "2026-09-03T18:04:37Z"
queued: "2026-09-03T18:18:58Z"
started: "2026-09-03T18:19:32Z"
completed: "2026-09-03T18:26:13Z"
branch: dark-factory/bug-executor-restart-drops-deferred-respawn-queue
---

<summary>
- The executor gains a read-only git-rest HTTP client (`List` / `Get` / `IsReady`) so a future reconcile loop can read vault task files without a vault mount or filesystem view — the same client shape `agent-task-controller` already uses, ported into this repo with no cross-repo import.
- The ported client sends git-rest's `X-Gateway-Secret` / `X-Gateway-Initator` auth headers when a gateway secret is configured; with no secret it sends no headers, so auth-disabled git-rest still works.
- The gateway secret is referenced by K8s secret NAME (the `JobKafkaClientCertSecret` pattern), never the value in env: a new helper reads the secret's `gateway-secret` data key from the K8s API and returns the value for the client to use in-memory only.
- Three new executor env config values are declared on the `application` struct — `GITREST_URL`, `GITREST_GATEWAY_SECRET` (secret name, default empty = no auth), `TASK_GLOB` (default `24 Tasks/*.md`) — so the config surface is fixed before the reconcile loop prompt consumes them.
- A counterfeiter mock (`FakeGitRestClient`) is generated so downstream prompts can test against a fake vault source.
- Full HTTP-level tests (httptest) cover success, 503-readiness, error, and auth-header paths for all three methods; the secret-read helper is tested against a fake clientset.
</summary>

<objective>
Establish the executor's vault-read contract: a small, read-only git-rest HTTP client (`List`/`Get`/`IsReady`) with gateway-secret auth, plus the three executor env config values it is configured from, so the reconcile loop built in the next prompt can derive task state from the vault without a vault mount or filesystem view.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for project conventions (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, counterfeiter mocks, glog V(n) gating, coverage ≥80% for new code).

Read these files fully before changing anything:
- `/workspace/main.go` — the `application` struct (lines 42-65) shows the exact field-tag pattern for new env config (`required:"false" arg:"..." env:"..." usage:"..." default:"..."`). Mirror it.
- `/workspace/pkg/metrics/metrics_test.go` and `/workspace/pkg/metrics/metrics_suite_test.go` — the Ginkgo suite-file pattern to mirror for the new `pkg/gitrestclient` package.
- `/workspace/pkg/spawner/job_spawner.go` (lines 36-37) — the `//counterfeiter:generate -o ../../mocks/...` annotation format to copy for the new interface.
- `/workspace/pkg/job_watcher_internal_test.go` (lines 19-21) — the `k8s.io/client-go/kubernetes/fake` `fake.NewSimpleClientset(...)` pattern for the secret-read helper test.
- `/workspace/pkg/config_resolver.go` (lines 18-20) — how `github.com/bborbe/errors` `errors.Errorf(ctx, ...)` / `errors.Wrapf(ctx, err, ...)` are used for config-resolution errors, matching the error style required here.

Reference for the ported client shape (read-only subset): the controller's client at `/home/node/go/pkg/mod/github.com/bborbe/agent-task-controller@v0.6.6/pkg/gitrestclient/git_rest_client.go` — `Get`, `List`, `IsReady`, `fileURL`, `setAuthHeaders`. The executor port drops `Post`/`PostIfAbsent`/`Delete`, the retry/backoff (reads fail-fast), and the metrics dependency (spec 005 Security: "git-rest reads are read-only (`List`/`Get`); the executor never writes vault files through git-rest"). If that module is not present in the container's module cache, the target code below is authoritative — do not add `agent-task-controller` to go.mod.

The git-rest auth contract (from the controller's client, do NOT alter the header names):
- `X-Gateway-Secret` — the shared secret value (exact string match).
- `X-Gateway-Initator` — caller identity (deliberate misspelling, do NOT change to "Initiator"). This executor sends `agent-task-executor`.
- `/readiness` returns 200 when ready and 503 while unavailable; 503 is a valid not-ready state, not an error.

Decision note for the reviewer: the env var names `GITREST_URL` / `GITREST_GATEWAY_SECRET` / `TASK_GLOB` and the Secret data key `gateway-secret` are chosen to match the controller's chart conventions (verified in the `github.com/bborbe/agent` helm templates: `controller.gatewaySecret` chart-created Secret data key `gateway-secret`, consumed via `secretKeyRef`). The chart-side wiring of these names (and the executor ServiceAccount `secrets: get` grant) is operator-ladder work outside this repo — flagged, not executed here.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega, external test packages, coverage ≥80%.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `github.com/bborbe/errors`, never `fmt.Errorf`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-security-linting.md` — gosec rules for the HTTP client (30s timeout, 10 MiB read cap are carried over from the controller and already lint-clean).
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.
</context>

<requirements>
1. Create the new package directory `/workspace/pkg/gitrestclient/` and the file `/workspace/pkg/gitrestclient/git_rest_client.go` with the following content verbatim (this is the ported read-only subset of the controller's client, minus metrics/backoff/writes). Keep the BSD license header:

```go
// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gitrestclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bborbe/errors"
)

//counterfeiter:generate -o ../../mocks/git_rest_client.go --fake-name FakeGitRestClient . GitRestClient

// GitRestClient is the HTTP client for git-rest's /api/v1/files REST API.
// All paths are relative to the repo root (e.g. "tasks/foo.md"). The executor
// uses only the read surface (List/Get/IsReady) — it never writes vault files
// through git-rest (spec 005 Security).
type GitRestClient interface {
	// Get retrieves the current content of the file at relPath.
	Get(ctx context.Context, relPath string) ([]byte, error)
	// List returns relative paths matching the single-level glob pattern (e.g. "tasks/*.md").
	List(ctx context.Context, glob string) ([]string, error)
	// IsReady reports whether git-rest's /readiness returns 200.
	// Returns (false, nil) when git-rest returns 503 — that is a valid not-ready state, not an error.
	// Returns (false, err) only on network failure or unexpected response.
	IsReady(ctx context.Context) (bool, error)
}

// NewGitRestClient creates a GitRestClient targeting the git-rest instance at
// baseURL. gatewaySecret is the shared secret enforced by git-rest's
// gateway-secret auth; when empty, no auth headers are sent (backward-compat
// with auth-disabled git-rest). gatewayInitiator is the caller identity logged
// by git-rest on auth failure (pass a stable value, e.g. "agent-task-executor").
func NewGitRestClient(baseURL, gatewaySecret, gatewayInitiator string) GitRestClient {
	return &gitRestClient{
		baseURL:          strings.TrimRight(baseURL, "/"),
		httpClient:       &http.Client{Timeout: 30 * time.Second},
		gatewaySecret:    gatewaySecret,
		gatewayInitiator: gatewayInitiator,
	}
}

type gitRestClient struct {
	baseURL          string
	httpClient       *http.Client
	gatewaySecret    string
	gatewayInitiator string
}

// setAuthHeaders sets the gateway-secret auth headers on req when the secret is configured.
// No-op when gatewaySecret is empty — keeps backward compatibility with auth-disabled git-rest.
//
// Header names are part of the git-rest public contract (spec 004) — do NOT alter:
//
//	X-Gateway-Secret   — the shared secret
//	X-Gateway-Initator — caller identity (deliberate misspelling, do NOT change to "Initiator")
func (g *gitRestClient) setAuthHeaders(req *http.Request) {
	if g.gatewaySecret == "" {
		return
	}
	req.Header.Set("X-Gateway-Secret", g.gatewaySecret)
	req.Header.Set("X-Gateway-Initator", g.gatewayInitiator)
}

// fileURL builds /api/v1/files/<relPath> with proper percent-escaping so
// characters like %, space, # in relPath survive the round-trip to git-rest.
func (g *gitRestClient) fileURL(relPath string) string {
	segments := strings.Split(relPath, "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return g.baseURL + "/api/v1/files/" + strings.Join(segments, "/")
}

// Get retrieves file content from git-rest. Does not retry — reads fail-fast.
func (g *gitRestClient) Get(ctx context.Context, relPath string) ([]byte, error) {
	reqURL := g.fileURL(relPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create GET request for %s", relPath)
	}
	g.setAuthHeaders(req)
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "GET %s", relPath)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10 MiB max
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "read GET response body for %s", relPath)
	}
	if resp.StatusCode != http.StatusOK {
		preview := string(body)
		if len(preview) > 100 {
			preview = preview[:100]
		}
		return nil, errors.Errorf(ctx, "GET %s returned %d: %s", relPath, resp.StatusCode, preview)
	}
	return body, nil
}

// List returns paths matching the glob pattern. Does not retry — reads fail-fast.
func (g *gitRestClient) List(ctx context.Context, glob string) ([]string, error) {
	reqURL := g.baseURL + "/api/v1/files/"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create LIST request for glob %s", glob)
	}
	q := url.Values{}
	q.Set("glob", glob)
	req.URL.RawQuery = q.Encode()
	g.setAuthHeaders(req)
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "LIST glob %s", glob)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10 MiB max
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "read LIST response body for glob %s", glob)
	}
	if resp.StatusCode != http.StatusOK {
		preview := string(body)
		if len(preview) > 100 {
			preview = preview[:100]
		}
		return nil, errors.Errorf(ctx, "LIST glob %s returned %d: %s", glob, resp.StatusCode, preview)
	}
	var paths []string
	if err := json.Unmarshal(body, &paths); err != nil {
		return nil, errors.Wrapf(ctx, err, "parse LIST response for glob %s", glob)
	}
	return paths, nil
}

// IsReady checks git-rest's /readiness endpoint.
func (g *gitRestClient) IsReady(ctx context.Context) (bool, error) {
	reqURL := g.baseURL + "/readiness"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return false, errors.Wrapf(ctx, err, "create readiness request")
	}
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return false, errors.Wrapf(ctx, err, "readiness check")
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusServiceUnavailable:
		return false, nil
	default:
		return false, errors.Errorf(ctx, "readiness returned unexpected status %d", resp.StatusCode)
	}
}
```

Do NOT add any other methods, metrics, or retry logic — the executor is a read-only consumer of git-rest (YAGNI; the spec's observability for this feature is the reconcile-loop log line + counter in the next prompt, not per-call git-rest metrics).

2. Create `/workspace/pkg/gitrestclient/gitrestclient_suite_test.go` in `package gitrestclient_test` mirroring the pattern in `/workspace/pkg/metrics/metrics_suite_test.go`, including the counterfeiter go:generate directive (copy the exact directive line from `/workspace/pkg/handler/task_event_handler_test.go:7`):
```go
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6@v6.12.2 -generate
```
It must contain `func TestGitRestClient(t *testing.T)` calling `RegisterFailHandler(Fail)` and `RunSpecs(t, "GitRestClient Suite")`.

3. Create `/workspace/pkg/gitrestclient/git_rest_client_test.go` in `package gitrestclient_test` using `net/http/httptest` servers (mirror the assertion style of the controller's `git_rest_client_test.go`: the handler asserts `r.Method`, `r.URL.Path`, and `r.URL.Query()` on each request). Cover:
   a. `Get` 200: returns the exact body bytes; handler asserts method GET and path `/api/v1/files/tasks/foo.md`.
   b. `Get` non-200 (404): returns an error whose message contains the status code.
   c. `Get` with a gateway secret configured: handler asserts `X-Gateway-Secret` and `X-Gateway-Initator` headers are present with the configured values, then returns 200.
   d. `Get` with empty gateway secret: handler asserts NEITHER auth header is present, then returns 200.
   e. `Get` path escaping: relPath `"24 Tasks/a b.md"` produces a request whose `r.URL.Path` contains the escaped segments (space and the path segment separators survive as separate path elements — assert the handler receives a 2-element path `"/api/v1/files/24 Tasks/a b.md"` decoded; `url.PathEscape` escapes the space in the URL, so assert on `r.URL.EscapedPath()` containing `%20`).
   f. `List` 200: handler asserts the query parameter `glob` equals the configured glob, returns JSON `["24 Tasks/a.md","24 Tasks/b.md"]`; client returns exactly those two strings.
   g. `List` non-200: returns an error containing the status code.
   h. `List` with unparseable body (`not json`): returns an error (no panic).
   i. `IsReady` 200: returns `(true, nil)`.
   j. `IsReady` 503: returns `(false, nil)` — a valid not-ready state, NOT an error.
   k. `IsReady` 500: returns `(false, err)`.
   l. `IsReady` against a closed server (`server.Close()` before the call): returns `(false, err)` — network failure path.

4. Create `/workspace/pkg/gitrest_gateway_secret.go` in `/workspace/pkg/` (package `pkg`) with:
```go
// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// gitRestGatewaySecretDataKey is the data key inside the referenced K8s Secret
// that holds the git-rest gateway secret value. Matches the controller chart's
// `gateway-secret` data key (the Secret git-rest and the controller share).
const gitRestGatewaySecretDataKey = "gateway-secret"

// ReadGitRestGatewaySecret returns the git-rest gateway secret value from the
// named K8s Secret, or "" when secretName is empty (gateway auth disabled).
// The value is read once at startup and held in memory only — it is never
// logged, never embedded in the image, and never in env (spec 005 Security;
// the env var GITREST_GATEWAY_SECRET references the secret by NAME, matching
// the JobKafkaClientCertSecret pattern). Returns an error when the Secret is
// missing or lacks the gateway-secret data key, so a misconfigured deployment
// fails loudly at startup instead of silently sending no auth header.
func ReadGitRestGatewaySecret(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	namespace libk8s.Namespace,
	secretName string,
) (string, error) {
	if secretName == "" {
		return "", nil
	}
	secret, err := kubeClient.CoreV1().Secrets(namespace.String()).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return "", errors.Wrapf(ctx, err, "get git-rest gateway secret %q", secretName)
	}
	value, ok := secret.Data[gitRestGatewaySecretDataKey]
	if !ok {
		return "", errors.Errorf(ctx, "secret %q lacks data key %q", secretName, gitRestGatewaySecretDataKey)
	}
	return string(value), nil
}
```
This function has no counterfeiter annotation — callers pass the real `kubernetes.Interface` and tests use the fake clientset.

5. Create `/workspace/pkg/gitrest_gateway_secret_test.go` in `package pkg_test` (this registers into the existing "Pkg Suite" launched by `TestPkg` in `/workspace/pkg/agent_configuration_test.go`). Use `fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "gw-secret", Namespace: "test-ns"}, Data: map[string][]byte{"gateway-secret": []byte("s3cret")}})` (imports `k8s.io/client-go/kubernetes/fake`, `corev1 "k8s.io/api/core/v1"`, `metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"`). Cover:
   a. empty secretName → `("", nil)` — auth disabled.
   b. secret present with `gateway-secret` data key → returns the value `"s3cret"`.
   c. secret missing (fake clientset without the secret) → returns an error mentioning the secret name.
   d. secret present WITHOUT the `gateway-secret` data key (e.g. `Data: map[string][]byte{"other": []byte("x")}`) → returns an error mentioning the data key.

6. Add three new env config fields to the `application` struct in `/workspace/main.go`. Place them directly after the `JobKafkaCaCertSecret` field (line 64), preserving the existing field-tag alignment style:
```go
	// GitRestURL is the git-rest HTTP API base URL the reconcile loop reads the
	// vault through. Matches the controller's default git-rest service address.
	// Consumed by the reconcile loop in the follow-up prompt.
	GitRestURL string `required:"false" arg:"git-rest-url" env:"GITREST_URL" usage:"git-rest HTTP API base URL the reconcile loop reads vault task files through" default:"http://vault-obsidian-openclaw:9090"`
	// GitRestGatewaySecret holds the NAME of the K8s Secret carrying the
	// git-rest gateway secret (data key `gateway-secret`) — not the secret
	// value itself. Same resource-reference pattern as JobKafkaClientCertSecret:
	// a name, not secret material, so nothing secret-shaped enters the image or
	// the Deployment manifest. Consumed by the reconcile loop in the follow-up
	// prompt.
	GitRestGatewaySecret string `required:"false" arg:"git-rest-gateway-secret" env:"GITREST_GATEWAY_SECRET" usage:"Name of the existing K8s secret holding the git-rest gateway secret (data key gateway-secret); empty disables gateway auth"`
	// TaskGlob is the git-rest single-level glob selecting the vault task files
	// the reconcile loop evaluates, relative to the repo root. Consumed by the
	// reconcile loop in the follow-up prompt.
	TaskGlob string `required:"false" arg:"task-glob" env:"TASK_GLOB" usage:"git-rest glob selecting vault task files to reconcile" default:"24 Tasks/*.md"`
```
Do NOT wire these values anywhere in this prompt — the reconcile loop that consumes them is the next prompt. They exist here to fix the config surface (names, defaults) before the consumer lands.

7. Regenerate the mock: run `go generate -mod=mod ./pkg/gitrestclient/...` (or `go generate -mod=mod ./...`) so `/workspace/mocks/git_rest_client.go` with `FakeGitRestClient` is produced from the `//counterfeiter:generate` annotation in requirement 1. The mock must satisfy `gitrestclient.GitRestClient` (methods `Get`, `List`, `IsReady` with the usual counterfeiter `...Returns`, `...CallCount`, `...ArgsForCall` accessors). Do not hand-write the mock.

8. Do NOT add a CHANGELOG entry in this prompt — the feature-level `feat:` bullet covering the whole reconcile feature lands in the next prompt, and dark-factory reads the prefix from that single entry for the version bump.
</requirements>

<constraints>
- Repo conventions are frozen: Ginkgo/Gomega v2 tests (no stdlib table tests), `github.com/bborbe/errors` wrapping (never `fmt.Errorf`), counterfeiter mocks for any new dependency (`//counterfeiter:generate`), glog `V(n)` gating.
- The executor does NOT gain a vault mount or filesystem view; the vault is read only through git-rest. This prompt ships the read-only client; it must NOT add any write method (`Post`/`PostIfAbsent`/`Delete`) or any retry/backoff — reads fail-fast, matching the controller's read behavior.
- The git-rest auth header names `X-Gateway-Secret` and `X-Gateway-Initator` (deliberate misspelling) are part of the public git-rest contract — do NOT alter them.
- `GITREST_GATEWAY_SECRET` holds a K8s Secret NAME (like `JOB_KAFKA_CLIENT_CERT_SECRET`), never the secret value; the value is read from the K8s API and held in memory only.
- Do NOT add `github.com/bborbe/agent-task-controller` to go.mod — the client is ported in, not imported.
- Do NOT wire the Helm chart or config-repo values — out of scope (the chart lives outside this repo; the deploy surface is the spec's operator ladder).
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `go test -mod=mod ./pkg/gitrestclient/... ./pkg/` iteratively after each change (fast feedback loop), then `make precommit` ONCE at the very end.

- `go test -mod=mod ./pkg/gitrestclient/...` — exits 0.
- `go test -mod=mod ./pkg/` — exits 0 (the new secret-read test registers into the Pkg Suite).
- `make precommit` — exits 0 (runs counterfeiter via `go generate -mod=mod ./...`; the new mock must be produced and committed by dark-factory).
- `ls mocks/git_rest_client.go` — the FakeGitRestClient mock exists.
- `grep -n 'GITREST_URL\|GITREST_GATEWAY_SECRET\|TASK_GLOB' main.go` — returns ≥1 line each (acceptance-criterion evidence for the env wiring).
- `grep -rn 'func (g \*gitRestClient) List\|func (g \*gitRestClient) Get\|func (g \*gitRestClient) IsReady' pkg/gitrestclient/` — all three read methods present.
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/gitrestclient/... && go tool cover -func=/tmp/cover.out` — confirm `Get`, `List`, `IsReady`, `fileURL`, `setAuthHeaders` and `pkg.ReadGitRestGatewaySecret` are each exercised by at least one test (≥80% statement coverage for new code).

Do NOT run `docker`, `make build`, `kubectl`, `dark-factory`, `gh`, or `scripts/*.sh` commands in this prompt — those are operator-executable and belong on the spec's verification ladder.
</verification>
