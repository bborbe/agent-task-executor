// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
)

// maxResponseBytes caps a response body read. A claim body is a few hundred
// bytes; the cap exists so a misbehaving API, or a proxy returning an HTML
// error page, cannot stream an unbounded value into memory. It matches the
// 10 MiB cap ref_client.go applies to every read.
const maxResponseBytes = 10 * 1024 * 1024

// Typed-claim errors. Each names a distinct server answer, so a caller can act
// on them without parsing a body: a held claim is retryable later, a lost
// lease must be re-acquired, and a free claim is the ordinary idle state.
var (
	// ErrClaimHeld is returned when another holder owns the claim. The returned
	// *ClaimHeldError carries who holds it and until when.
	ErrClaimHeld = stderrors.New("claim held")
	// ErrClaimFree is returned by Read when nothing holds the claim.
	ErrClaimFree = stderrors.New("claim free")
	// ErrLeaseLost is returned by Renew when the token is no longer current.
	// The caller must re-acquire rather than retry the renew.
	ErrLeaseLost = stderrors.New("lease lost")
	// ErrClaimTargetInvalid is returned when a ClaimTarget names neither a task
	// nor a repository, or names both. It is refused locally: sending an empty
	// path segment would be a server-side 404 that reads like a missing task,
	// and an ambiguous target would silently act on one of two objects.
	ErrClaimTargetInvalid = stderrors.New("claim target invalid")
)

// StatusError is returned for a status no route-specific branch claims. It
// exposes the code as a field so a caller can tell a retryable 5xx from a
// terminal 4xx without parsing the message.
type StatusError struct {
	Action string
	Target string
	Status int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s %s answered %d", e.Action, e.Target, e.Status)
}

// Retryable reports whether the status suggests a transient failure.
func (e *StatusError) Retryable() bool { return e.Status >= 500 }

// ClaimHeldError names the holder of a claim that could not be acquired.
// It wraps ErrClaimHeld so callers can test with errors.Is and still report
// who blocked them.
type ClaimHeldError struct {
	Holder     string
	LeaseUntil libtime.DateTime
}

func (e *ClaimHeldError) Error() string {
	return "claim held by " + e.Holder + " until " + e.LeaseUntil.String()
}

// Unwrap makes errors.Is(err, ErrClaimHeld) true for every held answer.
func (e *ClaimHeldError) Unwrap() error { return ErrClaimHeld }

// ClaimTarget is what a claim is held ON: a task identifier or a repository.
// It is the typed replacement for git-rest's free-form ref key — a caller
// never spells a path. Exactly one addressing form must be set; a target that
// sets both, or neither, is refused rather than silently resolved.
type ClaimTarget struct {
	// TaskID addresses a task claim. Mutually exclusive with Owner/Repo.
	TaskID string
	// Owner and Repo address a repository claim.
	Owner string
	Repo  string
}

// NewTaskClaim returns a target addressed by task identifier.
func NewTaskClaim(taskID string) ClaimTarget { return ClaimTarget{TaskID: taskID} }

// NewRepoClaim returns a target addressed by repository.
func NewRepoClaim(owner, repo string) ClaimTarget {
	return ClaimTarget{Owner: owner, Repo: repo}
}

// isRepo reports whether this target names a repository rather than a task.
func (t ClaimTarget) isRepo() bool { return t.Owner != "" || t.Repo != "" }

// path returns the vault-api path for this target, or "" when the target is
// not addressable. An ambiguous target (both forms set) is NOT addressable:
// picking one silently would act on a different object than the caller named.
func (t ClaimTarget) path() string {
	if t.TaskID != "" && t.isRepo() {
		return ""
	}
	if t.TaskID != "" {
		return "/api/v1/tasks/" + url.PathEscape(t.TaskID) + "/claim"
	}
	if t.Owner != "" && t.Repo != "" {
		return "/api/v1/repos/" + url.PathEscape(t.Owner) + "/" + url.PathEscape(t.Repo) + "/claim"
	}
	return ""
}

// renewPath returns the renew route, or "" when the target is not addressable.
// It must NOT be written as path() + "/renew": for an empty ClaimTarget path()
// is "", so that spelling yields the non-empty "/renew", the empty-path guard
// never fires, and the client would send a request to baseURL+"/renew" instead
// of refusing locally.
func (t ClaimTarget) renewPath() string {
	if p := t.path(); p != "" {
		return p + "/renew"
	}
	return ""
}

// describe renders the target for a log line or error, never as a path.
func (t ClaimTarget) describe() string {
	if t.TaskID != "" && t.isRepo() {
		return "ambiguous target (task " + t.TaskID + ", repo " + t.Owner + "/" + t.Repo + ")"
	}
	if t.TaskID != "" {
		return "task " + t.TaskID
	}
	if t.isRepo() {
		return "repo " + t.Owner + "/" + t.Repo
	}
	return "no target"
}

// Claim is the server's view of a held lease.
type Claim struct {
	Scope       string
	ID          string
	Holder      string
	LeaseUntil  libtime.DateTime
	RenewBefore libtime.DateTime
	LeaseToken  string
}

//counterfeiter:generate -o ../../mocks/vault_api_claim_client.go --fake-name FakeVaultAPIClaimClient . ClaimClient

// ClaimClient is the typed vault-api claim contract. It replaces the
// key/SHA-shaped ClaimRefClient for lease work: a claim is acquired on a
// target with a holder and a TTL, and released with the lease token the
// acquire returned. There is no key, no expected-SHA and no prefix listing —
// those are git-rest's vocabulary, not the claim's.
//
// ⚠️ ClaimRefClient stays: RedriveStreakStore uses the same git-rest refs as a
// counter, which is not a lease and has no typed home yet.
type ClaimClient interface {
	// Acquire takes the claim for holder with the given TTL in seconds. A held
	// claim returns a *ClaimHeldError. An expired lease is taken over, so a
	// dead holder cannot block the target forever.
	Acquire(ctx context.Context, target ClaimTarget, holder string, ttlSeconds int) (*Claim, error)
	// Renew extends an existing lease. It never takes over: a stale token
	// returns ErrLeaseLost and the caller re-acquires.
	Renew(
		ctx context.Context,
		target ClaimTarget,
		leaseToken string,
		ttlSeconds int,
	) (*Claim, error)
	// Release drops the claim. A token that is no longer current is
	// ErrLeaseLost, not a silent success.
	Release(ctx context.Context, target ClaimTarget, leaseToken string) error
	// Read returns the current claim, or ErrClaimFree when there is none.
	Read(ctx context.Context, target ClaimTarget) (*Claim, error)
}

// NewVaultAPIClaimClient creates a ClaimClient for the vault API at baseURL.
// gatewaySecret empty means no auth headers are sent. The secret is held in
// memory only and must never appear in a log line or an error string.
func NewVaultAPIClaimClient(baseURL, gatewaySecret, gatewayInitiator string) ClaimClient {
	return &vaultAPIClaimClient{
		baseURL:          strings.TrimRight(baseURL, "/"),
		httpClient:       &http.Client{Timeout: 30 * time.Second},
		gatewaySecret:    gatewaySecret,
		gatewayInitiator: gatewayInitiator,
	}
}

type vaultAPIClaimClient struct {
	baseURL          string
	httpClient       *http.Client
	gatewaySecret    string
	gatewayInitiator string
}

// claimRequestBody is the acquire/renew/release body. The omitempty tags are
// load-bearing, not tidiness: the routes share one struct, and the server
// REFUSES an acquire that carries a lease_token (400 unexpected_lease_token)
// just as a release must not carry a holder. Omitting the empty fields is what
// keeps each route's body to its own vocabulary.
type claimRequestBody struct {
	Holder     string `json:"holder,omitempty"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
	LeaseToken string `json:"lease_token,omitempty"`
}

// claimResponseBody is the server's claim representation.
type claimResponseBody struct {
	Scope       string `json:"scope"`
	ID          string `json:"id"`
	Holder      string `json:"holder"`
	LeaseUntil  string `json:"lease_until"`
	RenewBefore string `json:"renew_before"`
	LeaseToken  string `json:"lease_token"`
}

// errorResponseBody is the flat error envelope every existing vault-api route
// answers with.
type errorResponseBody struct {
	Error      string `json:"error"`
	Holder     string `json:"holder"`
	LeaseUntil string `json:"lease_until"`
}

func (c *vaultAPIClaimClient) setAuthHeaders(req *http.Request) {
	if c.gatewaySecret == "" {
		return
	}
	req.Header.Set("X-Gateway-Secret", c.gatewaySecret)
	req.Header.Set("X-Gateway-Initator", c.gatewayInitiator)
}

// Acquire takes the claim for holder.
func (c *vaultAPIClaimClient) Acquire(
	ctx context.Context,
	target ClaimTarget,
	holder string,
	ttlSeconds int,
) (*Claim, error) {
	body := claimRequestBody{Holder: holder, TTLSeconds: ttlSeconds}
	status, raw, err := c.do(ctx, http.MethodPost, target, body)
	if err != nil {
		return nil, err
	}
	if status == http.StatusConflict {
		return nil, heldError(ctx, raw)
	}
	if status != http.StatusCreated {
		return nil, statusError("acquire", target, status)
	}
	return decodeClaim(ctx, raw)
}

// Renew extends an existing lease.
func (c *vaultAPIClaimClient) Renew(
	ctx context.Context,
	target ClaimTarget,
	leaseToken string,
	ttlSeconds int,
) (*Claim, error) {
	body := claimRequestBody{LeaseToken: leaseToken, TTLSeconds: ttlSeconds}
	status, raw, err := c.doPath(ctx, http.MethodPost, target.renewPath(), target, body)
	if err != nil {
		return nil, err
	}
	if status == http.StatusConflict {
		return nil, errors.Wrapf(ctx, ErrLeaseLost, "renew %s", target.describe())
	}
	if status != http.StatusOK {
		return nil, statusError("renew", target, status)
	}
	return decodeClaim(ctx, raw)
}

// Release drops the claim.
func (c *vaultAPIClaimClient) Release(
	ctx context.Context,
	target ClaimTarget,
	leaseToken string,
) error {
	body := claimRequestBody{LeaseToken: leaseToken}
	status, _, err := c.do(ctx, http.MethodDelete, target, body)
	if err != nil {
		return err
	}
	if status == http.StatusConflict {
		return errors.Wrapf(ctx, ErrLeaseLost, "release %s", target.describe())
	}
	if status != http.StatusNoContent {
		return statusError("release", target, status)
	}
	return nil
}

// Read returns the current claim.
func (c *vaultAPIClaimClient) Read(ctx context.Context, target ClaimTarget) (*Claim, error) {
	status, raw, err := c.do(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, errors.Wrapf(ctx, ErrClaimFree, "read %s", target.describe())
	}
	if status != http.StatusOK {
		return nil, statusError("read", target, status)
	}
	return decodeClaim(ctx, raw)
}

// do issues one request against the target's own path.
func (c *vaultAPIClaimClient) do(
	ctx context.Context,
	method string,
	target ClaimTarget,
	body any,
) (int, []byte, error) {
	return c.doPath(ctx, method, target.path(), target, body)
}

// doPath issues one request and RETURNS THE BODY BYTES, not the response.
//
// ⚠️ This shape is deliberate and was a bug once. A per-request timeout
// context controls the entire lifetime of the request AND its response body,
// so returning *http.Response to a caller that reads it after this function
// returns means the deferred cancel() has already fired — every body not
// already buffered by the transport then fails with context.Canceled. Reading
// here, inside the context's lifetime, is what makes the cancel safe. A
// httptest server cannot reproduce the failure, because it buffers small
// bodies before Do returns; the bug would only have appeared in production.
func (c *vaultAPIClaimClient) doPath(
	ctx context.Context,
	method string,
	path string,
	target ClaimTarget,
	body any,
) (int, []byte, error) {
	if path == "" {
		return 0, nil, errors.Wrapf(
			ctx,
			ErrClaimTargetInvalid,
			"claim target %s",
			target.describe(),
		)
	}
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout.Duration())
	defer cancel()

	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, errors.Wrapf(ctx, err, "marshal %s body", target.describe())
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return 0, nil, errors.Wrapf(ctx, err, "create %s request for %s", method, target.describe())
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c.setAuthHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, errors.Wrapf(ctx, err, "%s %s", method, target.describe())
	}
	defer resp.Body.Close()
	glog.V(3).Infof(
		"vault-api claim %s %s status=%d", method, target.describe(), resp.StatusCode,
	)

	// LimitReader at cap+1 so an oversized body is detected rather than
	// silently truncated into a parse error that names the wrong cause.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return resp.StatusCode, nil, errors.Wrapf(ctx, err, "read %s response", target.describe())
	}
	if len(raw) > maxResponseBytes {
		return resp.StatusCode, nil, errors.Wrapf(
			ctx,
			stderrors.New("response too large"),
			"%s %s exceeded %d bytes",
			method,
			target.describe(),
			maxResponseBytes,
		)
	}
	return resp.StatusCode, raw, nil
}

// heldError decodes a 409 claim_held body into a *ClaimHeldError. An
// unparsable lease_until FAILS rather than leaving the zero value: a zero
// LeaseUntil reads as "already expired" to any caller comparing against the
// clock, which is the opposite of what a held claim means.
func heldError(ctx context.Context, raw []byte) error {
	var parsed errorResponseBody
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return errors.Wrapf(ctx, ErrClaimHeld, "decode held body")
	}
	until, err := libtime.ParseDateTime(ctx, parsed.LeaseUntil)
	if err != nil {
		return errors.Wrapf(ctx, err, "parse held lease_until %q", parsed.LeaseUntil)
	}
	return &ClaimHeldError{Holder: parsed.Holder, LeaseUntil: *until}
}

// statusError builds a *StatusError naming the action, the target and the
// status. The body is not echoed: a server body can carry a holder or a token.
func statusError(action string, target ClaimTarget, status int) error {
	return &StatusError{Action: action, Target: target.describe(), Status: status}
}

// decodeClaim turns a claim response body into a Claim. Both lease_until and
// lease_token are REQUIRED: a response that omits either is schema drift or a
// truncated body, and returning a Claim with a zero lease or an empty token
// would hand the caller something that cannot be renewed or released.
func decodeClaim(ctx context.Context, raw []byte) (*Claim, error) {
	var parsed claimResponseBody
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, errors.Wrapf(ctx, err, "decode claim response")
	}
	if parsed.LeaseUntil == "" {
		return nil, errors.Wrapf(ctx, stderrors.New("missing lease_until"), "decode claim response")
	}
	if parsed.LeaseToken == "" {
		return nil, errors.Wrapf(ctx, stderrors.New("missing lease_token"), "decode claim response")
	}
	until, err := libtime.ParseDateTime(ctx, parsed.LeaseUntil)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "parse lease_until %q", parsed.LeaseUntil)
	}
	result := &Claim{
		Scope:      parsed.Scope,
		ID:         parsed.ID,
		Holder:     parsed.Holder,
		LeaseUntil: *until,
		LeaseToken: parsed.LeaseToken,
	}
	if parsed.RenewBefore != "" {
		before, err := libtime.ParseDateTime(ctx, parsed.RenewBefore)
		if err != nil {
			return nil, errors.Wrapf(ctx, err, "parse renew_before %q", parsed.RenewBefore)
		}
		result.RenewBefore = *before
	}
	return result, nil
}
