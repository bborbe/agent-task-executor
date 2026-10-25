// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
)

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
)

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
// never spells a path.
type ClaimTarget struct {
	// TaskID addresses a task claim. Mutually exclusive with Owner/Repo.
	TaskID string
	// Owner and Repo address a repository claim.
	Owner string
	Repo  string
}

// TaskClaim returns a target addressed by task identifier.
func TaskClaim(taskID string) ClaimTarget { return ClaimTarget{TaskID: taskID} }

// RepoClaim returns a target addressed by repository.
func RepoClaim(owner, repo string) ClaimTarget { return ClaimTarget{Owner: owner, Repo: repo} }

// path returns the vault-api path for this target, or "" when the target is
// not addressable. An unaddressable target is refused locally rather than sent
// to the server as an empty path segment.
func (t ClaimTarget) path() string {
	if t.TaskID != "" {
		return "/api/v1/tasks/" + url.PathEscape(t.TaskID) + "/claim"
	}
	if t.Owner != "" && t.Repo != "" {
		return "/api/v1/repos/" + url.PathEscape(t.Owner) + "/" + url.PathEscape(t.Repo) + "/claim"
	}
	return ""
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
	resp, err := c.do(ctx, http.MethodPost, target, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return nil, c.heldError(ctx, resp.Body)
	}
	if resp.StatusCode != http.StatusCreated {
		return nil, c.statusError(ctx, "acquire", target, resp)
	}
	return decodeClaim(ctx, resp.Body)
}

// Renew extends an existing lease.
func (c *vaultAPIClaimClient) Renew(
	ctx context.Context,
	target ClaimTarget,
	leaseToken string,
	ttlSeconds int,
) (*Claim, error) {
	body := claimRequestBody{LeaseToken: leaseToken, TTLSeconds: ttlSeconds}
	resp, err := c.doPath(ctx, http.MethodPost, target.renewPath(), target, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return nil, errors.Wrapf(ctx, ErrLeaseLost, "renew %s", target.describe())
	}
	if resp.StatusCode != http.StatusOK {
		return nil, c.statusError(ctx, "renew", target, resp)
	}
	return decodeClaim(ctx, resp.Body)
}

// Release drops the claim.
func (c *vaultAPIClaimClient) Release(
	ctx context.Context,
	target ClaimTarget,
	leaseToken string,
) error {
	body := claimRequestBody{LeaseToken: leaseToken}
	resp, err := c.do(ctx, http.MethodDelete, target, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return errors.Wrapf(ctx, ErrLeaseLost, "release %s", target.describe())
	}
	if resp.StatusCode != http.StatusNoContent {
		return c.statusError(ctx, "release", target, resp)
	}
	return nil
}

// Read returns the current claim.
func (c *vaultAPIClaimClient) Read(ctx context.Context, target ClaimTarget) (*Claim, error) {
	resp, err := c.do(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.Wrapf(ctx, ErrClaimFree, "read %s", target.describe())
	}
	if resp.StatusCode != http.StatusOK {
		return nil, c.statusError(ctx, "read", target, resp)
	}
	return decodeClaim(ctx, resp.Body)
}

// do issues one request. pathOverride is used by Renew, whose route differs
// from the target's own path.
func (c *vaultAPIClaimClient) do(
	ctx context.Context,
	method string,
	target ClaimTarget,
	body any,
) (*http.Response, error) {
	path := target.path()
	if path == "" {
		return nil, errors.Wrapf(ctx, ErrClaimTargetInvalid, "claim target %s", target.describe())
	}
	return c.doPath(ctx, method, path, target, body)
}

func (c *vaultAPIClaimClient) doPath(
	ctx context.Context,
	method string,
	path string,
	target ClaimTarget,
	body any,
) (*http.Response, error) {
	if path == "" {
		return nil, errors.Wrapf(ctx, ErrClaimTargetInvalid, "claim target %s", target.describe())
	}
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout.Duration())
	defer cancel()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, errors.Wrapf(ctx, err, "marshal %s body", target.describe())
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create %s request for %s", method, target.describe())
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c.setAuthHeaders(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "%s %s", method, target.describe())
	}
	glog.V(3).Infof(
		"vault-api claim %s %s status=%d", method, target.describe(), resp.StatusCode,
	)
	return resp, nil
}

// ErrClaimTargetInvalid is returned when a ClaimTarget names neither a task
// nor a repository. It is refused locally: sending an empty path segment would
// be a server-side 404 that reads like a missing task.
var ErrClaimTargetInvalid = stderrors.New("claim target invalid")

// renewPath returns the renew route, or "" when the target is not addressable.
// It must NOT be written as path() + "/renew": for an empty ClaimTarget path()
// is "", so that spelling yields the non-empty "/renew", the empty-path guard
// in doPath never fires, and the client would send a request to baseURL+"/renew"
// instead of refusing locally.
func (t ClaimTarget) renewPath() string {
	if p := t.path(); p != "" {
		return p + "/renew"
	}
	return ""
}

// describe renders the target for a log line or error, never as a path.
func (t ClaimTarget) describe() string {
	if t.TaskID != "" {
		return "task " + t.TaskID
	}
	if t.Owner != "" || t.Repo != "" {
		return "repo " + t.Owner + "/" + t.Repo
	}
	return "no target"
}

// heldError decodes a 409 claim_held body into a *ClaimHeldError.
func (c *vaultAPIClaimClient) heldError(
	ctx context.Context,
	body io.Reader,
) error {
	var parsed errorResponseBody
	if err := json.NewDecoder(body).Decode(&parsed); err != nil {
		return errors.Wrapf(ctx, ErrClaimHeld, "decode held body")
	}
	held := &ClaimHeldError{Holder: parsed.Holder}
	if parsed.LeaseUntil != "" {
		if until, err := libtime.ParseDateTime(ctx, parsed.LeaseUntil); err == nil {
			held.LeaseUntil = *until
		}
	}
	return held
}

// statusError builds an error naming the method, the target and the status.
// The body is not echoed: a server body can carry a holder or a token.
func (c *vaultAPIClaimClient) statusError(
	ctx context.Context,
	action string,
	target ClaimTarget,
	resp *http.Response,
) error {
	return errors.Wrapf(
		ctx,
		stderrors.New("unexpected status"),
		"%s %s answered %d",
		action,
		target.describe(),
		resp.StatusCode,
	)
}

// decodeClaim turns a claim response body into a Claim.
func decodeClaim(ctx context.Context, body io.Reader) (*Claim, error) {
	var parsed claimResponseBody
	if err := json.NewDecoder(body).Decode(&parsed); err != nil {
		return nil, errors.Wrapf(ctx, err, "decode claim response")
	}
	result := &Claim{
		Scope:      parsed.Scope,
		ID:         parsed.ID,
		Holder:     parsed.Holder,
		LeaseToken: parsed.LeaseToken,
	}
	if parsed.LeaseUntil != "" {
		if until, err := libtime.ParseDateTime(ctx, parsed.LeaseUntil); err == nil {
			result.LeaseUntil = *until
		} else {
			return nil, errors.Wrapf(ctx, err, "parse lease_until %q", parsed.LeaseUntil)
		}
	}
	if parsed.RenewBefore != "" {
		if before, err := libtime.ParseDateTime(ctx, parsed.RenewBefore); err == nil {
			result.RenewBefore = *before
		} else {
			return nil, errors.Wrapf(ctx, err, "parse renew_before %q", parsed.RenewBefore)
		}
	}
	return result, nil
}
