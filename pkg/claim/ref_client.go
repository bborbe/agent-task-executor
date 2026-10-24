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

// RequestTimeout is the hard per-request cap for the write calls (Put, Delete).
// A write makes git-rest push a ref, so it keeps the full budget.
const RequestTimeout = 30 * libtime.Second

// ReadTimeout is the per-request cap for GET /api/v1/refs. Reads are bounded
// below RequestTimeout because git-rest answers a slow read with a 500 only
// after its own `git ls-remote` has already outlasted the caller's cap — so
// waiting the full RequestTimeout buys nothing and stalls the claim or redrive
// loop for 30 s on every failed read. A read that cannot be completed inside
// this bound returns ErrRefsUnavailable.
const ReadTimeout = 10 * libtime.Second

// ErrLeaseConflict is returned when git-rest answers 409 on a PUT or DELETE.
// It is final: the claim client never retries.
var ErrLeaseConflict = stderrors.New("lease conflict")

// ErrRefsUnavailable is returned when a read could not be completed — a
// transport failure, ReadTimeout expiring, or a 5xx. It is NOT an empty
// result: a caller must defer the task and never treat it as "no refs exist",
// which would let two clusters take the same claim.
var ErrRefsUnavailable = stderrors.New("refs unavailable")

// ErrRefNotFound is returned when git-rest answers 404 on a DELETE. It is
// final: the ref is already gone.
var ErrRefNotFound = stderrors.New("ref not found")

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
	Put(
		ctx context.Context,
		key string,
		expectedSha string,
		cluster string,
		taskID string,
		leaseUntil libtime.DateTime,
	) (string, error)
	// Delete removes the claim at key by compare-and-swap. A 409 is
	// ErrLeaseConflict.
	Delete(ctx context.Context, key string, expectedSha string) error
}

// NewClaimRefClient creates a ClaimRefClient for the git-rest instance at
// baseURL. gatewaySecret empty means no auth headers are sent. The secret is
// held in memory only and must never appear in a log line or an error string.
func NewClaimRefClient(baseURL, gatewaySecret, gatewayInitiator string) ClaimRefClient {
	return &claimRefClient{
		baseURL:          strings.TrimRight(baseURL, "/"),
		httpClient:       &http.Client{Timeout: 30 * time.Second},
		gatewaySecret:    gatewaySecret,
		gatewayInitiator: gatewayInitiator,
	}
}

type claimRefClient struct {
	baseURL          string
	httpClient       *http.Client
	gatewaySecret    string
	gatewayInitiator string
}

// putRefRequest is the PUT /api/v1/refs/<key> body. Every field is always
// serialised: an empty expected_sha means "must not exist".
type putRefRequest struct {
	ExpectedSha string `json:"expected_sha"`
	Cluster     string `json:"cluster"`
	TaskID      string `json:"task_id"`
	LeaseUntil  string `json:"lease_until"`
}

// putRefResponse is the 200 response body of PUT /api/v1/refs/<key>.
type putRefResponse struct {
	Ref string `json:"ref"`
	Sha string `json:"sha"`
}

// setAuthHeaders sets the gateway-secret auth headers on req when the secret
// is configured. Header names are part of the git-rest public contract — do
// NOT alter (X-Gateway-Initator is a deliberate misspelling).
func (c *claimRefClient) setAuthHeaders(req *http.Request) {
	if c.gatewaySecret == "" {
		return
	}
	req.Header.Set("X-Gateway-Secret", c.gatewaySecret)
	req.Header.Set("X-Gateway-Initator", c.gatewayInitiator)
}

// escapedKey escapes every path segment of key but keeps the "/" separators.
func escapedKey(key string) string {
	segments := strings.Split(key, "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return strings.Join(segments, "/")
}

// formatLeaseUntil renders a lease deadline as RFC3339 in UTC without
// fractional seconds. This is the single formatting implementation shared by
// the writer and the read-back comparison.
func formatLeaseUntil(leaseUntil libtime.DateTime) string {
	return leaseUntil.Time().UTC().Format(time.RFC3339)
}

// refsUnavailable reports one failed read at the default verbosity — the line
// SC2 measures — and returns ErrRefsUnavailable without the transport cause in
// the message. The cause goes to V(2), so a default-verbosity log carries no raw
// deadline text and callers classify on the sentinel rather than on a string.
func refsUnavailable(ctx context.Context, prefix string, start time.Time, cause error) error {
	glog.V(0).Infof(
		"event=refs_unavailable op=list prefix=%s task=%s elapsed_ms=%d bound_ms=%d",
		prefix,
		taskIDFromPrefix(prefix),
		time.Since(start).Milliseconds(),
		ReadTimeout.Duration().Milliseconds(),
	)
	glog.V(2).Infof("event=refs_unavailable_cause prefix=%s cause=%v", prefix, cause)
	return errors.Wrapf(
		ctx,
		ErrRefsUnavailable,
		"GET refs prefix %s exceeded the %s read bound",
		prefix,
		ReadTimeout,
	)
}

// taskIDFromPrefix returns the task id carried by a redrive-streak prefix
// ("claims/redrive/<taskID>"), or "" for any other prefix. Claim keys carry a
// task id only in the "claims/task/<id>" form, which is not guessed at here.
func taskIDFromPrefix(prefix string) string {
	trimmed, ok := strings.CutPrefix(prefix, RedriveStreakPrefix)
	if !ok {
		return ""
	}
	return trimmed
}

// List returns every claim ref under prefix. Reads fail-fast; no retry. A read
// that cannot be completed — transport failure, ReadTimeout, or a 5xx — returns
// ErrRefsUnavailable and never an empty slice.
func (c *claimRefClient) List(ctx context.Context, prefix string) ([]ClaimRef, error) {
	// Elapsed is a transport measurement, not domain time, so it is taken from
	// the monotonic clock rather than an injected CurrentDateTimeGetter.
	start := time.Now()
	// parent is kept so a caller's own cancellation or deadline still surfaces
	// as context.DeadlineExceeded / context.Canceled — only this read's bound
	// means "refs unavailable".
	parent := ctx
	ctx, cancel := context.WithTimeout(parent, ReadTimeout.Duration())
	defer cancel()
	q := url.Values{}
	q.Set("prefix", prefix)
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.baseURL+"/api/v1/refs?"+q.Encode(),
		nil,
	)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create GET refs request for prefix %s", prefix)
	}
	c.setAuthHeaders(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		if parentErr := parent.Err(); parentErr != nil {
			return nil, errors.Wrapf(ctx, parentErr, "GET refs prefix %s", prefix)
		}
		return nil, refsUnavailable(ctx, prefix, start, err)
	}
	defer resp.Body.Close()
	glog.V(3).Infof("git-rest refs GET prefix=%s status=%d", prefix, resp.StatusCode)
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		if parentErr := parent.Err(); parentErr != nil {
			return nil, errors.Wrapf(ctx, parentErr, "GET refs prefix %s", prefix)
		}
		return nil, refsUnavailable(ctx, prefix, start, err)
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode >= http.StatusInternalServerError {
			return nil, refsUnavailable(ctx, prefix, start, stderrors.New(resp.Status))
		}
		return nil, errors.Errorf(ctx, "GET refs prefix %s returned %d", prefix, resp.StatusCode)
	}
	var refs []ClaimRef
	if err := json.Unmarshal(body, &refs); err != nil {
		return nil, errors.Wrapf(
			ctx,
			err,
			"parse GET refs response for prefix %s (status 200)",
			prefix,
		)
	}
	return refs, nil
}

// Put creates or replaces the claim at key by compare-and-swap. A 409 is
// ErrLeaseConflict and is final — no retry.
func (c *claimRefClient) Put(
	ctx context.Context,
	key string,
	expectedSha string,
	cluster string,
	taskID string,
	leaseUntil libtime.DateTime,
) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout.Duration())
	defer cancel()
	payload, err := json.Marshal(putRefRequest{
		ExpectedSha: expectedSha,
		Cluster:     cluster,
		TaskID:      taskID,
		LeaseUntil:  formatLeaseUntil(leaseUntil),
	})
	if err != nil {
		return "", errors.Wrapf(ctx, err, "marshal PUT refs body for %s", key)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		c.baseURL+"/api/v1/refs/"+escapedKey(key),
		bytes.NewReader(payload),
	)
	if err != nil {
		return "", errors.Wrapf(ctx, err, "create PUT refs request for %s", key)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuthHeaders(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", errors.Wrapf(ctx, err, "PUT refs %s", key)
	}
	defer resp.Body.Close()
	glog.V(3).Infof("git-rest refs PUT key=%s status=%d", key, resp.StatusCode)
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return "", errors.Wrapf(ctx, err, "read PUT refs response body for %s", key)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		var result putRefResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return "", errors.Wrapf(
				ctx,
				err,
				"parse PUT refs response for %s (status 200)",
				key,
			)
		}
		if result.Sha == "" {
			return "", errors.Errorf(ctx, "PUT refs %s returned 200 without sha", key)
		}
		return result.Sha, nil
	case http.StatusConflict:
		return "", errors.Wrapf(ctx, ErrLeaseConflict, "PUT refs %s returned 409", key)
	default:
		return "", errors.Errorf(ctx, "PUT refs %s returned %d", key, resp.StatusCode)
	}
}

// Delete removes the claim at key by compare-and-swap. A 409 is
// ErrLeaseConflict and is final — no retry.
func (c *claimRefClient) Delete(ctx context.Context, key string, expectedSha string) error {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout.Duration())
	defer cancel()
	q := url.Values{}
	q.Set("expected_sha", expectedSha)
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		c.baseURL+"/api/v1/refs/"+escapedKey(key)+"?"+q.Encode(),
		nil,
	)
	if err != nil {
		return errors.Wrapf(ctx, err, "create DELETE refs request for %s", key)
	}
	c.setAuthHeaders(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrapf(ctx, err, "DELETE refs %s", key)
	}
	defer resp.Body.Close()
	glog.V(3).Infof("git-rest refs DELETE key=%s status=%d", key, resp.StatusCode)
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 10*1024*1024))
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusConflict:
		return errors.Wrapf(ctx, ErrLeaseConflict, "DELETE refs %s returned 409", key)
	case http.StatusNotFound:
		return errors.Wrapf(ctx, ErrRefNotFound, "DELETE refs %s returned 404", key)
	default:
		return errors.Errorf(ctx, "DELETE refs %s returned %d", key, resp.StatusCode)
	}
}
