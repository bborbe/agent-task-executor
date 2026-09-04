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
	"github.com/golang/glog"
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
	baseURL    string
	httpClient *http.Client
	// gatewaySecret holds the git-rest gateway secret VALUE (read from a K8s
	// Secret at startup). It is never logged, never included in error messages,
	// and never passed to argument.Parse (this is not an application-struct
	// field) — it is used only to set the X-Gateway-Secret header.
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
	glog.V(3).Infof("git-rest GET path=%s status=%d", relPath, resp.StatusCode)
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
	glog.V(3).Infof("git-rest LIST glob=%s status=%d", glob, resp.StatusCode)
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10 MiB max
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "read LIST response body for glob %s", glob)
	}
	if resp.StatusCode != http.StatusOK {
		preview := string(body)
		if len(preview) > 100 {
			preview = preview[:100]
		}
		return nil, errors.Errorf(
			ctx,
			"LIST glob %s returned %d: %s",
			glob,
			resp.StatusCode,
			preview,
		)
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
	glog.V(3).Infof("git-rest READINESS status=%d", resp.StatusCode)
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
