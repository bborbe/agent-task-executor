// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/bborbe/errors"
)

// maxIndexResponseBytes caps a task-index response body read. It is larger than
// the claim client's maxResponseBytes on purpose: the index carries every task
// in the vault (9,002 files on the personal vault), so the 10 MiB claim cap
// would reject a legitimate response.
const maxIndexResponseBytes = 32 * 1024 * 1024

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
//
// No //counterfeiter:generate directive, deliberately. Its only consumer is
// pkg/handler, whose tests double it with an in-package stub — and a generated
// fake would live in mocks/, which imports pkg/handler, so an internal test
// cannot use it. A generated mock here would be dead surface.
type VaultAPIIndexClient interface {
	Read(ctx context.Context) (*TaskIndex, error)
}

// NewVaultAPIIndexClient creates a VaultAPIIndexClient for the vault API at
// baseURL. gatewaySecret empty means no auth headers are sent. The secret is
// held in memory only and must never appear in a log line or an error string.
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

// taskIndexResponseBody is the wire form of the index response. It is a
// separate struct from TaskIndex so key presence is observable: a pointer or a
// nil RawMessage distinguishes "absent" from "present and zero", which is what
// lets a dropped required field fail loudly instead of reading as an empty
// index.
type taskIndexResponseBody struct {
	Items           json.RawMessage `json:"items"`
	Total           *int            `json:"total"`
	Warnings        *int            `json:"warnings"`
	IndexAgeSeconds *int            `json:"index_age_seconds"`
}

// Read fetches the whole task index in one call.
//
// The request is deliberately unfiltered: a cluster= filter omits files with no
// cluster frontmatter, which manufactures absent divergences for exactly the
// tasks most likely to be stale.
func (c *vaultAPIIndexClient) Read(ctx context.Context) (*TaskIndex, error) {
	if c.baseURL == "" {
		return nil, errors.Errorf(ctx, "vault-api task index: base URL is empty")
	}
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout.Duration())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/tasks", nil)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create vault-api task index request")
	}
	setVaultAPIAuthHeaders(req, c.gatewaySecret, c.gatewayInitiator)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "read vault-api task index")
	}
	defer resp.Body.Close()

	// LimitReader at cap+1 so an oversized body is detected rather than
	// silently truncated into a parse error that names the wrong cause.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxIndexResponseBytes+1))
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "read vault-api task index response")
	}
	if len(raw) > maxIndexResponseBytes {
		return nil, errors.Errorf(
			ctx,
			"vault-api task index response exceeded %d bytes",
			maxIndexResponseBytes,
		)
	}
	if resp.StatusCode != http.StatusOK {
		// The body is not echoed: a server body can carry a credential or a
		// holder.
		return nil, errors.Errorf(ctx, "vault-api task index answered %d", resp.StatusCode)
	}
	return decodeTaskIndex(ctx, raw)
}

// decodeTaskIndex parses the index envelope.
//
// items and index_age_seconds are required: a missing items key or a renamed
// field is schema drift, and returning an empty index for it would read as
// "compared=0", the one conclusion the divergence check must never draw by
// accident. items: null is different — a Go server marshalling a nil slice
// writes null, and that is a legitimate empty index. total and warnings are
// optional and default to 0, so a server that omits a zero value does not turn
// every healthy pass into a read error.
func decodeTaskIndex(ctx context.Context, raw []byte) (*TaskIndex, error) {
	var parsed taskIndexResponseBody
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, errors.Wrapf(ctx, err, "decode vault-api task index response")
	}
	if parsed.Items == nil {
		return nil, errors.Errorf(ctx, "decode vault-api task index response: missing items")
	}
	items := []TaskIndexItem{}
	if string(parsed.Items) != "null" {
		if err := json.Unmarshal(parsed.Items, &items); err != nil {
			return nil, errors.Wrapf(ctx, err, "decode vault-api task index items")
		}
	}
	if parsed.IndexAgeSeconds == nil {
		return nil, errors.Errorf(
			ctx,
			"decode vault-api task index response: missing index_age_seconds",
		)
	}
	total := 0
	if parsed.Total != nil {
		total = *parsed.Total
	}
	warnings := 0
	if parsed.Warnings != nil {
		warnings = *parsed.Warnings
	}
	return &TaskIndex{
		Items:           items,
		Total:           total,
		Warnings:        warnings,
		IndexAgeSeconds: *parsed.IndexAgeSeconds,
	}, nil
}
