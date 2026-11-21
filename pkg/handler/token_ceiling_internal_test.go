// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/prometheus/client_golang/prometheus/testutil"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// stubTokenSpendReader is an in-package double for claim.TokenSpendReader.
// claim's read interfaces deliberately carry no counterfeiter directive (their
// only consumer is this package), so the tests double them here.
type stubTokenSpendReader struct {
	forAssignee    []claim.TaskIndexItem
	forAssigneeErr error
	doc            *claim.TaskDocumentItem
	docErr         error
}

func (s *stubTokenSpendReader) ReadForAssignee(
	context.Context, string,
) ([]claim.TaskIndexItem, error) {
	return s.forAssignee, s.forAssigneeErr
}

func (s *stubTokenSpendReader) ReadDocument(
	context.Context, string,
) (*claim.TaskDocumentItem, error) {
	return s.doc, s.docErr
}

func TestSumRunTokensSince_SumsOnlyRunsInWindow(t *testing.T) {
	windowStart := time.Date(2026, 11, 15, 20, 0, 0, 0, time.UTC)
	fm := map[string]any{
		"metrics_runs": []any{
			map[string]any{
				"completed_at": "2026-11-15T21:00:00Z",
				"input_tokens": int64(100), "output_tokens": int64(20),
			},
			// Before the window: must not count.
			map[string]any{"completed_at": "2026-11-15T19:00:00Z", "input_tokens": int64(500)},
			map[string]any{
				"completed_at":      "2026-11-15T22:00:00Z",
				"cache_read_tokens": float64(30),
			},
			// Malformed entries are skipped, not fatal.
			"not-a-map",
			map[string]any{"completed_at": "not-a-time", "input_tokens": int64(999)},
		},
	}
	if got, want := sumRunTokensSince(fm, windowStart), int64(150); got != want {
		t.Fatalf("sumRunTokensSince = %d, want %d", got, want)
	}
}

func TestSumRunTokensSince_ToleratesQuotedStringCounts(t *testing.T) {
	windowStart := time.Date(2026, 11, 15, 20, 0, 0, 0, time.UTC)
	fm := map[string]any{
		"metrics_runs": []any{
			map[string]any{"completed_at": "2026-11-15T21:00:00Z", "input_tokens": "100"},
		},
	}
	if got := sumRunTokensSince(fm, windowStart); got != 100 {
		t.Fatalf("quoted-string sum = %d, want 100", got)
	}
}

func TestSumRunTokensSince_NoHistoryIsZero(t *testing.T) {
	if got := sumRunTokensSince(map[string]any{}, time.Now()); got != 0 {
		t.Fatalf("empty frontmatter = %d, want 0", got)
	}
}

// mustParseDateTime parses a fixed instant without Gomega. The bborbe time
// test helper calls Expect, and these in-package tests run under plain testing.T
// with no Gomega fail handler registered.
func mustParseDateTime(t *testing.T, value string) libtime.DateTime {
	t.Helper()
	parsed, err := libtime.ParseDateTime(context.Background(), value)
	if err != nil {
		t.Fatalf("parse datetime %q: %v", value, err)
	}
	return *parsed
}

// tokenCeilingReadErrorCount reads the fail-open counter.
func tokenCeilingReadErrorCount() float64 {
	return testutil.ToFloat64(
		metrics.TaskEventsTotal.WithLabelValues("token_ceiling_read_error"),
	)
}

func TestDeferIfOverTokenCeiling_FailsOpenOnAssigneeReadError(t *testing.T) {
	now := libtime.NewCurrentDateTime()
	now.SetNow(mustParseDateTime(t, "2026-11-16T10:00:00Z")) // Monday
	h := &taskEventHandler{
		currentDateTime: now,
		tokenSpendReader: &stubTokenSpendReader{
			forAssigneeErr: stderrors.New("dial tcp: connection refused"),
		},
		deferredRespawns: map[lib.TaskIdentifier]deferredEntry{},
	}
	ceiling := int64(100)
	config := &pkg.AgentConfiguration{
		Assignee:     "claude",
		TokenCeiling: &ceiling,
		RunWindow: &agentv1.RunWindow{
			Timezone:     "UTC",
			WeekdayStart: "00:00",
			WeekdayEnd:   "23:59",
		},
	}
	before := tokenCeilingReadErrorCount()

	over, err := h.deferIfOverTokenCeiling(
		context.Background(),
		lib.Task{TaskIdentifier: "tid-read-error"},
		config,
	)

	if err != nil {
		t.Fatalf("deferIfOverTokenCeiling error = %v, want nil (fail open)", err)
	}
	if over {
		t.Fatal("deferIfOverTokenCeiling over = true, want false (fail open)")
	}
	if _, parked := h.deferredRespawns[lib.TaskIdentifier("tid-read-error")]; parked {
		t.Fatal("task was parked in deferredRespawns, want fail open")
	}
	if got := tokenCeilingReadErrorCount() - before; got != 1 {
		t.Fatalf("token_ceiling_read_error delta = %v, want 1", got)
	}
}

func TestDeferIfOverTokenCeiling_FailsOpenOnDocumentReadError(t *testing.T) {
	now := libtime.NewCurrentDateTime()
	now.SetNow(mustParseDateTime(t, "2026-11-16T10:00:00Z")) // Monday
	h := &taskEventHandler{
		currentDateTime: now,
		tokenSpendReader: &stubTokenSpendReader{
			forAssignee: []claim.TaskIndexItem{{ID: "tid-1"}},
			docErr:      stderrors.New("dial tcp: connection refused"),
		},
		deferredRespawns: map[lib.TaskIdentifier]deferredEntry{},
	}
	ceiling := int64(100)
	config := &pkg.AgentConfiguration{
		Assignee:     "claude",
		TokenCeiling: &ceiling,
		RunWindow: &agentv1.RunWindow{
			Timezone:     "UTC",
			WeekdayStart: "00:00",
			WeekdayEnd:   "23:59",
		},
	}
	before := tokenCeilingReadErrorCount()

	over, err := h.deferIfOverTokenCeiling(
		context.Background(),
		lib.Task{TaskIdentifier: "tid-doc-read-error"},
		config,
	)

	if err != nil {
		t.Fatalf("deferIfOverTokenCeiling error = %v, want nil (fail open)", err)
	}
	if over {
		t.Fatal("deferIfOverTokenCeiling over = true, want false (fail open)")
	}
	if _, parked := h.deferredRespawns[lib.TaskIdentifier("tid-doc-read-error")]; parked {
		t.Fatal("task was parked in deferredRespawns, want fail open")
	}
	if got := tokenCeilingReadErrorCount() - before; got != 1 {
		t.Fatalf("token_ceiling_read_error delta = %v, want 1", got)
	}
}

func TestTokenSpendSince_WrapsReadErrorAsVaultReadError(t *testing.T) {
	ctx := context.Background()

	assigneeReader := &taskEventHandler{
		tokenSpendReader: &stubTokenSpendReader{
			forAssigneeErr: stderrors.New("boom"),
		},
	}
	_, err := assigneeReader.tokenSpendSince(ctx, "claude", time.Now())
	if err == nil {
		t.Fatal("tokenSpendSince assignee error = nil, want error")
	}
	var assigneeVaultErr *vaultReadError
	if !stderrors.As(err, &assigneeVaultErr) {
		t.Fatalf("tokenSpendSince assignee error = %v, want *vaultReadError", err)
	}

	documentReader := &taskEventHandler{
		tokenSpendReader: &stubTokenSpendReader{
			forAssignee: []claim.TaskIndexItem{{ID: "tid-1"}},
			docErr:      stderrors.New("boom"),
		},
	}
	_, err = documentReader.tokenSpendSince(ctx, "claude", time.Now())
	if err == nil {
		t.Fatal("tokenSpendSince document error = nil, want error")
	}
	var documentVaultErr *vaultReadError
	if !stderrors.As(err, &documentVaultErr) {
		t.Fatalf("tokenSpendSince document error = %v, want *vaultReadError", err)
	}
}
