// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// tokenSpendStubReader is a minimal claim.TokenSpendReader over fixed fixtures.
type tokenSpendStubReader struct {
	items []claim.TaskIndexItem
	docs  map[string]*claim.TaskDocumentItem
}

func (r tokenSpendStubReader) ReadForAssignee(
	context.Context,
	string,
) ([]claim.TaskIndexItem, error) {
	return r.items, nil
}

func (r tokenSpendStubReader) ReadDocument(
	_ context.Context,
	id string,
) (*claim.TaskDocumentItem, error) {
	return r.docs[id], nil
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

func TestSumRunTokensSinceWithCount_CountsOnlyInWindowEntries(t *testing.T) {
	windowStart := time.Date(2026, 11, 15, 20, 0, 0, 0, time.UTC)
	fm := map[string]any{
		"metrics_runs": []any{
			map[string]any{"completed_at": "2026-11-15T21:00:00Z", "input_tokens": int64(100)},
			map[string]any{"completed_at": "2026-11-15T19:00:00Z", "input_tokens": int64(500)},
			map[string]any{
				"completed_at":      "2026-11-15T22:00:00Z",
				"cache_read_tokens": float64(30),
			},
			"not-a-map",
			map[string]any{"completed_at": "not-a-time", "input_tokens": int64(999)},
		},
	}
	total, counted := sumRunTokensSinceWithCount(fm, windowStart)
	if total != 130 || counted != 2 {
		t.Fatalf("sum/count = %d/%d, want 130/2", total, counted)
	}
}

func TestDescribeFirstRun_ReportsEveryShape(t *testing.T) {
	tests := []struct {
		name        string
		frontmatter map[string]any
		want        runDiagnostics
	}{
		{
			name:        "absent list",
			frontmatter: map[string]any{},
			want: runDiagnostics{
				listType: "<absent>", completedAt: "<absent>",
				tokenKeys: "<absent>", tokenValues: "<absent>",
			},
		},
		{
			name:        "empty list",
			frontmatter: map[string]any{"metrics_runs": []any{}},
			want: runDiagnostics{
				listType: "[]interface {}", completedAt: "<no-entries>",
				tokenKeys: "<no-entries>", tokenValues: "<no-entries>",
			},
		},
		{
			name:        "wrong list type",
			frontmatter: map[string]any{"metrics_runs": "oops"},
			want: runDiagnostics{
				listType: "string", completedAt: "<no-entries>",
				tokenKeys: "<no-entries>", tokenValues: "<no-entries>",
			},
		},
		{
			name:        "non-map entry",
			frontmatter: map[string]any{"metrics_runs": []any{"raw"}},
			want: runDiagnostics{
				listType: "[]interface {}", completedAt: "<not-a-map:string>",
				tokenKeys: "<not-a-map>", tokenValues: "<not-a-map>",
			},
		},
		{
			name: "unparseable timestamp",
			frontmatter: map[string]any{"metrics_runs": []any{
				map[string]any{"completed_at": "yesterday"},
			}},
			want: runDiagnostics{
				listType: "[]interface {}", completedAt: "<unparsed:string:yesterday>",
				tokenKeys:   "input_tokens=<absent>,output_tokens=<absent>,cache_read_tokens=<absent>,cache_creation_tokens=<absent>",
				tokenValues: "input_tokens=<absent>,output_tokens=<absent>,cache_read_tokens=<absent>,cache_creation_tokens=<absent>",
			},
		},
		{
			name: "mixed keys and values",
			frontmatter: map[string]any{"metrics_runs": []any{
				map[string]any{
					"completed_at":      "2026-11-15T21:00:00Z",
					"input_tokens":      int64(16),
					"output_tokens":     "346",
					"cache_read_tokens": float64(10624),
				},
			}},
			want: runDiagnostics{
				listType:    "[]interface {}",
				completedAt: "2026-11-15T21:00:00Z",
				tokenKeys:   "input_tokens=int64,output_tokens=string,cache_read_tokens=float64,cache_creation_tokens=<absent>",
				tokenValues: "input_tokens=16,output_tokens=346,cache_read_tokens=10624,cache_creation_tokens=<absent>",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := describeFirstRun(tt.frontmatter); got != tt.want {
				t.Fatalf("describeFirstRun = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestTokenSpendSince_EmptyPopulationReadsAbsent pins branch (a): vault-api
// returns no tasks for the assignee. The population must read as an explicit zero
// with `<absent>` shape fields, not as a blank line that looks like a missing
// value, because "nothing to sum" and "summed and got nothing" need different
// fixes.
func TestTokenSpendSince_EmptyPopulationReadsAbsent(t *testing.T) {
	h := &taskEventHandler{tokenSpendReader: tokenSpendStubReader{}}
	since := time.Date(2026, 11, 15, 20, 0, 0, 0, time.UTC)
	var spent int64
	out := captureRefusalLog(t, func() {
		var err error
		spent, err = h.tokenSpendSince(context.Background(), "proto-agent", since)
		if err != nil {
			t.Fatalf("tokenSpendSince: %v", err)
		}
	})
	if spent != 0 {
		t.Fatalf("spent = %d, want 0", spent)
	}
	for _, want := range []string{
		"items=0",
		"ids=[]",
		"runs_after_since=0",
		"first_metrics_runs_type=<absent>",
		"first_completed_at=<absent>",
		"first_token_keys=<absent>",
		"first_token_values=<absent>",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("diagnostic missing %q\n%s", want, out)
		}
	}
}

// TestTokenSpendSince_LogsNineValueDiagnostic pins the V(2) line that names which
// branch produced a zero: the population, the window boundary, the post-filter
// count, and the first document's metrics_runs shape. A change that drops one of
// these values re-opens the ambiguity the line exists to close.
func TestTokenSpendSince_LogsNineValueDiagnostic(t *testing.T) {
	since := time.Date(2026, 11, 15, 20, 0, 0, 0, time.UTC)
	reader := tokenSpendStubReader{
		items: []claim.TaskIndexItem{{ID: "id-a"}, {ID: "id-b"}},
		docs: map[string]*claim.TaskDocumentItem{
			"id-a": {ID: "id-a", Frontmatter: map[string]any{
				"metrics_runs": []any{
					map[string]any{
						"completed_at":  "2026-11-15T21:00:00Z",
						"input_tokens":  int64(100),
						"output_tokens": "20",
					},
					map[string]any{
						"completed_at": "2026-11-15T19:00:00Z",
						"input_tokens": int64(500),
					},
				},
			}},
			"id-b": {ID: "id-b", Frontmatter: map[string]any{
				"metrics_runs": []any{
					map[string]any{
						"completed_at":      "2026-11-15T22:00:00Z",
						"cache_read_tokens": float64(30),
					},
				},
			}},
		},
	}
	h := &taskEventHandler{tokenSpendReader: reader}
	var spent int64
	out := captureRefusalLog(t, func() {
		var err error
		spent, err = h.tokenSpendSince(context.Background(), "proto-agent", since)
		if err != nil {
			t.Fatalf("tokenSpendSince: %v", err)
		}
	})
	if spent != 150 {
		t.Fatalf("spent = %d, want 150", spent)
	}
	for _, want := range []string{
		"event=token_spend_diagnostic",
		"assignee=proto-agent",
		"spent=150",
		"items=2",
		"ids=[id-a id-b]",
		"runs_after_since=2",
		"first_metrics_runs_type=[]interface {}",
		"first_completed_at=2026-11-15T21:00:00Z",
		"first_token_keys=input_tokens=int64,output_tokens=string,cache_read_tokens=<absent>,cache_creation_tokens=<absent>",
		"first_token_values=input_tokens=100,output_tokens=20,cache_read_tokens=<absent>,cache_creation_tokens=<absent>",
		"since=2026-11-15T20:00:00Z",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("diagnostic missing %q\n%s", want, out)
		}
	}
}
