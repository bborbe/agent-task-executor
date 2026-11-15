// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"testing"
	"time"
)

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
