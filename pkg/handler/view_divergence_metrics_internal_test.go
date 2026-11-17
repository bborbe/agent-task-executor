// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"testing"

	lib "github.com/bborbe/agent"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// stubVaultIndexClient satisfies claim.VaultAPIIndexClient without importing
// the mocks package, which imports pkg/handler and so cannot be used from an
// internal test of it.
type stubVaultIndexClient struct {
	index *claim.TaskIndex
	err   error
}

func (s *stubVaultIndexClient) Read(context.Context) (*claim.TaskIndex, error) {
	return s.index, s.err
}

// viewDivergenceMetricCase is one input to TestCheckViewDivergenceMetricUnits.
// The deltas are the contract: `ok` is per successful read, while `mismatch`
// and `absent` are per task id.
type viewDivergenceMetricCase struct {
	name         string
	owned        []lib.Task
	index        *claim.TaskIndex
	wantOK       float64
	wantMismatch float64
	wantAbsent   float64
}

func viewDivergenceMetricTask(id, status, phase string) lib.Task {
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier(id),
		Frontmatter: lib.TaskFrontmatter{
			"status": status,
			"phase":  phase,
		},
	}
}

// viewDivergenceMetricCases is the table, held outside the test function so
// its length stays within the lint budget.
var viewDivergenceMetricCases = []viewDivergenceMetricCase{
	{
		name: "one divergent and one absent id among three owned",
		owned: []lib.Task{
			viewDivergenceMetricTask("id-match", "in_progress", "execution"),
			viewDivergenceMetricTask("id-mismatch", "in_progress", "execution"),
			viewDivergenceMetricTask("id-absent", "in_progress", "execution"),
		},
		index: &claim.TaskIndex{Items: []claim.TaskIndexItem{
			{ID: "id-match", Status: "in_progress", Phase: "execution"},
			{ID: "id-mismatch", Status: "completed", Phase: "done"},
		}},
		wantOK:       1,
		wantMismatch: 1,
		wantAbsent:   1,
	},
	{
		name: "all three agreeing still counts one successful read",
		owned: []lib.Task{
			viewDivergenceMetricTask("id-a", "in_progress", "execution"),
			viewDivergenceMetricTask("id-b", "next", "planning"),
			viewDivergenceMetricTask("id-c", "completed", "done"),
		},
		index: &claim.TaskIndex{Items: []claim.TaskIndexItem{
			{ID: "id-a", Status: "in_progress", Phase: "execution"},
			{ID: "id-b", Status: "next", Phase: "planning"},
			{ID: "id-c", Status: "completed", Phase: "done"},
		}},
		wantOK: 1,
	},
}

// TestCheckViewDivergenceMetricUnits pins the increment unit: `ok` once per
// successful read regardless of findings, `mismatch` and `absent` once per id.
// Read errors and the nil-client skip are covered by the integration file.
func TestCheckViewDivergenceMetricUnits(t *testing.T) {
	for _, tc := range viewDivergenceMetricCases {
		t.Run(tc.name, func(t *testing.T) {
			before := viewDivergenceCounts()
			h := &taskEventHandler{
				vaultIndexClient: &stubVaultIndexClient{index: tc.index},
			}

			h.checkViewDivergence(context.Background(), tc.owned)

			after := viewDivergenceCounts()
			assertDelta(t, "ok", after["ok"]-before["ok"], tc.wantOK)
			assertDelta(t, "mismatch", after["mismatch"]-before["mismatch"], tc.wantMismatch)
			assertDelta(t, "absent", after["absent"]-before["absent"], tc.wantAbsent)
			assertDelta(t, "read_error", after["read_error"]-before["read_error"], 0)
		})
	}
}

// viewDivergenceCounts reads the child series rather than the bare collector:
// the vec is pre-initialised with more than one metric, which panics ToFloat64.
func viewDivergenceCounts() map[string]float64 {
	return map[string]float64{
		"ok":         testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("ok")),
		"mismatch":   testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("mismatch")),
		"absent":     testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("absent")),
		"read_error": testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("read_error")),
	}
}

func assertDelta(t *testing.T, result string, got, want float64) {
	t.Helper()
	if got != want {
		t.Fatalf("%s delta = %v, want %v", result, got, want)
	}
}
