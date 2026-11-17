// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite. The
// checkViewDivergence boundary (the Read call, the nil-client skip and the log
// events) is covered end to end by the httptest suite, so this file covers only
// the pure comparison.

import (
	"testing"

	lib "github.com/bborbe/agent"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

func viewDivergenceTask(id, status, phase string) lib.Task {
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier(id),
		Frontmatter: lib.TaskFrontmatter{
			"status": status,
			"phase":  phase,
		},
	}
}

type viewDivergenceTestCase struct {
	name          string
	owned         []lib.Task
	index         *claim.TaskIndex
	wantCompared  int
	wantDivergent int
	wantAbsent    int
	wantFindings  []viewDivergenceFinding
}

// viewDivergenceTestCases is package-level rather than a literal inside the
// test so the test function stays under funlen's bound.
var viewDivergenceTestCases = []viewDivergenceTestCase{
	{
		name: "status-only difference on one of three owned ids",
		owned: []lib.Task{
			viewDivergenceTask("t1", "in_progress", "execution"),
			viewDivergenceTask("t2", "next", "planning"),
			viewDivergenceTask("t3", "completed", "done"),
		},
		index: &claim.TaskIndex{Items: []claim.TaskIndexItem{
			{ID: "t1", Status: "completed", Phase: "execution"},
			{ID: "t2", Status: "next", Phase: "planning"},
			{ID: "t3", Status: "completed", Phase: "done"},
		}},
		wantCompared:  3,
		wantDivergent: 1,
		wantFindings: []viewDivergenceFinding{
			{
				ID:          "t1",
				ViewStatus:  "in_progress",
				VaultStatus: "completed",
				ViewPhase:   "execution",
				VaultPhase:  "execution",
			},
		},
	},
	{
		name: "phase-only difference",
		owned: []lib.Task{
			viewDivergenceTask("t1", "in_progress", "planning"),
		},
		index: &claim.TaskIndex{Items: []claim.TaskIndexItem{
			{ID: "t1", Status: "in_progress", Phase: "execution"},
		}},
		wantCompared:  1,
		wantDivergent: 1,
		wantFindings: []viewDivergenceFinding{
			{
				ID:          "t1",
				ViewStatus:  "in_progress",
				VaultStatus: "in_progress",
				ViewPhase:   "planning",
				VaultPhase:  "execution",
			},
		},
	},
	{
		name: "both match",
		owned: []lib.Task{
			viewDivergenceTask("t1", "in_progress", "execution"),
		},
		index: &claim.TaskIndex{Items: []claim.TaskIndexItem{
			{ID: "t1", Status: "in_progress", Phase: "execution"},
		}},
		wantCompared: 1,
	},
	{
		name: "legacy view alias against a canonical index value",
		owned: []lib.Task{
			viewDivergenceTask("t1", "todo", "planning"),
		},
		index: &claim.TaskIndex{Items: []claim.TaskIndexItem{
			{ID: "t1", Status: "next", Phase: "planning"},
		}},
		wantCompared: 1,
	},
	{
		name: "both fields differ",
		owned: []lib.Task{
			viewDivergenceTask("t1", "in_progress", "planning"),
		},
		index: &claim.TaskIndex{Items: []claim.TaskIndexItem{
			{ID: "t1", Status: "completed", Phase: "done"},
		}},
		wantCompared:  1,
		wantDivergent: 1,
		wantFindings: []viewDivergenceFinding{
			{
				ID:          "t1",
				ViewStatus:  "in_progress",
				VaultStatus: "completed",
				ViewPhase:   "planning",
				VaultPhase:  "done",
			},
		},
	},
	{
		name: "one owned id missing from the index",
		owned: []lib.Task{
			viewDivergenceTask("t1", "in_progress", "execution"),
			viewDivergenceTask("t2", "next", "planning"),
			viewDivergenceTask("t3", "completed", "done"),
		},
		index: &claim.TaskIndex{Items: []claim.TaskIndexItem{
			{ID: "t1", Status: "in_progress", Phase: "execution"},
			{ID: "t2", Status: "next", Phase: "planning"},
		}},
		wantCompared: 2,
		wantAbsent:   1,
		wantFindings: []viewDivergenceFinding{
			{
				ID:         "t3",
				Absent:     true,
				ViewStatus: "completed",
				ViewPhase:  "done",
			},
		},
	},
	{
		name: "empty index",
		owned: []lib.Task{
			viewDivergenceTask("t1", "in_progress", "execution"),
			viewDivergenceTask("t2", "next", "planning"),
			viewDivergenceTask("t3", "completed", "done"),
		},
		index:      &claim.TaskIndex{},
		wantAbsent: 3,
		wantFindings: []viewDivergenceFinding{
			{ID: "t1", Absent: true, ViewStatus: "in_progress", ViewPhase: "execution"},
			{ID: "t2", Absent: true, ViewStatus: "next", ViewPhase: "planning"},
			{ID: "t3", Absent: true, ViewStatus: "completed", ViewPhase: "done"},
		},
	},
	{
		name: "findings are ordered by id",
		owned: []lib.Task{
			viewDivergenceTask("z9", "in_progress", "execution"),
			viewDivergenceTask("a1", "in_progress", "execution"),
			viewDivergenceTask("m5", "in_progress", "execution"),
		},
		index: &claim.TaskIndex{Items: []claim.TaskIndexItem{
			{ID: "z9", Status: "completed", Phase: "done"},
			{ID: "m5", Status: "completed", Phase: "done"},
		}},
		wantCompared:  2,
		wantDivergent: 2,
		wantAbsent:    1,
		wantFindings: []viewDivergenceFinding{
			{ID: "a1", Absent: true, ViewStatus: "in_progress", ViewPhase: "execution"},
			{
				ID:          "m5",
				ViewStatus:  "in_progress",
				VaultStatus: "completed",
				ViewPhase:   "execution",
				VaultPhase:  "done",
			},
			{
				ID:          "z9",
				ViewStatus:  "in_progress",
				VaultStatus: "completed",
				ViewPhase:   "execution",
				VaultPhase:  "done",
			},
		},
	},
}

func TestCompareViewToIndex(t *testing.T) {
	for _, test := range viewDivergenceTestCases {
		t.Run(test.name, func(t *testing.T) {
			report := compareViewToIndex(test.owned, test.index)
			if report.Compared != test.wantCompared {
				t.Errorf("Compared = %d, want %d", report.Compared, test.wantCompared)
			}
			if report.Divergent != test.wantDivergent {
				t.Errorf("Divergent = %d, want %d", report.Divergent, test.wantDivergent)
			}
			if report.Absent != test.wantAbsent {
				t.Errorf("Absent = %d, want %d", report.Absent, test.wantAbsent)
			}
			if len(report.Findings) != len(test.wantFindings) {
				t.Fatalf("Findings = %+v, want %+v", report.Findings, test.wantFindings)
			}
			for i, want := range test.wantFindings {
				if report.Findings[i] != want {
					t.Errorf("Findings[%d] = %+v, want %+v", i, report.Findings[i], want)
				}
			}
		})
	}
}
