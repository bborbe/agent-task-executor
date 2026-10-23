// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"testing"
	"time"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// taskRerunCase is one input to TestRecordTaskRerun.
type taskRerunCase struct {
	name        string
	frontmatter lib.TaskFrontmatter
	succeeded   bool
	sucPhase    string
	sucAge      time.Duration
	wantReruns  float64
}

// taskRerunCases is the table, held outside the test function so its length
// stays within the lint budget.
var taskRerunCases = []taskRerunCase{
	{
		name: "same phase inside the window is a re-run",
		frontmatter: lib.TaskFrontmatter{
			"phase":     "execution",
			"task_type": "rerun-same-phase-inside",
		},
		succeeded:  true,
		sucPhase:   "execution",
		sucAge:     10 * time.Minute,
		wantReruns: 1,
	},
	{
		name: "same phase outside the window is not a re-run",
		frontmatter: lib.TaskFrontmatter{
			"phase":     "execution",
			"task_type": "rerun-same-phase-outside",
		},
		succeeded:  true,
		sucPhase:   "execution",
		sucAge:     2 * time.Hour,
		wantReruns: 0,
	},
	{
		name: "exactly at the window edge is not a re-run",
		frontmatter: lib.TaskFrontmatter{
			"phase":     "execution",
			"task_type": "rerun-at-boundary",
		},
		succeeded:  true,
		sucPhase:   "execution",
		sucAge:     taskRerunWindow,
		wantReruns: 0,
	},
	{
		name: "a success stamped in the future is not a re-run",
		frontmatter: lib.TaskFrontmatter{
			"phase":     "execution",
			"task_type": "rerun-negative-elapsed",
		},
		succeeded:  true,
		sucPhase:   "execution",
		sucAge:     -10 * time.Minute,
		wantReruns: 0,
	},
	{
		name: "a success at another phase is an advance, not a re-run",
		frontmatter: lib.TaskFrontmatter{
			"phase":     "execution",
			"task_type": "rerun-other-phase",
		},
		succeeded:  true,
		sucPhase:   "planning",
		sucAge:     10 * time.Minute,
		wantReruns: 0,
	},
	{
		name:        "an empty phase matches an empty success phase",
		frontmatter: lib.TaskFrontmatter{"task_type": "rerun-empty-phase"},
		succeeded:   true,
		sucPhase:    "",
		sucAge:      10 * time.Minute,
		wantReruns:  1,
	},
	{
		name:        "no recorded success is not a re-run",
		frontmatter: lib.TaskFrontmatter{"phase": "execution", "task_type": "rerun-no-success"},
		wantReruns:  0,
	},
	{
		name:        "an absent task_type still counts under the empty label",
		frontmatter: lib.TaskFrontmatter{"phase": "execution"},
		succeeded:   true,
		sucPhase:    "execution",
		sucAge:      10 * time.Minute,
		wantReruns:  1,
	},
}

// TestRecordTaskRerun pins what counts as a re-run. The phase case is the one
// that matters most: a task that succeeds at planning and is spawned again for
// execution has advanced, not re-run, and counting that would fire the re-run
// alert on every ordinary task.
func TestRecordTaskRerun(t *testing.T) {
	for _, tc := range taskRerunCases {
		t.Run(tc.name, func(t *testing.T) {
			store := pkg.NewTaskStore()
			if tc.succeeded {
				store.MarkSucceeded("tid-a", tc.sucPhase, time.Now().Add(-tc.sucAge))
			}
			h := &taskEventHandler{
				taskStore:       store,
				currentDateTime: libtime.NewCurrentDateTime(),
			}
			task := lib.Task{TaskIdentifier: "tid-a", Frontmatter: tc.frontmatter}

			taskType := string(task.Frontmatter.TaskType())
			counter := metrics.TaskRerunsTotal.WithLabelValues(taskType)
			before := testutil.ToFloat64(counter)
			h.recordTaskRerun(task)
			after := testutil.ToFloat64(counter)

			if got := after - before; got != tc.wantReruns {
				t.Fatalf("reruns delta = %v, want %v", got, tc.wantReruns)
			}
		})
	}
}

// TestRecordTaskRerunCreatesSeriesWithoutARerun covers the absent-tier alert's
// premise: the series has to exist at 0 before the first re-run, otherwise
// `absent_over_time` cannot tell "no re-run yet" from "the metric stopped being
// exported" — the second being the failure that alert exists to catch.
//
// Asserted against a gather of the vector filtered by label, not against a
// process-global series count: the CounterVec lives for the whole test process,
// so a count delta breaks under -count=2 or when another test created the label.
func TestRecordTaskRerunCreatesSeriesWithoutARerun(t *testing.T) {
	const taskType = "rerun-touch"

	h := &taskEventHandler{
		taskStore:       pkg.NewTaskStore(),
		currentDateTime: libtime.NewCurrentDateTime(),
	}
	task := lib.Task{
		TaskIdentifier: "tid-touch",
		Frontmatter: lib.TaskFrontmatter{
			"phase":     "execution",
			"task_type": taskType,
		},
	}

	h.recordTaskRerun(task)

	if v := testutil.ToFloat64(metrics.TaskRerunsTotal.WithLabelValues(taskType)); v != 0 {
		t.Fatalf("reruns = %v, want 0", v)
	}
	if !hasTaskRerunSeries(t, taskType) {
		t.Fatalf(
			"no executor_task_reruns_total series for task_type=%q: the counter must exist at 0",
			taskType,
		)
	}
}

// hasTaskRerunSeries reports whether the gathered vector carries a series for
// the given task_type, which a plain ToFloat64 cannot distinguish from an
// absent series that reads as 0.
func hasTaskRerunSeries(t *testing.T, taskType string) bool {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(metrics.TaskRerunsTotal); err != nil {
		t.Fatalf("register executor_task_reruns_total: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather executor_task_reruns_total: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "executor_task_reruns_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "task_type" && label.GetValue() == taskType {
					return true
				}
			}
		}
	}
	return false
}
