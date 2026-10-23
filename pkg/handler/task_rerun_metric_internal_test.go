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
	"github.com/prometheus/client_golang/prometheus/testutil"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// TestRecordTaskRerun pins what counts as a re-run. The phase case is the one
// that matters most: a task that succeeds at planning and is spawned again for
// execution has advanced, not re-run, and counting that would fire the re-run
// alert on every ordinary task.
func TestRecordTaskRerun(t *testing.T) {
	cases := []struct {
		name       string
		taskType   string
		taskPhase  string
		succeeded  bool
		sucPhase   string
		sucAge     time.Duration
		wantReruns float64
	}{
		{
			name:       "same phase inside the window is a re-run",
			taskType:   "rerun-same-phase-inside",
			taskPhase:  "execution",
			succeeded:  true,
			sucPhase:   "execution",
			sucAge:     10 * time.Minute,
			wantReruns: 1,
		},
		{
			name:       "same phase outside the window is not a re-run",
			taskType:   "rerun-same-phase-outside",
			taskPhase:  "execution",
			succeeded:  true,
			sucPhase:   "execution",
			sucAge:     2 * time.Hour,
			wantReruns: 0,
		},
		{
			name:       "a success at another phase is an advance, not a re-run",
			taskType:   "rerun-other-phase",
			taskPhase:  "execution",
			succeeded:  true,
			sucPhase:   "planning",
			sucAge:     10 * time.Minute,
			wantReruns: 0,
		},
		{
			name:       "no recorded success is not a re-run",
			taskType:   "rerun-no-success",
			taskPhase:  "execution",
			wantReruns: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := pkg.NewTaskStore()
			if tc.succeeded {
				store.MarkSucceeded("tid-a", tc.sucPhase, time.Now().Add(-tc.sucAge))
			}
			h := &taskEventHandler{
				taskStore:       store,
				currentDateTime: libtime.NewCurrentDateTime(),
			}
			task := lib.Task{
				TaskIdentifier: "tid-a",
				Frontmatter: lib.TaskFrontmatter{
					"phase":     tc.taskPhase,
					"task_type": tc.taskType,
				},
			}

			counter := metrics.TaskRerunsTotal.WithLabelValues(tc.taskType)
			before := testutil.ToFloat64(counter)
			h.recordTaskRerun(task)
			after := testutil.ToFloat64(counter)

			if got := after - before; got != tc.wantReruns {
				t.Fatalf("reruns delta = %v, want %v", got, tc.wantReruns)
			}
		})
	}
}

// TestRecordTaskRerunTouchesSeriesWithoutARerun covers the absent-tier alert's
// premise: the counter has to exist at 0 before the first re-run, otherwise
// `absent_over_time` cannot tell "no re-run yet" from "the metric stopped being
// exported" — the second being the failure that alert exists to catch.
func TestRecordTaskRerunTouchesSeriesWithoutARerun(t *testing.T) {
	h := &taskEventHandler{
		taskStore:       pkg.NewTaskStore(),
		currentDateTime: libtime.NewCurrentDateTime(),
	}
	task := lib.Task{
		TaskIdentifier: "tid-touch",
		Frontmatter: lib.TaskFrontmatter{
			"phase":     "execution",
			"task_type": "rerun-touch",
		},
	}

	before := testutil.CollectAndCount(metrics.TaskRerunsTotal, "executor_task_reruns_total")
	h.recordTaskRerun(task)
	after := testutil.CollectAndCount(metrics.TaskRerunsTotal, "executor_task_reruns_total")

	if after != before+1 {
		t.Fatalf("series count = %d, want %d: the counter must be created at 0", after, before+1)
	}
	if v := testutil.ToFloat64(metrics.TaskRerunsTotal.WithLabelValues("rerun-touch")); v != 0 {
		t.Fatalf("reruns = %v, want 0", v)
	}
}
