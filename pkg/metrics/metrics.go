// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TaskEventsTotal counts task event processing outcomes.
var TaskEventsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agent_executor_task_events_total",
		Help: "Total number of task events processed.",
	},
	[]string{"result"},
)

// JobsSpawnedTotal counts successfully spawned jobs.
var JobsSpawnedTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "agent_executor_jobs_spawned_total",
		Help: "Total number of jobs successfully spawned.",
	},
)

func init() {
	TaskEventsTotal.WithLabelValues("spawned").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_status").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_phase").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_assignee").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_unknown_assignee").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_active_job").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_stage").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_trigger_cap").Add(0)
	TaskEventsTotal.WithLabelValues("error").Add(0)
	TaskEventsTotal.WithLabelValues("type_mismatch").Add(0)
	TaskEventsTotal.WithLabelValues("spawn_suppressed_terminal_phase").Add(0)
	TaskEventsTotal.WithLabelValues("unknown_phase").Add(0)
	TaskEventsTotal.WithLabelValues("respawn_grace_window").Add(0)
	TaskEventsTotal.WithLabelValues("respawn_after_grace_window").Add(0)
}
