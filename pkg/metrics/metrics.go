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

// ReconcileRedrivenTotal counts tasks re-driven by the reconcile loop after a
// restart dropped their in-memory deferral state (spec 005).
var ReconcileRedrivenTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "executor_reconcile_redriven_total",
		Help: "Total number of tasks re-driven by the reconcile loop.",
	},
)

// SkippedUnknownAssigneeTotal counts tasks skipped because their assignee
// matches no agent Config CR, labelled by the unknown assignee name. The bare
// TaskEventsTotal{result="skipped_unknown_assignee"} counter says a skip
// happened but not which assignee is failing; this label makes the signal
// actionable. Observed 2026-08-26 → 2026-09-03: 12 tasks stranded 8 days with
// only the bare counter moving, invisible as a routable alert.
var SkippedUnknownAssigneeTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agent_executor_skipped_unknown_assignee_total",
		Help: "Total number of tasks skipped because their assignee matches no agent Config CR, by assignee.",
	},
	[]string{"assignee"},
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
	TaskEventsTotal.WithLabelValues("deferred_concurrency_cap").Add(0)
}
