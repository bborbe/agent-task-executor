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

// RedriveCappedTotal counts spawns the executor declined because it had already
// spawned the task maxConsecutiveRedrives times in a row at an unchanged phase.
// Without this counter the cap is silent: a capped task produces no Job, so the
// burn stops and looks exactly like a healthy quiet fleet — the same failure
// shape ReconcilePassesTotal was introduced to make visible.
//
// Not reconcile-scoped, and named accordingly. The cap is enforced in
// spawnIfNeeded, which the Kafka consumer and the deferred-respawn loop pass
// through as well as the reconcile floor; on the dev fixture (2026-10-14) the
// floor initiated only two of four spawns, so a reconcile-only counter would
// have missed the loop's dominant path.
var RedriveCappedTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "executor_redrive_capped_total",
		Help: "Total number of spawns skipped by the consecutive-re-drive cap.",
	},
)

// ReconcilePassesTotal counts reconcile pass outcomes, labelled by result. The
// failure reasons mirror ReconcileOnce's log events, and `ok` is counted on
// every pass that gets past the vault classification — so an alert can fire on a
// backstop that never succeeds. That is the exact shape of the 2026-09 outage:
// the reconcile loop logged `reconcile_list_failed` once a minute and no other
// reconcile outcome was ever recorded, for months. A failure-only counter cannot
// express it — it saturates and then looks identical to a healthy quiet fleet,
// which is why the pass is counted on both sides of the outcome.
var ReconcilePassesTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "executor_reconcile_passes_total",
		Help: "Total number of reconcile passes by outcome (ok, aborted, view_not_ready, task_vault_mismatch, task_vault_unstamped).",
	},
	[]string{"result"},
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

// ClaimTotal counts claim lease outcomes by result. The result set is closed;
// keys and task ids are never label values.
var ClaimTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "executor_claim_total",
		Help: "Total number of claim lease outcomes by result.",
	},
	[]string{"result"},
)

// ClaimMismatchTotal counts held-claim / running-Job mismatches, one per case.
var ClaimMismatchTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "executor_claim_mismatch_total",
		Help: "Total number of held-claim / running-Job mismatches.",
	},
)

// DeferredConcurrencyCapTotal counts tasks deferred because their assignee was
// already at its configured concurrency cap, labelled by that assignee. The
// bare TaskEventsTotal{result="deferred_concurrency_cap"} counter says a
// deferral happened but not which agent is queuing, so an alert on it cannot
// exclude an agent that is throttled on purpose — github-update-go-agent runs
// at maxConcurrentJobs: 1 by the owner's explicit choice, and its intended
// backpressure reads as a fault. Observed 2026-09-08: AgentQueueHigh fired on
// nukeprod for update-go alone, costing a manual silence every morning.
// Filtering the bare counter instead is not an option: it carries no assignee
// label, so an `assignee!="..."` matcher matches no series and silently
// disables the alert entirely.
//
// Deliberately NOT pre-initialised, unlike ReconcilePassesTotal. That counter
// seeds a 0 series because its alert is `rate(...) == 0` — an absent series
// would make a dead backstop look healthy. This one backs a positive threshold
// (`increase(...) > 5`), where an absent series correctly means "no deferrals"
// and silence is the right answer. The assignee domain is also dynamic — it
// comes from agent Config CRs, so it cannot be enumerated at init — and it is
// bounded by the number of those CRs, which keeps cardinality in check.
var DeferredConcurrencyCapTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agent_executor_deferred_concurrency_cap_total",
		Help: "Total number of tasks deferred because their assignee was at its concurrency cap, by assignee.",
	},
	[]string{"assignee"},
)

// TaskViewSkippedTotal counts task event messages the in-memory task view
// could not decode. A skipped message never blocks readiness.
var TaskViewSkippedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "executor_task_view_skipped_total",
	Help: "Total number of task event messages the task view could not decode.",
})

func init() {
	TaskEventsTotal.WithLabelValues("spawned").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_status").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_phase").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_assignee").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_unknown_assignee").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_active_job").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_recent_success").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_stage").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_foreign_cluster").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_foreign_vault").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_view_not_ready").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_untagged").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_trigger_cap").Add(0)
	TaskEventsTotal.WithLabelValues("error").Add(0)
	TaskEventsTotal.WithLabelValues("type_mismatch").Add(0)
	TaskEventsTotal.WithLabelValues("spawn_suppressed_terminal_phase").Add(0)
	TaskEventsTotal.WithLabelValues("unknown_phase").Add(0)
	TaskEventsTotal.WithLabelValues("respawn_grace_window").Add(0)
	TaskEventsTotal.WithLabelValues("skipped_foreign_job").Add(0)
	TaskEventsTotal.WithLabelValues("respawn_after_grace_window").Add(0)
	TaskEventsTotal.WithLabelValues("deferred_concurrency_cap").Add(0)
	TaskEventsTotal.WithLabelValues("deferred_claim_invalidated").Add(0)
	TaskEventsTotal.WithLabelValues("deferred_respawn_spawn_failed").Add(0)
	// Pre-initialise every reconcile outcome so rate() sees a 0 series rather
	// than no data — an absent series makes `rate(...)==0` skip silently and the
	// "backstop never succeeds" alert never fire.
	for _, result := range []string{
		"ok",
		"aborted",
		"view_not_ready",
		"task_vault_mismatch",
		"task_vault_unstamped",
	} {
		ReconcilePassesTotal.WithLabelValues(result).Add(0)
	}
	// Pre-initialise the closed claim result set so every series is visible
	// before any claim activity.
	for _, result := range []string{
		"claimed",
		"conflict",
		"renewed",
		"recovered",
		"renew_failed",
		"claim_lost",
		"fenced",
		"taken_over",
		"takeover_too_early",
		"released",
		"already_released",
		"adopted",
		"error",
	} {
		ClaimTotal.WithLabelValues(result).Add(0)
	}
}
