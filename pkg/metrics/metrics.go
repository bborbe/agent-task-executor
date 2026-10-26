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
//
// Labelled by task type and phase because the unlabelled counter said only that
// something was capped: a reader of `capped=N` could not tell which kinds of
// work were stranded. The phase matters because the cap is keyed per
// (task, phase) — a record stuck at one phase is a different problem from one
// that advances and re-caps, and only the second is healthy progress.
//
// Deliberately NOT pre-initialised in init() with .Add(0). The distinct
// task_type values are whatever this executor actually spawns, resolved from
// the agent Config CRs at runtime rather than at compile time, and phase is free
// text from the task frontmatter. That is the unbounded side of the
// counter-pre-initialization rule's own boundary: pre-register bounded enums,
// use absent() in alerting for unbounded domains. Do not "fix" the omission.
var RedriveCappedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "executor_redrive_capped_total",
		Help: "Total number of spawns skipped by the consecutive-re-drive cap, by task type and phase.",
	},
	[]string{"task_type", "phase"},
)

// The two reasons the consecutive-re-drive cap may close a record with. The
// vocabulary is closed at compile time, which is what lets
// RedriveAbortedTotal be pre-initialised.
const (
	// RedriveAbortReasonBrokenSignature means the most recent attempts all failed
	// with the same broken signature.
	RedriveAbortReasonBrokenSignature = "capped:broken-signature"
	// RedriveAbortReasonTransientExhausted means the failures were transient but
	// the bounded extension is spent.
	RedriveAbortReasonTransientExhausted = "capped:transient-retries-exhausted"
)

// RedriveAbortedTotal counts records the executor closed out because the
// consecutive-re-drive cap decided retrying would not help, labelled by abort
// reason.
//
// Unlike RedriveCappedTotal, it IS pre-initialised in init() with .Add(0) for
// both reason values. That is the bounded side of the counter-pre-initialization
// rule: this label domain is closed at compile time (two constants above), so
// every series can be registered before any abort. Its neighbour's labels —
// task_type from runtime Config CRs and free-text phase — are unbounded, which
// is why that one is deliberately left absent. Do not make the two match: the
// difference is the rule, not an oversight.
var RedriveAbortedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "executor_redrive_aborted_total",
		Help: "Total number of records closed out by the consecutive-re-drive cap, by abort reason.",
	},
	[]string{"reason"},
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

// ClaimKeyFallbackTotal counts pr-review tasks whose body carried no usable
// pull-request URL, so their claim fell back to their task id.
var ClaimKeyFallbackTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "executor_claim_key_fallback_total",
	Help: "Total number of pr-review claim keys that fell back to the task-id key.",
})

// ParkedLostClaimTotal counts tasks parked at phase human_review after another
// executor took over their claim, labelled by outcome. The unset default parks,
// so an unexpected fleet-wide park must be alertable from a metric rather than
// only visible by grepping for the log line.
var ParkedLostClaimTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "executor_parked_lost_claim_total",
	Help: "Total number of tasks parked at human_review after a lost claim, by outcome.",
}, []string{"result"})

// TaskRerunsTotal counts spawns of a task whose previous Job succeeded inside
// the re-run window. A re-run is a task that ran again after succeeding, which
// is what a claim regression looks like on live data.
//
// Scoped to this executor's own store: a task whose first success ran on
// another cluster is not recorded here and is not counted, so the counter
// under-reports cross-cluster re-runs. Those stay visible through
// ClaimTotal{result=~"taken_over|adopted"}. The Prometheus `cluster` external
// label distinguishes the clusters that do report.
var TaskRerunsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "executor_task_reruns_total",
		Help: "Total number of task spawns after a success inside the re-run window.",
	},
	[]string{"task_type"},
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

// JobLookupDeniedTotal counts active-job reads the API server refused on
// authentication (401) or authorization (403), by assignee and namespace. Such
// a read is not transient: retrying cannot help until the credential or the
// grant is restored, so the caller suppresses the spawn instead of failing the
// message, and this counter is the only surface saying an assignee went quiet
// because its identity broke — the deferral itself is silent.
//
// The series is seeded at 0 by PreInitJobLookupDenied on every successful read,
// not in init(): the assignee domain comes from agent Config CRs and the
// namespace is resolved per task, so neither can be enumerated at init — the
// same constraint recorded on DeferredConcurrencyCapTotal. Without the seed,
// `increase(...[5m]) > 0` cannot be told apart from a metric nobody publishes.
// Cardinality is bounded by the Config CRs times the namespaces they dispatch to.
var JobLookupDeniedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agent_executor_job_lookup_denied_total",
		Help: "Total number of active-job reads refused with 401/403, by assignee and namespace.",
	},
	[]string{"assignee", "namespace"},
)

// PreInitJobLookupDenied seeds JobLookupDeniedTotal at 0 for one served
// assignee/namespace pair. It is called after a successful read, so the seeded
// pairs are exactly the pairs this executor actually reads: an assignee it
// never serves gets no series, which is the truthful answer.
func PreInitJobLookupDenied(assignee, namespace string) {
	JobLookupDeniedTotal.WithLabelValues(assignee, namespace).Add(0)
}

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
	TaskEventsTotal.WithLabelValues("pin_outside_pool").Add(0)
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
	// Pre-initialise the closed abort vocabulary so both series are visible
	// before any abort. Unlike RedriveCappedTotal, whose labels are unbounded.
	for _, reason := range []string{
		RedriveAbortReasonBrokenSignature,
		RedriveAbortReasonTransientExhausted,
	} {
		RedriveAbortedTotal.WithLabelValues(reason).Add(0)
	}
	ClaimKeyFallbackTotal.Add(0)
	for _, result := range []string{"parked", "failed"} {
		ParkedLostClaimTotal.WithLabelValues(result).Add(0)
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
		"jobless_released",
	} {
		ClaimTotal.WithLabelValues(result).Add(0)
	}
}
