// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package metrics_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

var _ = Describe("Metrics", func() {
	It("registers all expected metric names in the default registry", func() {
		mfs, err := prometheus.DefaultGatherer.Gather()
		Expect(err).NotTo(HaveOccurred())

		names := make(map[string]bool, len(mfs))
		for _, mf := range mfs {
			names[mf.GetName()] = true
		}

		Expect(names).To(HaveKey("agent_executor_task_events_total"))
		Expect(names).To(HaveKey("agent_executor_jobs_spawned_total"))
		Expect(names).To(HaveKey("executor_reconcile_redriven_total"))
		Expect(names).To(HaveKey("executor_claim_total"))
		Expect(names).To(HaveKey("executor_claim_mismatch_total"))
		Expect(names).To(HaveKey("executor_claim_key_fallback_total"))
	})

	It("pre-initializes the claim-key fallback counter at zero", func() {
		mfs, err := prometheus.DefaultGatherer.Gather()
		Expect(err).NotTo(HaveOccurred())

		for _, mf := range mfs {
			if mf.GetName() != "executor_claim_key_fallback_total" {
				continue
			}
			Expect(mf.GetMetric()).To(HaveLen(1))
			Expect(mf.GetMetric()[0].GetCounter().GetValue()).To(Equal(float64(0)))
			return
		}
		Fail("executor_claim_key_fallback_total not registered")
	})

	It("pre-initializes all 14 claim result series", func() {
		mfs, err := prometheus.DefaultGatherer.Gather()
		Expect(err).NotTo(HaveOccurred())

		labels := gatherLabels(mfs, "executor_claim_total", "result")
		Expect(labels).To(ContainElements(
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
		))
	})

	It("pre-initializes all task_events_total label combinations", func() {
		mfs, err := prometheus.DefaultGatherer.Gather()
		Expect(err).NotTo(HaveOccurred())

		labels := gatherLabels(mfs, "agent_executor_task_events_total", "result")
		Expect(labels).To(ContainElements(
			"spawned",
			"skipped_status",
			"skipped_phase",
			"skipped_assignee",
			"skipped_unknown_assignee",
			"skipped_active_job",
			"skipped_recent_success",
			"error",
			"type_mismatch",
			"deferred_claim_invalidated",
			"deferred_respawn_spawn_failed",
			"deferred_respawn_refusal_write_failed",
			"event_refusal_write_failed",
		))
	})

	It("AC13: pre-initializes the reconcile pass outcomes and drops the git-rest ones", func() {
		mfs, err := prometheus.DefaultGatherer.Gather()
		Expect(err).NotTo(HaveOccurred())

		labels := gatherLabels(mfs, "executor_reconcile_passes_total", "result")
		Expect(labels).To(ContainElements(
			"ok",
			"aborted",
			"view_not_ready",
			"task_vault_mismatch",
			"task_vault_unstamped",
		))
		Expect(labels).NotTo(ContainElement("vault_unavailable"))
		Expect(labels).NotTo(ContainElement("list_failed"))
	})

	It("exposes the concurrency-cap deferral counter labelled by assignee", func() {
		// Asserted here rather than in the registry-name spec above: a
		// CounterVec with no observed label values emits no metric family at
		// all, so the family only exists once a label value has been touched.
		metrics.DeferredConcurrencyCapTotal.WithLabelValues("test-assignee").Add(0)

		mfs, err := prometheus.DefaultGatherer.Gather()
		Expect(err).NotTo(HaveOccurred())

		Expect(
			gatherLabels(mfs, "agent_executor_deferred_concurrency_cap_total", "assignee"),
		).To(ContainElement("test-assignee"))
	})

	It("pre-initialises executor_redrive_aborted_total for both abort reasons", func() {
		mfs, err := prometheus.DefaultGatherer.Gather()
		Expect(err).NotTo(HaveOccurred())

		Expect(
			gatherLabels(mfs, "executor_redrive_aborted_total", "reason"),
		).To(ConsistOf(
			metrics.RedriveAbortReasonBrokenSignature,
			metrics.RedriveAbortReasonTransientExhausted,
		))
	})

	It("moves only the matching abort series", func() {
		// Both reasons are pre-initialised, so ToFloat64 on the bare collector
		// panics; read the child series instead. Deltas, never absolutes: these
		// counters are process-global and never reset.
		brokenBefore := testutil.ToFloat64(
			metrics.RedriveAbortedTotal.WithLabelValues(
				metrics.RedriveAbortReasonBrokenSignature,
			),
		)
		transientBefore := testutil.ToFloat64(
			metrics.RedriveAbortedTotal.WithLabelValues(
				metrics.RedriveAbortReasonTransientExhausted,
			),
		)

		metrics.RedriveAbortedTotal.WithLabelValues(
			metrics.RedriveAbortReasonBrokenSignature,
		).Inc()

		Expect(testutil.ToFloat64(
			metrics.RedriveAbortedTotal.WithLabelValues(
				metrics.RedriveAbortReasonBrokenSignature,
			),
		)).To(Equal(brokenBefore + 1))
		Expect(testutil.ToFloat64(
			metrics.RedriveAbortedTotal.WithLabelValues(
				metrics.RedriveAbortReasonTransientExhausted,
			),
		)).To(Equal(transientBefore))
	})

	It("pre-initialises exactly the four view-divergence results at zero", func() {
		mfs, err := prometheus.DefaultGatherer.Gather()
		Expect(err).NotTo(HaveOccurred())

		Expect(
			gatherLabels(mfs, "executor_view_divergence_total", "result"),
		).To(ConsistOf("ok", "mismatch", "absent", "read_error"))

		// The series are pre-initialised, so ToFloat64 on the bare collector
		// panics; read each child series instead.
		for _, result := range []string{"ok", "mismatch", "absent", "read_error"} {
			Expect(testutil.ToFloat64(
				metrics.ViewDivergenceTotal.WithLabelValues(result),
			)).To(Equal(float64(0)), "result=%s", result)
		}
	})

	It("pre-initialises exactly the eight reconcile-refusal gates at zero", func() {
		mfs, err := prometheus.DefaultGatherer.Gather()
		Expect(err).NotTo(HaveOccurred())

		Expect(
			gatherLabels(mfs, "executor_reconcile_refused_total", "gate"),
		).To(ConsistOf(
			"foreign_cluster",
			"unresolved_config",
			"terminal_status",
			"status_not_triggered",
			"phase_not_triggered",
			"terminal_phase",
			"stage_mismatch",
			"no_routing",
		))

		// The series are pre-initialised, so ToFloat64 on the bare collector
		// panics; read each child series instead.
		for _, gate := range []string{
			metrics.ReconcileRefusedGateForeignCluster,
			metrics.ReconcileRefusedGateUnresolvedConfig,
			metrics.ReconcileRefusedGateTerminalStatus,
			metrics.ReconcileRefusedGateStatusNotTriggered,
			metrics.ReconcileRefusedGatePhaseNotTriggered,
			metrics.ReconcileRefusedGateTerminalPhase,
			metrics.ReconcileRefusedGateStageMismatch,
			metrics.ReconcileRefusedGateNoRouting,
		} {
			Expect(testutil.ToFloat64(
				metrics.ReconcileRefusedTotal.WithLabelValues(gate),
			)).To(Equal(float64(0)), "gate=%s", gate)
		}
	})

	It("labels the redrive-cap counter with task_type then phase", func() {
		// The touch is required: this vec is deliberately not pre-initialised,
		// so it emits no metric family until a label pair is observed. The two
		// sentinels pin the declaration order — a swap would still be reachable
		// by every caller, but Prometheus would then report task_type="execution".
		metrics.RedriveCappedTotal.WithLabelValues("probe-type", "probe-phase").Add(0)

		mfs, err := prometheus.DefaultGatherer.Gather()
		Expect(err).NotTo(HaveOccurred())

		Expect(
			gatherLabels(mfs, "executor_redrive_capped_total", "task_type"),
		).To(ContainElement("probe-type"))
		Expect(
			gatherLabels(mfs, "executor_redrive_capped_total", "phase"),
		).To(ContainElement("probe-phase"))
	})
})

func gatherLabels(mfs []*dto.MetricFamily, metricName string, labelName string) []string {
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		var values []string
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == labelName {
					values = append(values, lp.GetValue())
				}
			}
		}
		return values
	}
	return nil
}
