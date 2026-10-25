// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package metrics_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
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
