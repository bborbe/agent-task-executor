// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package metrics_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	_ "github.com/bborbe/agent-task-executor/pkg/metrics"
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
			"error",
			"type_mismatch",
		))
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
