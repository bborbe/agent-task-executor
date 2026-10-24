// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	stderrors "errors"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// quotaHarness builds a claim-mode handler whose admission is bounded by the
// given pod-quota reader and declared cap, with an active set of n Jobs.
func quotaHarness(
	limiter *mocks.FakePodQuotaLimiter,
	maxConcurrentJobs int,
	priorityClassName string,
	active int,
) *claimHarness {
	harness := newClaimHarness(pkg.ClaimModeOn, handler.WithPodQuotaLimiter(limiter))
	harness.resolver.ResolveReturns(pkg.AgentConfiguration{
		Assignee:          "claude",
		Image:             "img",
		MaxConcurrentJobs: maxConcurrentJobs,
		PriorityClassName: priorityClassName,
		RerunOnLostClaim:  claimBoolPtr(true),
	}, nil)
	harness.client.PutReturns("sha-1", nil)
	harness.client.ListReturns(nil, nil)
	harness.spawner.ListActiveJobsReturns(activeJobs(active), nil)
	return harness
}

// quotaTestPriorityClass is the priority class every quota fixture names.
const quotaTestPriorityClass = "github-pr-review-agent"

var _ = Describe("Pod quota admission", func() {
	deferredEvents := func() float64 {
		return testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("deferred_concurrency_cap"),
		)
	}

	deferredForAssignee := func() float64 {
		return testutil.ToFloat64(
			metrics.DeferredConcurrencyCapTotal.WithLabelValues("claude"),
		)
	}

	It("AC1: defers when the quota is below the declared cap and the quota is reached", func() {
		limiter := new(mocks.FakePodQuotaLimiter)
		limiter.PodQuotaReturns(3, true, nil)
		harness := quotaHarness(limiter, 5, quotaTestPriorityClass, 3)
		beforeEvents := deferredEvents()
		beforeAssignee := deferredForAssignee()

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())

		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(0))
		Expect(dispatchPutCount(harness)).To(Equal(0))
		Expect(deferredEvents() - beforeEvents).To(Equal(float64(1)))
		Expect(deferredForAssignee() - beforeAssignee).To(Equal(float64(1)))
	})

	It("AC2: the declared cap binds when it is smaller than the quota", func() {
		limiter := new(mocks.FakePodQuotaLimiter)
		limiter.PodQuotaReturns(8, true, nil)
		atCap := quotaHarness(limiter, 5, quotaTestPriorityClass, 5)

		Expect(atCap.consume(qualifyingClaimTask())).To(Succeed())
		Expect(atCap.spawner.SpawnJobCallCount()).To(Equal(0))
		Expect(dispatchPutCount(atCap)).To(Equal(0))

		belowCap := quotaHarness(limiter, 5, quotaTestPriorityClass, 4)
		Expect(belowCap.consume(qualifyingClaimTask())).To(Succeed())
		Expect(belowCap.spawner.SpawnJobCallCount()).To(Equal(1))
		Expect(dispatchPutCount(belowCap)).To(Equal(1))
	})

	It("AC3: a zero quota leaves the declared cap binding", func() {
		limiter := new(mocks.FakePodQuotaLimiter)
		limiter.PodQuotaReturns(0, false, nil)
		belowCap := quotaHarness(limiter, 5, quotaTestPriorityClass, 4)

		Expect(belowCap.consume(qualifyingClaimTask())).To(Succeed())
		Expect(belowCap.spawner.SpawnJobCallCount()).To(Equal(1))

		atCap := quotaHarness(limiter, 5, quotaTestPriorityClass, 5)
		Expect(atCap.consume(qualifyingClaimTask())).To(Succeed())
		Expect(atCap.spawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("AC4: a zero declared cap does not disable the quota bound", func() {
		limiter := new(mocks.FakePodQuotaLimiter)
		limiter.PodQuotaReturns(3, true, nil)
		atQuota := quotaHarness(limiter, 0, quotaTestPriorityClass, 3)

		Expect(atQuota.consume(qualifyingClaimTask())).To(Succeed())
		Expect(atQuota.spawner.SpawnJobCallCount()).To(Equal(0))

		belowQuota := quotaHarness(limiter, 0, quotaTestPriorityClass, 2)
		Expect(belowQuota.consume(qualifyingClaimTask())).To(Succeed())
		Expect(belowQuota.spawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("AC4b: a quota admitting zero pods defers every spawn instead of falling back", func() {
		// The defect this pins: treating pods: 0 as "no quota bound" would fall
		// back to the declared cap and claim work the quota can never start.
		limiter := new(mocks.FakePodQuotaLimiter)
		limiter.PodQuotaReturns(0, true, nil)
		harness := quotaHarness(limiter, 5, quotaTestPriorityClass, 0)

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())
		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(0))
		Expect(dispatchPutCount(harness)).To(Equal(0))
	})

	It("AC5: a failed quota read logs one warning and never blocks the spawn", func() {
		limiter := new(mocks.FakePodQuotaLimiter)
		limiter.PodQuotaReturns(0, false, stderrors.New("forbidden"))
		harness := quotaHarness(limiter, 5, quotaTestPriorityClass, 4)

		beforeFailedReads := testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("pod_quota_read_failed"),
		)
		out := capturePinReportLog(func() {
			Expect(harness.consume(qualifyingClaimTask())).To(Succeed())
		})

		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(1))
		Expect(strings.Count(out, "event=pod_quota_read_failed")).To(Equal(1))
		Expect(
			testutil.ToFloat64(
				metrics.TaskEventsTotal.WithLabelValues("pod_quota_read_failed"),
			) - beforeFailedReads,
		).To(Equal(float64(1)))
	})

	It("AC5: a successful read logs no quota warning", func() {
		limiter := new(mocks.FakePodQuotaLimiter)
		limiter.PodQuotaReturns(8, true, nil)
		harness := quotaHarness(limiter, 5, quotaTestPriorityClass, 4)

		out := capturePinReportLog(func() {
			Expect(harness.consume(qualifyingClaimTask())).To(Succeed())
		})

		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(1))
		Expect(strings.Count(out, "event=pod_quota_read_failed")).To(Equal(0))
	})

	It("AC8: the deferral line names the running count, cap, max and quota", func() {
		limiter := new(mocks.FakePodQuotaLimiter)
		limiter.PodQuotaReturns(3, true, nil)
		harness := quotaHarness(limiter, 5, quotaTestPriorityClass, 3)

		out := capturePinReportLog(func() {
			Expect(harness.consume(qualifyingClaimTask())).To(Succeed())
		})

		Expect(out).To(MatchRegexp("event=concurrency_cap .*running=3 cap=3 max=5 quota=3"))
	})

	It("AC8: the admit line names the running count, cap, max and quota", func() {
		limiter := new(mocks.FakePodQuotaLimiter)
		limiter.PodQuotaReturns(8, true, nil)
		harness := quotaHarness(limiter, 5, quotaTestPriorityClass, 4)

		out := capturePinReportLog(func() {
			Expect(harness.consume(qualifyingClaimTask())).To(Succeed())
		})

		Expect(out).To(MatchRegexp("event=concurrency_admit .*running=4 cap=5 max=5 quota=8"))
	})

	It("AC5b: a Config with no priority class never asks the reader", func() {
		limiter := new(mocks.FakePodQuotaLimiter)
		limiter.PodQuotaReturns(1, true, nil)
		harness := quotaHarness(limiter, 5, "", 4)

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())

		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(1))
		Expect(limiter.PodQuotaCallCount()).To(Equal(0))
	})

	It("clamps a negative bounded quota to a hard zero rather than admitting", func() {
		// A negative limit is not a state the reader produces; if one ever reaches
		// the handler it is clamped to 0. Because the limiter reported the class
		// bounded, that zero is authoritative and defers — never "uncapped".
		limiter := new(mocks.FakePodQuotaLimiter)
		limiter.PodQuotaReturns(-1, true, nil)
		harness := quotaHarness(limiter, 5, quotaTestPriorityClass, 4)

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())

		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(0))
		Expect(dispatchPutCount(harness)).To(Equal(0))
	})
})
