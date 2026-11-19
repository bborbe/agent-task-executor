// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

var _ = Describe("run_claim spawn gate", func() {
	var (
		ctx                 context.Context
		fakeSpawner         *mocks.FakeJobSpawner
		fakeResolver        *mocks.FakeConfigResolver
		fakeResultPublisher *mocks.FakeResultPublisher
		fakeView            *mocks.FakeTaskView
		taskStore           *pkg.TaskStore
		clock               libtime.CurrentDateTime
		h                   handler.TaskEventHandler
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeSpawner = new(mocks.FakeJobSpawner)
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeSpawner.ListActiveJobsReturns(map[lib.TaskIdentifier]bool{}, nil)
		fakeSpawner.CountActiveJobsReturns(0, nil)
		fakeSpawner.SpawnJobReturns("job-run-claim-1", nil)
		fakeResolver = new(mocks.FakeConfigResolver)
		fakeResolver.ResolveReturns(
			pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"},
			nil,
		)
		fakeResultPublisher = new(mocks.FakeResultPublisher)
		fakeView = new(mocks.FakeTaskView)
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		taskStore = pkg.NewTaskStore()
		clock = libtime.NewCurrentDateTime()
		h = handler.NewTaskEventHandler(
			fakeSpawner,
			"dev",
			base.Branch("prod"),
			"",
			true,
			true,
			fakeResolver,
			fakeResultPublisher,
			taskStore,
			clock,
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeOff,
			nil,
		)
	})

	buildTask := func(extra lib.TaskFrontmatter) lib.Task {
		fm := lib.TaskFrontmatter{
			"status":   "in_progress",
			"phase":    string(domain.TaskPhaseExecution),
			"assignee": "claude",
		}
		for k, v := range extra {
			fm[k] = v
		}
		return lib.Task{
			TaskIdentifier: "tid-run-claim",
			Vault:          testTaskVault,
			Frontmatter:    fm,
		}
	}

	buildMsg := func(task lib.Task) *sarama.ConsumerMessage {
		value, err := json.Marshal(task)
		Expect(err).To(BeNil())
		return &sarama.ConsumerMessage{Value: value}
	}

	runClaimMetric := func() float64 {
		return testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("skipped_run_claim"),
		)
	}

	activeJobMetric := func() float64 {
		return testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("skipped_active_job"),
		)
	}

	It("suppresses a spawn while the run_claim is live", func() {
		clock.SetNow(libtimetest.ParseDateTime("2026-05-16T20:19:16Z"))
		task := buildTask(lib.TaskFrontmatter{"run_claim": "2026-05-16T20:19:16Z"})

		before := runClaimMetric()
		out := deferredGateCaptureStderr(func() {
			Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())
		})

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		Expect(fakeResultPublisher.Invocations()).To(BeEmpty())
		Expect(runClaimMetric() - before).To(Equal(float64(1)))
		Expect(out).To(ContainSubstring(
			"event=spawn_suppressed_run_claim task=tid-run-claim " +
				"run_claim=2026-05-16T20:19:16Z age=0s",
		))
	})

	It("spawns when there is no run_claim", func() {
		clock.SetNow(libtimetest.ParseDateTime("2026-05-16T20:19:16Z"))
		task := buildTask(nil)

		before := runClaimMetric()
		Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(runClaimMetric() - before).To(Equal(float64(0)))
	})

	It("spawns when the run_claim is exactly at the TTL", func() {
		clock.SetNow(libtimetest.ParseDateTime("2026-05-16T22:19:16Z"))
		task := buildTask(lib.TaskFrontmatter{"run_claim": "2026-05-16T20:19:16Z"})

		before := runClaimMetric()
		Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(runClaimMetric() - before).To(Equal(float64(0)))
	})

	It("spawns when the run_claim is malformed", func() {
		clock.SetNow(libtimetest.ParseDateTime("2026-05-16T20:19:16Z"))
		task := buildTask(lib.TaskFrontmatter{"run_claim": "not-a-timestamp"})

		before := runClaimMetric()
		Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(runClaimMetric() - before).To(Equal(float64(0)))
	})

	It("lets current_job win when both are set", func() {
		fakeSpawner.IsJobActiveReturns(true, nil)
		clock.SetNow(libtimetest.ParseDateTime("2026-05-16T20:19:16Z"))
		task := buildTask(lib.TaskFrontmatter{
			"current_job": "pod-A",
			"run_claim":   "2026-05-16T20:19:16Z",
		})

		runClaimBefore := runClaimMetric()
		activeJobBefore := activeJobMetric()
		out := deferredGateCaptureStderr(func() {
			Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())
		})

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		Expect(runClaimMetric() - runClaimBefore).To(Equal(float64(0)))
		Expect(activeJobMetric() - activeJobBefore).To(Equal(float64(1)))
		Expect(strings.Contains(out, "spawn_suppressed_run_claim")).To(BeFalse())
	})

	It("re-drives the task once the claim expires", func() {
		clock.SetNow(libtimetest.ParseDateTime("2026-05-16T20:19:16Z"))
		task := buildTask(lib.TaskFrontmatter{"run_claim": "2026-05-16T20:19:16Z"})

		before := runClaimMetric()
		Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

		clock.SetNow(libtimetest.ParseDateTime("2026-05-16T22:19:16Z"))
		Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(runClaimMetric() - before).To(Equal(float64(1)))
	})
})
