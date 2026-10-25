// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

// The denied-read specs live in their own file: task_event_handler_test.go is at
// the 2000-line lint ceiling, and these are a cohesive group — every one of them
// drives a 401/403 from one of the executor's active-job reads.

import (
	"context"
	"encoding/json"
	stderrors "errors"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

var _ = Describe("JobLookupDenied", func() {
	var (
		ctx                 context.Context
		fakeSpawner         *mocks.FakeJobSpawner
		fakeResolver        *mocks.FakeConfigResolver
		fakeResultPublisher *mocks.FakeResultPublisher
		fakeView            *mocks.FakeTaskView
		h                   handler.TaskEventHandler
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeSpawner = new(mocks.FakeJobSpawner)
		fakeResolver = &mocks.FakeConfigResolver{}
		fakeResolver.ResolveReturns(
			pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"},
			nil,
		)
		fakeResultPublisher = &mocks.FakeResultPublisher{}
		fakeView = &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		h = handler.NewTaskEventHandler(
			fakeSpawner,
			"dev",
			base.Branch("prod"),
			"",
			true,
			true,
			fakeResolver,
			fakeResultPublisher,
			pkg.NewTaskStore(),
			libtime.NewCurrentDateTime(),
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeOff,
			nil,
		)
	})

	buildMsg := func(task lib.Task) *sarama.ConsumerMessage {
		if task.Vault == "" {
			task.Vault = testTaskVault
		}
		value, err := json.Marshal(task)
		Expect(err).To(BeNil())
		return &sarama.ConsumerMessage{Value: value}
	}

	dueTask := func(id string) lib.Task {
		return lib.Task{
			TaskIdentifier: lib.TaskIdentifier(id),
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(domain.TaskPhaseExecution),
				"assignee": "claude",
			},
		}
	}

	deniedTotal := func() float64 {
		return testutil.ToFloat64(
			metrics.JobLookupDeniedTotal.WithLabelValues("claude", "dev"),
		)
	}

	// SC1: a refused identity must not exit the consumer. Before this change the
	// 401 propagated out of ConsumeMessage as `run failed` and restarted the
	// executor for every assignee (Sentry NUKE-PROD-CG, 2026-10-24).
	It("defers instead of failing when the active-Job lookup is unauthorized", func() {
		fakeSpawner.ListActiveJobsReturns(nil, apierrors.NewUnauthorized("token expired"))
		before := deniedTotal()
		err := h.ConsumeMessage(ctx, buildMsg(dueTask("tid-401")))
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		Expect(deniedTotal() - before).To(Equal(float64(1)))
	})

	It("defers instead of failing when the active-Job lookup is forbidden", func() {
		fakeSpawner.ListActiveJobsReturns(nil, apierrors.NewForbidden(
			schema.GroupResource{Resource: "jobs"},
			"claude",
			errors.Errorf(ctx, "no list grant"),
		))
		before := deniedTotal()
		err := h.ConsumeMessage(ctx, buildMsg(dueTask("tid-403")))
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		// The labels are the whole visibility story — the deferral is silent — so
		// the forbidden branch is asserted exactly like the unauthorized one.
		Expect(deniedTotal() - before).To(Equal(float64(1)))
	})

	// The current_job gate reads the API a second time, and that read can be
	// refused where the first was granted (a `get jobs` 403 beside a `list jobs`
	// grant). It must defer too, not exit the consumer.
	It("defers when the current_job lookup is unauthorized", func() {
		fakeSpawner.ListActiveJobsReturns(map[lib.TaskIdentifier]bool{}, nil)
		fakeSpawner.IsJobActiveReturns(false, apierrors.NewUnauthorized("token expired"))
		task := dueTask("tid-401-current-job")
		task.Frontmatter["current_job"] = "claude-20261024000000"
		err := h.ConsumeMessage(ctx, buildMsg(task))
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		// Non-vacuity: the current_job read must have been reached.
		Expect(fakeSpawner.IsJobActiveCallCount()).To(BeNumerically(">=", 1))
	})

	// The cap count reads the API a third time; a refusal there must defer rather
	// than exit.
	It("defers when the concurrency-cap count is unauthorized", func() {
		fakeResolver.ResolveReturns(
			pkg.AgentConfiguration{
				Assignee:          "claude",
				Image:             "my-image:latest",
				MaxConcurrentJobs: 4,
			},
			nil,
		)
		fakeSpawner.ListActiveJobsReturnsOnCall(0, map[lib.TaskIdentifier]bool{}, nil)
		fakeSpawner.ListActiveJobsReturnsOnCall(1, nil, apierrors.NewUnauthorized("token expired"))
		err := h.ConsumeMessage(ctx, buildMsg(dueTask("tid-401-cap")))
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		// Non-vacuity: the second read must actually have happened, otherwise this
		// passes on any earlier suppression.
		Expect(fakeSpawner.ListActiveJobsCallCount()).To(BeNumerically(">=", 2))
	})

	// The negative controls. Suppressing every read error would also swallow
	// programming faults and transient conditions worth retrying, so a failure
	// that is not an identity refusal must still fail the message.
	DescribeTable("still fails the message for a non-identity read failure",
		func(err error) {
			fakeSpawner.ListActiveJobsReturns(nil, err)
			Expect(h.ConsumeMessage(ctx, buildMsg(dueTask("tid-fatal")))).NotTo(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		},
		Entry("server error", apierrors.NewInternalError(stderrors.New("boom"))),
		Entry("not found", apierrors.NewNotFound(schema.GroupResource{Resource: "jobs"}, "claude")),
		Entry("rate limited", apierrors.NewTooManyRequests("slow down", 1)),
	)
})
