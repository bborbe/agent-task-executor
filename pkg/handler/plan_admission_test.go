// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

var _ = Describe("Plan-section admission gate", func() {
	const (
		planPresent = "# Impact\n\nthe idea\n\n## Plan\n\n- step one\n"
		planAbsent  = "# Impact\n\nthe idea\n"
	)

	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	// newHandler builds a handler whose Config triggers on planning and
	// execution and carries no task types, so the type filter is skipped and the
	// plan gate is the only thing that can refuse an execution spawn.
	newHandler := func(
		publisher pkg.ResultPublisher,
	) (handler.TaskEventHandler, *mocks.FakeJobSpawner) {
		fakeSpawner := new(mocks.FakeJobSpawner)
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeSpawner.SpawnJobReturns("job-plan-admission", nil)
		fakeResolver := &mocks.FakeConfigResolver{}
		fakeResolver.ResolveReturns(pkg.AgentConfiguration{
			Assignee: "claude",
			Image:    "my-image:latest",
			Trigger: &agentv1.Trigger{
				Statuses: domain.TaskStatuses{domain.TaskStatusInProgress},
				Phases: domain.TaskPhases{
					domain.TaskPhasePlanning,
					domain.TaskPhaseExecution,
				},
			},
		}, nil)
		fakeView := &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		return handler.NewTaskEventHandler(
			fakeSpawner,
			"dev",
			base.Branch("prod"),
			"",
			true,
			true,
			fakeResolver,
			publisher,
			pkg.NewTaskStore(),
			libtime.NewCurrentDateTime(),
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeOff,
			nil,
		), fakeSpawner
	}

	prototypeTask := func(id string, content string) lib.Task {
		return lib.Task{
			TaskIdentifier: lib.TaskIdentifier(id),
			Vault:          testTaskVault,
			Content:        lib.TaskContent(content),
			Frontmatter: lib.TaskFrontmatter{
				"status":    "in_progress",
				"phase":     "execution",
				"task_type": "strategy-prototype",
				"assignee":  "claude",
				"stage":     "prod",
			},
		}
	}

	It("refuses a plan-less prototype task at execution, writing nothing to the record", func() {
		producer := &capturingSyncProducer{}
		currentDateTime := libtime.NewCurrentDateTime()
		currentDateTime.SetNow(libtimetest.ParseDateTime("2026-11-19T19:00:00Z"))
		publisher := pkg.NewResultPublisher(
			producer,
			base.TopicPrefix("develop"),
			currentDateTime,
			nil,
			nil,
		)
		h, fakeSpawner := newHandler(publisher)

		before := testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("skipped_missing_plan"),
		)

		Expect(h.ConsumeMessage(ctx, buildTriggerRefusalMsg(
			prototypeTask("prototype-plan-less", planAbsent),
		))).To(Succeed())

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		Expect(testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("skipped_missing_plan"),
		)).To(Equal(before + 1))
		// The refusal is a log line plus a metric, like every other admission
		// refusal: no task write at all, so status, phase, assignee and
		// trigger_count are untouched and the record is admitted automatically
		// once a "## Plan" appears.
		Expect(producer.messages).To(BeEmpty())
	})

	It("spawns the same task once its ## Plan section is present", func() {
		h, fakeSpawner := newHandler(new(mocks.FakeResultPublisher))

		Expect(h.ConsumeMessage(ctx, buildTriggerRefusalMsg(
			prototypeTask("prototype-with-plan", planPresent),
		))).To(Succeed())

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("spawns a plan-less prototype task at the planning phase", func() {
		h, fakeSpawner := newHandler(new(mocks.FakeResultPublisher))

		task := prototypeTask("prototype-planning", planAbsent)
		task.Frontmatter["phase"] = "planning"

		Expect(h.ConsumeMessage(ctx, buildTriggerRefusalMsg(task))).To(Succeed())

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("spawns a task type outside the plan-consuming allowlist", func() {
		h, fakeSpawner := newHandler(new(mocks.FakeResultPublisher))

		task := prototypeTask("other-task-type", planAbsent)
		task.Frontmatter["task_type"] = "llm"

		Expect(h.ConsumeMessage(ctx, buildTriggerRefusalMsg(task))).To(Succeed())

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("never reads an empty body as a missing section", func() {
		h, fakeSpawner := newHandler(new(mocks.FakeResultPublisher))

		task := prototypeTask("empty-body", " ")

		Expect(h.ConsumeMessage(ctx, buildTriggerRefusalMsg(task))).To(Succeed())

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})
})
