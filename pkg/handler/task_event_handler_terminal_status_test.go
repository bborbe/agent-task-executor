// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
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

// The rule under test: a task whose status is terminal is never a candidate,
// whatever its Config's trigger.statuses says. It is the status-side twin of the
// terminal-phase gate, and it is what keeps a finished task out of the reconcile
// candidate set — where the re-drive cap would otherwise charge it to its streak
// on every pass, forever.
//
// Every case below therefore uses a Config that DOES list the terminal status in
// trigger.statuses. With the default allow-list ({in_progress}) the allow-list
// alone would exclude these tasks, and a test written that way would pass with or
// without the fix.
var _ = Describe("terminal status is never a candidate", func() {
	var (
		ctx                 context.Context
		fakeSpawner         *mocks.FakeJobSpawner
		fakeResolver        *mocks.FakeConfigResolver
		fakeResultPublisher *mocks.FakeResultPublisher
		fakeView            *mocks.FakeTaskView
		taskStore           *pkg.TaskStore
		currentDateTime     libtime.CurrentDateTime
		h                   handler.TaskEventHandler
	)

	// permissiveConfig lists both terminal statuses in its trigger, which is the
	// only shape in which the status allow-list alone admits a finished task.
	permissiveConfig := func() pkg.AgentConfiguration {
		return pkg.AgentConfiguration{
			Assignee: "claude",
			Image:    "my-image:latest",
			Trigger: &agentv1.Trigger{
				Phases: domain.TaskPhases{domain.TaskPhaseExecution},
				Statuses: domain.TaskStatuses{
					domain.TaskStatusInProgress,
					domain.TaskStatusCompleted,
					domain.TaskStatusAborted,
				},
			},
		}
	}

	// terminalStatusTask is eligible on every other gate: a phase the Config lists,
	// a resolvable assignee, the executor's own stage and vault.
	terminalStatusTask := func(id lib.TaskIdentifier, status domain.TaskStatus) lib.Task {
		return lib.Task{
			TaskIdentifier: id,
			Vault:          testTaskVault,
			Frontmatter: lib.TaskFrontmatter{
				"status":   string(status),
				"phase":    string(domain.TaskPhaseExecution),
				"assignee": "claude",
				"stage":    "prod",
			},
		}
	}

	BeforeEach(func() {
		ctx = context.Background()
		fakeSpawner = new(mocks.FakeJobSpawner)
		fakeResolver = &mocks.FakeConfigResolver{}
		fakeResolver.ResolveReturns(permissiveConfig(), nil)
		fakeResultPublisher = &mocks.FakeResultPublisher{}
		fakeView = &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		taskStore = pkg.NewTaskStore()
		currentDateTime = libtime.NewCurrentDateTime()
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
			currentDateTime,
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeOff,
			nil,
		)
	})

	buildMsg := func(task lib.Task) *sarama.ConsumerMessage {
		value, err := json.Marshal(task)
		Expect(err).To(BeNil())
		return &sarama.ConsumerMessage{Value: value}
	}

	Describe("the event path", func() {
		DescribeTable("refuses a terminal status the Config lists",
			func(status domain.TaskStatus) {
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeSpawner.SpawnJobReturns("job-1", nil)

				before := testutil.ToFloat64(
					metrics.TaskEventsTotal.WithLabelValues("skipped_terminal_status"),
				)
				task := terminalStatusTask("tid-terminal-status", status)
				Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(Succeed())

				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
				after := testutil.ToFloat64(
					metrics.TaskEventsTotal.WithLabelValues("skipped_terminal_status"),
				)
				// The metric is what distinguishes this skip from the allow-list's:
				// both refuse the task, and only this one says why.
				Expect(after - before).To(Equal(float64(1)))
			},
			Entry("completed", domain.TaskStatusCompleted),
			Entry("aborted", domain.TaskStatusAborted),
		)

		It("still spawns the same task while it is in_progress (positive control)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("job-1", nil)

			task := terminalStatusTask("tid-terminal-status-control", domain.TaskStatusInProgress)
			Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(Succeed())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})
	})

	Describe("the reconcile candidate filter", func() {
		DescribeTable("keeps a terminal status out of the candidate set",
			func(status domain.TaskStatus) {
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeSpawner.SpawnJobReturns("job-1", nil)
				fakeView.SnapshotReturns([]lib.Task{
					terminalStatusTask("tid-terminal-reconcile", status),
				})

				Expect(h.ReconcileOnce(ctx)).To(Succeed())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			},
			Entry("completed", domain.TaskStatusCompleted),
			Entry("aborted", domain.TaskStatusAborted),
		)

		It("drives the same task while it is in_progress (positive control)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("job-1", nil)
			fakeView.SnapshotReturns([]lib.Task{
				terminalStatusTask("tid-terminal-reconcile-control", domain.TaskStatusInProgress),
			})

			Expect(h.ReconcileOnce(ctx)).To(Succeed())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})
	})
})
