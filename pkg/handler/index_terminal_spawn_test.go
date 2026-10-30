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
	libtimetest "github.com/bborbe/time/test"
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// spawnGateIndexStub is a counting claim.VaultAPIIndexClient double, declared
// here so this suite stays self-contained.
type spawnGateIndexStub struct {
	items []claim.TaskIndexItem
	err   error
	reads int
}

// Read counts the call and returns the configured index or error.
func (s *spawnGateIndexStub) Read(context.Context) (*claim.TaskIndex, error) {
	s.reads++
	if s.err != nil {
		return nil, s.err
	}
	return &claim.TaskIndex{
		Items:           s.items,
		Total:           len(s.items),
		IndexAgeSeconds: 1,
	}, nil
}

// The spawn-path gate is the fourth spawn path: a task event reaches
// spawnIfNeededClaimed directly from the Kafka consumer, so the deferred and
// reconcile gates never see it. Measured 2026-10-30 — nuke prod recorded success
// for 406feb87 at 11:51:26Z and sun created a Job for it at 11:52:15Z, with no
// suppression line on either cluster, because nothing on this path asked the
// index.
var _ = Describe("spawn-path index-terminal gate", func() {
	const pastGrace = "2026-05-17T09:40:00Z" // T+360s, outside SuccessGracePeriod

	var (
		ctx                 context.Context
		fakeSpawner         *mocks.FakeJobSpawner
		fakeResolver        *mocks.FakeConfigResolver
		fakeResultPublisher *mocks.FakeResultPublisher
		fakeView            *mocks.FakeTaskView
		taskStore           *pkg.TaskStore
		currentDateTime     libtime.CurrentDateTime
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeSpawner = new(mocks.FakeJobSpawner)
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeSpawner.SpawnJobReturns("job-1", nil)
		fakeResolver = &mocks.FakeConfigResolver{}
		fakeResolver.ResolveReturns(pkg.AgentConfiguration{
			Assignee: "claude",
			Image:    "my-image:latest",
			Trigger: &agentv1.Trigger{
				Statuses: domain.TaskStatuses{
					domain.TaskStatusInProgress,
					domain.TaskStatusCompleted,
					domain.TaskStatusAborted,
				},
				Phases: domain.TaskPhases{domain.TaskPhaseExecution},
			},
		}, nil)
		fakeResultPublisher = &mocks.FakeResultPublisher{}
		fakeView = &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		taskStore = pkg.NewTaskStore()
		currentDateTime = libtime.NewCurrentDateTime()
		currentDateTime.SetNow(libtimetest.ParseDateTime(pastGrace))
	})

	spawnGateBuild := func(client claim.VaultAPIIndexClient) handler.TaskEventHandler {
		return handler.NewTaskEventHandler(
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
			handler.WithVaultAPIIndexClient(client),
		)
	}

	// No current_job: the foreign-Job guard must not be what suppresses the
	// spawn, or the spec would pass without the index gate existing.
	spawnGateTask := func(id string) lib.Task {
		return lib.Task{
			TaskIdentifier: lib.TaskIdentifier(id),
			Vault:          testTaskVault,
			Frontmatter: lib.TaskFrontmatter{
				"status":        "in_progress",
				"phase":         string(domain.TaskPhaseExecution),
				"trigger_scope": string(domain.TaskPhaseExecution) + ":",
				"assignee":      "claude",
				"stage":         "prod",
			},
		}
	}

	spawnGateMessage := func(task lib.Task) *sarama.ConsumerMessage {
		value, err := json.Marshal(task)
		Expect(err).ToNot(HaveOccurred())
		return &sarama.ConsumerMessage{Value: value}
	}

	indexTerminalCounter := func() float64 {
		return testutil.ToFloat64(
			metrics.ReconcileRefusedTotal.WithLabelValues(
				metrics.ReconcileRefusedGateIndexTerminal,
			),
		)
	}

	It("refuses a task event the vault index reports terminal, and counts it", func() {
		stub := &spawnGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-terminal", Status: "completed", Phase: "done"},
		}}
		h := spawnGateBuild(stub)

		before := indexTerminalCounter()
		Expect(h.ConsumeMessage(ctx, spawnGateMessage(spawnGateTask("tid-terminal")))).
			To(Succeed())

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		Expect(indexTerminalCounter() - before).To(Equal(float64(1)))
		Expect(stub.reads).To(BeNumerically(">", 0))
	})

	// The control. Without it the spec above would pass on a handler that never
	// spawns anything, which is the failure mode an absence assertion hides.
	It("still spawns a task event the index reports workable", func() {
		stub := &spawnGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-live", Status: "in_progress", Phase: "execution"},
		}}
		h := spawnGateBuild(stub)

		Expect(h.ConsumeMessage(ctx, spawnGateMessage(spawnGateTask("tid-live")))).
			To(Succeed())

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		_, spawnedTask, _ := fakeSpawner.SpawnJobArgsForCall(0)
		Expect(string(spawnedTask.TaskIdentifier)).To(Equal("tid-live"))
	})
})
