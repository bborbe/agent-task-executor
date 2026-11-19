// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// notReadyClock is a settable CurrentDateTimeGetter, so a test can cross the
// stuck threshold without waiting for it.
type notReadyClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *notReadyClock) Now() libtime.DateTime {
	c.mu.Lock()
	defer c.mu.Unlock()
	return libtime.DateTime(c.now)
}

func (c *notReadyClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

var _ = Describe("TaskEventHandler view-not-ready hand-off", func() {
	var (
		ctx             context.Context
		fakeSpawner     *mocks.FakeJobSpawner
		fakeResolver    *mocks.FakeConfigResolver
		fakeView        *mocks.FakeTaskView
		clock           *notReadyClock
		h               handler.TaskEventHandler
		eligibleTask    func(phase domain.TaskPhase) lib.Task
		buildMsg        func(t lib.Task) *sarama.ConsumerMessage
		stuckCount      func() float64
		notReadyCount   func() float64
		viewNotReadyCap int
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeSpawner = new(mocks.FakeJobSpawner)
		fakeResolver = &mocks.FakeConfigResolver{}
		fakeResolver.ResolveReturns(
			pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"},
			nil,
		)
		fakeView = &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		clock = &notReadyClock{now: time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)}
		viewNotReadyCap = 512

		h = handler.NewTaskEventHandler(
			fakeSpawner,
			"dev",
			base.Branch("prod"),
			"",
			true,
			true,
			fakeResolver,
			&mocks.FakeResultPublisher{},
			pkg.NewTaskStore(),
			clock,
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeOff,
			nil,
		)

		eligibleTask = func(phase domain.TaskPhase) lib.Task {
			return lib.Task{
				TaskIdentifier: "tid-a",
				Vault:          testTaskVault,
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(phase),
					"assignee": "claude",
					"stage":    "prod",
				},
			}
		}

		buildMsg = func(t lib.Task) *sarama.ConsumerMessage {
			if t.Vault == "" {
				t.Vault = testTaskVault
			}
			value, err := json.Marshal(t)
			Expect(err).To(BeNil())
			return &sarama.ConsumerMessage{Value: value}
		}

		stuckCount = func() float64 {
			return testutil.ToFloat64(
				metrics.ReconcilePassesTotal.WithLabelValues("view_not_ready_stuck"),
			)
		}
		notReadyCount = func() float64 {
			return testutil.ToFloat64(
				metrics.ReconcilePassesTotal.WithLabelValues("view_not_ready"),
			)
		}
	})

	It("drives an event dropped while not ready once readiness arrives, with no snapshot", func() {
		fakeView.ReadyReturns(false)
		task := eligibleTask(domain.TaskPhaseExecution)
		task.TaskIdentifier = "tid-dropped"
		// An empty snapshot is the point: the reconcile floor alone would drive
		// nothing, so a spawn can only come from the held event.
		fakeView.SnapshotReturns(nil)

		Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

		fakeView.ReadyReturns(true)
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeSpawner.SpawnJobReturns("job-1", nil)

		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(out).To(ContainSubstring("event=task_view_not_ready_drained pending=1 spawned=1"))
	})

	It("drains each dropped event once, not on every later pass", func() {
		fakeView.ReadyReturns(false)
		task := eligibleTask(domain.TaskPhaseExecution)
		task.TaskIdentifier = "tid-once"
		fakeView.SnapshotReturns(nil)
		Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(Succeed())

		fakeView.ReadyReturns(true)
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeSpawner.SpawnJobReturns("job-1", nil)
		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))

		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("keeps reporting view_not_ready below the stuck threshold", func() {
		fakeView.ReadyReturns(false)
		beforeStuck := stuckCount()

		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})
		Expect(out).To(ContainSubstring("event=reconcile_view_not_ready task_vault=openclaw"))
		Expect(out).NotTo(ContainSubstring("task_view_not_ready_stuck"))
		Expect(stuckCount()).To(Equal(beforeStuck))
	})

	It("reports a stuck view once not-ready passes the threshold", func() {
		fakeView.ReadyReturns(false)
		beforeStuck := stuckCount()
		beforeNotReady := notReadyCount()

		captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})
		Expect(notReadyCount()).To(Equal(beforeNotReady + 1))

		clock.Advance(20 * time.Minute)
		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})
		Expect(out).To(ContainSubstring("event=task_view_not_ready_stuck task_vault=openclaw"))
		Expect(out).To(ContainSubstring("pending=0"))
		Expect(stuckCount()).To(Equal(beforeStuck + 1))
	})

	It("bounds the held events at the cap and warns when it overflows", func() {
		fakeView.ReadyReturns(false)
		fakeView.SnapshotReturns(nil)

		// Filled outside captureStderr: that helper drains the pipe only after fn
		// returns, so hundreds of log lines written inside it fill the pipe buffer
		// and deadlock the spec.
		for i := 0; i < viewNotReadyCap; i++ {
			task := eligibleTask(domain.TaskPhaseExecution)
			task.TaskIdentifier = lib.TaskIdentifier(fmt.Sprintf("tid-%d", i))
			Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(Succeed())
		}

		// The next one overflows the cap and is the single line captured.
		overflow := eligibleTask(domain.TaskPhaseExecution)
		overflow.TaskIdentifier = "tid-overflow"
		out := captureStderr(func() {
			Expect(h.ConsumeMessage(ctx, buildMsg(overflow))).To(Succeed())
		})
		Expect(out).To(ContainSubstring("event=task_view_not_ready_pending_full"))
		Expect(out).To(ContainSubstring("cap=512"))

		fakeView.ReadyReturns(true)
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeSpawner.SpawnJobReturns("job-1", nil)
		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(viewNotReadyCap))
	})
})
