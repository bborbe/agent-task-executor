// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// capturePinReportLog runs fn with glog writing to a pipe at V(2) — the level
// the deployed executors run at — and returns everything written. The suite
// runs serially, so the global os.Stderr swap cannot race another spec's
// logging.
func capturePinReportLog(fn func()) string {
	Expect(flag.Set("logtostderr", "true")).To(Succeed())
	Expect(flag.Set("v", "2")).To(Succeed())
	defer func() {
		Expect(flag.Set("v", "0")).To(Succeed())
		Expect(flag.Set("logtostderr", "false")).To(Succeed())
	}()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	Expect(err).ToNot(HaveOccurred())
	os.Stderr = writer

	fn()
	glog.Flush()

	os.Stderr = original
	Expect(writer.Close()).To(Succeed())
	out, err := io.ReadAll(reader)
	Expect(err).ToNot(HaveOccurred())
	Expect(reader.Close()).To(Succeed())
	return string(out)
}

// A task pinned to a cluster whose pool does not contain the task's stage can
// never run there, and before this change nothing recorded it: the pin is not
// foreign (so the foreign-cluster skip does not fire) and it is not runnable,
// so the task vanished without a trace anywhere in the fleet. Observed
// 2026-10-23: canary 8523953f sat 19+ min with no trigger_count in any cluster.
var _ = Describe("A cluster pin outside its stage pool", func() {
	var (
		ctx                 context.Context
		fakeSpawner         *mocks.FakeJobSpawner
		fakeResolver        *mocks.FakeConfigResolver
		fakeResultPublisher *mocks.FakeResultPublisher
		fakeView            *mocks.FakeTaskView
		taskStore           *pkg.TaskStore
		h                   handler.TaskEventHandler
	)

	// buildHandler models sun: it is a prod-pool member (BRANCH=prod,
	// STAGE_FILTER=true) that also accepts untagged tasks (RUN_UNTAGGED=true),
	// which is what lets a task with no cluster pin reach the stage filter at
	// all — the case the guard below must not mislabel.
	buildHandler := func(
		cluster string,
		stageFilter bool,
		runUntagged bool,
	) handler.TaskEventHandler {
		return handler.NewTaskEventHandler(
			fakeSpawner,
			"dev",
			base.Branch("prod"),
			cluster,
			stageFilter,
			runUntagged,
			fakeResolver,
			fakeResultPublisher,
			taskStore,
			libtime.NewCurrentDateTime(),
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeOff,
			nil,
		)
	}

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
		taskStore = pkg.NewTaskStore()
		h = buildHandler("sun", true, true)
	})

	// routedTask builds a task that passes every gate ahead of the stage filter,
	// so the spec exercises the branch under test rather than an earlier skip.
	routedTask := func(frontmatter lib.TaskFrontmatter) lib.Task {
		return lib.Task{
			TaskIdentifier: "pin-canary-1",
			Vault:          testTaskVault,
			Frontmatter:    frontmatter,
		}
	}

	buildMsg := func(task lib.Task) *sarama.ConsumerMessage {
		value, err := json.Marshal(task)
		Expect(err).To(BeNil())
		return &sarama.ConsumerMessage{Value: value}
	}

	pinCount := func() float64 {
		return testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("pin_outside_pool"),
		)
	}

	stageSkipCount := func() float64 {
		return testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("skipped_stage"),
		)
	}

	Describe("on the Kafka path", func() {
		It("reports a pin naming its own cluster whose stage is outside its pool", func() {
			task := routedTask(lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(domain.TaskPhaseExecution),
				"assignee": "claude",
				"cluster":  "sun",
				"stage":    "dev",
			})

			beforePin := pinCount()
			beforeStage := stageSkipCount()

			out := capturePinReportLog(func() {
				Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())
			})

			Expect(out).To(ContainSubstring("event=pin_outside_pool"))
			Expect(out).To(ContainSubstring("pin-canary-1"))
			Expect(pinCount()).To(Equal(beforePin + 1))
			// The report replaces skipped_stage for this branch: the metric's
			// `result` label carries one outcome per event, so a second
			// increment would double-count the same skip.
			Expect(stageSkipCount()).To(Equal(beforeStage))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("keeps skipped_stage for a task with no cluster pin", func() {
			task := routedTask(lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(domain.TaskPhaseExecution),
				"assignee": "claude",
				"stage":    "dev",
			})

			beforePin := pinCount()
			beforeStage := stageSkipCount()

			Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())

			// An untagged task whose stage is wrong is a plain stage skip, not a
			// dead pin — reporting it would fire on every untagged task on a
			// stage-filtering executor.
			Expect(pinCount()).To(Equal(beforePin))
			Expect(stageSkipCount()).To(Equal(beforeStage + 1))
		})

		It("keeps skipped_stage when the pin names a cluster other than this executor", func() {
			// A foreign pin never reaches the stage filter (the foreign-cluster
			// skip returns first), so this asserts the guard cannot be reached by
			// that route either.
			task := routedTask(lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(domain.TaskPhaseExecution),
				"assignee": "claude",
				"cluster":  "nuke",
				"stage":    "dev",
			})

			beforePin := pinCount()

			Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())

			Expect(pinCount()).To(Equal(beforePin))
		})
	})

	Describe("on the reconcile floor", func() {
		It("reports the same pin", func() {
			// The floor has no live Kafka event behind it: it re-derives from the
			// in-memory view on every pass, so Kafka-path coverage says nothing
			// about it.
			fakeView.SnapshotReturns([]lib.Task{
				routedTask(lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
					"cluster":  "sun",
					"stage":    "dev",
				}),
			})
			fakeSpawner.IsJobActiveReturns(false, nil)

			beforePin := pinCount()

			out := capturePinReportLog(func() {
				Expect(h.ReconcileOnce(ctx)).To(BeNil())
			})

			Expect(out).To(ContainSubstring("event=pin_outside_pool"))
			Expect(pinCount()).To(Equal(beforePin + 1))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})
	})
})
