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
	"strings"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"
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

// deferredGateIndexStub is a counting claim.VaultAPIIndexClient double. It is
// declared here rather than in a shared helper file so this suite is
// self-contained: copying it alone into an older worktree still compiles.
type deferredGateIndexStub struct {
	items []claim.TaskIndexItem
	err   error
	reads int
}

// Read counts the call and returns the configured index or error.
func (s *deferredGateIndexStub) Read(context.Context) (*claim.TaskIndex, error) {
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

// deferredGateConfig is the Config every spec in this suite resolves to. The
// trigger lists in_progress/completed/aborted and execution so the seed's
// tasks are eligible on their own frontmatter values.
func deferredGateConfig() pkg.AgentConfiguration {
	return pkg.AgentConfiguration{
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
	}
}

// buildDeferredGateMessage encodes task as a Kafka consumer message.
func buildDeferredGateMessage(task lib.Task) *sarama.ConsumerMessage {
	value, err := json.Marshal(task)
	Expect(err).ToNot(HaveOccurred())
	return &sarama.ConsumerMessage{Value: value}
}

// deferredGateCaptureStderr runs fn with glog writing to a pipe at V(2) — the
// level the deployed executors run at — and returns everything written. Mirrors
// capturePinReportLog; defined locally so this file stays self-contained.
func deferredGateCaptureStderr(fn func()) string {
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

var _ = Describe("deferred index-terminal gate", func() {
	const (
		anchorTime  = "2026-05-17T09:34:00Z" // T+0
		insideGrace = "2026-05-17T09:34:59Z" // T+59s
		pastGrace   = "2026-05-17T09:40:00Z" // T+360s
	)

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
		fakeResolver.ResolveReturns(deferredGateConfig(), nil)
		fakeResultPublisher = &mocks.FakeResultPublisher{}
		fakeView = &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		taskStore = pkg.NewTaskStore()
		currentDateTime = libtime.NewCurrentDateTime()
	})

	// deferredGateBuild builds the real handler over the given store. A nil
	// client means no WithVaultAPIIndexClient option, the no-URL deployment shape.
	deferredGateBuild := func(
		store *pkg.TaskStore,
		client claim.VaultAPIIndexClient,
	) handler.TaskEventHandler {
		if client == nil {
			return handler.NewTaskEventHandler(
				fakeSpawner,
				"dev",
				base.Branch("prod"),
				"",
				true,
				true,
				fakeResolver,
				fakeResultPublisher,
				store,
				currentDateTime,
				fakeView,
				testTaskVault,
				handler.DefaultReconcilePassTimeout,
				handler.DefaultReconcileInterval,
				pkg.ClaimModeOff,
				nil,
			)
		}
		return handler.NewTaskEventHandler(
			fakeSpawner,
			"dev",
			base.Branch("prod"),
			"",
			true,
			true,
			fakeResolver,
			fakeResultPublisher,
			store,
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

	deferredGateTask := func(id string) lib.Task {
		return lib.Task{
			TaskIdentifier: lib.TaskIdentifier(id),
			Vault:          testTaskVault,
			Frontmatter: lib.TaskFrontmatter{
				"status":         "in_progress",
				"phase":          string(domain.TaskPhaseExecution),
				"trigger_scope":  string(domain.TaskPhaseExecution) + ":",
				"assignee":       "claude",
				"stage":          "prod",
				"current_job":    "job-" + id,
				"job_started_at": anchorTime,
			},
		}
	}

	deferredGateSpawnedIDs := func() []string {
		ids := make([]string, 0, fakeSpawner.SpawnJobCallCount())
		for i := 0; i < fakeSpawner.SpawnJobCallCount(); i++ {
			_, task, _ := fakeSpawner.SpawnJobArgsForCall(i)
			ids = append(ids, string(task.TaskIdentifier))
		}
		return ids
	}

	// deferredGateParkInsideGrace drives a task through the Kafka path inside the
	// grace window, so the spawn funnel parks it as a deferred entry.
	deferredGateParkInsideGrace := func(h handler.TaskEventHandler, id string) {
		currentDateTime.SetNow(libtimetest.ParseDateTime(insideGrace))
		Expect(h.ConsumeMessage(ctx, buildDeferredGateMessage(deferredGateTask(id)))).
			To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	}

	It("suppresses the seeded entries the vault reports terminal (AC 3)", func() {
		stub := &deferredGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-a", Status: "completed", Phase: "execution"},
			{ID: "tid-b", Status: "aborted", Phase: "execution"},
			{ID: "tid-c", Status: "in_progress", Phase: "execution"},
		}}
		seedStore := pkg.NewTaskStore()
		for _, id := range []string{"tid-a", "tid-b", "tid-c"} {
			task := deferredGateTask(id)
			seedStore.Store(task.TaskIdentifier, task)
		}
		currentDateTime.SetNow(libtimetest.ParseDateTime(pastGrace))
		h := deferredGateBuild(seedStore, stub)

		loopCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- h.RunDeferredRespawnLoop(loopCtx) }()
		// The initial eval runs synchronously before the ticker starts, so
		// cancelling after the first spawn is observed is safe. Never assert the
		// exact count inside Eventually: the pre-change commit spawns all three.
		Eventually(func() int {
			return fakeSpawner.SpawnJobCallCount()
		}).Should(BeNumerically(">=", 1))
		cancel()
		Expect(<-done).To(BeNil())

		ids := deferredGateSpawnedIDs()
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1), "spawned ids: %v", ids)
		Expect(ids).To(Equal([]string{"tid-c"}))
	})

	It("fails open with no index client (AC 6)", func() {
		h := deferredGateBuild(taskStore, nil)
		deferredGateParkInsideGrace(h, "tid-open")

		currentDateTime.SetNow(libtimetest.ParseDateTime(pastGrace))
		Expect(h.EvalDeferredRespawns(ctx)).To(Succeed())

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("fails open when the index read fails, without a divergence read_error (AC 8)", func() {
		stub := &deferredGateIndexStub{err: io.ErrUnexpectedEOF}
		h := deferredGateBuild(taskStore, stub)
		deferredGateParkInsideGrace(h, "tid-readerr")

		before := testutil.ToFloat64(
			metrics.ViewDivergenceTotal.WithLabelValues("read_error"),
		)
		currentDateTime.SetNow(libtimetest.ParseDateTime(pastGrace))
		out := deferredGateCaptureStderr(func() {
			Expect(h.EvalDeferredRespawns(ctx)).To(Succeed())
		})

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(
			testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("read_error")),
		).To(Equal(before))
		Expect(strings.Count(out, "event=view_divergence_read_error")).To(Equal(0))
	})

	It("reads the index at most once for three due entries (AC 15c)", func() {
		stub := &deferredGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-d1", Status: "in_progress", Phase: "execution"},
			{ID: "tid-d2", Status: "in_progress", Phase: "execution"},
			{ID: "tid-d3", Status: "in_progress", Phase: "execution"},
		}}
		h := deferredGateBuild(taskStore, stub)
		deferredGateParkInsideGrace(h, "tid-d1")
		deferredGateParkInsideGrace(h, "tid-d2")
		deferredGateParkInsideGrace(h, "tid-d3")

		currentDateTime.SetNow(libtimetest.ParseDateTime(pastGrace))
		Expect(h.EvalDeferredRespawns(ctx)).To(Succeed())

		Expect(stub.reads).To(Equal(1))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(3))
	})

	It("makes no index read when nothing is due (AC 15d)", func() {
		stub := &deferredGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-e1", Status: "completed", Phase: "execution"},
		}}
		h := deferredGateBuild(pkg.NewTaskStore(), stub)

		Expect(h.EvalDeferredRespawns(ctx)).To(Succeed())
		Expect(stub.reads).To(Equal(0))
	})
})
