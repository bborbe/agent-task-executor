// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	stderrors "errors"
	"strings"

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
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// reconcileGateIndexStub is a counting claim.VaultAPIIndexClient double. It is
// declared here rather than in a shared helper file so this suite is
// self-contained: copying it alone into an older worktree still compiles.
type reconcileGateIndexStub struct {
	items []claim.TaskIndexItem
	err   error
	reads int
}

// Read counts the call and returns the configured index or error.
func (s *reconcileGateIndexStub) Read(context.Context) (*claim.TaskIndex, error) {
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

var _ = Describe("reconcile index-terminal gate", func() {
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
		fakeResultPublisher = &mocks.FakeResultPublisher{}
		fakeView = &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		taskStore = pkg.NewTaskStore()
		currentDateTime = libtime.NewCurrentDateTime()
	})

	// reconcileGateBuild mirrors the reconcile fixture's construction. The
	// returned structural interface avoids naming a handler type that may not
	// exist in an older worktree. A nil client means no WithVaultAPIIndexClient
	// option, the no-URL deployment's shape.
	reconcileGateBuild := func(
		view *mocks.FakeTaskView,
		client claim.VaultAPIIndexClient,
	) interface{ ReconcileOnce(context.Context) error } {
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
				taskStore,
				currentDateTime,
				view,
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
			taskStore,
			currentDateTime,
			view,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeOff,
			nil,
			handler.WithVaultAPIIndexClient(client),
		)
	}

	reconcileGateTask := func(id, status, phase string) lib.Task {
		return lib.Task{
			TaskIdentifier: lib.TaskIdentifier(id),
			Vault:          testTaskVault,
			Frontmatter: lib.TaskFrontmatter{
				"status":   status,
				"phase":    phase,
				"assignee": "claude",
				"stage":    "prod",
			},
		}
	}

	reconcileGateConfig := func(phases ...domain.TaskPhase) pkg.AgentConfiguration {
		return pkg.AgentConfiguration{
			Assignee: "claude",
			Image:    "my-image:latest",
			Trigger: &agentv1.Trigger{
				Statuses: domain.TaskStatuses{
					domain.TaskStatusInProgress,
					domain.TaskStatusCompleted,
					domain.TaskStatusAborted,
				},
				Phases: domain.TaskPhases(phases),
			},
		}
	}

	reconcileGateSpawnedIDs := func() []string {
		ids := make([]string, 0, fakeSpawner.SpawnJobCallCount())
		for i := 0; i < fakeSpawner.SpawnJobCallCount(); i++ {
			_, task, _ := fakeSpawner.SpawnJobArgsForCall(i)
			ids = append(ids, string(task.TaskIdentifier))
		}
		return ids
	}

	It("suppresses a candidate whose index status is terminal (AC 2)", func() {
		fakeView.SnapshotReturns([]lib.Task{
			reconcileGateTask("tid-a", "in_progress", "execution"),
			reconcileGateTask("tid-b", "in_progress", "execution"),
			reconcileGateTask("tid-c", "in_progress", "execution"),
		})
		fakeResolver.ResolveReturns(reconcileGateConfig(domain.TaskPhaseExecution), nil)
		stub := &reconcileGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-a", Status: "completed", Phase: "done"},
			{ID: "tid-b", Status: "aborted", Phase: "done"},
			{ID: "tid-c", Status: "in_progress", Phase: "execution"},
		}}
		h := reconcileGateBuild(fakeView, stub)

		Expect(h.ReconcileOnce(ctx)).To(Succeed())

		ids := reconcileGateSpawnedIDs()
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1), "spawned ids: %v", ids)
		Expect(ids).To(Equal([]string{"tid-c"}))
	})

	It("suppresses a candidate whose index phase is terminal", func() {
		fakeView.SnapshotReturns([]lib.Task{
			reconcileGateTask("tid-a", "in_progress", "execution"),
		})
		fakeResolver.ResolveReturns(reconcileGateConfig(domain.TaskPhaseExecution), nil)
		stub := &reconcileGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-a", Status: "in_progress", Phase: "human_review"},
		}}
		h := reconcileGateBuild(fakeView, stub)

		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("still reports a non-terminal divergence (AC 9)", func() {
		fakeView.SnapshotReturns([]lib.Task{
			reconcileGateTask("tid-a", "in_progress", "execution"),
		})
		fakeResolver.ResolveReturns(reconcileGateConfig(domain.TaskPhaseExecution), nil)
		stub := &reconcileGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-a", Status: "in_progress", Phase: "ai_review"},
		}}
		h := reconcileGateBuild(fakeView, stub)

		before := testutil.ToFloat64(
			metrics.ViewDivergenceTotal.WithLabelValues("mismatch"),
		)
		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(strings.Count(out, "event=view_divergence task=")).To(Equal(1))
		Expect(
			testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("mismatch")),
		).To(Equal(before + 1))
	})

	It("does not over-suppress a task whose index state is not terminal (AC 10)", func() {
		fakeView.SnapshotReturns([]lib.Task{
			reconcileGateTask("tid-a", "in_progress", "ai_review"),
		})
		fakeResolver.ResolveReturns(
			reconcileGateConfig(domain.TaskPhaseExecution, domain.TaskPhaseAIReview),
			nil,
		)
		stub := &reconcileGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-a", Status: "in_progress", Phase: "execution"},
		}}
		h := reconcileGateBuild(fakeView, stub)

		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("fails open with no index client (AC 6)", func() {
		fakeView.SnapshotReturns([]lib.Task{
			reconcileGateTask("tid-a", "in_progress", "execution"),
		})
		fakeResolver.ResolveReturns(reconcileGateConfig(domain.TaskPhaseExecution), nil)
		h := reconcileGateBuild(fakeView, nil)

		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("fails open when the index read fails (AC 7)", func() {
		fakeView.SnapshotReturns([]lib.Task{
			reconcileGateTask("tid-a", "in_progress", "execution"),
		})
		fakeResolver.ResolveReturns(reconcileGateConfig(domain.TaskPhaseExecution), nil)
		stub := &reconcileGateIndexStub{err: stderrors.New("index read failed")}
		h := reconcileGateBuild(fakeView, stub)

		before := testutil.ToFloat64(
			metrics.ViewDivergenceTotal.WithLabelValues("read_error"),
		)
		_ = captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(
			testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("read_error")),
		).To(Equal(before + 1))
	})

	It("shares one index read between the gate and the divergence check (AC 15a)", func() {
		fakeView.SnapshotReturns([]lib.Task{
			reconcileGateTask("tid-a", "in_progress", "execution"),
		})
		fakeResolver.ResolveReturns(reconcileGateConfig(domain.TaskPhaseExecution), nil)
		stub := &reconcileGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-a", Status: "completed", Phase: "human_review"},
		}}
		h := reconcileGateBuild(fakeView, stub)

		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(out).To(ContainSubstring("event=view_divergence_summary"))
		Expect(stub.reads).To(Equal(1))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("makes no index call when the view is not ready (AC 15b)", func() {
		fakeView.ReadyReturns(false)
		stub := &reconcileGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-a", Status: "completed", Phase: "done"},
		}}
		h := reconcileGateBuild(fakeView, stub)

		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(stub.reads).To(Equal(0))
	})
})
