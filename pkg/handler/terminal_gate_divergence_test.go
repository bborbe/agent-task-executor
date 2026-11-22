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

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/handler"
)

// terminalGateIndexStub is a counting claim.VaultAPIIndexClient double. It is
// declared here rather than in a shared helper file so this suite is
// self-contained: copying it alone into an older worktree still compiles.
type terminalGateIndexStub struct {
	items []claim.TaskIndexItem
	err   error
	reads int
}

// Read counts the call and returns the configured index or error.
func (s *terminalGateIndexStub) Read(context.Context) (*claim.TaskIndex, error) {
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

var _ = Describe("terminal gate divergence", func() {
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

	// terminalGateBuild mirrors the reconcile fixture's construction. A nil
	// client means no WithVaultAPIIndexClient option, the no-URL deployment's
	// shape.
	terminalGateBuild := func(
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

	terminalGateTask := func(id, status, phase string) lib.Task {
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

	// terminalGateConfig lists only the statuses a live re-arm produces. Listing
	// completed/aborted here would let a stale view pass the trigger-status gate
	// on its own terminal status and mask an override that never took effect.
	terminalGateConfig := func(phases ...domain.TaskPhase) pkg.AgentConfiguration {
		return pkg.AgentConfiguration{
			Assignee: "claude",
			Image:    "my-image:latest",
			Trigger: &agentv1.Trigger{
				Statuses: domain.TaskStatuses{
					domain.TaskStatusInProgress,
				},
				Phases: domain.TaskPhases(phases),
			},
		}
	}

	terminalGateSpawnedIDs := func() []string {
		ids := make([]string, 0, fakeSpawner.SpawnJobCallCount())
		for i := 0; i < fakeSpawner.SpawnJobCallCount(); i++ {
			_, task, _, _ := fakeSpawner.SpawnJobArgsForCall(i)
			ids = append(ids, string(task.TaskIdentifier))
		}
		return ids
	}

	It("evaluates a terminal view over a non-terminal vault (AC 6)", func() {
		// The incident's exact shape: the view is completed/done and the vault
		// has been re-armed to in_progress/execution. Both pairs must reach the
		// spawn — a gate-only override would be refused one line later on the
		// stale phase.
		fakeView.SnapshotReturns([]lib.Task{
			terminalGateTask("tid-a", "completed", "done"),
		})
		fakeResolver.ResolveReturns(terminalGateConfig(domain.TaskPhaseExecution), nil)
		stub := &terminalGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-a", Status: "in_progress", Phase: "execution"},
		}}
		h := terminalGateBuild(fakeView, stub)

		out := deferredGateCaptureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(strings.Count(out, "event=reconcile_refused")).To(Equal(0))
		Expect(
			fakeSpawner.SpawnJobCallCount(),
		).To(Equal(1), "spawned ids: %v", terminalGateSpawnedIDs())
		Expect(terminalGateSpawnedIDs()).To(Equal([]string{"tid-a"}))
		_, spawned, _, _ := fakeSpawner.SpawnJobArgsForCall(0)
		Expect(spawned.Frontmatter.Status()).To(Equal(domain.TaskStatusInProgress))
		Expect(spawned.Frontmatter.Phase()).NotTo(BeNil())
		Expect(*spawned.Frontmatter.Phase()).To(Equal(domain.TaskPhaseExecution))
		Expect(strings.Count(out, "event=terminal_gate_divergence")).To(Equal(1))
		Expect(out).To(ContainSubstring("view_status=completed"))
		Expect(out).To(ContainSubstring("vault_status=in_progress"))
		Expect(out).To(ContainSubstring("decision_source=vault"))
	})

	It("still refuses a terminal view over a terminal vault (AC 7)", func() {
		fakeView.SnapshotReturns([]lib.Task{
			terminalGateTask("tid-a", "completed", "execution"),
		})
		fakeResolver.ResolveReturns(terminalGateConfig(domain.TaskPhaseExecution), nil)
		stub := &terminalGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-a", Status: "completed", Phase: "execution"},
		}}
		h := terminalGateBuild(fakeView, stub)

		out := deferredGateCaptureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(strings.Count(out, "event=reconcile_refused")).To(Equal(1))
		Expect(out).To(ContainSubstring("gate=terminal_status"))
		Expect(strings.Count(out, "event=terminal_gate_divergence")).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("falls back to view-only when the index has no entry", func() {
		fakeView.SnapshotReturns([]lib.Task{
			terminalGateTask("tid-a", "completed", "execution"),
		})
		fakeResolver.ResolveReturns(terminalGateConfig(domain.TaskPhaseExecution), nil)
		stub := &terminalGateIndexStub{}
		h := terminalGateBuild(fakeView, stub)

		out := deferredGateCaptureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(strings.Count(out, "event=reconcile_refused")).To(Equal(1))
		Expect(out).To(ContainSubstring("gate=terminal_status"))
		Expect(strings.Count(out, "event=terminal_gate_divergence")).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("falls back to view-only when the index read fails (failure mode)", func() {
		fakeView.SnapshotReturns([]lib.Task{
			terminalGateTask("tid-a", "completed", "execution"),
		})
		fakeResolver.ResolveReturns(terminalGateConfig(domain.TaskPhaseExecution), nil)
		stub := &terminalGateIndexStub{err: stderrors.New("index read failed")}
		h := terminalGateBuild(fakeView, stub)

		out := deferredGateCaptureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(strings.Count(out, "event=reconcile_refused")).To(Equal(1))
		Expect(out).To(ContainSubstring("gate=terminal_status"))
		Expect(strings.Count(out, "event=terminal_gate_divergence")).To(Equal(0))
		Expect(
			strings.Count(out, "event=terminal_gate_index_unavailable gate=terminal_status"),
		).To(Equal(1))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("is silent with no index client", func() {
		fakeView.SnapshotReturns([]lib.Task{
			terminalGateTask("tid-a", "completed", "execution"),
		})
		fakeResolver.ResolveReturns(terminalGateConfig(domain.TaskPhaseExecution), nil)
		h := terminalGateBuild(fakeView, nil)

		out := deferredGateCaptureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(strings.Count(out, "event=reconcile_refused")).To(Equal(1))
		Expect(out).To(ContainSubstring("gate=terminal_status"))
		Expect(strings.Count(out, "event=terminal_gate_divergence")).To(Equal(0))
		Expect(strings.Count(out, "event=terminal_gate_index_unavailable")).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("shares one index read across the gate, the index gate and the divergence check", func() {
		fakeView.SnapshotReturns([]lib.Task{
			terminalGateTask("tid-a", "completed", "done"),
		})
		fakeResolver.ResolveReturns(terminalGateConfig(domain.TaskPhaseExecution), nil)
		stub := &terminalGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-a", Status: "in_progress", Phase: "execution"},
		}}
		h := terminalGateBuild(fakeView, stub)

		out := deferredGateCaptureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(out).To(ContainSubstring("event=view_divergence_summary"))
		Expect(stub.reads).To(Equal(1))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("refuses when the vault phase is terminal even though its status is not", func() {
		// indexTerminalIDs treats status OR phase as terminal; the override must
		// apply the same rule or it would release a task the candidate gate
		// suppressed for the same state.
		fakeView.SnapshotReturns([]lib.Task{
			terminalGateTask("tid-a", "completed", "done"),
		})
		fakeResolver.ResolveReturns(terminalGateConfig(domain.TaskPhaseExecution), nil)
		stub := &terminalGateIndexStub{items: []claim.TaskIndexItem{
			{ID: "tid-a", Status: "in_progress", Phase: "done"},
		}}
		h := terminalGateBuild(fakeView, stub)

		out := deferredGateCaptureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(strings.Count(out, "event=reconcile_refused")).To(Equal(1))
		Expect(out).To(ContainSubstring("gate=terminal_status"))
		Expect(strings.Count(out, "event=terminal_gate_divergence")).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("logs the index-unavailable line once per pass, not once per task", func() {
		fakeView.SnapshotReturns([]lib.Task{
			terminalGateTask("tid-a", "completed", "done"),
			terminalGateTask("tid-b", "completed", "done"),
			terminalGateTask("tid-c", "completed", "done"),
		})
		fakeResolver.ResolveReturns(terminalGateConfig(domain.TaskPhaseExecution), nil)
		stub := &terminalGateIndexStub{err: stderrors.New("index read failed")}
		h := terminalGateBuild(fakeView, stub)

		out := deferredGateCaptureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(
			strings.Count(out, "event=terminal_gate_index_unavailable gate=terminal_status"),
		).To(Equal(1))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})
})
