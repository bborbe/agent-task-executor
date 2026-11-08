// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	stderrors "errors"
	"flag"
	"io"
	"os"
	"strings"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libk8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// ghostCaptureLog runs fn with glog writing to a pipe at V(2) — the level the
// deployed executors run at — and returns everything written. Mirrors
// deferredGateCaptureStderr; defined locally so this file stays self-contained.
func ghostCaptureLog(fn func()) string {
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

// ghostBaseTask is the record every spec starts from. phase human_review is
// deliberately not in the default trigger phases, so the drive loop never
// spawns it and every publisher call in the pass comes from the ghost step.
func ghostBaseTask() lib.Task {
	return lib.Task{
		TaskIdentifier: "ghost-task-1",
		Vault:          testTaskVault,
		Frontmatter: lib.TaskFrontmatter{
			"status":      "in_progress",
			"phase":       string(domain.TaskPhaseHumanReview),
			"assignee":    "claude",
			"stage":       "prod",
			"current_job": "ghost-job-1",
		},
	}
}

// ghostHarness is the real handler wired with the fakes the decision table
// varies.
type ghostHarness struct {
	h           handler.TaskEventHandler
	spawner     *mocks.FakeJobSpawner
	resolver    *mocks.FakeConfigResolver
	publisher   *mocks.FakeResultPublisher
	claimClient *mocks.FakeVaultAPIClaimClient
	view        *mocks.FakeTaskView
	store       *pkg.TaskStore
}

// newGhostHarness builds a handler whose defaults let a clean ghost record be
// cleared exactly once.
func newGhostHarness(mode pkg.ClaimMode, stageFilter bool) *ghostHarness {
	spawner := new(mocks.FakeJobSpawner)
	spawner.ListActiveJobsReturns(map[lib.TaskIdentifier]bool{}, nil)
	spawner.IsJobActiveReturns(false, nil)
	spawner.JobExistsReturns(false, nil)

	resolver := new(mocks.FakeConfigResolver)
	resolver.ResolveReturns(pkg.AgentConfiguration{
		Assignee: "claude",
		Image:    "my-image:latest",
	}, nil)

	publisher := new(mocks.FakeResultPublisher)
	claimClient := new(mocks.FakeVaultAPIClaimClient)
	claimClient.ReadReturns(nil, claim.ErrClaimFree)

	view := new(mocks.FakeTaskView)
	view.ReadyReturns(true)
	view.GetReturns(lib.Task{}, false)
	view.SnapshotReturns(nil)

	store := pkg.NewTaskStore()
	h := handler.NewTaskEventHandler(
		spawner,
		"dev",
		base.Branch("prod"),
		"",
		stageFilter,
		true,
		resolver,
		publisher,
		store,
		libtime.NewCurrentDateTime(),
		view,
		testTaskVault,
		handler.DefaultReconcilePassTimeout,
		handler.DefaultReconcileInterval,
		mode,
		func() claim.ClaimRefClient { return new(mocks.FakeClaimRefClient) },
		handler.WithClaimJobControl(noopClaimJobControl{}),
		handler.WithClaimClientFactory(func() claim.ClaimClient { return claimClient }),
	)
	return &ghostHarness{
		h:           h,
		spawner:     spawner,
		resolver:    resolver,
		publisher:   publisher,
		claimClient: claimClient,
		view:        view,
		store:       store,
	}
}

// ghostTaskEvents reads one TaskEventsTotal label.
func ghostTaskEvents(label string) float64 {
	return testutil.ToFloat64(metrics.TaskEventsTotal.WithLabelValues(label))
}

// ghostSkippedUnknown reads one SkippedUnknownAssigneeTotal label.
func ghostSkippedUnknown(label string) float64 {
	return testutil.ToFloat64(metrics.SkippedUnknownAssigneeTotal.WithLabelValues(label))
}

var _ = Describe("ghost current_job", func() {
	var (
		ctx      context.Context
		harness  *ghostHarness
		baseTask lib.Task
	)

	BeforeEach(func() {
		ctx = context.Background()
		harness = newGhostHarness(pkg.ClaimModeOn, true)
		baseTask = ghostBaseTask()
	})

	runPass := func() string {
		return ghostCaptureLog(func() {
			Expect(harness.h.ReconcileOnce(ctx)).To(Succeed())
		})
	}

	It("clears a current_job whose Job is gone everywhere (row a)", func() {
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(1))
		_, task, jobName := harness.publisher.PublishClearCurrentJobArgsForCall(0)
		Expect(jobName).To(Equal("ghost-job-1"))
		Expect(string(task.TaskIdentifier)).To(Equal("ghost-task-1"))
		Expect(out).To(ContainSubstring(
			"event=ghost_current_job_cleared task=ghost-task-1 job=ghost-job-1",
		))
		Expect(out).To(ContainSubstring(
			"event=ghost_current_job_pass scanned=1 cleared=1 kept=0 unreached=0",
		))
	})

	It("keeps the field when the named Job still exists (row b)", func() {
		harness.spawner.JobExistsReturns(true, nil)
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(harness.claimClient.ReadCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("guard=job_exists"))
	})

	It("keeps the field when an active Job exists for the task (row c1)", func() {
		harness.spawner.ListActiveJobsReturns(
			map[lib.TaskIdentifier]bool{"ghost-task-1": true},
			nil,
		)
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(harness.spawner.JobExistsCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("guard=job_active"))
	})

	It("keeps the field when the active-Job read names the task by type (row c2)", func() {
		baseTask.Frontmatter["assignee"] = ""
		baseTask.Frontmatter["task_type"] = "ghost-type"
		harness.resolver.ResolveByTaskTypeReturns(
			pkg.AgentConfiguration{Assignee: "", Image: "my-image:latest"},
			nil,
		)
		harness.spawner.IsJobActiveReturns(true, nil)
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(harness.spawner.JobExistsCallCount()).To(Equal(0))
		Expect(harness.spawner.IsJobActiveCallCount()).To(BeNumerically(">=", 1))
		Expect(out).To(ContainSubstring("guard=job_active"))
	})

	It("keeps the field when the claim is held (row d)", func() {
		harness.claimClient.ReadReturns(&claim.Claim{
			Holder:     "sun",
			LeaseUntil: libtime.DateTime(time.Now().Add(2 * time.Minute)),
		}, nil)
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("guard=claim_held"))
	})

	It("keeps the field when the claim read fails (row e1)", func() {
		harness.claimClient.ReadReturns(nil, stderrors.New("vault-api down"))
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("guard=claim_unprobeable"))
	})

	It("keeps the field when claim mode takes no claim (row e2)", func() {
		harness = newGhostHarness(pkg.ClaimModeOff, true)
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("guard=claim_unprobeable"))
	})

	It("keeps the field when the named-Job read fails (row f1)", func() {
		harness.spawner.JobExistsReturns(false, stderrors.New("api down"))
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("guard=read_failed"))
	})

	It("keeps the field when the active-Job read fails (row f2)", func() {
		harness.spawner.ListActiveJobsReturns(nil, stderrors.New("api down"))
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("guard=read_failed"))
	})

	It("keeps the field when a store entry appears during the claim read (row g)", func() {
		harness.claimClient.ReadCalls(
			func(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
				harness.store.Store("ghost-task-1", baseTask)
				return nil, claim.ErrClaimFree
			},
		)
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("guard=store_entry"))
		// The store entry was inserted after the first local check, so only the
		// locked re-check can see it: the probe must have run exactly once.
		Expect(harness.claimClient.ReadCallCount()).To(Equal(1))
	})

	It("ignores a completed record (row i1)", func() {
		baseTask.Frontmatter["status"] = "completed"
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(harness.spawner.JobExistsCallCount()).To(Equal(0))
		Expect(out).ToNot(ContainSubstring("ghost_current_job_cleared task=ghost-task-1"))
		Expect(out).ToNot(ContainSubstring("ghost_current_job_kept task=ghost-task-1"))
		Expect(out).To(ContainSubstring("scanned=0"))
	})

	It("ignores an aborted record (row i2)", func() {
		baseTask.Frontmatter["status"] = "aborted"
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(harness.spawner.JobExistsCallCount()).To(Equal(0))
		Expect(out).ToNot(ContainSubstring("ghost_current_job_cleared task=ghost-task-1"))
		Expect(out).ToNot(ContainSubstring("ghost_current_job_kept task=ghost-task-1"))
		Expect(out).To(ContainSubstring("scanned=0"))
	})

	It("ignores a record on another stage (row j1)", func() {
		baseTask.Frontmatter["stage"] = "dev"
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(harness.spawner.JobExistsCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("scanned=0"))
	})

	It("ignores another stage even when STAGE_FILTER is off (row j2)", func() {
		harness = newGhostHarness(pkg.ClaimModeOn, false)
		baseTask.Frontmatter["stage"] = "dev"
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(harness.spawner.JobExistsCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("scanned=0"))
	})

	It("ignores a record with no current_job (row k)", func() {
		baseTask.Frontmatter["current_job"] = ""
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(harness.spawner.JobExistsCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("scanned=0"))
	})

	It("keeps the field when the assignee is unknown (row l1)", func() {
		baseTask.Frontmatter["assignee"] = "ghost-unknown-agent"
		harness.resolver.ResolveReturns(pkg.AgentConfiguration{}, pkg.ErrConfigNotFound)
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		beforeEvents := ghostTaskEvents("skipped_unknown_assignee")
		beforeSkipped := ghostSkippedUnknown("ghost-unknown-agent")

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(harness.spawner.JobExistsCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("guard=namespace_unresolved"))
		// The gather's own increments; a counting step would make them 2.
		Expect(ghostTaskEvents("skipped_unknown_assignee") - beforeEvents).To(Equal(1.0))
		Expect(ghostSkippedUnknown("ghost-unknown-agent") - beforeSkipped).To(Equal(1.0))
	})

	It("keeps the field when the task_type resolves to no config (row l2)", func() {
		baseTask.Frontmatter["assignee"] = ""
		baseTask.Frontmatter["task_type"] = "ghost-type"
		harness.resolver.ResolveByTaskTypeReturns(pkg.AgentConfiguration{}, pkg.ErrConfigNotFound)
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		beforeEvents := ghostTaskEvents("skipped_unknown_assignee")
		beforeErrors := ghostTaskEvents("error")

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("guard=namespace_unresolved"))
		Expect(ghostTaskEvents("skipped_unknown_assignee") - beforeEvents).To(Equal(1.0))
		Expect(ghostTaskEvents("error") - beforeErrors).To(Equal(0.0))
	})

	It("keeps the field when the Config's Job namespace is invalid (row l3)", func() {
		harness.resolver.ResolveReturns(pkg.AgentConfiguration{
			Assignee:     "claude",
			Image:        "my-image:latest",
			JobNamespace: "dev",
		}, nil)
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		beforeEvents := ghostTaskEvents("skipped_unknown_assignee")
		beforeErrors := ghostTaskEvents("error")

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(harness.spawner.JobExistsCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("guard=namespace_unresolved"))
		Expect(ghostTaskEvents("skipped_unknown_assignee") - beforeEvents).To(Equal(0.0))
		Expect(ghostTaskEvents("error") - beforeErrors).To(Equal(0.0))
	})

	It("keeps the field and retries when the publish fails (row m)", func() {
		harness.publisher.PublishClearCurrentJobReturns(stderrors.New("kafka down"))
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := ghostCaptureLog(func() {
			Expect(harness.h.ReconcileOnce(ctx)).To(Succeed())
			Expect(harness.h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(2))
		Expect(strings.Count(out, "guard=publish_failed")).To(Equal(2))
	})

	It("ignores a record tagged for another cluster (row n)", func() {
		baseTask.Frontmatter["cluster"] = "sun"
		harness.view.SnapshotReturns([]lib.Task{baseTask})

		out := runPass()

		Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		Expect(harness.spawner.JobExistsCallCount()).To(Equal(0))
		Expect(out).To(ContainSubstring("scanned=0"))
	})

	Context("ghost current_job pass summary", func() {
		It("emits one summary line per zero-candidate pass", func() {
			harness.view.SnapshotReturns(nil)

			out := ghostCaptureLog(func() {
				Expect(harness.h.ReconcileOnce(ctx)).To(Succeed())
				Expect(harness.h.ReconcileOnce(ctx)).To(Succeed())
			})

			Expect(strings.Count(
				out,
				"event=ghost_current_job_pass scanned=0 cleared=0 kept=0 unreached=0",
			)).To(Equal(2))
		})

		It("emits no summary line when the view is not ready", func() {
			harness.view.ReadyReturns(false)

			out := runPass()

			Expect(strings.Count(out, "event=ghost_current_job_pass")).To(Equal(0))
			Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(0))
		})

		It("stops on expiry and counts the unreached candidates", func() {
			secondTask := ghostBaseTask()
			secondTask.TaskIdentifier = "ghost-task-2"
			secondTask.Frontmatter["current_job"] = "ghost-job-2"
			harness.view.SnapshotReturns([]lib.Task{baseTask, secondTask})

			passCtx, cancel := context.WithCancel(context.Background())
			harness.spawner.JobExistsCalls(
				func(context.Context, libk8s.Namespace, string) (bool, error) {
					cancel()
					return false, nil
				},
			)

			out := ghostCaptureLog(func() {
				Expect(harness.h.ReconcileOnce(passCtx)).To(Succeed())
			})

			Expect(out).To(ContainSubstring(
				"event=ghost_current_job_pass scanned=1 cleared=1 kept=0 unreached=1",
			))
			Expect(harness.publisher.PublishClearCurrentJobCallCount()).To(Equal(1))
			Expect(out).ToNot(ContainSubstring("ghost_current_job_cleared task=ghost-task-2"))
			Expect(out).ToNot(ContainSubstring("ghost_current_job_kept task=ghost-task-2"))
		})
	})
})
