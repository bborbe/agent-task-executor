// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package probe_test

import (
	"context"
	"fmt"

	taskcmd "github.com/bborbe/agent/command/task"
	cqrsmocks "github.com/bborbe/cqrs/mocks"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/pkg/probe"
	"github.com/bborbe/agent-task-executor/pkg/probe/mocks"
)

var _ = Describe("HealthcheckRunner", func() {
	var (
		ctx            context.Context
		configProvider *mocks.FakeConfigProvider
		publisher      *mocks.FakeCommandPublisher
		runner         probe.HealthcheckRunner
	)

	BeforeEach(func() {
		ctx = context.Background()
		configProvider = new(mocks.FakeConfigProvider)
		publisher = new(mocks.FakeCommandPublisher)
		runner = probe.NewHealthcheckRunner(configProvider, publisher, "dev")
	})

	Context("N configs produce 2N commands in the expected order", func() {
		BeforeEach(func() {
			configProvider.GetReturns([]agentv1.Config{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "agent-a"},
					Spec:       agentv1.ConfigSpec{Assignee: "agent-a"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "agent-b"},
					Spec:       agentv1.ConfigSpec{Assignee: "agent-b"},
				},
			}, nil)
		})

		It("calls Publish exactly 4 times for 2 configs", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			Expect(publisher.PublishCallCount()).To(Equal(4))
		})

		It("first call is create-task for agent-a", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, op, _ := publisher.PublishArgsForCall(0)
			Expect(op).To(Equal("create-task"))
		})

		It("second call is update-frontmatter for agent-a", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, op, _ := publisher.PublishArgsForCall(1)
			Expect(op).To(Equal("update-frontmatter"))
		})

		It("third call is create-task for agent-b", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, op, _ := publisher.PublishArgsForCall(2)
			Expect(op).To(Equal("create-task"))
		})

		It("fourth call is update-frontmatter for agent-b", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, op, _ := publisher.PublishArgsForCall(3)
			Expect(op).To(Equal("update-frontmatter"))
		})

		It("returns no error", func() {
			Expect(runner.Run(ctx)).To(Succeed())
		})
	})

	Context("empty lister", func() {
		BeforeEach(func() {
			configProvider.GetReturns([]agentv1.Config{}, nil)
		})

		It("produces zero Publish calls", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			Expect(publisher.PublishCallCount()).To(Equal(0))
		})

		It("returns no error", func() {
			Expect(runner.Run(ctx)).To(Succeed())
		})
	})

	Context("create-task publish fails", func() {
		BeforeEach(func() {
			configProvider.GetReturns([]agentv1.Config{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "agent-a"},
					Spec:       agentv1.ConfigSpec{Assignee: "agent-a"},
				},
			}, nil)
			publisher.PublishReturnsOnCall(0, fmt.Errorf("kafka unavailable"))
		})

		It("returns a wrapped error", func() {
			err := runner.Run(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("kafka unavailable"))
		})

		It("makes exactly 1 Publish call — no rollback", func() {
			Expect(runner.Run(ctx)).To(HaveOccurred())
			Expect(publisher.PublishCallCount()).To(Equal(1))
		})
	})

	Context("update-frontmatter publish fails after create-task succeeded", func() {
		BeforeEach(func() {
			configProvider.GetReturns([]agentv1.Config{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "agent-a"},
					Spec:       agentv1.ConfigSpec{Assignee: "agent-a"},
				},
			}, nil)
			publisher.PublishReturnsOnCall(0, nil) // create-task succeeds
			publisher.PublishReturnsOnCall(
				1,
				fmt.Errorf("write timeout"),
			) // update-frontmatter fails
		})

		It("returns a wrapped error containing the timeout message", func() {
			err := runner.Run(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("write timeout"))
		})

		It("makes exactly 2 Publish calls — no rollback", func() {
			Expect(runner.Run(ctx)).To(HaveOccurred())
			Expect(publisher.PublishCallCount()).To(Equal(2))
		})
	})

	Context("CommandPublisher real implementation", func() {
		var (
			sender    *cqrsmocks.CDBCommandObjectSender
			publisher probe.CommandPublisher
		)

		BeforeEach(func() {
			sender = new(cqrsmocks.CDBCommandObjectSender)
			publisher = probe.NewCommandPublisher(sender)
		})

		It("publishes a command via the sender", func() {
			cmd := taskcmd.CreateCommand{
				TaskIdentifier: "probe-agent-a",
				Title:          "probe-agent-a",
				Frontmatter: map[string]interface{}{
					"phase": "planning",
				},
			}
			Expect(publisher.Publish(ctx, "create-task", cmd)).To(Succeed())
			Expect(sender.SendCommandObjectCallCount()).To(Equal(1))
		})

		It("propagates sender error", func() {
			sender.SendCommandObjectReturns(fmt.Errorf("broker down"))
			cmd := taskcmd.CreateCommand{
				TaskIdentifier: "probe-agent-a",
				Title:          "probe-agent-a",
			}
			err := publisher.Publish(ctx, "create-task", cmd)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("broker down"))
		})
	})

	Context("task IDs are deterministic UUIDv5s per agent (boundary contract)", func() {
		BeforeEach(func() {
			configProvider.GetReturns([]agentv1.Config{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "agent-a"},
					Spec:       agentv1.ConfigSpec{Assignee: "agent-a"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "agent-b"},
					Spec:       agentv1.ConfigSpec{Assignee: "agent-b"},
				},
			}, nil)
		})

		It("create-task task_identifier is a valid UUID string", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, _, payload := publisher.PublishArgsForCall(0)
			createCmd, ok := payload.(taskcmd.CreateCommand)
			Expect(ok).To(BeTrue())
			_, err := uuid.Parse(string(createCmd.TaskIdentifier))
			Expect(
				err,
			).NotTo(HaveOccurred(), "task_identifier %q must parse as a UUID", createCmd.TaskIdentifier)
		})

		It("repeated invocations produce identical task IDs per agent", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			Expect(runner.Run(ctx)).To(Succeed())

			_, _, agentACreate1 := publisher.PublishArgsForCall(0) // first run, agent-a create
			_, _, agentACreate2 := publisher.PublishArgsForCall(4) // second run, agent-a create

			cmd1, ok1 := agentACreate1.(taskcmd.CreateCommand)
			Expect(ok1).To(BeTrue())
			cmd2, ok2 := agentACreate2.(taskcmd.CreateCommand)
			Expect(ok2).To(BeTrue())
			Expect(cmd1.TaskIdentifier).To(Equal(cmd2.TaskIdentifier))
		})

		It("different agents produce different task IDs", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, _, agentACreate := publisher.PublishArgsForCall(0)
			_, _, agentBCreate := publisher.PublishArgsForCall(2)
			cmdA, okA := agentACreate.(taskcmd.CreateCommand)
			Expect(okA).To(BeTrue())
			cmdB, okB := agentBCreate.(taskcmd.CreateCommand)
			Expect(okB).To(BeTrue())
			Expect(cmdA.TaskIdentifier).NotTo(Equal(cmdB.TaskIdentifier))
		})

		It("title still uses the human-readable form", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, _, payload := publisher.PublishArgsForCall(0)
			createCmd, ok := payload.(taskcmd.CreateCommand)
			Expect(ok).To(BeTrue())
			Expect(createCmd.Title).To(Equal("probe-agent-a-dev"))
		})
	})

	Context("per-stage identity (boundary contract)", func() {
		BeforeEach(func() {
			configProvider.GetReturns([]agentv1.Config{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "agent-a"},
					Spec:       agentv1.ConfigSpec{Assignee: "agent-a"},
				},
			}, nil)
		})

		It("title includes the stage suffix", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, _, payload := publisher.PublishArgsForCall(0)
			createCmd, ok := payload.(taskcmd.CreateCommand)
			Expect(ok).To(BeTrue())
			Expect(createCmd.Title).To(Equal("probe-agent-a-dev"))
		})

		It("create-task frontmatter includes stage field matching the runner's branch", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, _, payload := publisher.PublishArgsForCall(0)
			createCmd, ok := payload.(taskcmd.CreateCommand)
			Expect(ok).To(BeTrue())
			Expect(createCmd.Frontmatter).To(HaveKeyWithValue("stage", "dev"))
		})

		It("create-task frontmatter has phase in_progress", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, _, payload := publisher.PublishArgsForCall(0)
			createCmd, ok := payload.(taskcmd.CreateCommand)
			Expect(ok).To(BeTrue())
			Expect(createCmd.Frontmatter).To(HaveKeyWithValue("phase", "in_progress"))
		})

		It("update-frontmatter has phase in_progress", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, _, payload := publisher.PublishArgsForCall(1)
			updateCmd, ok := payload.(taskcmd.UpdateFrontmatterCommand)
			Expect(ok).To(BeTrue())
			Expect(updateCmd.Updates).To(HaveKeyWithValue("phase", "in_progress"))
		})

		It("update-frontmatter resets status to in_progress (spec AC line 76)", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, _, payload := publisher.PublishArgsForCall(1)
			updateCmd, ok := payload.(taskcmd.UpdateFrontmatterCommand)
			Expect(ok).To(BeTrue())
			Expect(updateCmd.Updates).To(HaveKeyWithValue("status", "in_progress"))
		})

		It(
			"update-frontmatter clears prior-run executor run-state so re-trigger does not respawn",
			func() {
				// Regression: reused probe files carried a stale current_job/job_started_at
				// from the previous run, defeating the executor grace window and causing
				// 2-3 respawns per probe. The re-trigger must reset all run-state fields.
				Expect(runner.Run(ctx)).To(Succeed())
				_, _, payload := publisher.PublishArgsForCall(1)
				updateCmd, ok := payload.(taskcmd.UpdateFrontmatterCommand)
				Expect(ok).To(BeTrue())
				Expect(updateCmd.Updates).To(HaveKeyWithValue("current_job", ""))
				Expect(updateCmd.Updates).To(HaveKeyWithValue("job_started_at", ""))
				Expect(updateCmd.Updates).To(HaveKeyWithValue("spawn_notification", false))
				Expect(updateCmd.Updates).To(HaveKeyWithValue("trigger_count", 0))
			},
		)

		It("probeTaskID is a pure function of (agent, stage) — boundary contract", func() {
			// Direct unit-level boundary test per spec AC line 75:
			// probeTaskID must be a pure function (no state, no randomness) so
			// every caller passing the same (agent, stage) gets the same UUID,
			// including across a process restart.
			// We can't import the package-private probeTaskID directly here, so
			// we drive it via two fresh runners and compare the published TaskIdentifiers.
			agentName := "boundary-agent"
			configProvider.GetReturns([]agentv1.Config{
				{
					ObjectMeta: metav1.ObjectMeta{Name: agentName},
					Spec:       agentv1.ConfigSpec{Assignee: agentName},
				},
			}, nil)

			pubA := new(mocks.FakeCommandPublisher)
			pubB := new(mocks.FakeCommandPublisher)
			runnerA := probe.NewHealthcheckRunner(configProvider, pubA, "dev")
			runnerB := probe.NewHealthcheckRunner(configProvider, pubB, "dev")
			Expect(runnerA.Run(ctx)).To(Succeed())
			Expect(runnerB.Run(ctx)).To(Succeed())

			_, _, payloadA := pubA.PublishArgsForCall(0)
			_, _, payloadB := pubB.PublishArgsForCall(0)
			cmdA, okA2 := payloadA.(taskcmd.CreateCommand)
			Expect(okA2).To(BeTrue())
			cmdB, okB2 := payloadB.(taskcmd.CreateCommand)
			Expect(okB2).To(BeTrue())

			Expect(cmdA.TaskIdentifier).To(Equal(cmdB.TaskIdentifier),
				"probeTaskID must be a pure function of (agent, stage); same inputs → same UUID")
		})

		It("dev and prod runners produce different task IDs for the same agent", func() {
			devRunner := probe.NewHealthcheckRunner(configProvider, publisher, "dev")
			prodPublisher := new(mocks.FakeCommandPublisher)
			configProvider.GetReturns([]agentv1.Config{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "agent-a"},
					Spec:       agentv1.ConfigSpec{Assignee: "agent-a"},
				},
			}, nil)
			prodRunner := probe.NewHealthcheckRunner(configProvider, prodPublisher, "prod")

			Expect(devRunner.Run(ctx)).To(Succeed())
			Expect(prodRunner.Run(ctx)).To(Succeed())

			_, _, devPayload := publisher.PublishArgsForCall(0)
			_, _, prodPayload := prodPublisher.PublishArgsForCall(0)
			devCmd, okDev := devPayload.(taskcmd.CreateCommand)
			Expect(okDev).To(BeTrue())
			prodCmd, okProd := prodPayload.(taskcmd.CreateCommand)
			Expect(okProd).To(BeTrue())

			Expect(devCmd.TaskIdentifier).NotTo(Equal(prodCmd.TaskIdentifier),
				"dev and prod probes for the same agent must have different task identifiers")
		})
	})

	Context("emitted commands satisfy library validation (boundary contract)", func() {
		BeforeEach(func() {
			configProvider.GetReturns([]agentv1.Config{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "agent-a"},
					Spec:       agentv1.ConfigSpec{Assignee: "agent-a"},
				},
			}, nil)
		})

		It("the create-task payload passes CreateCommand.Validate", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, _, payload := publisher.PublishArgsForCall(0)
			createCmd, ok := payload.(taskcmd.CreateCommand)
			Expect(ok).To(BeTrue(), "payload at call 0 must be a CreateCommand")
			Expect(createCmd.Validate(ctx)).To(Succeed())
		})

		It("the update-frontmatter payload passes UpdateFrontmatterCommand.Validate", func() {
			Expect(runner.Run(ctx)).To(Succeed())
			_, _, payload := publisher.PublishArgsForCall(1)
			updateCmd, ok := payload.(taskcmd.UpdateFrontmatterCommand)
			Expect(ok).To(BeTrue(), "payload at call 1 must be an UpdateFrontmatterCommand")
			Expect(updateCmd.Validate(ctx)).To(Succeed())
		})
	})
})
