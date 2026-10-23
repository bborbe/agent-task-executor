// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	stderrors "errors"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// heldTaskIDs lists the task ids the manager currently holds.
func heldTaskIDs(manager claim.LeaseManager) []string {
	held := manager.HeldClaims()
	ids := make([]string, 0, len(held))
	for _, h := range held {
		ids = append(ids, h.TaskID)
	}
	return ids
}

// shaPut returns the sha the fake ref client reports for a created claim.
func shaPut(taskID string) string { return "sha-" + taskID }

// terminalStatusTask is a qualifying task whose frontmatter status is status.
func terminalStatusTask(taskID, status string) lib.Task {
	task := qualifyingClaimTask()
	task.TaskIdentifier = lib.TaskIdentifier(taskID)
	task.Frontmatter["status"] = status
	return task
}

// terminalPhaseTask is a qualifying task whose frontmatter phase is phase.
func terminalPhaseTask(taskID, phase string) lib.Task {
	task := qualifyingClaimTask()
	task.TaskIdentifier = lib.TaskIdentifier(taskID)
	task.Frontmatter["phase"] = phase
	return task
}

var _ = Describe("claim release at the Job-watcher end points", func() {
	var (
		ctx     context.Context
		harness *claimHarness
	)

	BeforeEach(func() {
		ctx = context.Background()
	})

	When("the claim mode is shadow", func() {
		BeforeEach(func() {
			harness = newClaimHarness(pkg.ClaimModeShadow)
			harness.client.PutStub = func(
				_ context.Context, _, _, _, taskID string, _ libtime.DateTime,
			) (string, error) {
				return shaPut(taskID), nil
			}
		})

		It("AC1(a) releases only the claim of the Job that succeeded", func() {
			harness.holdClaim("task-a")
			harness.holdClaim("task-b")
			manager, err := harness.eventHandler.ClaimLeaseManager(ctx)
			Expect(err).NotTo(HaveOccurred())
			before := snapshotClaims()

			harness.watcher().
				HandleJob(ctx, terminalJob("job-a", "task-a", succeededCondition()))

			// A succeeded Job no longer deletes its claim: it is held for
			// claim.SuccessGracePeriod, so a peer cannot claim the task while the
			// result is still on its way to the vault. The release that follows the
			// window is pinned by TestReleaseAfterSuccessHoldsTheClaimAgainstAPeer.
			Expect(harness.client.DeleteCallCount()).To(Equal(0))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{})
			Expect(heldTaskIDs(manager)).To(ContainElement("task-b"))
			Expect(heldTaskIDs(manager)).To(ContainElement("task-a"))
		})

		It("AC1(a) releases on success even when the task store holds no entry", func() {
			harness.holdClaim("task-a")
			before := snapshotClaims()

			harness.watcher().
				HandleJob(ctx, terminalJob("job-a", "task-a", succeededCondition()))

			Expect(harness.client.DeleteCallCount()).To(Equal(0))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{})
		})

		It("does not release the claim of a newer Job on a stale terminal event", func() {
			harness.holdClaim("task-a")
			task := qualifyingClaimTask()
			task.TaskIdentifier = lib.TaskIdentifier("task-a")
			task.Frontmatter["current_job"] = "job-new"
			harness.taskStore.Store(lib.TaskIdentifier("task-a"), task)
			manager, err := harness.eventHandler.ClaimLeaseManager(ctx)
			Expect(err).NotTo(HaveOccurred())
			before := snapshotClaims()

			harness.watcher().
				HandleJob(ctx, terminalJob("job-old", "task-a", succeededCondition()))

			Expect(harness.client.DeleteCallCount()).To(Equal(0))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{})
			Expect(heldTaskIDs(manager)).To(ContainElement("task-a"))
		})

		It("releases when the store entry tracks this very Job", func() {
			harness.holdClaim("task-a")
			task := qualifyingClaimTask()
			task.TaskIdentifier = lib.TaskIdentifier("task-a")
			task.Frontmatter["current_job"] = "job-a"
			harness.taskStore.Store(lib.TaskIdentifier("task-a"), task)
			before := snapshotClaims()

			harness.watcher().
				HandleJob(ctx, terminalJob("job-a", "task-a", succeededCondition()))

			Expect(harness.client.DeleteCallCount()).To(Equal(0))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{})
		})

		It("AC1(b) releases only the claim of the Job that failed", func() {
			harness.holdClaim("task-a")
			harness.holdClaim("task-b")
			manager, err := harness.eventHandler.ClaimLeaseManager(ctx)
			Expect(err).NotTo(HaveOccurred())
			before := snapshotClaims()

			harness.watcher().
				HandleJob(ctx, terminalJob("job-a", "task-a", failedCondition("OOMKilled")))

			Expect(harness.client.DeleteCallCount()).To(Equal(1))
			_, key, sha := harness.client.DeleteArgsForCall(0)
			Expect(key).To(Equal("claims/task/task-a"))
			Expect(sha).To(Equal(shaPut("task-a")))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
			Expect(heldTaskIDs(manager)).To(ContainElement("task-b"))
		})

		It("AC2 leaves a failed release to the lease loop's next tick", func() {
			harness.holdClaim("task-a")
			manager, err := harness.eventHandler.ClaimLeaseManager(ctx)
			Expect(err).NotTo(HaveOccurred())
			harness.client.DeleteReturnsOnCall(0, stderrors.New("git-rest throttled"))
			before := snapshotClaims()

			// A FAILED Job still releases at once — only a success holds — so this
			// stays the spec for "a release that failed is retried on the next tick".
			harness.watcher().
				HandleJob(ctx, terminalJob("job-a", "task-a", failedCondition("OOMKilled")))

			Expect(harness.client.DeleteCallCount()).To(Equal(1))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"error": 1})
			Expect(heldTaskIDs(manager)).To(ContainElement("task-a"))

			afterRelease := snapshotClaims()
			Expect(manager.Tick(ctx)).To(Succeed())
			Expect(harness.client.DeleteCallCount()).To(Equal(2))
			expectClaimDelta(afterRelease, snapshotClaims(), map[string]float64{"released": 1})
			Expect(heldTaskIDs(manager)).NotTo(ContainElement("task-a"))
		})
	})

	When("the claim mode is on", func() {
		BeforeEach(func() {
			harness = newClaimHarness(pkg.ClaimModeOn)
			harness.client.PutStub = func(
				_ context.Context, _, _, _, taskID string, _ libtime.DateTime,
			) (string, error) {
				return shaPut(taskID), nil
			}
		})

		It("AC1(e) releases the claim it just took when the spawn fails", func() {
			harness.spawner.SpawnJobReturns("", stderrors.New("job create failed"))
			before := snapshotClaims()

			err := harness.consume(qualifyingClaimTask())
			Expect(err).To(HaveOccurred())

			Expect(harness.client.PutCallCount()).To(Equal(1))
			Expect(harness.client.DeleteCallCount()).To(Equal(1))
			_, key, sha := harness.client.DeleteArgsForCall(0)
			Expect(key).To(Equal("claims/task/" + string(qualifyingClaimTask().TaskIdentifier)))
			Expect(sha).To(Equal(shaPut(string(qualifyingClaimTask().TaskIdentifier))))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{
				"claimed":  1,
				"released": 1,
			})
		})

		It("AC2 releases nothing for a task it holds no claim for", func() {
			before := snapshotClaims()

			harness.eventHandler.ReleaseTaskClaim(ctx, lib.TaskIdentifier("unknown-task"))

			Expect(harness.client.DeleteCallCount()).To(Equal(0))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{})
		})
	})
})

var _ = Describe("claim release at the vault end points", func() {
	var (
		ctx     context.Context
		harness *claimHarness
	)

	BeforeEach(func() {
		ctx = context.Background()
		harness = newClaimHarness(pkg.ClaimModeShadow)
		harness.client.PutStub = func(
			_ context.Context, _, _, _, taskID string, _ libtime.DateTime,
		) (string, error) {
			return shaPut(taskID), nil
		}
	})

	It("AC1(c) releases only the claim of the task whose status is completed", func() {
		harness.holdClaim("task-a")
		harness.holdClaim("task-b")
		manager, err := harness.eventHandler.ClaimLeaseManager(ctx)
		Expect(err).NotTo(HaveOccurred())
		before := snapshotClaims()

		Expect(harness.consume(terminalStatusTask("task-a", "completed"))).To(Succeed())

		Expect(harness.client.DeleteCallCount()).To(Equal(1))
		_, key, sha := harness.client.DeleteArgsForCall(0)
		Expect(key).To(Equal("claims/task/task-a"))
		Expect(sha).To(Equal(shaPut("task-a")))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
		Expect(heldTaskIDs(manager)).To(ContainElement("task-b"))
		Expect(heldTaskIDs(manager)).NotTo(ContainElement("task-a"))
	})

	It("AC1(c) releases on aborted the same way", func() {
		harness.holdClaim("task-a")
		harness.holdClaim("task-b")
		manager, err := harness.eventHandler.ClaimLeaseManager(ctx)
		Expect(err).NotTo(HaveOccurred())
		before := snapshotClaims()

		Expect(harness.consume(terminalStatusTask("task-a", "aborted"))).To(Succeed())

		Expect(harness.client.DeleteCallCount()).To(Equal(1))
		_, key, sha := harness.client.DeleteArgsForCall(0)
		Expect(key).To(Equal("claims/task/task-a"))
		Expect(sha).To(Equal(shaPut("task-a")))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
		Expect(heldTaskIDs(manager)).NotTo(ContainElement("task-a"))
	})

	It("AC1(d) releases only the claim of the task whose phase is human_review", func() {
		harness.holdClaim("task-a")
		harness.holdClaim("task-b")
		manager, err := harness.eventHandler.ClaimLeaseManager(ctx)
		Expect(err).NotTo(HaveOccurred())
		before := snapshotClaims()

		Expect(harness.consume(terminalPhaseTask("task-a", "human_review"))).To(Succeed())

		Expect(harness.client.DeleteCallCount()).To(Equal(1))
		_, key, sha := harness.client.DeleteArgsForCall(0)
		Expect(key).To(Equal("claims/task/task-a"))
		Expect(sha).To(Equal(shaPut("task-a")))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
		Expect(heldTaskIDs(manager)).To(ContainElement("task-b"))
		Expect(heldTaskIDs(manager)).NotTo(ContainElement("task-a"))
	})

	It("AC1(d) releases on the done phase the same way", func() {
		harness.holdClaim("task-a")
		harness.holdClaim("task-b")
		manager, err := harness.eventHandler.ClaimLeaseManager(ctx)
		Expect(err).NotTo(HaveOccurred())
		before := snapshotClaims()

		Expect(harness.consume(terminalPhaseTask("task-a", "done"))).To(Succeed())

		Expect(harness.client.DeleteCallCount()).To(Equal(1))
		_, key, sha := harness.client.DeleteArgsForCall(0)
		Expect(key).To(Equal("claims/task/task-a"))
		Expect(sha).To(Equal(shaPut("task-a")))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
		Expect(heldTaskIDs(manager)).NotTo(ContainElement("task-a"))
	})

	It("AC2 leaves a failed release on the vault path to the tick", func() {
		harness.holdClaim("task-a")
		manager, err := harness.eventHandler.ClaimLeaseManager(ctx)
		Expect(err).NotTo(HaveOccurred())
		harness.client.DeleteReturnsOnCall(0, stderrors.New("git-rest throttled"))
		before := snapshotClaims()

		Expect(harness.consume(terminalStatusTask("task-a", "completed"))).To(Succeed())

		Expect(harness.client.DeleteCallCount()).To(Equal(1))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"error": 1})
		Expect(heldTaskIDs(manager)).To(ContainElement("task-a"))
	})

	It("AC1(c) releases once when the same end event arrives twice", func() {
		harness.holdClaim("task-a")
		before := snapshotClaims()

		Expect(harness.consume(terminalStatusTask("task-a", "completed"))).To(Succeed())
		Expect(harness.consume(terminalStatusTask("task-a", "completed"))).To(Succeed())

		Expect(harness.client.DeleteCallCount()).To(Equal(1))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
	})

	It("does not release on a stale terminal-phase event for a newer Job", func() {
		harness.holdClaim("task-a")
		event := terminalPhaseTask("task-a", "human_review")
		event.Frontmatter["current_job"] = "job-old"
		stored := qualifyingClaimTask()
		stored.TaskIdentifier = lib.TaskIdentifier("task-a")
		stored.Frontmatter["current_job"] = "job-new"
		harness.taskStore.Store(lib.TaskIdentifier("task-a"), stored)
		before := snapshotClaims()

		Expect(harness.consume(event)).To(Succeed())

		Expect(harness.client.DeleteCallCount()).To(Equal(0))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{})
	})
})
