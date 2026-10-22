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

			Expect(harness.client.DeleteCallCount()).To(Equal(1))
			_, key, sha := harness.client.DeleteArgsForCall(0)
			Expect(key).To(Equal("claims/task/task-a"))
			Expect(sha).To(Equal(shaPut("task-a")))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
			Expect(heldTaskIDs(manager)).To(ContainElement("task-b"))
			Expect(heldTaskIDs(manager)).NotTo(ContainElement("task-a"))
		})

		It("AC1(a) releases on success even when the task store holds no entry", func() {
			harness.holdClaim("task-a")
			before := snapshotClaims()

			harness.watcher().
				HandleJob(ctx, terminalJob("job-a", "task-a", succeededCondition()))

			Expect(harness.client.DeleteCallCount()).To(Equal(1))
			_, key, sha := harness.client.DeleteArgsForCall(0)
			Expect(key).To(Equal("claims/task/task-a"))
			Expect(sha).To(Equal(shaPut("task-a")))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
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

			harness.watcher().
				HandleJob(ctx, terminalJob("job-a", "task-a", succeededCondition()))

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

		// Pending until this branch is rebased onto #105. The claim gate is
		// spec 011 part 2 and is not on this base, so nothing takes a claim
		// before SpawnJob runs and there is nothing for the failure path to
		// release. The hook itself is in place; flip this to It on the rebase.
		PIt("AC1(e) releases the claim it just took when the spawn fails", func() {
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
