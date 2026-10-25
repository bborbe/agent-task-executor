// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	"net/http"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

var _ = Describe("Dual claim restart recovery", func() {
	var (
		ctx   context.Context
		fake  *fakeGitRest
		clock libtime.CurrentDateTime
		job   *fakeJobControl
		mgr   claim.LeaseManager
	)

	const (
		taskID  = "t-dual"
		prRef   = "refs/claims/pr/bborbe-nuke-658"
		repoRef = "refs/claims/repo/bborbe-nuke"
	)

	far := func() string {
		return rfc3339(leaseTestT0.Add(1000 * libtime.Second))
	}

	seedPair := func() {
		fake.Seed(repoRef, "SP", "self", taskID, far())
		fake.Seed(prRef, "S-PR", "self", taskID, far())
	}

	BeforeEach(func() {
		ctx = context.Background()
		fake = newFakeGitRest()
		clock = newTestClock(leaseTestT0)
		job = newFakeJobControl(clock)
		mgr = newTestManager("self", claim.NewClaimRefClient(fake.URL(), "", ""), job, clock)
	})

	AfterEach(func() {
		fake.Close()
	})

	It("AC7(a) adopts both refs of a running pair without a PUT", func() {
		seedPair()
		job.SetRunning(map[string]bool{taskID: true})
		before := snapshotClaims()

		Expect(mgr.Recover(ctx)).To(Succeed())

		Expect(requestsMatching(fake.Requests(), http.MethodPut, "")).To(BeEmpty())
		held := mgr.HeldClaims()
		Expect(held).To(HaveLen(2))
		Expect(held[0].Key).To(Equal("claims/pr/bborbe-nuke-658"))
		Expect(held[0].TaskID).To(Equal(taskID))
		Expect(held[0].Sha).To(Equal("S-PR"))
		Expect(held[1].Key).To(Equal("claims/repo/bborbe-nuke"))
		Expect(held[1].TaskID).To(Equal(taskID))
		Expect(held[1].Sha).To(Equal("SP"))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"adopted": 1})
	})

	It("adopts a per-PR-only claim without writing a per-repository partner", func() {
		fake.Seed(prRef, "S-PR", "self", taskID, far())
		job.SetRunning(map[string]bool{taskID: true})
		before := snapshotClaims()

		Expect(mgr.Recover(ctx)).To(Succeed())

		Expect(requestsMatching(fake.Requests(), http.MethodPut, "")).To(BeEmpty())
		held := mgr.HeldClaims()
		Expect(held).To(HaveLen(1))
		Expect(held[0].Key).To(Equal("claims/pr/bborbe-nuke-658"))
		Expect(held[0].Sha).To(Equal("S-PR"))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"adopted": 1})
	})

	It("adopts a per-repo-only claim without guessing a partner", func() {
		fake.Seed(repoRef, "SP", "self", taskID, far())
		job.SetRunning(map[string]bool{taskID: true})
		before := snapshotClaims()

		Expect(mgr.Recover(ctx)).To(Succeed())

		Expect(requestsMatching(fake.Requests(), http.MethodPut, "")).To(BeEmpty())
		held := mgr.HeldClaims()
		Expect(held).To(HaveLen(1))
		Expect(held[0].Key).To(Equal("claims/repo/bborbe-nuke"))
		Expect(held[0].Sha).To(Equal("SP"))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"adopted": 1})
	})

	It("fences both refs of a pair", func() {
		near := rfc3339(leaseTestT0.Add(65 * libtime.Second))
		fake.Seed(repoRef, "SP", "self", taskID, near)
		fake.Seed(prRef, "S-PR", "self", taskID, near)
		job.SetRunning(map[string]bool{taskID: true})
		before := snapshotClaims()

		Expect(mgr.Recover(ctx)).To(Succeed())

		Expect(job.DeleteCallCount(taskID)).To(Equal(1))
		deletes := requestsMatching(fake.Requests(), http.MethodDelete, "")
		Expect(deletes).To(HaveLen(2))
		shas := []string{deleteExpectedSha(deletes[0]), deleteExpectedSha(deletes[1])}
		Expect(shas).To(ConsistOf("SP", "S-PR"))
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"fenced": 1})
	})

	It("releases both refs of a pair whose Job is gone", func() {
		seedPair()
		job.SetRunning(map[string]bool{taskID: false})
		before := snapshotClaims()

		Expect(mgr.Recover(ctx)).To(Succeed())

		deletes := requestsMatching(fake.Requests(), http.MethodDelete, "")
		Expect(deletes).To(HaveLen(2))
		shas := []string{deleteExpectedSha(deletes[0]), deleteExpectedSha(deletes[1])}
		Expect(shas).To(ConsistOf("SP", "S-PR"))
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
	})
})
