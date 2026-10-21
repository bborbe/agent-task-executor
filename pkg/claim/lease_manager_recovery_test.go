// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	stderrors "errors"
	"net/http"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

const recoveryKey = "claims/task/r"

// faultAllPuts makes every PUT answer status.
func faultAllPuts(status int, apply bool) func(fakeRequest) *fakeFault {
	return func(request fakeRequest) *fakeFault {
		if request.Method == http.MethodPut {
			return &fakeFault{Status: status, Apply: apply}
		}
		return nil
	}
}

// faultPut makes every PUT with a non-empty expected_sha answer status.
func faultPut(status int, apply bool) func(fakeRequest) *fakeFault {
	return func(request fakeRequest) *fakeFault {
		if request.Method == http.MethodPut && putExpectedSha(request) != "" {
			return &fakeFault{Status: status, Apply: apply}
		}
		return nil
	}
}

// newClaimedManager claims recoveryKey and returns the manager and held sha.
func newClaimedManager(
	fake *fakeGitRest,
	clock libtime.CurrentDateTime,
	job *fakeJobControl,
) (claim.LeaseManager, string) {
	mgr := newTestManager("self", claim.NewClaimRefClient(fake.URL(), "", ""), job, clock)
	held, err := mgr.Claim(context.Background(), recoveryKey, "t-r")
	Expect(err).NotTo(HaveOccurred())
	return mgr, held.Sha
}

var _ = Describe("LeaseManager lost-response recovery", func() {
	var (
		ctx   context.Context
		fake  *fakeGitRest
		clock libtime.CurrentDateTime
		job   *fakeJobControl
	)

	BeforeEach(func() {
		ctx = context.Background()
		fake = newFakeGitRest()
		clock = newTestClock(leaseTestT0)
		job = newFakeJobControl(clock)
	})

	AfterEach(func() {
		fake.Close()
	})

	DescribeTable("renew, create and takeover read-back",
		func(run func()) {
			run()
		},
		Entry("row 1: an applied renew answered 500 is recovered", func() {
			mgr, s0 := newClaimedManager(fake, clock, job)
			fake.SetFault(faultPut(http.StatusInternalServerError, true))
			advance(clock, claim.RenewInterval)
			before := snapshotClaims()
			Expect(mgr.Tick(ctx)).To(Succeed())
			ref, ok := fake.Ref("refs/" + recoveryKey)
			Expect(ok).To(BeTrue())
			Expect(ref.Sha).NotTo(Equal(s0))
			held := mgr.HeldClaims()
			Expect(held).To(HaveLen(1))
			Expect(held[0].Sha).To(Equal(ref.Sha))
			Expect(job.DeleteCallCount("t-r")).To(Equal(0))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"recovered": 1})

			fake.SetFault(nil)
			fake.ResetRequests()
			advance(clock, claim.RenewInterval)
			before = snapshotClaims()
			Expect(mgr.Tick(ctx)).To(Succeed())
			puts := requestsMatching(fake.Requests(), http.MethodPut, recoveryKey)
			Expect(puts).To(HaveLen(1))
			Expect(putExpectedSha(puts[0])).To(Equal(ref.Sha))
			Expect(puts[0].Status).To(Equal(http.StatusOK))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"renewed": 1})
		}),
		Entry("row 2: a 500 with a foreign write is a lost claim", func() {
			mgr, _ := newClaimedManager(fake, clock, job)
			advance(clock, claim.RenewInterval)
			fake.Seed(
				"refs/"+recoveryKey,
				"S-other",
				"other",
				"t-other",
				rfc3339(clock.Now().Add(claim.LeaseDuration)),
			)
			fake.SetFault(faultPut(http.StatusInternalServerError, false))
			before := snapshotClaims()
			Expect(mgr.Tick(ctx)).To(Succeed())
			Expect(job.DeleteCallCount("t-r")).To(Equal(1))
			Expect(mgr.HeldClaims()).To(BeEmpty())
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"claim_lost": 1})
		}),
		Entry("row 3: a 500 with the ref still at the held sha is one failed interval", func() {
			mgr, s0 := newClaimedManager(fake, clock, job)
			fake.SetFault(faultPut(http.StatusInternalServerError, false))
			advance(clock, claim.RenewInterval)
			before := snapshotClaims()
			Expect(mgr.Tick(ctx)).To(Succeed())
			Expect(job.DeleteCallCount("t-r")).To(Equal(0))
			held := mgr.HeldClaims()
			Expect(held).To(HaveLen(1))
			Expect(held[0].Sha).To(Equal(s0))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"renew_failed": 1})

			fake.SetFault(nil)
			fake.ResetRequests()
			advance(clock, claim.RenewInterval)
			Expect(mgr.Tick(ctx)).To(Succeed())
			puts := requestsMatching(fake.Requests(), http.MethodPut, recoveryKey)
			Expect(puts).To(HaveLen(1))
			Expect(putExpectedSha(puts[0])).To(Equal(s0))
		}),
		Entry("row 4: a sibling carrying the own triple is not the key", func() {
			mgr, _ := newClaimedManager(fake, clock, job)
			advance(clock, claim.RenewInterval)
			fake.DeleteRef("refs/" + recoveryKey)
			fake.Seed(
				"refs/"+recoveryKey+"/x",
				"S-sibling",
				"self",
				"t-r",
				rfc3339(clock.Now().Add(claim.LeaseDuration)),
			)
			fake.SetFault(faultPut(http.StatusInternalServerError, false))
			before := snapshotClaims()
			Expect(mgr.Tick(ctx)).To(Succeed())
			Expect(job.DeleteCallCount("t-r")).To(Equal(1))
			Expect(mgr.HeldClaims()).To(BeEmpty())
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"claim_lost": 1})
		}),
		Entry("row 5: a failed read-back keeps the claim", func() {
			mgr, s0 := newClaimedManager(fake, clock, job)
			fake.SetFault(func(fakeRequest) *fakeFault {
				return &fakeFault{Status: http.StatusInternalServerError}
			})
			advance(clock, claim.RenewInterval)
			before := snapshotClaims()
			Expect(mgr.Tick(ctx)).To(Succeed())
			Expect(job.DeleteCallCount("t-r")).To(Equal(0))
			held := mgr.HeldClaims()
			Expect(held).To(HaveLen(1))
			Expect(held[0].Sha).To(Equal(s0))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"renew_failed": 1})
		}),
		Entry("row 6: an applied create answered 500 is recovered", func() {
			fake.SetFault(faultAllPuts(http.StatusInternalServerError, true))
			mgr := newTestManager(
				"self",
				claim.NewClaimRefClient(fake.URL(), "", ""),
				job,
				clock,
			)
			before := snapshotClaims()
			held, err := mgr.Claim(ctx, recoveryKey, "t-r")
			Expect(err).NotTo(HaveOccurred())
			ref, ok := fake.Ref("refs/" + recoveryKey)
			Expect(ok).To(BeTrue())
			Expect(held.Sha).To(Equal(ref.Sha))
			Expect(mgr.HeldClaims()).To(HaveLen(1))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"recovered": 1})
		}),
		Entry("row 7: a create answered 500 and not applied is an error", func() {
			fake.SetFault(faultAllPuts(http.StatusInternalServerError, false))
			mgr := newTestManager(
				"self",
				claim.NewClaimRefClient(fake.URL(), "", ""),
				job,
				clock,
			)
			before := snapshotClaims()
			held, err := mgr.Claim(ctx, recoveryKey, "t-r")
			Expect(held).To(BeNil())
			Expect(err).To(HaveOccurred())
			Expect(stderrors.Is(err, claim.ErrLeaseConflict)).To(BeFalse())
			Expect(mgr.HeldClaims()).To(BeEmpty())
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"error": 1})
		}),
		Entry("row 8: an applied takeover answered 500 is recovered", func() {
			fake.Seed("refs/"+recoveryKey, "SX", "other", "t-old", rfc3339(leaseTestT0))
			clock.SetNow(leaseTestT0.Add(claim.TakeoverGrace))
			fake.SetFault(faultPut(http.StatusInternalServerError, true))
			mgr := newTestManager(
				"self",
				claim.NewClaimRefClient(fake.URL(), "", ""),
				job,
				clock,
			)
			before := snapshotClaims()
			result, err := mgr.Takeover(ctx, recoveryKey, "t-r")
			Expect(err).NotTo(HaveOccurred())
			Expect(result.PreviousTaskID).To(Equal("t-old"))
			Expect(mgr.HeldClaims()).To(HaveLen(1))
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"recovered": 1})
		}),
		Entry("row 9: a takeover answered 500 and not applied is an error", func() {
			fake.Seed("refs/"+recoveryKey, "SX", "other", "t-old", rfc3339(leaseTestT0))
			clock.SetNow(leaseTestT0.Add(claim.TakeoverGrace))
			fake.SetFault(faultPut(http.StatusInternalServerError, false))
			mgr := newTestManager(
				"self",
				claim.NewClaimRefClient(fake.URL(), "", ""),
				job,
				clock,
			)
			before := snapshotClaims()
			result, err := mgr.Takeover(ctx, recoveryKey, "t-r")
			Expect(result).To(BeNil())
			Expect(err).To(HaveOccurred())
			Expect(mgr.HeldClaims()).To(BeEmpty())
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"error": 1})
		}),
	)
})
