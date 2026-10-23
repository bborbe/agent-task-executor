// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"net/http"
	"strings"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

const dualTaskID = "t-dual"

// claimDualPair claims the per-repo/per-PR pair and returns the manager plus
// the two held shas, read back from the manager's held view.
func claimDualPair(
	ctx context.Context,
	fake *fakeGitRest,
	clock libtime.CurrentDateTime,
	job *fakeJobControl,
) (claim.LeaseManager, string, string) {
	mgr := newTestManager("self", claim.NewClaimRefClient(fake.URL(), "", ""), job, clock)
	_, err := mgr.ClaimPair(ctx, dualRepoKey, dualPRKey, dualTaskID)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	primary, secondary := heldPairShas(mgr)
	return mgr, primary, secondary
}

// heldPairShas returns the held sha of each ref of the pair.
func heldPairShas(mgr claim.LeaseManager) (string, string) {
	var primary, secondary string
	for _, held := range mgr.HeldClaims() {
		switch held.Key {
		case dualRepoKey:
			primary = held.Sha
		case dualPRKey:
			secondary = held.Sha
		}
	}
	ExpectWithOffset(1, primary).NotTo(BeEmpty())
	ExpectWithOffset(1, secondary).NotTo(BeEmpty())
	return primary, secondary
}

// assertPairDeletes asserts exactly one DELETE per ref, each carrying its held
// sha.
func assertPairDeletes(fake *fakeGitRest, primarySha, secondarySha string) {
	primaryDeletes := requestsMatching(fake.Requests(), http.MethodDelete, dualRepoKey)
	ExpectWithOffset(1, primaryDeletes).To(HaveLen(1))
	ExpectWithOffset(1, primaryDeletes[0].RawQuery).
		To(ContainSubstring("expected_sha=" + primarySha))

	secondaryDeletes := requestsMatching(fake.Requests(), http.MethodDelete, dualPRKey)
	ExpectWithOffset(1, secondaryDeletes).To(HaveLen(1))
	ExpectWithOffset(1, secondaryDeletes[0].RawQuery).
		To(ContainSubstring("expected_sha=" + secondarySha))
}

var _ = Describe("Dual claim release paths", func() {
	// Two AC4 rows deliberately deviate from the blanket "two DELETEs"
	// sentence in the spec. The takeover row deletes both refs only after the
	// takeover succeeds, because before that the pair belongs to the previous
	// owner. The claim-lost row deletes none: a lost claim is dropped without
	// contacting git-rest, because the ref already belongs to another cluster,
	// and only its Job is deleted.
	DescribeTable("every release path removes both refs",
		func(run func(context.Context, *fakeGitRest, libtime.CurrentDateTime, *fakeJobControl)) {
			ctx := context.Background()
			fake := newFakeGitRest()
			defer fake.Close()
			clock := newTestClock(leaseTestT0)
			job := newFakeJobControl(clock)
			run(ctx, fake, clock, job)
		},
		Entry("plain Release", func(
			ctx context.Context,
			fake *fakeGitRest,
			clock libtime.CurrentDateTime,
			job *fakeJobControl,
		) {
			mgr, primary, secondary := claimDualPair(ctx, fake, clock, job)
			fake.ResetRequests()
			before := snapshotClaims()
			Expect(mgr.Release(ctx, dualRepoKey)).To(Succeed())
			assertPairDeletes(fake, primary, secondary)
			Expect(mgr.HeldClaims()).To(BeEmpty())
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
		}),
		Entry("ReleaseAfterSuccess", func(
			ctx context.Context,
			fake *fakeGitRest,
			clock libtime.CurrentDateTime,
			job *fakeJobControl,
		) {
			mgr, primary, secondary := claimDualPair(ctx, fake, clock, job)
			fake.ResetRequests()
			Expect(mgr.ReleaseAfterSuccess(ctx, dualRepoKey, clock.Now())).To(Succeed())
			advance(clock, libtime.Duration(claim.SuccessGracePeriod))
			Expect(mgr.Tick(ctx)).To(Succeed())
			assertPairDeletes(fake, primary, secondary)
			Expect(mgr.HeldClaims()).To(BeEmpty())
		}),
		Entry("fenced", func(
			ctx context.Context,
			fake *fakeGitRest,
			clock libtime.CurrentDateTime,
			job *fakeJobControl,
		) {
			mgr, primary, secondary := claimDualPair(ctx, fake, clock, job)
			fake.SetFault(func(req fakeRequest) *fakeFault {
				if req.Method == http.MethodPut {
					return &fakeFault{Status: http.StatusInternalServerError}
				}
				return nil
			})
			fake.ResetRequests()
			clock.SetNow(leaseTestT0.Add(240 * libtime.Second))
			before := snapshotClaims()
			Expect(mgr.Tick(ctx)).To(Succeed())
			assertPairDeletes(fake, primary, secondary)
			Expect(mgr.HeldClaims()).To(BeEmpty())
			expectClaimDelta(before, snapshotClaims(), map[string]float64{"fenced": 1})
		}),
		Entry("jobless release", func(
			ctx context.Context,
			fake *fakeGitRest,
			clock libtime.CurrentDateTime,
			job *fakeJobControl,
		) {
			mgr, _, _ := claimDualPair(ctx, fake, clock, job)
			job.SetRunning(map[string]bool{})
			// Renewals move the held shas, so read them from the store at the
			// last renew tick and then let the jobless pass release at T0+310.
			tickAt(mgr, clock, leaseTestT0.Add(300*libtime.Second))
			repoRef, ok := fake.Ref("refs/" + dualRepoKey)
			Expect(ok).To(BeTrue())
			prRef, ok := fake.Ref("refs/" + dualPRKey)
			Expect(ok).To(BeTrue())
			fake.ResetRequests()
			clock.SetNow(leaseTestT0.Add(310 * libtime.Second))
			Expect(mgr.Tick(ctx)).To(Succeed())
			assertPairDeletes(fake, repoRef.Sha, prRef.Sha)
			Expect(mgr.HeldClaims()).To(BeEmpty())
		}),
		Entry("release-after-running-check", func(
			ctx context.Context,
			fake *fakeGitRest,
			clock libtime.CurrentDateTime,
			job *fakeJobControl,
		) {
			far := rfc3339(leaseTestT0.Add(1000 * libtime.Second))
			fake.Seed("refs/"+dualRepoKey, "SR", "self", dualTaskID, far)
			fake.Seed("refs/"+dualPRKey, "SP", "self", dualTaskID, far)
			job.runningFn = func(_ string, call int) (bool, error) {
				if call == 1 {
					return false, stderrors.New("kube api unavailable")
				}
				return false, nil
			}
			mgr := newTestManager(
				"self",
				claim.NewClaimRefClient(fake.URL(), "", ""),
				job,
				clock,
			)
			fake.ResetRequests()
			Expect(mgr.Recover(ctx)).To(Succeed())
			Expect(mgr.Tick(ctx)).To(Succeed())
			assertPairDeletes(fake, "SR", "SP")
			Expect(mgr.HeldClaims()).To(BeEmpty())
		}),
		Entry("takeover", func(
			ctx context.Context,
			fake *fakeGitRest,
			clock libtime.CurrentDateTime,
			job *fakeJobControl,
		) {
			expired := rfc3339(leaseTestT0)
			fake.Seed("refs/"+dualRepoKey, "SX", "other", "t-old", expired)
			fake.Seed("refs/"+dualPRKey, "SY", "other", "t-old", expired)
			clock.SetNow(leaseTestT0.Add(claim.TakeoverGrace))
			mgr := newTestManager(
				"self",
				claim.NewClaimRefClient(fake.URL(), "", ""),
				job,
				clock,
			)
			fake.ResetRequests()
			_, err := mgr.TakeoverPair(ctx, dualRepoKey, dualPRKey, dualTaskID)
			Expect(err).NotTo(HaveOccurred())
			primary, secondary := heldPairShas(mgr)
			Expect(mgr.Release(ctx, dualRepoKey)).To(Succeed())
			assertPairDeletes(fake, primary, secondary)
			Expect(mgr.HeldClaims()).To(BeEmpty())
		}),
		Entry("claim-lost", func(
			ctx context.Context,
			fake *fakeGitRest,
			clock libtime.CurrentDateTime,
			job *fakeJobControl,
		) {
			mgr, _, _ := claimDualPair(ctx, fake, clock, job)
			fake.Seed(
				"refs/"+dualRepoKey,
				"S-other",
				"other",
				"t-other",
				rfc3339(leaseTestT0.Add(claim.LeaseDuration)),
			)
			fake.ResetRequests()
			advance(clock, claim.RenewInterval)
			Expect(mgr.Tick(ctx)).To(Succeed())
			Expect(mgr.HeldClaims()).To(BeEmpty())
			Expect(requestsMatching(fake.Requests(), http.MethodDelete, "claims/")).
				To(BeEmpty())
			Expect(job.DeleteCallCount(dualTaskID)).To(Equal(1))
		}),
	)

	It("renews both refs with one deadline and fences both", func() {
		ctx := context.Background()
		fake := newFakeGitRest()
		defer fake.Close()
		clock := newTestClock(leaseTestT0)
		job := newFakeJobControl(clock)
		mgr, primary, secondary := claimDualPair(ctx, fake, clock, job)

		fake.ResetRequests()
		before := snapshotClaims()
		advance(clock, claim.RenewInterval)
		Expect(mgr.Tick(ctx)).To(Succeed())

		puts := requestsMatching(fake.Requests(), http.MethodPut, "claims/")
		Expect(puts).To(HaveLen(2))
		Expect([]string{putExpectedSha(puts[0]), putExpectedSha(puts[1])}).
			To(ConsistOf(primary, secondary))
		leases := make([]string, 0, 2)
		for _, put := range puts {
			var body fakePutBody
			Expect(json.Unmarshal(put.Body, &body)).To(Succeed())
			leases = append(leases, body.LeaseUntil)
		}
		Expect(leases[0]).To(Equal(leases[1]))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"renewed": 1})

		fake.SetFault(func(req fakeRequest) *fakeFault {
			if req.Method == http.MethodPut {
				return &fakeFault{Status: http.StatusInternalServerError}
			}
			return nil
		})
		// The successful renew moved the pair's fence point to T0+290, so walk
		// the clock past it; T0+240 is the fence point of the original lease.
		tickAt(mgr, clock, leaseTestT0.Add(300*libtime.Second))
		Expect(mgr.HeldClaims()).To(BeEmpty())
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, "claims/")).
			To(HaveLen(2))
	})

	It("does not advance the pair expiry when the primary renew fails", func() {
		ctx := context.Background()
		fake := newFakeGitRest()
		defer fake.Close()
		clock := newTestClock(leaseTestT0)
		job := newFakeJobControl(clock)
		mgr, _, _ := claimDualPair(ctx, fake, clock, job)

		fake.SetFault(func(req fakeRequest) *fakeFault {
			if req.Method == http.MethodPut && strings.Contains(req.Path, dualRepoKey) {
				return &fakeFault{Status: http.StatusInternalServerError}
			}
			return nil
		})
		advance(clock, claim.RenewInterval)
		Expect(mgr.Tick(ctx)).To(Succeed())

		held := mgr.HeldClaims()
		Expect(held).To(HaveLen(2))
		for _, heldClaim := range held {
			Expect(heldClaim.LeaseUntil).
				To(Equal(leaseTestT0.Add(claim.LeaseDuration)))
		}
	})

	It("counts a pair whose refs are both already gone as already_released", func() {
		ctx := context.Background()
		fake := newFakeGitRest()
		defer fake.Close()
		clock := newTestClock(leaseTestT0)
		job := newFakeJobControl(clock)
		mgr, _, _ := claimDualPair(ctx, fake, clock, job)

		fake.SetFault(faultMethod(http.MethodDelete, http.StatusConflict))
		fake.ResetRequests()
		before := snapshotClaims()
		Expect(mgr.Release(ctx, dualRepoKey)).To(Succeed())
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"already_released": 1})
	})

	It("keeps the pair held when the primary release fails", func() {
		ctx := context.Background()
		fake := newFakeGitRest()
		defer fake.Close()
		clock := newTestClock(leaseTestT0)
		job := newFakeJobControl(clock)
		mgr, _, _ := claimDualPair(ctx, fake, clock, job)

		fake.SetFault(func(req fakeRequest) *fakeFault {
			if req.Method == http.MethodDelete && strings.Contains(req.Path, dualRepoKey) {
				return &fakeFault{Status: http.StatusInternalServerError}
			}
			return nil
		})
		Expect(mgr.Release(ctx, dualRepoKey)).To(HaveOccurred())
		Expect(mgr.HeldClaims()).To(HaveLen(2))
	})

	It("fences a pair whose release is pending", func() {
		ctx := context.Background()
		fake := newFakeGitRest()
		defer fake.Close()
		clock := newTestClock(leaseTestT0)
		job := newFakeJobControl(clock)
		mgr, _, _ := claimDualPair(ctx, fake, clock, job)

		fake.SetFault(faultMethod(http.MethodDelete, http.StatusInternalServerError))
		Expect(mgr.Release(ctx, dualRepoKey)).To(HaveOccurred())

		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(240 * libtime.Second))
		before := snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(mgr.HeldClaims()).To(BeEmpty())
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, dualRepoKey)).To(HaveLen(1))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, dualPRKey)).To(HaveLen(1))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"fenced": 1})
	})

	It("keeps the pair held and retries only the secondary ref", func() {
		ctx := context.Background()
		fake := newFakeGitRest()
		defer fake.Close()
		clock := newTestClock(leaseTestT0)
		job := newFakeJobControl(clock)
		mgr, _, _ := claimDualPair(ctx, fake, clock, job)

		fake.SetFault(func(req fakeRequest) *fakeFault {
			if req.Method == http.MethodDelete && strings.Contains(req.Path, "claims/pr/") {
				return &fakeFault{Status: http.StatusInternalServerError}
			}
			return nil
		})
		fake.ResetRequests()
		Expect(mgr.Release(ctx, dualRepoKey)).To(HaveOccurred())

		held := mgr.HeldClaims()
		Expect(held).To(HaveLen(2))
		Expect(held[0].Key).To(Equal(dualPRKey))
		Expect(held[1].Key).To(Equal(dualRepoKey))

		fake.SetFault(nil)
		fake.ResetRequests()
		advance(clock, claim.TickInterval)
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, "claims/pr/")).
			NotTo(BeEmpty())
		Expect(mgr.HeldClaims()).To(BeEmpty())
	})
})
