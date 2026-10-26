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

const (
	releaseKey  = "claims/task/rel"
	releaseTask = "t-rel"
)

// faultMethod makes every request of method answer status.
func faultMethod(method string, status int) func(fakeRequest) *fakeFault {
	return func(request fakeRequest) *fakeFault {
		if request.Method == method {
			return &fakeFault{Status: status}
		}
		return nil
	}
}

var _ = Describe("LeaseManager release", func() {
	var (
		ctx   context.Context
		fake  *fakeGitRest
		clock libtime.CurrentDateTime
		job   *fakeJobControl
		mgr   claim.LeaseManager
	)

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

	It("deletes the ref with the held sha", func() {
		held, err := mgr.Claim(ctx, releaseKey, releaseTask)
		Expect(err).NotTo(HaveOccurred())
		fake.ResetRequests()
		before := snapshotClaims()
		Expect(mgr.Release(ctx, releaseKey)).To(Succeed())
		deletes := requestsMatching(fake.Requests(), http.MethodDelete, releaseKey)
		Expect(deletes).To(HaveLen(1))
		Expect(deletes[0].RawQuery).To(ContainSubstring("expected_sha=" + held.LeaseToken))
		Expect(deletes[0].Status).To(Equal(http.StatusOK))
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
	})

	It("keeps the claim held while this cluster's Job is still running", func() {
		_, err := mgr.Claim(ctx, releaseKey, releaseTask)
		Expect(err).NotTo(HaveOccurred())
		job.SetRunning(map[string]bool{releaseTask: true})
		fake.ResetRequests()
		before := snapshotClaims()

		Expect(mgr.Release(ctx, releaseKey)).To(Succeed())

		Expect(requestsMatching(fake.Requests(), http.MethodDelete, releaseKey)).To(BeEmpty())
		Expect(mgr.HeldClaims()).To(HaveLen(1))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{})

		// Still held, so the loop keeps renewing it.
		clock.SetNow(leaseTestT0.Add(2 * libtime.Minute))
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(requestsMatching(fake.Requests(), http.MethodPut, releaseKey)).NotTo(BeEmpty())
	})

	It("releases once this cluster's Job is gone", func() {
		_, err := mgr.Claim(ctx, releaseKey, releaseTask)
		Expect(err).NotTo(HaveOccurred())
		job.SetRunning(map[string]bool{})
		fake.ResetRequests()
		before := snapshotClaims()

		Expect(mgr.Release(ctx, releaseKey)).To(Succeed())

		Expect(requestsMatching(fake.Requests(), http.MethodDelete, releaseKey)).To(HaveLen(1))
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
	})

	It("treats a 409 as already released", func() {
		_, err := mgr.Claim(ctx, releaseKey, releaseTask)
		Expect(err).NotTo(HaveOccurred())
		fake.SetFault(faultMethod(http.MethodDelete, http.StatusConflict))
		before := snapshotClaims()
		Expect(mgr.Release(ctx, releaseKey)).To(Succeed())
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"already_released": 1})
	})

	It("keeps the claim held when the release fails and retries on the tick", func() {
		held, err := mgr.Claim(ctx, releaseKey, releaseTask)
		Expect(err).NotTo(HaveOccurred())
		fake.SetFault(faultMethod(http.MethodDelete, http.StatusInternalServerError))
		before := snapshotClaims()
		releaseErr := mgr.Release(ctx, releaseKey)
		Expect(releaseErr).To(HaveOccurred())
		Expect(stderrors.Is(releaseErr, claim.ErrLeaseConflict)).To(BeFalse())
		Expect(mgr.HeldClaims()).To(HaveLen(1))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"error": 1})

		fake.SetFault(nil)
		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(claim.TickInterval))
		before = snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(requestsMatching(fake.Requests(), http.MethodPut, releaseKey)).To(BeEmpty())
		deletes := requestsMatching(fake.Requests(), http.MethodDelete, releaseKey)
		Expect(deletes).To(HaveLen(1))
		Expect(deletes[0].RawQuery).To(ContainSubstring("expected_sha=" + held.LeaseToken))
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
	})

	It("fences a release that keeps failing without deleting the Job", func() {
		held, err := mgr.Claim(ctx, releaseKey, releaseTask)
		Expect(err).NotTo(HaveOccurred())
		fake.SetFault(faultMethod(http.MethodDelete, http.StatusInternalServerError))
		before := snapshotClaims()
		Expect(mgr.Release(ctx, releaseKey)).NotTo(Succeed())
		// Initial release plus one retry per tick from T0+10 through T0+230.
		tickAt(mgr, clock, leaseTestT0.Add(230*libtime.Second))
		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(240 * libtime.Second))
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(job.DeleteCallCount(releaseTask)).To(Equal(0))
		deletes := requestsMatching(fake.Requests(), http.MethodDelete, releaseKey)
		Expect(deletes).To(HaveLen(1))
		Expect(deletes[0].RawQuery).To(ContainSubstring("expected_sha=" + held.LeaseToken))
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"error":  24,
			"fenced": 1,
		})

		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(250 * libtime.Second))
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(fake.Requests()).To(BeEmpty())
	})

	It("is a no-op for a key it does not hold", func() {
		before := snapshotClaims()
		Expect(mgr.Release(ctx, "claims/task/absent")).To(Succeed())
		Expect(fake.Requests()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{})
	})

	It("drops a pending release when the retry sees a conflict", func() {
		_, err := mgr.Claim(ctx, releaseKey, releaseTask)
		Expect(err).NotTo(HaveOccurred())
		fake.SetFault(faultMethod(http.MethodDelete, http.StatusInternalServerError))
		Expect(mgr.Release(ctx, releaseKey)).NotTo(Succeed())
		fake.SetFault(faultMethod(http.MethodDelete, http.StatusConflict))
		clock.SetNow(leaseTestT0.Add(claim.TickInterval))
		before := snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"already_released": 1})
	})
})
