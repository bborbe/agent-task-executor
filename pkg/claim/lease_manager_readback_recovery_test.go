// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	stderrors "errors"
	"net/http"
	"sync"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

var _ = Describe("LeaseManager read-back recovery", func() {
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

	// The defect: the read-back after a non-conflict error ran on the caller's
	// context. A caller whose deadline expired mid-Put therefore killed the
	// recovery exactly when it was needed, and a push that had in fact landed was
	// recorded as an error — leaving a lease nobody owned until it expired.
	It("recovers the claim when the caller's context is cancelled mid-Put", func() {
		mgr := newTestManager("self", claim.NewClaimRefClient(fake.URL(), "", ""), job, clock)
		callerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var once sync.Once
		fake.SetFault(func(req fakeRequest) *fakeFault {
			if req.Method != http.MethodPut {
				return nil
			}
			// The write lands at the store, but the caller's context is cancelled
			// before the response reaches it: the cancelled-request case.
			once.Do(cancel)
			return &fakeFault{Status: http.StatusInternalServerError, Apply: true}
		})

		before := snapshotClaims()
		held, err := mgr.Claim(callerCtx, "claims/task/cancel", "t-cancel")
		Expect(err).NotTo(HaveOccurred())
		Expect(held).NotTo(BeNil())
		Expect(held.Key).To(Equal("claims/task/cancel"))
		Expect(mgr.HeldClaims()).To(HaveLen(1))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"recovered": 1})
	})

	// Negative control: a 409 is a conflict, never a recovery. If the conflict
	// path reached the read-back at all, the listing request below would appear
	// and this test would fail.
	It("treats a 409 as a conflict and never reads the claim back", func() {
		mgr := newTestManager("self", claim.NewClaimRefClient(fake.URL(), "", ""), job, clock)
		fake.Seed(
			"refs/claims/task/taken",
			"0000000000000000000000000000000000000001",
			"other",
			"t-other",
			rfc3339(clock.Now().Add(claim.LeaseDuration)),
		)

		before := snapshotClaims()
		held, err := mgr.Claim(ctx, "claims/task/taken", "t-taken")
		Expect(held).To(BeNil())
		Expect(stderrors.Is(err, claim.ErrLeaseConflict)).To(BeTrue())
		Expect(requestsMatching(fake.Requests(), http.MethodGet, "")).To(BeEmpty())
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"conflict": 1})
	})
})
