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

		fake.SetFault(func(req fakeRequest) *fakeFault {
			if req.Method != http.MethodPut {
				return nil
			}
			// The write is stored first, then the caller's context is cancelled
			// before the response reaches it: the cancelled-request case, with the
			// stored state guaranteed observable to the read-back.
			return &fakeFault{
				Status:     http.StatusInternalServerError,
				Apply:      true,
				AfterApply: cancel,
			}
		})

		before := snapshotClaims()
		held, err := mgr.Claim(callerCtx, "claims/task/cancel", "t-cancel")
		Expect(err).NotTo(HaveOccurred())
		Expect(held).NotTo(BeNil())
		Expect(held.Key).To(Equal("claims/task/cancel"))
		Expect(mgr.HeldClaims()).To(HaveLen(1))
		stored, ok := fake.Ref("refs/claims/task/cancel")
		Expect(ok).To(BeTrue())
		Expect(held.Sha).To(Equal(stored.Sha))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"recovered": 1})
	})

	// Negative control: a 409 is a conflict, never a recovery. If the conflict
	// path reached the read-back at all, the counter delta would read `error`
	// rather than `conflict`, and the listing request below would appear.
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

	// The recovery is only for our own write. A read-back that shows another
	// writer must return the original error and leave the claim unheld — adopting
	// it would hand us a lease someone else holds.
	It("does not recover a read-back that shows another writer", func() {
		mgr := newTestManager("self", claim.NewClaimRefClient(fake.URL(), "", ""), job, clock)
		fake.SetFault(func(req fakeRequest) *fakeFault {
			if req.Method != http.MethodPut {
				return nil
			}
			return &fakeFault{
				Status: http.StatusInternalServerError,
				Apply:  true,
				// Overwrite our write with another cluster's triple once it is stored,
				// so the read-back sees a writer that is not us.
				AfterApply: func() {
					fake.Seed(
						"refs/claims/task/other",
						"0000000000000000000000000000000000000003",
						"other",
						"t-other",
						rfc3339(clock.Now().Add(claim.LeaseDuration)),
					)
				},
			}
		})

		before := snapshotClaims()
		held, err := mgr.Claim(ctx, "claims/task/other", "t-mine")
		Expect(held).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(stderrors.Is(err, claim.ErrLeaseConflict)).To(BeFalse())
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"error": 1})
	})

	// The other half of the fix: only the post-error read-back escapes the
	// caller's cancellation. A pre-check listing is not recovery — an
	// already-expired caller must still be refused, so widening the escape to it
	// would be a bug. If this listing moved onto the recovery context it would
	// succeed and answer `takeover_too_early` for the live seeded lease, not
	// `error`.
	It("keeps a pre-check listing on the caller's context", func() {
		mgr := newTestManager("self", claim.NewClaimRefClient(fake.URL(), "", ""), job, clock)
		fake.Seed(
			"refs/claims/task/precheck",
			"0000000000000000000000000000000000000002",
			"other",
			"t-other",
			rfc3339(clock.Now().Add(claim.LeaseDuration)),
		)
		callerCtx, cancel := context.WithCancel(ctx)
		cancel()

		before := snapshotClaims()
		_, err := mgr.Takeover(callerCtx, "claims/task/precheck", "t-mine")
		Expect(err).To(HaveOccurred())
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"error": 1})
	})
})
