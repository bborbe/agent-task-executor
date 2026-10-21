// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"net/http"
	"sync"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

var _ = Describe("LeaseManager claim", func() {
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

	It("rejects an empty cluster name", func() {
		_, err := claim.NewLeaseManager(
			"",
			claim.NewClaimRefClient(fake.URL(), "", ""),
			job,
			clock,
			libtime.NewWaiterDuration(),
		)
		Expect(err).To(HaveOccurred())
	})

	It("refuses to claim a key it already holds without a request", func() {
		mgr := newTestManager("self", claim.NewClaimRefClient(fake.URL(), "", ""), job, clock)
		_, err := mgr.Claim(ctx, "claims/task/dup", "t-dup")
		Expect(err).NotTo(HaveOccurred())
		fake.ResetRequests()
		before := snapshotClaims()
		held, err := mgr.Claim(ctx, "claims/task/dup", "t-dup")
		Expect(held).To(BeNil())
		Expect(stderrors.Is(err, claim.ErrLeaseConflict)).To(BeTrue())
		Expect(fake.Requests()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"conflict": 1})
	})

	It("lets exactly one of two concurrent creates win", func() {
		fake.SetPutBarrier(2)
		client := claim.NewClaimRefClient(fake.URL(), "", "")
		mgrA := newTestManager("a", client, job, clock)
		mgrB := newTestManager("b", client, job, clock)

		type outcome struct {
			held *claim.HeldClaim
			err  error
		}
		results := make(chan outcome, 2)
		before := snapshotClaims()
		var wg sync.WaitGroup
		wg.Add(2)
		for _, mgr := range []claim.LeaseManager{mgrA, mgrB} {
			manager := mgr
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				held, err := manager.Claim(ctx, "claims/task/race", "t-race")
				results <- outcome{held: held, err: err}
			}()
		}
		wg.Wait()
		close(results)

		var winner *claim.HeldClaim
		winners, losers := 0, 0
		for result := range results {
			if result.err == nil {
				Expect(result.held).NotTo(BeNil())
				winners++
				winner = result.held
			} else {
				Expect(stderrors.Is(result.err, claim.ErrLeaseConflict)).To(BeTrue())
				losers++
			}
		}
		Expect(winners).To(Equal(1))
		Expect(losers).To(Equal(1))

		puts := requestsMatching(fake.Requests(), http.MethodPut, "claims/task/race")
		Expect(puts).To(HaveLen(2))
		for _, put := range puts {
			var body fakePutBody
			Expect(json.Unmarshal(put.Body, &body)).To(Succeed())
			Expect(body.ExpectedSha).To(BeEmpty())
		}
		ref, ok := fake.Ref("refs/claims/task/race")
		Expect(ok).To(BeTrue())
		Expect(ref.Sha).To(Equal(winner.Sha))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"claimed":  1,
			"conflict": 1,
		})
	})

	It("self-check: the holder counter sees two winners in an upsert", func() {
		client := claim.NewClaimRefClient(fake.URL(), "", "")
		mgr := newTestManager("a", client, job, clock)
		held, err := mgr.Claim(ctx, "claims/task/selfcheck", "t-self")
		Expect(err).NotTo(HaveOccurred())

		refs, err := client.List(ctx, "claims/task/selfcheck")
		Expect(err).NotTo(HaveOccurred())
		Expect(refs).To(HaveLen(1))
		Expect(refs[0].Sha).To(Equal(held.Sha))

		_, err = client.Put(
			ctx,
			"claims/task/selfcheck",
			refs[0].Sha,
			"upsert",
			"t-upsert",
			clock.Now().Add(claim.LeaseDuration),
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(countSuccessfulPuts(fake.Requests())).To(Equal(2))
	})

	It("returns the read-back error when a create fails and the listing fails too", func() {
		mgr := newTestManager("self", claim.NewClaimRefClient(fake.URL(), "", ""), job, clock)
		fake.SetFault(func(fakeRequest) *fakeFault {
			return &fakeFault{Status: http.StatusInternalServerError}
		})
		before := snapshotClaims()
		held, err := mgr.Claim(ctx, "claims/task/readback", "t-readback")
		Expect(held).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(stderrors.Is(err, claim.ErrLeaseConflict)).To(BeFalse())
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"error": 1})
	})
})
