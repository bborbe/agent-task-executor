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

const takeoverKey = "claims/task/K"

var _ = Describe("LeaseManager takeover", func() {
	var (
		ctx     context.Context
		fake    *fakeGitRest
		clock   libtime.CurrentDateTime
		job     *fakeJobControl
		manager claim.LeaseManager
		other   claim.LeaseManager
	)

	BeforeEach(func() {
		ctx = context.Background()
		fake = newFakeGitRest()
		clock = newTestClock(leaseTestT0)
		job = newFakeJobControl(clock)
		client := claim.NewClaimRefClient(fake.URL(), "", "")
		manager = newTestManager("a", client, job, clock)
		other = newTestManager("b", client, job, clock)
	})

	AfterEach(func() {
		fake.Close()
	})

	It("refuses a takeover before lease_until + 30 s", func() {
		fake.Seed("refs/"+takeoverKey, "SX", "other", "t-old", rfc3339(leaseTestT0))
		before := snapshotClaims()
		for _, offset := range []libtime.Duration{0, 29 * libtime.Second} {
			clock.SetNow(leaseTestT0.Add(offset))
			fake.ResetRequests()
			_, err := manager.Takeover(ctx, takeoverKey, "t-new")
			Expect(stderrors.Is(err, claim.ErrLeaseNotExpired)).To(BeTrue())
			_, err = other.Takeover(ctx, takeoverKey, "t-new")
			Expect(stderrors.Is(err, claim.ErrLeaseNotExpired)).To(BeTrue())
			Expect(requestsMatching(fake.Requests(), http.MethodPut, takeoverKey)).To(BeEmpty())
			Expect(requestsMatching(fake.Requests(), http.MethodDelete, takeoverKey)).To(BeEmpty())
		}
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"takeover_too_early": 4,
		})
	})

	It("lets exactly one of two concurrent takeovers win", func() {
		fake.Seed("refs/"+takeoverKey, "SX", "other", "t-old", rfc3339(leaseTestT0))
		clock.SetNow(leaseTestT0.Add(claim.TakeoverGrace))
		fake.SetPutBarrier(2)

		type outcome struct {
			result *claim.TakeoverResult
			err    error
		}
		results := make(chan outcome, 2)
		before := snapshotClaims()
		var wg sync.WaitGroup
		wg.Add(2)
		for _, candidate := range []claim.LeaseManager{manager, other} {
			taker := candidate
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				result, err := taker.Takeover(ctx, takeoverKey, "t-new")
				results <- outcome{result: result, err: err}
			}()
		}
		wg.Wait()
		close(results)

		winners, losers := 0, 0
		for result := range results {
			if result.err == nil {
				Expect(result.result).NotTo(BeNil())
				Expect(result.result.PreviousTaskID).To(Equal("t-old"))
				winners++
			} else {
				Expect(stderrors.Is(result.err, claim.ErrLeaseConflict)).To(BeTrue())
				losers++
			}
		}
		Expect(winners).To(Equal(1))
		Expect(losers).To(Equal(1))

		puts := requestsMatching(fake.Requests(), http.MethodPut, takeoverKey)
		Expect(puts).To(HaveLen(2))
		for _, put := range puts {
			var body fakePutBody
			Expect(json.Unmarshal(put.Body, &body)).To(Succeed())
			Expect(body.ExpectedSha).To(Equal("SX"))
		}
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, takeoverKey)).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"taken_over": 1,
			"conflict":   1,
		})
	})

	It("refuses an unparseable lease_until", func() {
		before := snapshotClaims()
		for _, lease := range []string{"not-a-time", ""} {
			fake.Seed("refs/"+takeoverKey, "SX", "other", "t-old", lease)
			fake.ResetRequests()
			_, err := manager.Takeover(ctx, takeoverKey, "t-new")
			Expect(stderrors.Is(err, claim.ErrLeaseNotExpired)).To(BeTrue())
			Expect(requestsMatching(fake.Requests(), http.MethodPut, takeoverKey)).To(BeEmpty())
			Expect(requestsMatching(fake.Requests(), http.MethodDelete, takeoverKey)).To(BeEmpty())
		}
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"takeover_too_early": 2,
		})
	})

	It("claims an absent key instead of taking a sibling", func() {
		fake.Seed(
			"refs/"+takeoverKey+"/x",
			"SX-sibling",
			"other",
			"t-sibling",
			rfc3339(leaseTestT0),
		)
		before := snapshotClaims()
		result, err := manager.Takeover(ctx, takeoverKey, "t-new")
		Expect(err).NotTo(HaveOccurred())
		Expect(result.PreviousTaskID).To(BeEmpty())
		puts := requestsMatching(fake.Requests(), http.MethodPut, takeoverKey)
		Expect(puts).To(HaveLen(1))
		var body fakePutBody
		Expect(json.Unmarshal(puts[0].Body, &body)).To(Succeed())
		Expect(body.ExpectedSha).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"claimed": 1})
	})

	It("refuses to take over a key it already holds", func() {
		_, err := manager.Claim(ctx, takeoverKey, "t-mine")
		Expect(err).NotTo(HaveOccurred())
		fake.ResetRequests()
		before := snapshotClaims()
		result, err := manager.Takeover(ctx, takeoverKey, "t-new")
		Expect(result).To(BeNil())
		Expect(stderrors.Is(err, claim.ErrLeaseConflict)).To(BeTrue())
		Expect(fake.Requests()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"conflict": 1})
	})

	It("returns the read-back error when the listing fails", func() {
		fake.SetFault(func(request fakeRequest) *fakeFault {
			if request.Method == http.MethodGet {
				return &fakeFault{Status: http.StatusInternalServerError}
			}
			return nil
		})
		before := snapshotClaims()
		result, err := manager.Takeover(ctx, takeoverKey, "t-new")
		Expect(result).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(manager.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"error": 1})
	})
})
