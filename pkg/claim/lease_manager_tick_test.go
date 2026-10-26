// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/mocks"
	"github.com/bborbe/agent-task-executor/pkg/claim"
)

const (
	tickKey  = "claims/task/tick"
	tickTask = "t-tick"
)

// recordRenewTicks returns the tick offsets that sent a PUT for key.
func recordRenewTicks(
	mgr claim.LeaseManager,
	clock libtime.CurrentDateTime,
	fake *fakeGitRest,
	key string,
	start int,
	end int,
) []libtime.Duration {
	renewTicks := make([]libtime.Duration, 0)
	for offset := start; offset <= end; offset += 10 {
		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(libtime.Duration(offset) * libtime.Second))
		Expect(mgr.Tick(context.Background())).To(Succeed())
		if len(requestsMatching(fake.Requests(), http.MethodPut, key)) > 0 {
			renewTicks = append(renewTicks, libtime.Duration(offset)*libtime.Second)
		}
	}
	return renewTicks
}

var _ = Describe("LeaseManager tick", func() {
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

	It("deletes the Job when the renew loses the compare-and-swap", func() {
		held, err := mgr.Claim(ctx, tickKey, tickTask)
		Expect(err).NotTo(HaveOccurred())
		fake.Seed(
			"refs/"+tickKey,
			"S-other",
			"other",
			"t-other",
			rfc3339(leaseTestT0.Add(claim.LeaseDuration)),
		)
		clock.SetNow(leaseTestT0.Add(60 * libtime.Second))
		before := snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(job.DeleteCallCount(tickTask)).To(Equal(1))
		Expect(mgr.HeldClaims()).To(BeEmpty())
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, tickKey)).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"claim_lost": 1})
		Expect(held.LeaseToken).NotTo(BeEmpty())
	})

	It("fences at T0+240 before any renew request", func() {
		held, err := mgr.Claim(ctx, tickKey, tickTask)
		Expect(err).NotTo(HaveOccurred())
		fake.SetFault(func(fakeRequest) *fakeFault {
			return &fakeFault{Status: http.StatusInternalServerError}
		})
		before := snapshotClaims()
		renewTicks := recordRenewTicks(mgr, clock, fake, tickKey, 10, 230)
		Expect(renewTicks).To(Equal([]libtime.Duration{
			60 * libtime.Second,
			120 * libtime.Second,
			180 * libtime.Second,
		}))
		Expect(job.DeleteCallCount(tickTask)).To(Equal(0))

		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(240 * libtime.Second))
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(job.DeleteCallCount(tickTask)).To(Equal(1))
		Expect(requestsMatching(fake.Requests(), http.MethodPut, tickKey)).To(BeEmpty())
		deletes := requestsMatching(fake.Requests(), http.MethodDelete, tickKey)
		Expect(deletes).To(HaveLen(1))
		Expect(deletes[0].Status).To(Equal(http.StatusInternalServerError))
		Expect(deletes[0].RawQuery).To(ContainSubstring("expected_sha=" + held.LeaseToken))
		Expect(mgr.HeldClaims()).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"renew_failed": 3,
			"fenced":       1,
		})

		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(250 * libtime.Second))
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(fake.Requests()).To(BeEmpty())
	})

	It("fences a mid-phase claim at T0+280", func() {
		clock.SetNow(leaseTestT0.Add(45 * libtime.Second))
		_, err := mgr.Claim(ctx, tickKey, tickTask)
		Expect(err).NotTo(HaveOccurred())
		fake.SetFault(func(fakeRequest) *fakeFault {
			return &fakeFault{Status: http.StatusInternalServerError}
		})
		before := snapshotClaims()
		clock.SetNow(leaseTestT0.Add(50 * libtime.Second))
		for offset := 50; offset <= 270; offset += 10 {
			clock.SetNow(leaseTestT0.Add(libtime.Duration(offset) * libtime.Second))
			Expect(mgr.Tick(ctx)).To(Succeed())
			Expect(job.DeleteCallCount(tickTask)).To(Equal(0))
		}
		clock.SetNow(leaseTestT0.Add(280 * libtime.Second))
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(job.DeleteCallCount(tickTask)).To(Equal(1))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"renew_failed": 3,
			"fenced":       1,
		})
	})

	It("caps a hanging renew at the earliest fenceAt", func() {
		client := &mocks.FakeClaimRefClient{}
		var (
			stateMu   sync.Mutex
			xSha      = "SX0"
			xLease    = rfc3339(leaseTestT0.Add(35*libtime.Second + claim.LeaseDuration))
			remaining time.Duration
			renews    int
			yLists    int
			xLists    int
		)
		client.PutStub = func(
			reqCtx context.Context,
			key string,
			expectedSha string,
			_ string,
			_ string,
			lease libtime.DateTime,
		) (string, error) {
			stateMu.Lock()
			defer stateMu.Unlock()
			if expectedSha == "" {
				return "S0", nil
			}
			if key == "claims/task/y" {
				return "", stderrors.New("renew failed")
			}
			if clock.Now().Equal(leaseTestT0.Add(220 * libtime.Second)) {
				deadline, ok := reqCtx.Deadline()
				Expect(ok).To(BeTrue())
				remaining = deadline.Sub(clock.Now().Time())
				advance(clock, libtime.Duration(remaining))
				return "", context.DeadlineExceeded
			}
			renews++
			xSha = fmt.Sprintf("SX%d", renews)
			xLease = rfc3339(lease)
			return xSha, nil
		}
		client.ListStub = func(_ context.Context, prefix string) ([]claim.ClaimRef, error) {
			stateMu.Lock()
			defer stateMu.Unlock()
			switch prefix {
			case "claims/task/y":
				// The typed acquire reads before it writes, so the first read must
				// answer "free"; only the later read-back fails, which is what makes
				// the renew a failed interval rather than a lost claim.
				yLists++
				if yLists <= 2 {
					return nil, nil
				}
				return nil, stderrors.New("list failed")
			case "claims/task/x":
				// Same reason as y: the acquire reads first, so the first read must
				// answer "free". The seeded ref is what the later renews see.
				xLists++
				if xLists <= 2 {
					return nil, nil
				}
				return []claim.ClaimRef{{
					Ref:        "refs/claims/task/x",
					Sha:        xSha,
					Cluster:    "self",
					TaskID:     "t-x",
					LeaseUntil: xLease,
				}}, nil
			default:
				return nil, nil
			}
		}
		client.DeleteStub = func(context.Context, string, string) error { return nil }
		hanging := newTestManager("self", client, job, clock)
		_, err := hanging.Claim(ctx, "claims/task/y", "t-y")
		Expect(err).NotTo(HaveOccurred())
		advance(clock, 35*libtime.Second)
		_, err = hanging.Claim(ctx, "claims/task/x", "t-x")
		Expect(err).NotTo(HaveOccurred())

		before := snapshotClaims()
		for offset := 40; offset <= 220; offset += 10 {
			clock.SetNow(leaseTestT0.Add(libtime.Duration(offset) * libtime.Second))
			Expect(hanging.Tick(ctx)).To(Succeed())
		}
		Expect(remaining).To(BeNumerically(">", 0))
		Expect(remaining).To(BeNumerically("<=", 20*time.Second))
		Expect(job.Deleted()).To(HaveLen(1))
		Expect(job.Deleted()[0].taskID).To(Equal("t-y"))
		Expect(job.Deleted()[0].at.Equal(leaseTestT0.Add(240 * libtime.Second))).To(BeTrue())
		held := hanging.HeldClaims()
		Expect(held).To(HaveLen(1))
		Expect(held[0].Key).To(Equal("claims/task/x"))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"renewed":      2,
			"renew_failed": 4,
			"fenced":       1,
		})
	})

	It("retries a failed Job deletion on the fence path", func() {
		_, err := mgr.Claim(ctx, tickKey, tickTask)
		Expect(err).NotTo(HaveOccurred())
		fake.SetFault(func(fakeRequest) *fakeFault {
			return &fakeFault{Status: http.StatusInternalServerError}
		})
		job.deleteFn = func(_ string, call int) error {
			if call == 1 {
				return stderrors.New("delete failed")
			}
			return nil
		}
		clock.SetNow(leaseTestT0.Add(230 * libtime.Second))
		Expect(mgr.Tick(ctx)).To(Succeed())

		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(240 * libtime.Second))
		before := snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(job.DeleteCallCount(tickTask)).To(Equal(1))
		Expect(mgr.HeldClaims()).To(HaveLen(1))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, tickKey)).To(BeEmpty())
		Expect(requestsMatching(fake.Requests(), http.MethodPut, tickKey)).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"fenced": 1,
			"error":  1,
		})

		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(250 * libtime.Second))
		before = snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(job.DeleteCallCount(tickTask)).To(Equal(2))
		Expect(mgr.HeldClaims()).To(BeEmpty())
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, tickKey)).To(HaveLen(1))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{})
	})

	It("retries a failed Job deletion on the lost path", func() {
		_, err := mgr.Claim(ctx, tickKey, tickTask)
		Expect(err).NotTo(HaveOccurred())
		fake.Seed(
			"refs/"+tickKey,
			"S-other",
			"other",
			"t-other",
			rfc3339(leaseTestT0.Add(claim.LeaseDuration)),
		)
		job.deleteFn = func(_ string, call int) error {
			if call == 1 {
				return stderrors.New("delete failed")
			}
			return nil
		}
		clock.SetNow(leaseTestT0.Add(60 * libtime.Second))
		before := snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(job.DeleteCallCount(tickTask)).To(Equal(1))
		Expect(mgr.HeldClaims()).To(HaveLen(1))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"claim_lost": 1,
			"error":      1,
		})

		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(70 * libtime.Second))
		before = snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(job.DeleteCallCount(tickTask)).To(Equal(2))
		Expect(mgr.HeldClaims()).To(BeEmpty())
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, tickKey)).To(BeEmpty())
		expectClaimDelta(before, snapshotClaims(), map[string]float64{})
	})

	It("returns the context error when the context is already done", func() {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		Expect(mgr.Tick(cancelled)).To(HaveOccurred())
	})
})
