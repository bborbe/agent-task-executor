// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"net/http"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

var _ = Describe("LeaseManager restart recovery", func() {
	var (
		ctx   context.Context
		fake  *fakeGitRest
		clock libtime.CurrentDateTime
		job   *fakeJobControl
		mgr   claim.LeaseManager
	)

	seedRestartFixture := func() {
		far := rfc3339(leaseTestT0.Add(1000 * libtime.Second))
		fake.Seed("refs/claims/task/a", "SA", "self", "t1", far)
		fake.Seed("refs/claims/task/b", "SB", "self", "t2", far)
		fake.Seed("refs/claims/task/c", "SC", "other", "t3", far)
		fake.Seed(
			"refs/claims/task/d",
			"SD",
			"self",
			"t4",
			rfc3339(leaseTestT0.Add(65*libtime.Second)),
		)
		fake.Seed("refs/claims/task/e", "SE", "self", "t5", far)
	}

	setupRestartJob := func() {
		job.runningFn = func(taskID string, call int) (bool, error) {
			switch taskID {
			case "t1", "t4":
				return true, nil
			case "t2":
				return false, nil
			case "t5":
				if call == 1 {
					return false, stderrors.New("kube api unavailable")
				}
				return true, nil
			default:
				return false, nil
			}
		}
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

	It("adopts, releases and fences the claims it held", func() {
		seedRestartFixture()
		setupRestartJob()
		before := snapshotClaims()
		Expect(mgr.Recover(ctx)).To(Succeed())

		Expect(requestsMatching(fake.Requests(), http.MethodPut, "")).To(BeEmpty())
		Expect(job.Deleted()).To(HaveLen(1))
		Expect(job.Deleted()[0].taskID).To(Equal("t4"))
		deletes := requestsMatching(fake.Requests(), http.MethodDelete, "")
		Expect(deletes).To(HaveLen(2))
		Expect(requestsMatching(fake.Requests(), http.MethodPut, "claims/task/c")).To(BeEmpty())
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, "claims/task/c")).To(BeEmpty())

		held := mgr.HeldClaims()
		Expect(held).To(HaveLen(2))
		Expect(held[0].Key).To(Equal("claims/task/a"))
		Expect(held[0].LeaseToken).To(Equal("SA"))
		Expect(held[1].Key).To(Equal("claims/task/e"))
		Expect(held[1].LeaseToken).To(Equal("SE"))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"adopted":  1,
			"released": 1,
			"fenced":   1,
			"error":    1,
		})

		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(10 * libtime.Second))
		before = snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		puts := requestsMatching(fake.Requests(), http.MethodPut, "")
		Expect(puts).To(HaveLen(2))
		shas := make([]string, 0, 2)
		for _, put := range puts {
			var body fakePutBody
			Expect(json.Unmarshal(put.Body, &body)).To(Succeed())
			Expect(put.Status).To(Equal(http.StatusOK))
			shas = append(shas, body.ExpectedSha)
		}
		Expect(shas).To(ConsistOf("SA", "SE"))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"renewed": 2,
			"adopted": 1,
		})
		for _, ref := range []string{
			"refs/claims/task/a", "refs/claims/task/c", "refs/claims/task/e",
		} {
			_, ok := fake.Ref(ref)
			Expect(ok).To(BeTrue(), ref)
		}
		for _, ref := range []string{"refs/claims/task/b", "refs/claims/task/d"} {
			_, ok := fake.Ref(ref)
			Expect(ok).To(BeFalse(), ref)
		}
	})

	It("keeps a fence claim pending when its Job deletion fails", func() {
		seedRestartFixture()
		setupRestartJob()
		job.deleteFn = func(taskID string, call int) error {
			if taskID == "t4" && call == 1 {
				return stderrors.New("delete failed")
			}
			return nil
		}
		before := snapshotClaims()
		Expect(mgr.Recover(ctx)).To(Succeed())

		Expect(requestsMatching(fake.Requests(), http.MethodDelete, "claims/task/d")).To(BeEmpty())
		held := mgr.HeldClaims()
		Expect(held).To(HaveLen(3))
		Expect(held[1].Key).To(Equal("claims/task/d"))
		// error +2: E's failed running check and D's failed Job deletion.
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"adopted":  1,
			"released": 1,
			"fenced":   1,
			"error":    2,
		})

		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(10 * libtime.Second))
		before = snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(job.DeleteCallCount("t4")).To(Equal(2))
		deletes := requestsMatching(fake.Requests(), http.MethodDelete, "claims/task/d")
		Expect(deletes).To(HaveLen(1))
		Expect(deletes[0].RawQuery).To(ContainSubstring("expected_sha=SD"))
		for _, claimHeld := range mgr.HeldClaims() {
			Expect(claimHeld.Key).NotTo(Equal("claims/task/d"))
		}
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"renewed": 2,
			"adopted": 1,
		})
	})

	It("releases a claim whose running check later says the Job is gone", func() {
		seedRestartFixture()
		job.runningFn = func(taskID string, call int) (bool, error) {
			switch taskID {
			case "t1", "t4":
				return true, nil
			case "t5":
				if call == 1 {
					return false, stderrors.New("kube api unavailable")
				}
				return false, nil
			default:
				return false, nil
			}
		}
		Expect(mgr.Recover(ctx)).To(Succeed())

		fake.ResetRequests()
		clock.SetNow(leaseTestT0.Add(10 * libtime.Second))
		before := snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		deletes := requestsMatching(fake.Requests(), http.MethodDelete, "claims/task/e")
		Expect(deletes).To(HaveLen(1))
		Expect(deletes[0].RawQuery).To(ContainSubstring("expected_sha=SE"))
		for _, held := range mgr.HeldClaims() {
			Expect(held.Key).NotTo(Equal("claims/task/e"))
		}
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"renewed":  1,
			"released": 1,
		})
	})

	It("keeps a claim pending when the release after a failed check fails", func() {
		seedRestartFixture()
		job.runningFn = func(taskID string, call int) (bool, error) {
			switch taskID {
			case "t1", "t4":
				return true, nil
			case "t5":
				if call == 1 {
					return false, stderrors.New("kube api unavailable")
				}
				return false, nil
			default:
				return false, nil
			}
		}
		Expect(mgr.Recover(ctx)).To(Succeed())
		fake.SetFault(faultMethod(http.MethodDelete, http.StatusInternalServerError))

		clock.SetNow(leaseTestT0.Add(10 * libtime.Second))
		before := snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(mgr.HeldClaims()).To(HaveLen(2))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"renewed": 1,
			"error":   1,
		})

		fake.SetFault(nil)
		clock.SetNow(leaseTestT0.Add(20 * libtime.Second))
		before = snapshotClaims()
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(mgr.HeldClaims()).To(HaveLen(1))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"released": 1})
	})

	It("fences an own-cluster claim with an unparseable lease_until", func() {
		fake.Seed("refs/claims/task/u", "SU", "self", "t6", "not-a-time")
		job.SetRunning(map[string]bool{"t6": false})
		before := snapshotClaims()
		Expect(mgr.Recover(ctx)).To(Succeed())
		Expect(mgr.HeldClaims()).To(BeEmpty())
		deletes := requestsMatching(fake.Requests(), http.MethodDelete, "claims/task/u")
		Expect(deletes).To(HaveLen(1))
		Expect(deletes[0].RawQuery).To(ContainSubstring("expected_sha=SU"))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"error":  1,
			"fenced": 1,
		})
	})

	It("does not hold a claim whose release fails during recovery", func() {
		seedRestartFixture()
		setupRestartJob()
		fake.SetFault(faultMethod(http.MethodDelete, http.StatusInternalServerError))
		before := snapshotClaims()
		Expect(mgr.Recover(ctx)).To(Succeed())
		for _, held := range mgr.HeldClaims() {
			Expect(held.Key).NotTo(Equal("claims/task/b"))
		}
		// A adopted, D fenced, B release failed, E running check failed.
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"adopted": 1,
			"fenced":  1,
			"error":   2,
		})
	})
})
