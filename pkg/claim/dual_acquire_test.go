// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// dualRepoKey and dualPRKey are the two refs of one pr-review claim.
const (
	dualRepoKey = "claims/repo/bborbe/nuke"
	dualPRKey   = "claims/pr/bborbe/nuke/658"
)

// newDualManager builds a manager over fake without any Ginkgo helper, so the
// plain Go test below can use it too.
func newDualManager(
	cluster string,
	fake *fakeGitRest,
	clock libtime.CurrentDateTime,
	job claim.JobControl,
) claim.LeaseManager {
	mgr, err := claim.NewLeaseManager(
		cluster,
		claim.NewClaimRefClient(fake.URL(), "", ""),
		job,
		clock,
		libtime.NewWaiterDuration(),
	)
	if err != nil {
		panic(err)
	}
	return mgr
}

// TestDualAcquireKeys is AC1's evidence. It is deliberately a plain Go test:
// -run TestDualAcquireKeys against a Ginkgo It selects zero tests and exits 0
// vacuously.
func TestDualAcquireKeys(t *testing.T) {
	ctx := context.Background()

	// Case A: the old scheme holds the per-repo ref first; the new scheme's
	// dual acquire conflicts on the primary.
	fakeA := newFakeGitRest()
	defer fakeA.Close()
	clockA := newTestClock(leaseTestT0)
	jobA := newFakeJobControl(clockA)
	oldA := newDualManager("old", fakeA, clockA, jobA)
	newA := newDualManager("new", fakeA, clockA, jobA)

	if _, err := oldA.Claim(ctx, dualRepoKey, "t-1"); err != nil {
		t.Fatalf("old scheme Claim: %v", err)
	}
	if _, err := newA.ClaimPair(ctx, dualRepoKey, dualPRKey, "t-1"); err == nil {
		t.Fatal("new scheme ClaimPair succeeded while the old scheme held the repo key")
	} else if !stderrors.Is(err, claim.ErrLeaseConflict) {
		t.Fatalf("new scheme ClaimPair error = %v, want ErrLeaseConflict", err)
	}

	// Case B: the new scheme holds the pair first; the old scheme's single
	// Claim conflicts on the shared per-repo ref.
	fakeB := newFakeGitRest()
	defer fakeB.Close()
	clockB := newTestClock(leaseTestT0)
	jobB := newFakeJobControl(clockB)
	oldB := newDualManager("old", fakeB, clockB, jobB)
	newB := newDualManager("new", fakeB, clockB, jobB)

	if _, err := newB.ClaimPair(ctx, dualRepoKey, dualPRKey, "t-1"); err != nil {
		t.Fatalf("new scheme ClaimPair: %v", err)
	}
	if _, err := oldB.Claim(ctx, dualRepoKey, "t-1"); err == nil {
		t.Fatal("old scheme Claim succeeded while the new scheme held the repo key")
	} else if !stderrors.Is(err, claim.ErrLeaseConflict) {
		t.Fatalf("old scheme Claim error = %v, want ErrLeaseConflict", err)
	}
}

var _ = Describe("dual acquire", func() {
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

	It("AC2 writes the per-repo ref first and the per-PR ref second", func() {
		_, err := mgr.ClaimPair(ctx, dualRepoKey, dualPRKey, "t-1")
		Expect(err).NotTo(HaveOccurred())

		puts := requestsMatching(fake.Requests(), http.MethodPut, "claims/")
		Expect(puts).To(HaveLen(2))
		Expect(puts[0].Path).To(ContainSubstring(dualRepoKey))
		Expect(puts[1].Path).To(ContainSubstring(dualPRKey))
		Expect(putExpectedSha(puts[0])).To(BeEmpty())
		Expect(putExpectedSha(puts[1])).To(BeEmpty())
	})

	It("AC3 releases the primary when the secondary conflicts", func() {
		fake.SetFault(func(req fakeRequest) *fakeFault {
			if req.Method == http.MethodPut && strings.Contains(req.Path, "claims/pr/") {
				return &fakeFault{Status: http.StatusConflict}
			}
			return nil
		})

		_, err := mgr.ClaimPair(ctx, dualRepoKey, dualPRKey, "t-1")
		Expect(err).To(HaveOccurred())
		Expect(stderrors.Is(err, claim.ErrLeaseConflict)).To(BeTrue())

		// The conflict path reads the secondary back between the second PUT and
		// the DELETE, so only PUT/DELETE requests are compared here.
		recorded := make([]fakeRequest, 0, 3)
		for _, req := range fake.Requests() {
			if req.Method == http.MethodPut || req.Method == http.MethodDelete {
				recorded = append(recorded, req)
			}
		}
		Expect(recorded).To(HaveLen(3))
		Expect(recorded[0].Method).To(Equal(http.MethodPut))
		Expect(recorded[0].Path).To(ContainSubstring(dualRepoKey))
		Expect(recorded[0].Status).To(Equal(http.StatusOK))
		Expect(recorded[1].Method).To(Equal(http.MethodPut))
		Expect(recorded[1].Path).To(ContainSubstring(dualPRKey))
		Expect(recorded[1].Status).To(Equal(http.StatusConflict))
		Expect(recorded[2].Method).To(Equal(http.MethodDelete))
		Expect(recorded[2].Path).To(ContainSubstring(dualRepoKey))
		Expect(recorded[2].Status).To(Equal(http.StatusOK))
		// The fake assigns its first sha to the first successful PUT.
		Expect(deleteExpectedSha(recorded[2])).To(Equal(fmt.Sprintf("%040x", 1)))
		Expect(mgr.HeldClaims()).To(BeEmpty())
	})

	It("AC8(a) writes one ref for a task-id-keyed claim", func() {
		_, err := mgr.ClaimPair(ctx, "claims/task/t1", "", "t1")
		Expect(err).NotTo(HaveOccurred())

		requests := fake.Requests()
		Expect(requestsMatching(requests, http.MethodPut, "claims/")).To(HaveLen(1))
		Expect(requestsMatching(requests, http.MethodDelete, "claims/")).To(BeEmpty())
		Expect(mgr.HeldClaims()).To(HaveLen(1))
	})

	It("AC8(b) writes one ref for a pr-review task with no bare PR URL", func() {
		primary, secondary, fallback, err := claim.DeriveClaimKeysWithBody(
			ctx,
			"pr-review",
			"t2",
			"bborbe",
			"nuke",
			"no pull request url here",
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(primary).To(Equal("claims/task/t2"))
		Expect(secondary).To(BeEmpty())
		Expect(fallback).To(BeTrue())

		_, err = mgr.ClaimPair(ctx, primary, secondary, "t2")
		Expect(err).NotTo(HaveOccurred())

		requests := fake.Requests()
		puts := requestsMatching(requests, http.MethodPut, "claims/")
		Expect(puts).To(HaveLen(1))
		Expect(puts[0].Path).To(ContainSubstring("claims/task/t2"))
		Expect(requestsMatching(requests, http.MethodDelete, "claims/")).To(BeEmpty())
	})

	It("logs the dual acquire line with both keys", func() {
		out := captureGlogStderr(func() {
			_, err := mgr.ClaimPair(ctx, dualRepoKey, dualPRKey, "t-1")
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("event=claim_dual_acquired"))
		Expect(out).To(ContainSubstring("key=" + dualRepoKey))
		Expect(out).To(ContainSubstring("second=" + dualPRKey))
	})

	It("logs the dual conflict line when the secondary conflicts", func() {
		fake.SetFault(func(req fakeRequest) *fakeFault {
			if req.Method == http.MethodPut && strings.Contains(req.Path, "claims/pr/") {
				return &fakeFault{Status: http.StatusConflict}
			}
			return nil
		})
		out := captureGlogStderr(func() {
			_, err := mgr.ClaimPair(ctx, dualRepoKey, dualPRKey, "t-1")
			Expect(err).To(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("event=claim_dual_conflict"))
		Expect(out).To(ContainSubstring("key=" + dualPRKey))
	})
})

// deleteExpectedSha returns the expected_sha query value of a recorded DELETE.
func deleteExpectedSha(request fakeRequest) string {
	values, err := url.ParseQuery(request.RawQuery)
	if err != nil {
		return ""
	}
	return values.Get("expected_sha")
}
