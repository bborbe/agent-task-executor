// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	"net/http"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

var _ = Describe("LeaseManager restart recovery with streak refs", func() {
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

	It("skips streak refs regardless of their cluster field", func() {
		// Inside the fence window: without the prefix skip, this own-cluster
		// streak ref would be fenced and its "Job" deleted.
		fake.Seed(
			"refs/claims/redrive/t-streak/ai_review/1",
			"S-STREAK",
			"self",
			"t-streak",
			rfc3339(leaseTestT0.Add(30*libtime.Second)),
		)
		// Proves the skip is by key, not by the cluster sentinel.
		fake.Seed(
			"refs/claims/redrive/t-other/ai_review/1",
			"S-OTHER",
			"other",
			"t-other",
			rfc3339(leaseTestT0.Add(30*libtime.Second)),
		)

		runningCalls := 0
		job.runningFn = func(_ string, _ int) (bool, error) {
			runningCalls++
			return true, nil
		}

		before := snapshotClaims()
		Expect(mgr.Recover(ctx)).To(Succeed())

		Expect(requestsMatching(fake.Requests(), http.MethodDelete, "")).To(BeEmpty())
		Expect(runningCalls).To(Equal(0))
		Expect(job.DeleteCallCount("t-streak")).To(Equal(0))
		Expect(job.Deleted()).To(BeEmpty())
		Expect(requestsMatching(fake.Requests(), http.MethodPut, "")).To(BeEmpty())
		Expect(mgr.HeldClaims()).To(BeEmpty())

		ref, ok := fake.Ref("refs/claims/redrive/t-streak/ai_review/1")
		Expect(ok).To(BeTrue())
		Expect(ref.Sha).To(Equal("S-STREAK"))

		expectClaimDelta(before, snapshotClaims(), map[string]float64{})
	})
})
