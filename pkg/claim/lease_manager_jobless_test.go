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
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

var _ = Describe("LeaseManager jobless claim release", func() {
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

	It("releases a held claim whose Job has been absent for 300 s", func() {
		key := "claims/repo/bborbe/nuke"
		_, err := mgr.Claim(ctx, key, "4684419b")
		Expect(err).NotTo(HaveOccurred())
		job.SetRunning(map[string]bool{})
		fake.ResetRequests()

		beforeJobless := testutil.ToFloat64(
			metrics.ClaimTotal.WithLabelValues("jobless_released"),
		)
		beforeReleased := testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues("released"))

		tickAt(mgr, clock, leaseTestT0.Add(300*libtime.Second))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, key)).To(BeEmpty())

		held := mgr.HeldClaims()
		Expect(held).To(HaveLen(1))
		sha := held[0].Sha

		tickAt(mgr, clock, leaseTestT0.Add(370*libtime.Second))
		deletes := requestsMatching(fake.Requests(), http.MethodDelete, key)
		Expect(deletes).To(HaveLen(1))
		Expect(deletes[0].RawQuery).To(ContainSubstring(sha))
		Expect(mgr.HeldClaims()).To(BeEmpty())

		Expect(
			testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues("jobless_released")) -
				beforeJobless,
		).To(Equal(1.0))
		Expect(
			testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues("released")) -
				beforeReleased,
		).To(Equal(1.0))

		_, err = mgr.Claim(ctx, key, "ca0dc7eb")
		Expect(err).NotTo(HaveOccurred())
	})
})
