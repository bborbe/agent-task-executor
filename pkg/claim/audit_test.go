// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

var _ = Describe("LeaseManager AuditMismatch", func() {
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

	It("counts one mismatch per Job without a claim and per claim without a Job", func() {
		_, err := mgr.Claim(ctx, "claims/task/t1", "t1")
		Expect(err).NotTo(HaveOccurred())

		before := testutil.ToFloat64(metrics.ClaimMismatchTotal)
		mgr.AuditMismatch(ctx, map[string]bool{"t2": true})
		Expect(testutil.ToFloat64(metrics.ClaimMismatchTotal) - before).To(Equal(float64(2)))
	})

	It("counts nothing when a task is both held and running", func() {
		_, err := mgr.Claim(ctx, "claims/task/t1", "t1")
		Expect(err).NotTo(HaveOccurred())

		before := testutil.ToFloat64(metrics.ClaimMismatchTotal)
		mgr.AuditMismatch(ctx, map[string]bool{"t1": true})
		Expect(testutil.ToFloat64(metrics.ClaimMismatchTotal) - before).To(Equal(float64(0)))
	})

	It("counts one mismatch for a dual claim without a Job, not one per ref", func() {
		_, err := mgr.ClaimPair(ctx, "claims/repo/bborbe/nuke", "claims/pr/bborbe/nuke/658", "t1")
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr.HeldClaims()).To(HaveLen(2))

		before := testutil.ToFloat64(metrics.ClaimMismatchTotal)
		mgr.AuditMismatch(ctx, map[string]bool{})
		Expect(testutil.ToFloat64(metrics.ClaimMismatchTotal) - before).To(Equal(float64(1)))
	})

	It("counts one mismatch for a running Job with no held claims", func() {
		before := testutil.ToFloat64(metrics.ClaimMismatchTotal)
		mgr.AuditMismatch(ctx, map[string]bool{"t3": true})
		Expect(testutil.ToFloat64(metrics.ClaimMismatchTotal) - before).To(Equal(float64(1)))
	})
})
