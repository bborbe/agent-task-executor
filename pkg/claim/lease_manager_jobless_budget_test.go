// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

var _ = Describe("LeaseManager jobless pass budget", func() {
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

	claimFive := func() {
		for _, suffix := range []string{"a", "b", "c", "d", "e"} {
			_, err := mgr.Claim(
				ctx,
				"claims/task/budget-"+suffix,
				"t-budget-"+suffix,
			)
			Expect(err).NotTo(HaveOccurred())
		}
	}

	It("starts no check after the pass budget is spent", func() {
		claimFive()
		checked := []string{}
		job.runningFn = func(taskID string, _ int) (bool, error) {
			checked = append(checked, taskID)
			advance(clock, 4*libtime.Second)
			return true, nil
		}

		clock.SetNow(leaseTestT0.Add(10 * libtime.Second))
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(len(checked)).To(BeNumerically("<=", 3))
		Expect(checked).To(Equal([]string{"t-budget-a", "t-budget-b", "t-budget-c"}))

		advance(clock, 10*libtime.Second)
		Expect(mgr.Tick(ctx)).To(Succeed())
		Expect(checked).To(Equal([]string{
			"t-budget-a", "t-budget-b", "t-budget-c", "t-budget-d", "t-budget-e",
		}))

		Expect(mgr.HeldClaims()).To(HaveLen(5))
	})

	It("does not starve late claims when only one check fits per tick", func() {
		claimFive()
		checked := []string{}
		job.runningFn = func(taskID string, _ int) (bool, error) {
			checked = append(checked, taskID)
			advance(clock, 12*libtime.Second)
			return true, nil
		}

		for i := 0; i < 5; i++ {
			advance(clock, claim.TickInterval)
			Expect(mgr.Tick(ctx)).To(Succeed())
		}

		Expect(checked).To(Equal([]string{
			"t-budget-a", "t-budget-b", "t-budget-c", "t-budget-d", "t-budget-e",
		}))
	})
})
