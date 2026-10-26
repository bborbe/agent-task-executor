// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	stderrors "errors"
	"time"

	libtime "github.com/bborbe/time"
	libtimemocks "github.com/bborbe/time/mocks"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/mocks"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/claim/claimtest"
)

var _ = Describe("LeaseManager loop", func() {
	It("waits exactly TickInterval and returns nil on cancellation", func() {
		clock := newTestClock(leaseTestT0)
		client := &mocks.FakeClaimRefClient{}
		client.ListStub = func(context.Context, string) ([]claim.ClaimRef, error) {
			return []claim.ClaimRef{}, nil
		}
		job := newFakeJobControl(clock)
		waiter := &libtimemocks.WaiterDuration{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		waiter.WaitStub = func(waitCtx context.Context, _ libtime.Duration) error {
			cancel()
			return waitCtx.Err()
		}
		mgr, err := claim.NewLeaseManager(
			"self",
			claimtest.NewClient(client, clock.Now),
			job,
			clock, waiter)
		Expect(err).NotTo(HaveOccurred())

		done := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			done <- mgr.Run(ctx)
		}()

		var runErr error
		Eventually(func() bool {
			select {
			case runErr = <-done:
				return true
			default:
				return false
			}
		}, time.Second).Should(BeTrue())
		Expect(runErr).NotTo(HaveOccurred())
		Expect(waiter.WaitCallCount()).To(BeNumerically(">=", 1))
		_, duration := waiter.WaitArgsForCall(0)
		Expect(duration).To(Equal(claim.TickInterval))
	})

	It("returns the waiter error while the context is still live", func() {
		clock := newTestClock(leaseTestT0)
		client := &mocks.FakeClaimRefClient{}
		job := newFakeJobControl(clock)
		waiter := &libtimemocks.WaiterDuration{}
		waiter.WaitStub = func(context.Context, libtime.Duration) error {
			return stderrors.New("wait failed")
		}
		mgr, err := claim.NewLeaseManager(
			"self",
			claimtest.NewClient(client, clock.Now),
			job,
			clock, waiter)
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr.Run(context.Background())).NotTo(Succeed())
	})
})
