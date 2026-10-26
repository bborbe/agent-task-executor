// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	stderrors "errors"
	"net/http"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// claimCount reads one executor_claim_total series directly.
func claimCount(result string) float64 {
	return testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues(result))
}

var _ = Describe("LeaseManager jobless claim release edge cases", func() {
	var (
		ctx   context.Context
		fake  *fakeGitRest
		clock libtime.CurrentDateTime
		job   *fakeJobControl
		mgr   claim.LeaseManager
	)

	const (
		joblessKey  = "claims/repo/bborbe/nuke"
		joblessTask = "t-jobless"
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

	It("never releases a claim while the Job keeps running", func() {
		_, err := mgr.Claim(ctx, joblessKey, joblessTask)
		Expect(err).NotTo(HaveOccurred())
		job.SetRunning(map[string]bool{joblessTask: true})
		before := claimCount("jobless_released")

		tickAt(mgr, clock, leaseTestT0.Add(30*libtime.Minute))

		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(BeEmpty())
		Expect(countSuccessfulPuts(fake.Requests())).To(BeNumerically(">=", 25))
		Expect(job.runningCalls[joblessTask]).To(BeNumerically("<=", 31))
		Expect(claimCount("jobless_released") - before).To(Equal(0.0))
	})

	It("starts the grace clock when the Job stops", func() {
		_, err := mgr.Claim(ctx, joblessKey, joblessTask)
		Expect(err).NotTo(HaveOccurred())
		running := map[string]bool{joblessTask: true}
		job.SetRunning(running)

		tickAt(mgr, clock, leaseTestT0.Add(600*libtime.Second))
		running[joblessTask] = false
		tickAt(mgr, clock, leaseTestT0.Add(900*libtime.Second))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(BeEmpty())

		tickAt(mgr, clock, leaseTestT0.Add(970*libtime.Second))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(HaveLen(1))
	})

	It("resets the grace clock when a check sees the Job running", func() {
		_, err := mgr.Claim(ctx, joblessKey, joblessTask)
		Expect(err).NotTo(HaveOccurred())
		running := map[string]bool{joblessTask: false}
		job.SetRunning(running)

		tickAt(mgr, clock, leaseTestT0.Add(200*libtime.Second))
		running[joblessTask] = true
		tickAt(mgr, clock, leaseTestT0.Add(260*libtime.Second))
		running[joblessTask] = false
		tickAt(mgr, clock, leaseTestT0.Add(320*libtime.Second))

		tickAt(mgr, clock, leaseTestT0.Add(600*libtime.Second))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(BeEmpty())

		tickAt(mgr, clock, leaseTestT0.Add(680*libtime.Second))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(HaveLen(1))
	})

	It("does not touch a claim inside its success hold", func() {
		_, err := mgr.Claim(ctx, joblessKey, joblessTask)
		Expect(err).NotTo(HaveOccurred())
		job.SetRunning(map[string]bool{})
		tickAt(mgr, clock, leaseTestT0.Add(200*libtime.Second))
		Expect(mgr.ReleaseAfterSuccess(ctx, joblessKey, clock.Now())).To(Succeed())
		beforeJobless := claimCount("jobless_released")
		beforeReleased := claimCount("released")

		tickAt(mgr, clock, leaseTestT0.Add(490*libtime.Second))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(BeEmpty())
		Expect(claimCount("jobless_released") - beforeJobless).To(Equal(0.0))

		tickAt(mgr, clock, leaseTestT0.Add(560*libtime.Second))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(HaveLen(1))
		Expect(claimCount("released") - beforeReleased).To(Equal(1.0))
	})

	It("does not touch a claim with a pending release", func() {
		_, err := mgr.Claim(ctx, joblessKey, joblessTask)
		Expect(err).NotTo(HaveOccurred())
		job.SetRunning(map[string]bool{})
		tickAt(mgr, clock, leaseTestT0.Add(250*libtime.Second))
		fake.SetFault(faultMethod(http.MethodDelete, http.StatusInternalServerError))
		Expect(mgr.Release(ctx, joblessKey)).NotTo(Succeed())
		before := claimCount("jobless_released")

		tickAt(mgr, clock, leaseTestT0.Add(600*libtime.Second))
		Expect(claimCount("jobless_released") - before).To(Equal(0.0))
	})

	It("does not touch a claim with a pending Job deletion", func() {
		_, err := mgr.Claim(ctx, joblessKey, joblessTask)
		Expect(err).NotTo(HaveOccurred())
		job.deleteFn = func(taskID string, call int) error {
			return stderrors.New("delete failed")
		}
		fake.SetFault(faultMethod(http.MethodPut, http.StatusInternalServerError))
		before := claimCount("jobless_released")

		tickAt(mgr, clock, leaseTestT0.Add(240*libtime.Second))
		tickAt(mgr, clock, leaseTestT0.Add(840*libtime.Second))
		Expect(claimCount("jobless_released") - before).To(Equal(0.0))
	})

	It("does not touch an adopted claim whose running check has not answered", func() {
		fake.Seed(
			"refs/"+joblessKey,
			"SA",
			"self",
			joblessTask,
			rfc3339(leaseTestT0.Add(1000*libtime.Second)),
		)
		job.runningFn = func(taskID string, call int) (bool, error) {
			return false, stderrors.New("kube api unavailable")
		}
		Expect(mgr.Recover(ctx)).To(Succeed())
		before := claimCount("jobless_released")

		tickAt(mgr, clock, leaseTestT0.Add(600*libtime.Second))
		Expect(claimCount("jobless_released") - before).To(Equal(0.0))
		Expect(job.runningCalls[joblessTask]).To(Equal(61))
	})

	It("keeps the claim when the running lookup keeps failing", func() {
		_, err := mgr.Claim(ctx, joblessKey, joblessTask)
		Expect(err).NotTo(HaveOccurred())
		job.runningFn = func(taskID string, call int) (bool, error) {
			return false, stderrors.New("kube api unavailable")
		}
		beforeError := claimCount("error")
		beforeJobless := claimCount("jobless_released")

		tickAt(mgr, clock, leaseTestT0.Add(10*libtime.Minute))

		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(BeEmpty())
		Expect(mgr.HeldClaims()).To(HaveLen(1))
		Expect(mgr.HeldClaims()[0].Key).To(Equal(joblessKey))
		Expect(claimCount("error") - beforeError).To(BeNumerically(">", 0))
		Expect(claimCount("jobless_released") - beforeJobless).To(Equal(0.0))
	})

	It("keeps the grace clock across an erroring check", func() {
		_, err := mgr.Claim(ctx, joblessKey, joblessTask)
		Expect(err).NotTo(HaveOccurred())
		job.runningFn = func(taskID string, call int) (bool, error) {
			if call == 3 || call == 4 {
				return false, stderrors.New("kube api unavailable")
			}
			return false, nil
		}
		beforeError := claimCount("error")
		beforeJobless := claimCount("jobless_released")

		tickAt(mgr, clock, leaseTestT0.Add(300*libtime.Second))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(BeEmpty())

		tickAt(mgr, clock, leaseTestT0.Add(370*libtime.Second))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(HaveLen(1))
		Expect(claimCount("error") - beforeError).To(Equal(2.0))
		Expect(claimCount("jobless_released") - beforeJobless).To(Equal(1.0))
	})

	It("counts the decision once when the DELETE fails and retries it", func() {
		_, err := mgr.Claim(ctx, joblessKey, joblessTask)
		Expect(err).NotTo(HaveOccurred())
		job.SetRunning(map[string]bool{})
		deleteCalls := 0
		fake.SetFault(func(request fakeRequest) *fakeFault {
			if request.Method != http.MethodDelete {
				return nil
			}
			deleteCalls++
			if deleteCalls == 1 {
				return &fakeFault{Status: http.StatusInternalServerError}
			}
			return nil
		})
		beforeError := claimCount("error")
		beforeJobless := claimCount("jobless_released")
		beforeReleased := claimCount("released")

		tickAt(mgr, clock, leaseTestT0.Add(310*libtime.Second))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(HaveLen(1))
		Expect(mgr.HeldClaims()).To(HaveLen(1))
		Expect(claimCount("jobless_released") - beforeJobless).To(Equal(1.0))
		Expect(claimCount("error") - beforeError).To(Equal(1.0))
		Expect(claimCount("released") - beforeReleased).To(Equal(0.0))

		tickAt(mgr, clock, leaseTestT0.Add(320*libtime.Second))
		Expect(mgr.HeldClaims()).To(BeEmpty())
		Expect(claimCount("released") - beforeReleased).To(Equal(1.0))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(HaveLen(2))
		Expect(claimCount("jobless_released") - beforeJobless).To(Equal(1.0))
	})

	It("drops the claim when the DELETE answers 409", func() {
		_, err := mgr.Claim(ctx, joblessKey, joblessTask)
		Expect(err).NotTo(HaveOccurred())
		job.SetRunning(map[string]bool{})
		fake.SetFault(faultMethod(http.MethodDelete, http.StatusConflict))
		beforeJobless := claimCount("jobless_released")
		beforeAlready := claimCount("already_released")

		tickAt(mgr, clock, leaseTestT0.Add(370*libtime.Second))

		Expect(mgr.HeldClaims()).To(BeEmpty())
		Expect(claimCount("already_released") - beforeAlready).To(Equal(1.0))
		Expect(claimCount("jobless_released") - beforeJobless).To(Equal(1.0))
		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(HaveLen(1))
	})

	It("frees an adopted repository-keyed claim once its Job stops", func() {
		fake.Seed(
			"refs/"+joblessKey,
			"SA",
			"self",
			joblessTask,
			rfc3339(leaseTestT0.Add(1000*libtime.Second)),
		)
		running := map[string]bool{joblessTask: true}
		job.SetRunning(running)
		Expect(mgr.Recover(ctx)).To(Succeed())
		Expect(mgr.HeldClaims()).To(HaveLen(1))
		Expect(mgr.HeldClaims()[0].Key).To(Equal(joblessKey))
		fake.ResetRequests()
		running[joblessTask] = false
		before := claimCount("jobless_released")

		tickAt(mgr, clock, leaseTestT0.Add(370*libtime.Second))

		Expect(requestsMatching(fake.Requests(), http.MethodDelete, joblessKey)).To(HaveLen(1))
		Expect(claimCount("jobless_released") - before).To(Equal(1.0))
	})
})
