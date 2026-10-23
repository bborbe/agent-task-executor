// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	"flag"
	"io"
	"net/http"
	"os"

	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// captureGlogStderr runs fn with glog's stderr output redirected into a pipe
// and returns everything written. The suite runs serially, so the global
// os.Stderr swap cannot race another spec's logging.
func captureGlogStderr(fn func()) string {
	Expect(flag.Set("logtostderr", "true")).To(Succeed())
	defer func() {
		Expect(flag.Set("logtostderr", "false")).To(Succeed())
	}()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	Expect(err).ToNot(HaveOccurred())
	os.Stderr = writer

	fn()
	glog.Flush()

	os.Stderr = original
	Expect(writer.Close()).To(Succeed())
	out, err := io.ReadAll(reader)
	Expect(err).ToNot(HaveOccurred())
	Expect(reader.Close()).To(Succeed())
	return string(out)
}

var _ = Describe("LeaseManager claim release log", func() {
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

	It("logs one line with result=released", func() {
		key := "claims/task/log-released"
		task := "t-log-released"
		_, err := mgr.Claim(ctx, key, task)
		Expect(err).NotTo(HaveOccurred())

		out := captureGlogStderr(func() {
			Expect(mgr.Release(ctx, key)).To(Succeed())
		})

		Expect(out).To(ContainSubstring(
			"event=claim_released key=" + key + " task=" + task + " result=released",
		))
	})

	It("logs one line with result=already_released", func() {
		key := "claims/task/log-already-released"
		task := "t-log-already-released"
		fake.SetFault(faultMethod(http.MethodDelete, http.StatusConflict))
		_, err := mgr.Claim(ctx, key, task)
		Expect(err).NotTo(HaveOccurred())

		out := captureGlogStderr(func() {
			Expect(mgr.Release(ctx, key)).To(Succeed())
		})

		Expect(out).To(ContainSubstring(
			"event=claim_released key=" + key + " task=" + task + " result=already_released",
		))
	})

	It("logs one line with result=fenced", func() {
		key := "claims/task/log-fenced"
		task := "t-log-fenced"
		_, err := mgr.Claim(ctx, key, task)
		Expect(err).NotTo(HaveOccurred())

		out := captureGlogStderr(func() {
			clock.SetNow(leaseTestT0.Add(240 * libtime.Second))
			Expect(mgr.Tick(ctx)).To(Succeed())
		})

		Expect(out).To(ContainSubstring(
			"event=claim_released key=" + key + " task=" + task + " result=fenced",
		))
	})

	It("logs one line with result=claim_lost", func() {
		key := "claims/task/log-claim-lost"
		task := "t-log-claim-lost"
		_, err := mgr.Claim(ctx, key, task)
		Expect(err).NotTo(HaveOccurred())
		fake.Seed(
			"refs/"+key,
			"S-other",
			"other",
			"t-other",
			rfc3339(leaseTestT0.Add(claim.LeaseDuration)),
		)

		out := captureGlogStderr(func() {
			clock.SetNow(leaseTestT0.Add(60 * libtime.Second))
			Expect(mgr.Tick(ctx)).To(Succeed())
		})

		Expect(out).To(ContainSubstring(
			"event=claim_released key=" + key + " task=" + task + " result=claim_lost",
		))
	})

	It("logs result=released and not result=fenced when the hold expires", func() {
		key := "claims/task/log-hold-expiry"
		task := "t-log-hold-expiry"
		_, err := mgr.Claim(ctx, key, task)
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr.ReleaseAfterSuccess(ctx, key, clock.Now())).To(Succeed())

		// The 10 s ticks renew the lease, so the fence never comes due.
		tickAt(mgr, clock, leaseTestT0.Add(290*libtime.Second))

		out := captureGlogStderr(func() {
			tickAt(mgr, clock, leaseTestT0.Add(310*libtime.Second))
		})

		Expect(out).To(ContainSubstring("result=released"))
		Expect(out).NotTo(ContainSubstring("result=fenced"))
	})
})
