// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg"
)

var _ = Describe("ZombieReason", func() {
	Describe("ParseZombieReason", func() {
		// The table covers the whole closed vocabulary. A new ZombieReason must
		// be added here as well as to the const block — the per-value String()
		// specs below do not exercise the parser.
		DescribeTable("round-trips every vocabulary value",
			func(reason pkg.ZombieReason) {
				parsed, ok := pkg.ParseZombieReason(reason.String())
				Expect(ok).To(BeTrue())
				Expect(parsed).To(Equal(reason))
			},
			Entry("image_pull_backoff", pkg.ZombieReasonImagePullBackOff),
			Entry("pod_evicted", pkg.ZombieReasonPodEvicted),
			Entry("deadline_exceeded", pkg.ZombieReasonDeadlineExceeded),
			Entry("pod_not_scheduled", pkg.ZombieReasonPodNotScheduled),
			Entry("pod_crash_no_stdout", pkg.ZombieReasonPodCrashNoStdout),
			Entry("executor_watch_lost", pkg.ZombieReasonExecutorWatchLost),
			Entry("type_mismatch", pkg.ZombieReasonTypeMismatch),
			Entry("pod_oom_killed", pkg.ZombieReasonPodOOMKilled),
		)

		It("parses the formatted backoffFailureReason form up to the first space", func() {
			parsed, ok := pkg.ParseZombieReason("pod_oom_killed (OOMKilled/137)")
			Expect(ok).To(BeTrue())
			Expect(parsed).To(Equal(pkg.ZombieReasonPodOOMKilled))
		})

		It("rejects a string outside the vocabulary", func() {
			_, ok := pkg.ParseZombieReason("not_a_reason")
			Expect(ok).To(BeFalse())
		})

		It("rejects the empty string", func() {
			_, ok := pkg.ParseZombieReason("")
			Expect(ok).To(BeFalse())
		})
	})

	Describe("String()", func() {
		It("returns image_pull_backoff for ZombieReasonImagePullBackOff", func() {
			Expect(pkg.ZombieReasonImagePullBackOff.String()).To(Equal("image_pull_backoff"))
		})
		It("returns pod_evicted for ZombieReasonPodEvicted", func() {
			Expect(pkg.ZombieReasonPodEvicted.String()).To(Equal("pod_evicted"))
		})
		It("returns deadline_exceeded for ZombieReasonDeadlineExceeded", func() {
			Expect(pkg.ZombieReasonDeadlineExceeded.String()).To(Equal("deadline_exceeded"))
		})
		It("returns pod_not_scheduled for ZombieReasonPodNotScheduled", func() {
			Expect(pkg.ZombieReasonPodNotScheduled.String()).To(Equal("pod_not_scheduled"))
		})
		It("returns pod_crash_no_stdout for ZombieReasonPodCrashNoStdout", func() {
			Expect(pkg.ZombieReasonPodCrashNoStdout.String()).To(Equal("pod_crash_no_stdout"))
		})
		It("returns executor_watch_lost for ZombieReasonExecutorWatchLost", func() {
			Expect(pkg.ZombieReasonExecutorWatchLost.String()).To(Equal("executor_watch_lost"))
		})
		It("returns type_mismatch for ZombieReasonTypeMismatch", func() {
			Expect(pkg.ZombieReasonTypeMismatch.String()).To(Equal("type_mismatch"))
		})
		It("returns pod_oom_killed for ZombieReasonPodOOMKilled", func() {
			Expect(pkg.ZombieReasonPodOOMKilled.String()).To(Equal("pod_oom_killed"))
		})
	})
})
