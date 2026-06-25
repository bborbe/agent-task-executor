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
	})
})
