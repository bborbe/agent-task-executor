// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package memlimit_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/memlimit"
)

var _ = Describe("ParseCgroupLimit", func() {
	DescribeTable(
		"reads a container memory ceiling out of a cgroup file",
		func(raw string, wantLimit int64, wantOK bool) {
			limit, ok := memlimit.ParseCgroupLimit(raw)
			Expect(ok).To(Equal(wantOK))
			Expect(limit).To(Equal(wantLimit))
		},
		Entry("cgroup v2 limit", "268435456", int64(268435456), true),
		Entry("cgroup v1 limit", "536870912", int64(536870912), true),
		// The kernel writes a trailing newline; a parser that does not trim it
		// silently treats every real limit as unreadable.
		Entry("trailing newline", "268435456\n", int64(268435456), true),
		Entry("surrounding whitespace", "  268435456  ", int64(268435456), true),
		// cgroup v2 publishes "max" instead of omitting the file.
		Entry("cgroup v2 unlimited", "max", int64(0), false),
		Entry("cgroup v2 unlimited with newline", "max\n", int64(0), false),
		// cgroup v1 publishes a near-int64-max sentinel when unlimited.
		Entry("cgroup v1 unlimited sentinel", "9223372036854771712", int64(0), false),
		Entry("empty", "", int64(0), false),
		Entry("zero", "0", int64(0), false),
		Entry("negative", "-1", int64(0), false),
		Entry("not a number", "abc", int64(0), false),
	)
})
