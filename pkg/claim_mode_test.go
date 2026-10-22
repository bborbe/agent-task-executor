// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	stderrors "errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg"
)

var _ = Describe("ParseClaimMode", func() {
	DescribeTable("valid and absent values",
		func(value string, present bool, expected pkg.ClaimMode) {
			mode, err := pkg.ParseClaimMode(context.Background(), value, present)
			Expect(err).NotTo(HaveOccurred())
			Expect(mode).To(Equal(expected))
		},
		Entry("absent variable is off", "", false, pkg.ClaimModeOff),
		Entry("off", "off", true, pkg.ClaimModeOff),
		Entry("shadow", "shadow", true, pkg.ClaimModeShadow),
		Entry("on", "on", true, pkg.ClaimModeOn),
	)

	DescribeTable("invalid values",
		func(value string) {
			mode, err := pkg.ParseClaimMode(context.Background(), value, true)
			Expect(err).To(HaveOccurred())
			Expect(stderrors.Is(err, pkg.ErrClaimModeInvalid)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring(`"` + value + `"`))
			Expect(err.Error()).To(ContainSubstring("off|shadow|on"))
			Expect(mode).To(Equal(pkg.ClaimMode("")))
		},
		Entry("explicitly empty", ""),
		Entry("uppercase ON", "ON"),
		Entry("trailing character", "shadoww"),
		Entry("boolean", "true"),
	)
})
