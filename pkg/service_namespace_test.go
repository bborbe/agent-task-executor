// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libk8s "github.com/bborbe/k8s"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

var _ = Describe("ResolveServiceNamespace", func() {
	It("defaults an empty value to the executor namespace", func() {
		resolved, err := pkg.ResolveServiceNamespace(
			context.Background(),
			libk8s.Namespace("agent"),
			"",
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(resolved).To(Equal(libk8s.Namespace("agent")))
	})

	It("defaults an empty value to a dev executor namespace", func() {
		resolved, err := pkg.ResolveServiceNamespace(
			context.Background(),
			libk8s.Namespace("dev"),
			"",
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(resolved).To(Equal(libk8s.Namespace("dev")))
	})

	It("uses an explicit different namespace", func() {
		resolved, err := pkg.ResolveServiceNamespace(
			context.Background(),
			libk8s.Namespace("dev"),
			"agent-sandbox",
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(resolved).To(Equal(libk8s.Namespace("agent-sandbox")))
	})

	It("rejects a value equal to the executor namespace", func() {
		_, err := pkg.ResolveServiceNamespace(
			context.Background(),
			libk8s.Namespace("agent"),
			"agent",
		)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("may not equal the executor namespace"))
	})
})
