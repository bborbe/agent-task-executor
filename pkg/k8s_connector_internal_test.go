// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"regexp"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apiextensionsv1 "k8s.io/apiextensions-api/pkg/apis/apiextensions/v1"
)

var _ = Describe("crdSpecEqual", func() {
	It("treats a spec the API server defaulted as equal to the desired spec", func() {
		// The server stores spec.conversion.strategy: None on create, while
		// desiredCRDSpec leaves Conversion nil. Comparing a stored spec against an
		// undefaulted desired one would therefore never be equal, and the Update would
		// be re-issued on every executor start — the needless write the no-op check in
		// ensureCRD exists to prevent. The fake clientset does not apply the server's
		// defaulting, so it is applied by hand here; drop the normalisation from
		// crdSpecEqual and this spec fails.
		desired := desiredCRDSpec()
		Expect(desired.Conversion).To(BeNil())

		stored := desired.DeepCopy()
		apiextensionsv1.SetDefaults_CustomResourceDefinitionSpec(stored)
		Expect(stored.Conversion).ToNot(BeNil())

		Expect(crdSpecEqual(*stored, desired)).To(BeTrue())
	})

	It("reports a genuinely different spec as unequal", func() {
		desired := desiredCRDSpec()
		changed := desired.DeepCopy()
		changed.Group = "other.benjamin-borbe.de"
		Expect(crdSpecEqual(*changed, desired)).To(BeFalse())
	})
})

var _ = Describe("configSpecProperties", func() {
	It("declares jobNamespace so the API server cannot prune it", func() {
		props := configSpecProperties()
		Expect(props).To(HaveKey("jobNamespace"))
		Expect(props["jobNamespace"].Type).To(Equal("string"))
	})

	It("declares serviceNamespace with the same contract as jobNamespace", func() {
		props := configSpecProperties()
		Expect(props).To(HaveKey("serviceNamespace"))
		Expect(props["serviceNamespace"].Type).To(Equal("string"))
		Expect(props["serviceNamespace"].Pattern).To(Equal(crdDNS1123Label))
		Expect(props["serviceNamespace"].MaxLength).NotTo(BeNil())
		Expect(*props["serviceNamespace"].MaxLength).To(Equal(int64(63)))
	})

	// Pins serviceNamespace to its sibling jobNamespace rather than to literals:
	// a field missing from the map is pruned from every Config within seconds of
	// the next restart, silently. Comparing the two entries keeps the pair in
	// lockstep when either contract moves.
	It("pins serviceNamespace's schema to jobNamespace's", func() {
		props := configSpecProperties()
		service, ok := props["serviceNamespace"]
		Expect(ok).To(BeTrue(), "serviceNamespace missing from configSpecProperties")
		job, ok := props["jobNamespace"]
		Expect(ok).To(BeTrue(), "jobNamespace missing from configSpecProperties")

		Expect(service.Type).To(Equal(job.Type))
		Expect(service.Pattern).To(Equal(job.Pattern))
		Expect(service.MaxLength).ToNot(BeNil())
		Expect(job.MaxLength).ToNot(BeNil())
		Expect(*service.MaxLength).To(Equal(*job.MaxLength))
	})

	It("declares targetVault so the API server cannot prune it", func() {
		props := configSpecProperties()
		Expect(props).To(HaveKey("targetVault"))
		Expect(props["targetVault"].Type).To(Equal("string"))
		Expect(props["targetVault"].Pattern).To(Equal(crdVaultSlug))
	})

	// Comparing Pattern to crdVaultSlug asserts what was typed, not what
	// admission accepts. Compile the pattern and exercise it, including the
	// hyphenated vaults the probe actually targets: a pattern that lost "-"
	// from its character class would pass the assertions above and then reject
	// every one of those Configs at admission.
	DescribeTable("targetVault pattern admits vault slugs and rejects non-slugs",
		func(value string, want bool) {
			props := configSpecProperties()
			pattern := regexp.MustCompile(props["targetVault"].Pattern)
			Expect(pattern.MatchString(value)).To(Equal(want))
		},
		Entry("openclaw", "openclaw", true),
		Entry("personal", "personal", true),
		Entry("private-agent", "private-agent", true),
		Entry("private-personal", "private-personal", true),
		Entry("private-trading", "private-trading", true),
		Entry("uppercase", "Private", false),
		Entry("leading digit", "1abc", false),
		Entry("underscore", "private_agent", false),
		Entry("space", "private agent", false),
		Entry("leading hyphen", "-private", false),
	)
})
