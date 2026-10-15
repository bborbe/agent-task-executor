// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
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
