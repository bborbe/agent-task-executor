// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"testing"

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

// TestServiceNamespaceSchemaParity pins serviceNamespace's schema to the sibling
// jobNamespace entry rather than to literals: a field missing from the map is
// pruned from every Config within seconds of the next restart, silently.
func TestServiceNamespaceSchemaParity(t *testing.T) {
	props := configSpecProperties()

	service, ok := props["serviceNamespace"]
	if !ok {
		t.Fatal("serviceNamespace missing from configSpecProperties")
	}
	job, ok := props["jobNamespace"]
	if !ok {
		t.Fatal("jobNamespace missing from configSpecProperties")
	}

	if service.Type != job.Type {
		t.Fatalf("expected Type %q, got %q", job.Type, service.Type)
	}
	if service.Pattern != job.Pattern {
		t.Fatalf("expected Pattern %q, got %q", job.Pattern, service.Pattern)
	}
	if service.MaxLength == nil || job.MaxLength == nil {
		t.Fatalf("expected both MaxLength set, got service=%v job=%v",
			service.MaxLength, job.MaxLength)
	}
	if *service.MaxLength != *job.MaxLength {
		t.Fatalf("expected MaxLength %d, got %d", *job.MaxLength, *service.MaxLength)
	}
}

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
})
