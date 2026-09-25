// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package v1_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// TestV1 is the package's Ginkgo entry point. It lives in a *_suite_test.go
// file, as in pkg/probe, pkg/metrics and pkg/spawner, so the suite is
// discoverable by convention rather than by reading whichever test file
// happens to hold RunSpecs.
func TestV1(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "V1 Suite")
}
