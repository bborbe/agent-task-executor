// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// TestPkg is the Ginkgo suite entry-point for the pkg package. It lives in its
// own *_suite_test.go (package convention) so spec discovery is obvious — the
// bootstrap previously sat at the top of agent_configuration_test.go, where a
// suite-file check could not see it.
func TestPkg(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pkg Suite")
}
