// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// NOTE: Explicit "Compiles" spec removed because spawning a child
// process from this race-instrumented test binary segfaults on the
// GH Actions runner (works locally; only reproduces on Linux CI under
// -race). The test binary itself IS package main built — if main.go
// does not compile, `go test` fails immediately, so the assertion is
// redundant. See vault note [[Github Workflow Actions]] gotchas.

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6@v6.12.2 -generate
func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	suiteConfig, reporterConfig := GinkgoConfiguration()
	suiteConfig.Timeout = 60 * time.Second
	RunSpecs(t, "Test Suite", suiteConfig, reporterConfig)
}
