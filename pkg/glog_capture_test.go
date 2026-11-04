// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"flag"
	"io"
	"os"

	"github.com/golang/glog"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// captureGlogStderr runs fn with glog writing at V(2) to stderr and returns
// everything written. crashFailureUpdates logs at V(2), so the verbosity flag
// must be raised here too: without it the line is filtered out and an assertion
// would pass vacuously against an empty string. The suite runs serially, so the
// global os.Stderr swap cannot race another spec's logging.
//
// It lives here rather than in result_publisher_test.go because that file is at
// the revive file-length limit and two files now capture glog output.
func captureGlogStderr(fn func()) string {
	GinkgoHelper()
	Expect(flag.Set("logtostderr", "true")).To(Succeed())
	Expect(flag.Set("v", "2")).To(Succeed())
	defer func() {
		// Deliberately not Expect: this runs during the panic unwinding that
		// follows a failed spec assertion, and a second failure here would
		// replace the real one with "Test Panicked".
		_ = flag.Set("logtostderr", "false")
		_ = flag.Set("v", "0")
	}()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	Expect(err).ToNot(HaveOccurred())
	os.Stderr = writer
	// Deferred, not sequential: fn() calls PublishFailure with Expect inside it,
	// and a failed Expect panics. A sequential restore would then be skipped and
	// the pipe would stay installed as the process stderr for the rest of the
	// suite, so the failure diagnostics are exactly what disappears.
	defer func() { os.Stderr = original }()

	fn()
	glog.Flush()

	Expect(writer.Close()).To(Succeed())
	out, err := io.ReadAll(reader)
	Expect(err).ToNot(HaveOccurred())
	Expect(reader.Close()).To(Succeed())
	return string(out)
}
