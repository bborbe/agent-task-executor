// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("redactLog", func() {
	DescribeTable("redacts each credential class",
		func(input string, gone []string, kept []string) {
			redacted := redactLog(input)
			for _, literal := range gone {
				Expect(redacted).NotTo(ContainSubstring(literal))
			}
			for _, literal := range kept {
				Expect(redacted).To(ContainSubstring(literal))
			}
			Expect(redacted).To(ContainSubstring(redactionPlaceholder))
		},
		Entry(
			"PEM private-key block",
			"-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA1a2b3c4d5e6f7g8h9i0j\n-----END RSA PRIVATE KEY-----\n",
			[]string{"BEGIN RSA PRIVATE KEY", "MIIEowIBAAKCAQEA1a2b3c4d5e6f7g8h9i0j"},
			nil,
		),
		Entry("GitHub token",
			"pushed with ghp_0123456789abcdefghijABCDEFGHIJ012345",
			[]string{"ghp_0123456789abcdefghijABCDEFGHIJ012345"},
			nil,
		),
		Entry("URL userinfo",
			"clone https://deploy:hunter2hunter2@git.example.com/repo.git",
			[]string{"hunter2hunter2"},
			[]string{"git.example.com/repo.git", "https://"},
		),
		Entry("Bearer token",
			"Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln",
			[]string{"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln"},
			[]string{"Bearer"},
		),
		Entry("secret-named assignment at the floor",
			"ANTHROPIC_AUTH_TOKEN=0123456789abc",
			[]string{"0123456789abc"},
			[]string{"ANTHROPIC_AUTH_TOKEN="},
		),
	)

	It("leaves an ordinary log line unchanged", func() {
		input := "2026-11-03T10:00:00Z INFO starting worker ref=abc123\n"
		Expect(redactLog(input)).To(Equal(input))
	})

	// This is the regression pin for the key-shape requirement: a scrubber that
	// redacts every long value fails here, and that is exactly the shape that
	// would make a captured tail useless.
	It("keeps the executor's logfmt fields readable", func() {
		input := `I1104 02:00:00.123456 1 watcher.go:42] event=crash_arm_decision task=service-analysis-needs-input job=agent-task-executor-7f9c reason="deadline_exceeded"` + "\n"
		redacted := redactLog(input)
		Expect(redacted).To(ContainSubstring("event=crash_arm_decision"))
		Expect(redacted).To(ContainSubstring("task=service-analysis-needs-input"))
		Expect(redacted).To(ContainSubstring("job=agent-task-executor-7f9c"))
		Expect(redacted).NotTo(ContainSubstring(redactionPlaceholder))
	})

	It("keeps a value shorter than the floor readable", func() {
		value := strings.Repeat("a", redactionMinSecretLength-1)
		assignment := "AUTH_TOKEN=" + value
		Expect(redactLog(assignment)).To(ContainSubstring(assignment))
	})

	It("redacts a value at exactly the floor", func() {
		value := strings.Repeat("b", redactionMinSecretLength)
		assignment := "AUTH_TOKEN=" + value
		redacted := redactLog(assignment)
		Expect(redacted).NotTo(ContainSubstring(value))
		Expect(redacted).To(ContainSubstring(redactionPlaceholder))
	})

	It("redacts every class in one multi-line log", func() {
		input := "2026-11-03T10:00:00Z INFO starting worker REF=abc123\n" +
			"pushed with ghp_0123456789abcdefghijABCDEFGHIJ012345\n" +
			"Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln\n" +
			"clone https://deploy:hunter2hunter2@git.example.com/repo.git\n" +
			"ANTHROPIC_AUTH_TOKEN=zz99887766554\n" +
			"-----BEGIN RSA PRIVATE KEY-----\n" +
			"MIIEowIBAAKCAQEA1a2b3c4d5e6f7g8h9i0j\n" +
			"-----END RSA PRIVATE KEY-----\n" +
			"event=crash_arm_decision task=service-analysis-needs-input\n"

		redacted := redactLog(input)

		for _, literal := range []string{
			"ghp_0123456789abcdefghijABCDEFGHIJ012345",
			"BEGIN RSA PRIVATE KEY",
			"MIIEowIBAAKCAQEA1a2b3c4d5e6f7g8h9i0j",
			"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln",
			"hunter2hunter2",
			"zz99887766554",
		} {
			Expect(redacted).NotTo(ContainSubstring(literal))
		}
		Expect(redacted).To(ContainSubstring(redactionPlaceholder))
		Expect(redacted).To(ContainSubstring("REF=abc123"))
		Expect(redacted).To(ContainSubstring("event=crash_arm_decision"))
	})
})
