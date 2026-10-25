// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

var _ = Describe("DeriveClaimKeysWithBody", func() {
	DescribeTable(
		"maps a task and its body to its claim keys",
		func(
			taskType, taskID, owner, repo, body string,
			wantPrimary, wantSecondary string,
			wantFallback bool,
			wantErr bool,
		) {
			primary, secondary, fallback, err := claim.DeriveClaimKeysWithBody(
				context.Background(),
				taskType,
				taskID,
				owner,
				repo,
				body,
			)
			if wantErr {
				Expect(err).To(HaveOccurred())
				Expect(primary).To(BeEmpty())
				Expect(secondary).To(BeEmpty())
				Expect(fallback).To(Equal(wantFallback))
				return
			}
			Expect(err).NotTo(HaveOccurred())
			Expect(primary).To(Equal(wantPrimary))
			Expect(secondary).To(Equal(wantSecondary))
			Expect(fallback).To(Equal(wantFallback))
		},
		// A standalone pull-request URL keys the task per pull request, and the
		// per-repository key is not used at all — no secondary is produced.
		Entry(
			"standalone pull-request URL",
			"pr-review",
			"abc123",
			"bborbe",
			"nuke",
			"https://github.com/bborbe/nuke/pull/658",
			"claims/pr/bborbe-nuke-658",
			"",
			false,
			false,
		),
		Entry(
			"first standalone URL wins over a copied description",
			"pr-review",
			"abc123",
			"bborbe",
			"nuke",
			"title\n\n\nhttps://github.com/bborbe/nuke/pull/658\n"+
				"copied description\nhttps://github.com/bborbe/nuke/pull/700\n",
			"claims/pr/bborbe-nuke-658",
			"",
			false,
			false,
		),
		Entry(
			"another repository's URL does not win",
			"pr-review",
			"abc123",
			"bborbe",
			"nuke",
			"https://github.com/other/repo/pull/1\n"+
				"https://github.com/bborbe/nuke/pull/658\n",
			"claims/pr/bborbe-nuke-658",
			"",
			false,
			false,
		),
		Entry(
			"owner and repo are lower-cased",
			"pr-review",
			"abc123",
			"BBorbe",
			"Nuke",
			"https://github.com/BBorbe/Nuke/pull/658",
			"claims/pr/bborbe-nuke-658",
			"",
			false,
			false,
		),
		Entry(
			"surrounding whitespace is trimmed",
			"pr-review",
			"abc123",
			"bborbe",
			"nuke",
			"   https://github.com/bborbe/nuke/pull/658  ",
			"claims/pr/bborbe-nuke-658",
			"",
			false,
			false,
		),
		Entry(
			"leading zeroes are canonicalised",
			"pr-review",
			"abc123",
			"bborbe",
			"nuke",
			"https://github.com/bborbe/nuke/pull/0658",
			"claims/pr/bborbe-nuke-658",
			"",
			false,
			false,
		),
		// Anything else falls back to the repository key, with no secondary.
		Entry(
			"URL in the middle of a line",
			"pr-review",
			"abc123",
			"bborbe",
			"nuke",
			"see https://github.com/bborbe/nuke/pull/700 for context",
			"claims/repo/bborbe-nuke",
			"",
			true,
			false,
		),
		Entry(
			"pull number zero",
			"pr-review",
			"abc123",
			"bborbe",
			"nuke",
			"https://github.com/bborbe/nuke/pull/0",
			"claims/repo/bborbe-nuke",
			"",
			true,
			false,
		),
		Entry(
			"eleven-digit pull number",
			"pr-review",
			"abc123",
			"bborbe",
			"nuke",
			"https://github.com/bborbe/nuke/pull/12345678901",
			"claims/repo/bborbe-nuke",
			"",
			true,
			false,
		),
		Entry(
			"files suffix",
			"pr-review",
			"abc123",
			"bborbe",
			"nuke",
			"https://github.com/bborbe/nuke/pull/658/files",
			"claims/repo/bborbe-nuke",
			"",
			true,
			false,
		),
		Entry(
			"trailing slash",
			"pr-review",
			"abc123",
			"bborbe",
			"nuke",
			"https://github.com/bborbe/nuke/pull/658/",
			"claims/repo/bborbe-nuke",
			"",
			true,
			false,
		),
		// The other repo-keyed task types never key per pull request.
		Entry(
			"update-go delegates",
			"update-go",
			"abc123",
			"bborbe",
			"nuke",
			"https://github.com/bborbe/nuke/pull/658",
			"claims/repo/bborbe-nuke",
			"",
			false,
			false,
		),
		Entry(
			"release delegates",
			"release",
			"abc123",
			"bborbe",
			"nuke",
			"https://github.com/bborbe/nuke/pull/658",
			"claims/repo/bborbe-nuke",
			"",
			false,
			false,
		),
		Entry(
			"build-fix delegates",
			"build-fix",
			"abc123",
			"bborbe",
			"nuke",
			"https://github.com/bborbe/nuke/pull/658",
			"claims/repo/bborbe-nuke",
			"",
			false,
			false,
		),
		Entry(
			"claude keeps the task-id key",
			"claude",
			"abc123",
			"bborbe",
			"nuke",
			"https://github.com/bborbe/nuke/pull/658",
			"claims/task/abc123",
			"",
			false,
			false,
		),
		// An unusable repository still fails loudly through the repo key.
		Entry(
			"pr-review with empty owner errors",
			"pr-review",
			"abc123",
			"",
			"nuke",
			"https://github.com/bborbe/nuke/pull/658",
			"",
			"",
			true,
			true,
		),
		// The derived-number validation failure is not a fallback.
		// A derived number whose repository component is unsafe is an error.
		Entry(
			"pr-review with unsafe owner errors",
			"pr-review",
			"abc123",
			"bad..owner",
			"nuke",
			"https://github.com/bad..owner/nuke/pull/1",
			"",
			"",
			false,
			true,
		),
	)

	It("wraps a per-PR validation error with its origin", func() {
		_, _, _, err := claim.DeriveClaimKeysWithBody(
			context.Background(),
			"pr-review",
			"abc123",
			".hidden",
			"agent",
			"https://github.com/.hidden/agent/pull/1",
		)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("derive pr claim key"))
	})
})
