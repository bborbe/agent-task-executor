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

var _ = Describe("DeriveClaimKey", func() {
	DescribeTable(
		"maps a task to its claim key",
		func(taskType, taskID, owner, repo, wantKey string, wantErr bool) {
			key, err := claim.DeriveClaimKey(
				context.Background(),
				taskType,
				taskID,
				owner,
				repo,
			)
			if wantErr {
				Expect(err).To(HaveOccurred())
				Expect(key).To(BeEmpty())
				return
			}
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal(wantKey))
		},
		// Repo-keyed types share one key per repository.
		Entry(
			"update-go",
			"update-go",
			"abc123",
			"bborbe",
			"agent",
			"claims/repo/bborbe-agent",
			false,
		),
		Entry("release", "release", "abc123", "bborbe", "agent", "claims/repo/bborbe-agent", false),
		Entry(
			"pr-review",
			"pr-review",
			"abc123",
			"bborbe",
			"agent",
			"claims/repo/bborbe-agent",
			false,
		),
		Entry(
			"build-fix",
			"build-fix",
			"abc123",
			"bborbe",
			"agent",
			"claims/repo/bborbe-agent",
			false,
		),
		Entry(
			"lower-cases owner and repo",
			"update-go",
			"abc123",
			"Bborbe",
			"Agent",
			"claims/repo/bborbe-agent",
			false,
		),
		// Every other type is keyed by task id.
		Entry("claude", "claude", "abc123", "bborbe", "agent", "claims/task/abc123", false),
		Entry("backtest", "backtest", "abc123", "bborbe", "agent", "claims/task/abc123", false),
		Entry("empty type", "", "abc123", "bborbe", "agent", "claims/task/abc123", false),
		Entry(
			"task id keeps case and punctuation",
			"claude",
			"ABC-1.2_3",
			"",
			"",
			"claims/task/ABC-1.2_3",
			false,
		),
		// Unsafe owner or repo on a repo-keyed type.
		Entry("empty owner", "update-go", "abc123", "", "agent", "", true),
		Entry("empty repo", "update-go", "abc123", "bborbe", "", "", true),
		Entry("owner with slash", "release", "abc123", "bb/orbe", "agent", "", true),
		Entry("repo with dotdot", "pr-review", "abc123", "bborbe", "ag..ent", "", true),
		Entry("owner leading dot", "build-fix", "abc123", ".bborbe", "agent", "", true),
		Entry("repo trailing lock", "update-go", "abc123", "bborbe", "agent.lock", "", true),
		Entry("owner invalid character", "update-go", "abc123", "bb orbe", "agent", "", true),
		// Unsafe task ids on a non-repo type.
		Entry("empty task id", "claude", "", "", "", "", true),
		Entry("task id with slash", "claude", "a/b", "", "", "", true),
		Entry("task id with dotdot", "claude", "a..b", "", "", "", true),
		Entry("task id leading dot", "claude", ".abc", "", "", "", true),
		Entry("task id trailing lock", "claude", "abc.lock", "", "", "", true),
		Entry("task id invalid character", "claude", "abc def", "", "", "", true),
	)
})
