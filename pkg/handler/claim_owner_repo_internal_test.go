// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"testing"

	lib "github.com/bborbe/agent"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// TestClaimOwnerRepoDerivesFromCloneURL pins the fallback that unblocks
// pr-review claims: those tasks carry clone_url and no repo key.
func TestClaimOwnerRepoDerivesFromCloneURL(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		frontmatter lib.TaskFrontmatter
		wantKey     string
		wantErr     bool
	}{
		{
			name: "clone_url https with .git",
			frontmatter: lib.TaskFrontmatter{
				"clone_url": "https://github.com/bborbe/go-skeleton.git",
			},
			wantKey: "claims/repo/bborbe-go-skeleton",
		},
		{
			name:        "clone_url https without .git",
			frontmatter: lib.TaskFrontmatter{"clone_url": "https://github.com/bborbe/agent"},
			wantKey:     "claims/repo/bborbe-agent",
		},
		{
			name:        "clone_url scp-style ssh",
			frontmatter: lib.TaskFrontmatter{"clone_url": "git@github.com:bborbe/agent.git"},
			wantKey:     "claims/repo/bborbe-agent",
		},
		{
			name:        "clone_url ssh scheme",
			frontmatter: lib.TaskFrontmatter{"clone_url": "ssh://git@github.com/bborbe/agent.git"},
			wantKey:     "claims/repo/bborbe-agent",
		},
		{
			name: "repo wins over clone_url",
			frontmatter: lib.TaskFrontmatter{
				"repo":      "bborbe/agent",
				"clone_url": "https://github.com/other/thing.git",
			},
			wantKey: "claims/repo/bborbe-agent",
		},
		{
			name:        "neither key fails loudly",
			frontmatter: lib.TaskFrontmatter{},
			wantErr:     true,
		},
		{
			name:        "unparseable clone_url fails loudly",
			frontmatter: lib.TaskFrontmatter{"clone_url": "not-a-url"},
			wantErr:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-1"),
				Frontmatter:    test.frontmatter,
			}
			owner, repo := claimOwnerRepo(task)
			key, err := claim.DeriveClaimKey(
				ctx,
				"pr-review",
				string(task.TaskIdentifier),
				owner,
				repo,
			)
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got key %q", key)
				}
				return
			}
			if err != nil {
				t.Fatalf("derive key: %v", err)
			}
			if key != test.wantKey {
				t.Fatalf("key = %q, want %q", key, test.wantKey)
			}
		})
	}
}
