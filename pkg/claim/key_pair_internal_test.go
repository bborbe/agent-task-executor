// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import "testing"

func TestRepoKeyFromPRKey(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		want   string
		wantOK bool
	}{
		{"pr key", "claims/pr/bborbe-nuke-658", "claims/repo/bborbe-nuke", true},
		{"single digit", "claims/pr/bborbe-nuke-1", "claims/repo/bborbe-nuke", true},
		{"owner and repo keep dashes", "claims/pr/foo-2-bar-3", "claims/repo/foo-2-bar", true},
		{"repo ends in digits", "claims/pr/bborbe-nuke2-658", "claims/repo/bborbe-nuke2", true},
		{"not a pr key", "claims/repo/bborbe-nuke", "", false},
		{"task key", "claims/task/t1", "", false},
		{"no number", "claims/pr/bborbe-nuke", "", false},
		{"non-numeric number", "claims/pr/bborbe-nuke-abc", "", false},
		{"zero", "claims/pr/bborbe-nuke-0", "", false},
		{"trailing dash", "claims/pr/bborbe-nuke-", "", false},
		{"empty owner and repo", "claims/pr/-658", "", false},
		{"number too long", "claims/pr/bborbe-nuke-12345678901", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := repoKeyFromPRKey(tt.key)
			if ok != tt.wantOK {
				t.Fatalf("repoKeyFromPRKey(%q) ok = %v, want %v", tt.key, ok, tt.wantOK)
			}
			if got != tt.want {
				t.Fatalf("repoKeyFromPRKey(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}
