// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import "testing"

func TestEffectiveConcurrencyCap(t *testing.T) {
	tests := []struct {
		name              string
		maxConcurrentJobs int
		podQuota          int
		quotaBounded      bool
		wantCap           int
		wantBounded       bool
	}{
		{
			name:    "no quota, both zero is uncapped",
			wantCap: 0, wantBounded: false,
		},
		{
			name:              "no quota, declared only",
			maxConcurrentJobs: 5, wantCap: 5, wantBounded: false,
		},
		{
			name:              "no quota, negative declared is uncapped",
			maxConcurrentJobs: -1, wantCap: 0, wantBounded: false,
		},
		{
			name:     "quota only",
			podQuota: 3, quotaBounded: true, wantCap: 3, wantBounded: true,
		},
		{
			name:              "declared smaller",
			maxConcurrentJobs: 5, podQuota: 8, quotaBounded: true,
			wantCap: 5, wantBounded: true,
		},
		{
			name:              "quota smaller",
			maxConcurrentJobs: 5, podQuota: 3, quotaBounded: true,
			wantCap: 3, wantBounded: true,
		},
		{
			name:              "negative declared ignored when quota bounds",
			maxConcurrentJobs: -1, podQuota: 3, quotaBounded: true,
			wantCap: 3, wantBounded: true,
		},
		{
			name:              "negative quota is a hard zero",
			maxConcurrentJobs: 5, podQuota: -1, quotaBounded: true,
			wantCap: 0, wantBounded: true,
		},
		{
			// The case the reader exists for: a quota that admits no pod must not
			// be mistaken for an absent quota, or admission falls back to the
			// declared cap and claims work the quota can never start.
			name:              "zero quota is a hard zero, not uncapped",
			maxConcurrentJobs: 5, podQuota: 0, quotaBounded: true,
			wantCap: 0, wantBounded: true,
		},
		{
			name:              "zero declared and zero quota is a hard zero",
			maxConcurrentJobs: 0, podQuota: 0, quotaBounded: true,
			wantCap: 0, wantBounded: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotCap, gotBounded := effectiveConcurrencyCap(
				test.maxConcurrentJobs,
				test.podQuota,
				test.quotaBounded,
			)
			if gotCap != test.wantCap || gotBounded != test.wantBounded {
				t.Fatalf(
					"effectiveConcurrencyCap(%d, %d, %t) = (%d, %t), want (%d, %t)",
					test.maxConcurrentJobs,
					test.podQuota,
					test.quotaBounded,
					gotCap,
					gotBounded,
					test.wantCap,
					test.wantBounded,
				)
			}
		})
	}
}
