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
		want              int
	}{
		{name: "both zero is uncapped", maxConcurrentJobs: 0, podQuota: 0, want: 0},
		{name: "quota only", maxConcurrentJobs: 0, podQuota: 3, want: 3},
		{name: "declared only", maxConcurrentJobs: 5, podQuota: 0, want: 5},
		{name: "declared smaller", maxConcurrentJobs: 5, podQuota: 8, want: 5},
		{name: "quota smaller", maxConcurrentJobs: 5, podQuota: 3, want: 3},
		{name: "negative declared ignored", maxConcurrentJobs: -1, podQuota: 3, want: 3},
		{name: "negative quota ignored", maxConcurrentJobs: 5, podQuota: -1, want: 5},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := effectiveConcurrencyCap(test.maxConcurrentJobs, test.podQuota)
			if got != test.want {
				t.Fatalf(
					"effectiveConcurrencyCap(%d, %d) = %d, want %d",
					test.maxConcurrentJobs, test.podQuota, got, test.want,
				)
			}
		})
	}
}
