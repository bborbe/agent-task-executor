// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package factory

import (
	"testing"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// TestCreateClaimLifecycleRunFuncs asserts the lease loop is present exactly in
// the modes that take a claim. Plain testing, not Ginkgo: this repo's Ginkgo
// specs declared in an internal test package never run.
func TestCreateClaimLifecycleRunFuncs(t *testing.T) {
	tests := []struct {
		name string
		mode pkg.ClaimMode
		want int
	}{
		{name: "off adds no lease loop", mode: pkg.ClaimModeOff, want: 0},
		{name: "shadow adds the lease loop", mode: pkg.ClaimModeShadow, want: 1},
		{name: "on adds the lease loop", mode: pkg.ClaimModeOn, want: 1},
		{name: "an unparsed mode adds no lease loop", mode: pkg.ClaimMode(""), want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := CreateClaimLifecycleRunFuncs(test.mode, nil)
			if len(got) != test.want {
				t.Fatalf("run funcs = %d, want %d", len(got), test.want)
			}
		})
	}
}
