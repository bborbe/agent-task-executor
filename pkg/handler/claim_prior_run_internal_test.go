// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"testing"

	lib "github.com/bborbe/agent"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// TestAttributablePriorRun pins each limb of the lost-claim park guard.
//
// The marker on the displaced claim is the fix: it travels on the claim, which
// both clusters share, so a peer no longer depends on its own vault copy having
// absorbed the previous run. taskShowsPriorRun survives only as the fallback for
// claims written before the marker existed.
func TestAttributablePriorRun(t *testing.T) {
	tests := []struct {
		name     string
		takeover *claim.TakeoverResult
		task     lib.Task
		want     bool
	}{
		{
			name: "a displaced claim carrying spawned_at parks even when the peer shows no prior run",
			takeover: &claim.TakeoverResult{
				PreviousTaskID:    "tid-gate-1",
				PreviousSpawnedAt: "2026-11-18T22:10:00Z",
			},
			task: gateTask("", ""),
			want: true,
		},
		{
			name: "a pre-marker claim still parks on the task's own prior-run evidence",
			takeover: &claim.TakeoverResult{
				PreviousTaskID: "tid-gate-1",
			},
			task: ranGateTask(),
			want: true,
		},
		{
			name: "a displaced claim with no marker and no prior run does not park",
			takeover: &claim.TakeoverResult{
				PreviousTaskID: "tid-gate-1",
			},
			task: gateTask("", ""),
			want: false,
		},
		{
			name: "a takeover that displaced nobody does not park, marker or not",
			takeover: &claim.TakeoverResult{
				PreviousSpawnedAt: "2026-11-18T22:10:00Z",
			},
			task: ranGateTask(),
			want: false,
		},
		{
			name:     "a nil takeover does not park",
			takeover: nil,
			task:     ranGateTask(),
			want:     false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := attributablePriorRun(test.takeover, test.task); got != test.want {
				t.Fatalf("attributablePriorRun = %v, want %v", got, test.want)
			}
		})
	}
}
