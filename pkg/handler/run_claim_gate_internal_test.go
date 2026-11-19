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
	libtime "github.com/bborbe/time"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// TestLiveRunClaimReturnsSuppressedOutcome drives the unexported funnel
// directly: a live run_claim must end in spawnOutcomeSuppressed without a
// SpawnJob call. The outcome is what every caller discards, so this is the
// only place the suppressed branch itself is observable.
func TestLiveRunClaimReturnsSuppressedOutcome(t *testing.T) {
	ctx := context.Background()
	spawner := &stubJobSpawner{}
	built := NewTaskEventHandler(
		spawner,
		"dev",
		"prod",
		"self",
		true,
		true,
		staticResolver{config: pkg.AgentConfiguration{Assignee: "claude", Image: "img"}},
		stubResultPublisher{},
		pkg.NewTaskStore(),
		libtime.NewCurrentDateTime(),
		nil,
		"v",
		DefaultReconcilePassTimeout,
		DefaultReconcileInterval,
		pkg.ClaimModeOff,
		func() claim.ClaimRefClient { return noopClaimRefClient{} },
	)
	h, ok := built.(*taskEventHandler)
	if !ok {
		t.Fatalf("expected *taskEventHandler, got %T", built)
	}

	task := lib.Task{
		TaskIdentifier: lib.TaskIdentifier("tid-run-claim-internal"),
		Vault:          "v",
		Frontmatter: lib.TaskFrontmatter{
			"status":    "in_progress",
			"phase":     "execution",
			"assignee":  "claude",
			"run_claim": "2999-01-01T00:00:00Z",
		},
	}

	outcome, err := h.spawnIfNeededClaimed(ctx, task, &pkg.AgentConfiguration{
		Assignee: "claude",
		Image:    "img",
	}, nil)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if outcome != spawnOutcomeSuppressed {
		t.Fatalf("outcome = %v, want spawnOutcomeSuppressed", outcome)
	}
	if spawner.spawned != 0 {
		t.Fatalf("spawned = %d, want 0", spawner.spawned)
	}
}
