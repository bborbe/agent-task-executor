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
	"github.com/prometheus/client_golang/prometheus/testutil"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/claim/claimtest"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// spawnClaimPair is one (result, stage) child of executor_spawn_claim_total.
type spawnClaimPair [2]string

// snapshotSpawnClaims reads every child of executor_spawn_claim_total.
func snapshotSpawnClaims() map[spawnClaimPair]float64 {
	snapshot := make(map[spawnClaimPair]float64, len(metrics.SpawnClaimPairs))
	for _, pair := range metrics.SpawnClaimPairs {
		snapshot[spawnClaimPair{pair[0], pair[1]}] = testutil.ToFloat64(
			metrics.SpawnClaimTotal.WithLabelValues(pair[0], pair[1]),
		)
	}
	return snapshot
}

// expectSpawnClaimDelta asserts that exactly one child moved, by exactly one,
// and that it is the expected one. Every other child must be unchanged: a
// branch that increments two children is as wrong as one that increments none,
// and asserting only the expected child would let a double-count through.
func expectSpawnClaimDelta(
	t *testing.T,
	before, after map[spawnClaimPair]float64,
	wantResult string,
	wantStage string,
) {
	t.Helper()
	want := spawnClaimPair{wantResult, wantStage}
	for _, pair := range metrics.SpawnClaimPairs {
		key := spawnClaimPair{pair[0], pair[1]}
		got := after[key] - before[key]
		expected := 0.0
		if key == want {
			expected = 1
		}
		if got != expected {
			t.Errorf(
				"executor_spawn_claim_total{result=%q,stage=%q} delta = %v, want %v",
				pair[0], pair[1], got, expected,
			)
		}
	}
}

// conflictOnlyClient always fails a create and lists no claim, so the takeover
// attempt has nothing to take over.
func conflictOnlyClient() *recordingClaimRefClient {
	return &recordingClaimRefClient{
		put: func(
			context.Context, string, string, string, string, libtime.DateTime,
		) (string, error) {
			return "", claim.ErrLeaseConflict
		},
	}
}

// usableClaimHandler builds a handler that constructs a lease manager
// successfully, so the branch under test is the one after construction.
func usableClaimHandler(mode pkg.ClaimMode, client claim.ClaimRefClient) *taskEventHandler {
	return &taskEventHandler{
		claimMode:             mode,
		cluster:               "self",
		currentDateTime:       libtime.NewCurrentDateTime(),
		claimRefClientFactory: func() claim.ClaimRefClient { return client },
		claimClientFactory: func() claim.ClaimClient {
			return claimtest.NewClient(client, libtime.NewCurrentDateTime().Now)
		},
		claimJobControl: noopJobControl{},
	}
}

// spawnClaimCase is one spawn-claim branch under test: the handler and task
// that drive it, the child it must move, and whether the spawn proceeds.
type spawnClaimCase struct {
	name        string
	handler     *taskEventHandler
	task        lib.Task
	wantResult  string
	wantStage   string
	wantProceed bool
}

// spawnClaimCases builds the branch table. It is a separate function only to
// keep the test body inside the repository's funlen budget.
func spawnClaimCases() []spawnClaimCase {
	return []spawnClaimCase{
		{
			name: "mode off proceeds without attempting a claim",
			handler: &taskEventHandler{
				claimMode:       pkg.ClaimModeOff,
				currentDateTime: libtime.NewCurrentDateTime(),
				claimJobControl: noopJobControl{},
			},
			task:        gateTask("", ""),
			wantResult:  metrics.SpawnClaimResultModeOff,
			wantStage:   metrics.SpawnClaimStageNone,
			wantProceed: true,
		},
		{
			name:        "a fresh claim is acquired",
			handler:     usableClaimHandler(pkg.ClaimModeOn, &recordingClaimRefClient{}),
			task:        gateTask("", ""),
			wantResult:  metrics.SpawnClaimResultAcquired,
			wantStage:   metrics.SpawnClaimStageNone,
			wantProceed: true,
		},
		{
			name: "an unusable lease manager blocks the spawn in claim mode on",
			handler: &taskEventHandler{
				claimMode:             pkg.ClaimModeOn,
				cluster:               "self",
				currentDateTime:       libtime.NewCurrentDateTime(),
				claimRefClientFactory: func() claim.ClaimRefClient { return &recordingClaimRefClient{} },
				claimClientFactory: func() claim.ClaimClient {
					return claimtest.NewClient(
						&recordingClaimRefClient{},
						libtime.NewCurrentDateTime().Now,
					)
				},
				// claimJobControl nil: leaseManager refuses to construct.
			},
			task:        gateTask("", ""),
			wantResult:  metrics.SpawnClaimResultUnclaimed,
			wantStage:   metrics.SpawnClaimStageLeaseManager,
			wantProceed: false,
		},
		{
			name:        "an underivable claim key blocks the spawn in claim mode on",
			handler:     usableClaimHandler(pkg.ClaimModeOn, &recordingClaimRefClient{}),
			task:        lib.Task{TaskIdentifier: "", Frontmatter: lib.TaskFrontmatter{}},
			wantResult:  metrics.SpawnClaimResultUnclaimed,
			wantStage:   metrics.SpawnClaimStageDeriveKey,
			wantProceed: false,
		},
		{
			name:        "an underivable claim key proceeds in claim mode shadow",
			handler:     usableClaimHandler(pkg.ClaimModeShadow, &recordingClaimRefClient{}),
			task:        lib.Task{TaskIdentifier: "", Frontmatter: lib.TaskFrontmatter{}},
			wantResult:  metrics.SpawnClaimResultFailOpen,
			wantStage:   metrics.SpawnClaimStageDeriveKey,
			wantProceed: true,
		},
		{
			name:        "a conflict with no takeover blocks the spawn in claim mode on",
			handler:     usableClaimHandler(pkg.ClaimModeOn, conflictOnlyClient()),
			task:        gateTask("", ""),
			wantResult:  metrics.SpawnClaimResultUnclaimed,
			wantStage:   metrics.SpawnClaimStageTakeover,
			wantProceed: false,
		},
		{
			name:        "a conflict with no takeover proceeds in claim mode shadow",
			handler:     usableClaimHandler(pkg.ClaimModeShadow, conflictOnlyClient()),
			task:        gateTask("", ""),
			wantResult:  metrics.SpawnClaimResultFailOpen,
			wantStage:   metrics.SpawnClaimStageTakeover,
			wantProceed: true,
		},
	}
}

// TestAcquireSpawnClaimCountsExactlyOneOutcome drives each spawn-claim branch
// through acquireSpawnClaim and asserts that exactly one outcome child moves.
// It is the wiring half of the metric's contract: a build that pre-initialises
// the children but wires only one branch passes every presence check and fails
// here, which is the whole reason this test exists.
func TestAcquireSpawnClaimCountsExactlyOneOutcome(t *testing.T) {
	ctx := context.Background()
	for _, tc := range spawnClaimCases() {
		t.Run(tc.name, func(t *testing.T) {
			before := snapshotSpawnClaims()
			proceed := tc.handler.acquireSpawnClaim(ctx, tc.task, redispatchConfig())
			expectSpawnClaimDelta(t, before, snapshotSpawnClaims(), tc.wantResult, tc.wantStage)
			if proceed != tc.wantProceed {
				t.Errorf("acquireSpawnClaim proceeded = %v, want %v", proceed, tc.wantProceed)
			}
		})
	}
}

// TestSpawnClaimPairsAreClosed pins the seeding list against the declared
// vocabulary: every result and every stage must appear in at least one pair,
// and no pair may repeat. A vocabulary constant added without a pair would
// otherwise be silently unseedable, which is exactly the absent-series
// ambiguity the pre-initialisation exists to remove.
func TestSpawnClaimPairsAreClosed(t *testing.T) {
	results := []string{
		metrics.SpawnClaimResultAcquired,
		metrics.SpawnClaimResultUnclaimed,
		metrics.SpawnClaimResultFailOpen,
		metrics.SpawnClaimResultModeOff,
		metrics.SpawnClaimResultLapsed,
	}
	stages := []string{
		metrics.SpawnClaimStageNone,
		metrics.SpawnClaimStageLeaseManager,
		metrics.SpawnClaimStageClaimScheme,
		metrics.SpawnClaimStageDeriveKey,
		metrics.SpawnClaimStageTakeover,
		metrics.SpawnClaimStageClaim,
		metrics.SpawnClaimStageRenew,
		metrics.SpawnClaimStageLost,
	}

	seen := make(map[spawnClaimPair]bool, len(metrics.SpawnClaimPairs))
	for _, pair := range metrics.SpawnClaimPairs {
		key := spawnClaimPair{pair[0], pair[1]}
		if seen[key] {
			t.Errorf("duplicate seed pair %v", key)
		}
		seen[key] = true
	}

	for _, result := range results {
		found := false
		for _, pair := range metrics.SpawnClaimPairs {
			if pair[0] == result {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("result %q has no seed pair", result)
		}
	}
	for _, stage := range stages {
		found := false
		for _, pair := range metrics.SpawnClaimPairs {
			if pair[1] == stage {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("stage %q has no seed pair", stage)
		}
	}
}

// TestSpawnClaimLabelsCarryNoTaskIdentity is the cardinality guard. The metric
// must never label a task id or a claim key, so every child's label values must
// come from the closed vocabulary — anything else is a per-task series.
func TestSpawnClaimLabelsCarryNoTaskIdentity(t *testing.T) {
	allowed := map[string]bool{}
	for _, pair := range metrics.SpawnClaimPairs {
		allowed[pair[0]] = true
		allowed[pair[1]] = true
	}
	for _, pair := range metrics.SpawnClaimPairs {
		for _, value := range pair {
			if !allowed[value] {
				t.Errorf("label value %q is outside the closed vocabulary", value)
			}
		}
	}
}
