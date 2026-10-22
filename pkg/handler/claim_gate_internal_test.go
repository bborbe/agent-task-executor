// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	stderrors "errors"
	"testing"
	stdtime "time"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/prometheus/client_golang/prometheus/testutil"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// claimPutCall records one Put argument set.
type claimPutCall struct {
	key         string
	expectedSha string
	cluster     string
	taskID      string
	leaseUntil  libtime.DateTime
}

// recordingClaimRefClient is the hand-written ClaimRefClient fake for the
// internal test package. The counterfeiter fake lives in mocks, which cannot be
// imported from package handler without an import cycle.
type recordingClaimRefClient struct {
	put       func(ctx context.Context, key, expectedSha, cluster, taskID string, leaseUntil libtime.DateTime) (string, error)
	list      func(ctx context.Context, prefix string) ([]claim.ClaimRef, error)
	delete    func(ctx context.Context, key, expectedSha string) error
	putCalls  []claimPutCall
	listCalls []string
}

// Put records the call and delegates to the optional func.
func (c *recordingClaimRefClient) Put(
	ctx context.Context,
	key string,
	expectedSha string,
	cluster string,
	taskID string,
	leaseUntil libtime.DateTime,
) (string, error) {
	c.putCalls = append(c.putCalls, claimPutCall{
		key:         key,
		expectedSha: expectedSha,
		cluster:     cluster,
		taskID:      taskID,
		leaseUntil:  leaseUntil,
	})
	if c.put != nil {
		return c.put(ctx, key, expectedSha, cluster, taskID, leaseUntil)
	}
	return "sha-1", nil
}

// List records the call and delegates to the optional func.
func (c *recordingClaimRefClient) List(
	ctx context.Context,
	prefix string,
) ([]claim.ClaimRef, error) {
	c.listCalls = append(c.listCalls, prefix)
	if c.list != nil {
		return c.list(ctx, prefix)
	}
	return nil, nil
}

// Delete records the call and delegates to the optional func.
func (c *recordingClaimRefClient) Delete(ctx context.Context, key, expectedSha string) error {
	if c.delete != nil {
		return c.delete(ctx, key, expectedSha)
	}
	return nil
}

// gateClaimResultLabels is the closed result set of executor_claim_total.
var gateClaimResultLabels = []string{
	"claimed", "conflict", "renewed", "recovered", "renew_failed",
	"claim_lost", "fenced", "taken_over", "takeover_too_early",
	"released", "already_released", "adopted", "error",
}

// snapshotGateClaims reads every series of executor_claim_total.
func snapshotGateClaims() map[string]float64 {
	snapshot := make(map[string]float64, len(gateClaimResultLabels))
	for _, name := range gateClaimResultLabels {
		snapshot[name] = testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues(name))
	}
	return snapshot
}

// expectGateClaimDelta asserts the delta for every result series; unlisted
// series must not change.
func expectGateClaimDelta(
	t *testing.T,
	before, after map[string]float64,
	expected map[string]float64,
) {
	t.Helper()
	for _, name := range gateClaimResultLabels {
		if got := after[name] - before[name]; got != expected[name] {
			t.Errorf(
				"executor_claim_total{result=%q} delta = %v, want %v",
				name,
				got,
				expected[name],
			)
		}
	}
}

// gateTask builds a task for the claim gate. taskType and repo are written only
// when non-empty.
func gateTask(taskType, repo string) lib.Task {
	frontmatter := lib.TaskFrontmatter{}
	if taskType != "" {
		frontmatter["task_type"] = taskType
	}
	if repo != "" {
		frontmatter["repo"] = repo
	}
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier("tid-gate-1"),
		Frontmatter:    frontmatter,
	}
}

// newGateHandler builds a handler wired to client in the given claim mode.
func newGateHandler(mode pkg.ClaimMode, client claim.ClaimRefClient) *taskEventHandler {
	return &taskEventHandler{
		claimMode:             mode,
		cluster:               "self",
		currentDateTime:       libtime.NewCurrentDateTime(),
		claimRefClientFactory: func() claim.ClaimRefClient { return client },
	}
}

func TestAcquireSpawnClaimOffSkipsFactory(t *testing.T) {
	ctx := context.Background()
	h := &taskEventHandler{
		claimMode: pkg.ClaimModeOff,
		claimRefClientFactory: func() claim.ClaimRefClient {
			panic("factory must not be called in claim mode off")
		},
		currentDateTime: libtime.NewCurrentDateTime(),
	}
	if !h.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected the spawn to proceed in claim mode off")
	}
}

func TestAcquireSpawnClaimShadowRecordsCreate(t *testing.T) {
	ctx := context.Background()
	client := &recordingClaimRefClient{}
	h := newGateHandler(pkg.ClaimModeShadow, client)

	if !h.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected the spawn to proceed in claim mode shadow")
	}
	if len(client.putCalls) != 1 {
		t.Fatalf("expected one Put, got %d", len(client.putCalls))
	}
	if client.putCalls[0].expectedSha != "" {
		t.Fatalf("expected an empty expected_sha, got %q", client.putCalls[0].expectedSha)
	}
}

func TestAcquireSpawnClaimOnRecordsCreate(t *testing.T) {
	ctx := context.Background()
	client := &recordingClaimRefClient{}
	h := newGateHandler(pkg.ClaimModeOn, client)

	if !h.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected the spawn to proceed after a successful claim")
	}
	if len(client.putCalls) != 1 {
		t.Fatalf("expected one Put, got %d", len(client.putCalls))
	}
}

func TestAcquireSpawnClaimShadowProceedsOnError(t *testing.T) {
	ctx := context.Background()
	client := &recordingClaimRefClient{
		put: func(context.Context, string, string, string, string, libtime.DateTime) (string, error) {
			return "", stderrors.New("git-rest down")
		},
	}
	h := newGateHandler(pkg.ClaimModeShadow, client)
	before := snapshotGateClaims()

	if !h.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected the spawn to proceed in claim mode shadow")
	}
	after := snapshotGateClaims()
	expectGateClaimDelta(t, before, after, map[string]float64{"error": 1})
}

func TestAcquireSpawnClaimOnFailsClosedOnError(t *testing.T) {
	ctx := context.Background()
	client := &recordingClaimRefClient{
		put: func(context.Context, string, string, string, string, libtime.DateTime) (string, error) {
			return "", stderrors.New("git-rest down")
		},
	}
	h := newGateHandler(pkg.ClaimModeOn, client)
	before := snapshotGateClaims()

	if h.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected the spawn to be blocked in claim mode on")
	}
	after := snapshotGateClaims()
	expectGateClaimDelta(t, before, after, map[string]float64{"error": 1})
}

func TestAcquireSpawnClaimOnFailsClosedOnConflict(t *testing.T) {
	ctx := context.Background()
	client := &recordingClaimRefClient{
		put: func(context.Context, string, string, string, string, libtime.DateTime) (string, error) {
			return "", claim.ErrLeaseConflict
		},
	}
	h := newGateHandler(pkg.ClaimModeOn, client)

	if h.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected the spawn to be blocked on a lease conflict")
	}
}

// gateClaimKey is the claim key for gateTask("", "").
const gateClaimKey = "claims/task/tid-gate-1"

// takeoverClaimRef is the listed claim the fake reports for key.
func takeoverClaimRef(sha string, leaseUntil stdtime.Time) claim.ClaimRef {
	return claim.ClaimRef{
		Ref:        "refs/" + gateClaimKey,
		Sha:        sha,
		Cluster:    "other",
		TaskID:     "previous-task",
		LeaseUntil: leaseUntil.UTC().Format(stdtime.RFC3339),
	}
}

// takeoverClient returns a client whose create conflicts and whose takeover of
// the listed sha succeeds. Any other expected sha conflicts too, so a test can
// assert exactly which write the takeover attempted.
func takeoverClient(ref claim.ClaimRef) *recordingClaimRefClient {
	return &recordingClaimRefClient{
		put: func(
			_ context.Context,
			_ string,
			expectedSha string,
			_ string,
			_ string,
			_ libtime.DateTime,
		) (string, error) {
			if expectedSha == ref.Sha {
				return "new-sha", nil
			}
			return "", claim.ErrLeaseConflict
		},
		list: func(context.Context, string) ([]claim.ClaimRef, error) {
			return []claim.ClaimRef{ref}, nil
		},
	}
}

func TestAcquireSpawnClaimTakeoverExpired(t *testing.T) {
	ctx := context.Background()
	client := takeoverClient(takeoverClaimRef(
		"expired-sha",
		stdtime.Now().UTC().Add(-31*stdtime.Second),
	))
	h := newGateHandler(pkg.ClaimModeOn, client)
	before := snapshotGateClaims()

	if !h.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected the spawn to proceed after an expired-lease takeover")
	}
	expectGateClaimDelta(t, before, snapshotGateClaims(), map[string]float64{
		"conflict":   1,
		"taken_over": 1,
	})
	if len(client.putCalls) != 2 {
		t.Fatalf("expected two Put calls, got %d", len(client.putCalls))
	}
	if client.putCalls[1].expectedSha != "expired-sha" {
		t.Errorf(
			"takeover expected_sha = %q, want %q",
			client.putCalls[1].expectedSha,
			"expired-sha",
		)
	}
	if client.putCalls[1].cluster != "self" {
		t.Errorf("takeover cluster = %q, want %q", client.putCalls[1].cluster, "self")
	}
}

func TestAcquireSpawnClaimTakeoverStillValid(t *testing.T) {
	ctx := context.Background()
	client := takeoverClient(takeoverClaimRef(
		"live-sha",
		stdtime.Now().UTC().Add(5*stdtime.Minute),
	))
	h := newGateHandler(pkg.ClaimModeOn, client)
	before := snapshotGateClaims()

	if h.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected a live lease to block the spawn in claim mode on")
	}
	expectGateClaimDelta(t, before, snapshotGateClaims(), map[string]float64{
		"conflict":           1,
		"takeover_too_early": 1,
	})
	if len(client.putCalls) != 1 {
		t.Fatalf("expected the takeover to write nothing, got %d Put calls", len(client.putCalls))
	}
}

func TestAcquireSpawnClaimTakeoverInsideGrace(t *testing.T) {
	ctx := context.Background()
	client := takeoverClient(takeoverClaimRef(
		"grace-sha",
		stdtime.Now().UTC().Add(-5*stdtime.Second),
	))
	h := newGateHandler(pkg.ClaimModeOn, client)
	before := snapshotGateClaims()

	if h.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected a lease inside the takeover grace to block the spawn")
	}
	expectGateClaimDelta(t, before, snapshotGateClaims(), map[string]float64{
		"conflict":           1,
		"takeover_too_early": 1,
	})
	if len(client.putCalls) != 1 {
		t.Fatalf("expected the takeover to write nothing, got %d Put calls", len(client.putCalls))
	}
}

func TestAcquireSpawnClaimTakeoverRaceLost(t *testing.T) {
	ctx := context.Background()
	client := &recordingClaimRefClient{
		put: func(context.Context, string, string, string, string, libtime.DateTime) (string, error) {
			return "", claim.ErrLeaseConflict
		},
		list: func(context.Context, string) ([]claim.ClaimRef, error) {
			return []claim.ClaimRef{takeoverClaimRef(
				"expired-sha",
				stdtime.Now().UTC().Add(-31*stdtime.Second),
			)}, nil
		},
	}
	h := newGateHandler(pkg.ClaimModeOn, client)
	before := snapshotGateClaims()

	if h.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected a lost takeover race to block the spawn")
	}
	expectGateClaimDelta(t, before, snapshotGateClaims(), map[string]float64{
		"conflict": 2,
	})
}

func TestAcquireSpawnClaimTakeoverError(t *testing.T) {
	ctx := context.Background()
	newClient := func() *recordingClaimRefClient {
		return &recordingClaimRefClient{
			put: func(
				_ context.Context,
				_ string,
				expectedSha string,
				_ string,
				_ string,
				_ libtime.DateTime,
			) (string, error) {
				if expectedSha == "" {
					return "", claim.ErrLeaseConflict
				}
				return "", stderrors.New("git-rest down")
			},
			list: func(context.Context, string) ([]claim.ClaimRef, error) {
				return []claim.ClaimRef{takeoverClaimRef(
					"expired-sha",
					stdtime.Now().UTC().Add(-31*stdtime.Second),
				)}, nil
			},
		}
	}
	expected := map[string]float64{"conflict": 1, "error": 1}

	onClient := newClient()
	onHandler := newGateHandler(pkg.ClaimModeOn, onClient)
	before := snapshotGateClaims()
	if onHandler.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected a takeover error to block the spawn in claim mode on")
	}
	expectGateClaimDelta(t, before, snapshotGateClaims(), expected)

	shadowClient := newClient()
	shadowHandler := newGateHandler(pkg.ClaimModeShadow, shadowClient)
	before = snapshotGateClaims()
	if !shadowHandler.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected a takeover error to proceed in claim mode shadow")
	}
	expectGateClaimDelta(t, before, snapshotGateClaims(), expected)
}

func TestAcquireSpawnClaimTakeoverExpiredInShadow(t *testing.T) {
	ctx := context.Background()
	client := takeoverClient(takeoverClaimRef(
		"expired-sha",
		stdtime.Now().UTC().Add(-31*stdtime.Second),
	))
	h := newGateHandler(pkg.ClaimModeShadow, client)
	before := snapshotGateClaims()

	if !h.acquireSpawnClaim(ctx, gateTask("", "")) {
		t.Fatal("expected the spawn to proceed after an expired-lease takeover")
	}
	expectGateClaimDelta(t, before, snapshotGateClaims(), map[string]float64{
		"conflict":   1,
		"taken_over": 1,
	})
}

func TestAcquireSpawnClaimDeriveKeyFailure(t *testing.T) {
	ctx := context.Background()
	// update-go is repo-keyed; a repo value without "/" cannot produce a key.
	task := gateTask("update-go", "noslash")

	client := &recordingClaimRefClient{}
	onHandler := newGateHandler(pkg.ClaimModeOn, client)
	before := snapshotGateClaims()
	if onHandler.acquireSpawnClaim(ctx, task) {
		t.Fatal("expected claim mode on to block a task with an underivable key")
	}
	if len(client.putCalls) != 0 {
		t.Fatalf("expected no Put, got %d", len(client.putCalls))
	}
	expectGateClaimDelta(t, before, snapshotGateClaims(), map[string]float64{})

	shadowClient := &recordingClaimRefClient{}
	shadowHandler := newGateHandler(pkg.ClaimModeShadow, shadowClient)
	if !shadowHandler.acquireSpawnClaim(ctx, task) {
		t.Fatal("expected claim mode shadow to proceed on a key derivation failure")
	}
	if len(shadowClient.putCalls) != 0 {
		t.Fatalf("expected no Put, got %d", len(shadowClient.putCalls))
	}
}
