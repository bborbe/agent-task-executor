// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	stderrors "errors"
	"strconv"
	"sync"
	"testing"
	"time"

	libtime "github.com/bborbe/time"
)

// successHoldRefClient is a compare-and-swap claim store shared by two managers,
// so one test can put two clusters on the same ref — which is the whole point:
// the hold only means something if a peer cannot take the claim.
type successHoldRefClient struct {
	mu   sync.Mutex
	refs map[string]string
	seq  int
}

func newSuccessHoldRefClient() *successHoldRefClient {
	return &successHoldRefClient{refs: map[string]string{}}
}

func (c *successHoldRefClient) List(context.Context, string) ([]ClaimRef, error) {
	return nil, nil
}

func (c *successHoldRefClient) Put(
	_ context.Context,
	key string,
	expectedSha string,
	_ string,
	_ string,
	_ libtime.DateTime,
) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, exists := c.refs[key]
	if expectedSha == "" && exists {
		return "", ErrLeaseConflict
	}
	if expectedSha != "" && (!exists || current != expectedSha) {
		return "", ErrLeaseConflict
	}
	c.seq++
	sha := "sha-" + strconv.Itoa(c.seq)
	c.refs[key] = sha
	return sha, nil
}

func (c *successHoldRefClient) Delete(_ context.Context, key string, expectedSha string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, exists := c.refs[key]
	if !exists || current != expectedSha {
		return ErrLeaseConflict
	}
	delete(c.refs, key)
	return nil
}

// finishedJobControl reports every Job as finished, so a release is never
// deferred by the Job-running guard and the test measures the success hold alone.
type finishedJobControl struct{}

func (finishedJobControl) IsJobRunning(context.Context, string) (bool, error) {
	return false, nil
}

func (finishedJobControl) DeleteJob(context.Context, string) error { return nil }

// stepClock is a settable clock so a test can move one cluster through the
// window while the other stays put.
type stepClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *stepClock) Now() libtime.DateTime {
	c.mu.Lock()
	defer c.mu.Unlock()
	return libtime.DateTime(c.now)
}

func (c *stepClock) set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = at
}

// TestReleaseAfterSuccessHoldsTheClaimAgainstAPeer is the two-cluster regression
// for the 2026-10-23 re-runs: task 1788bf3c succeeded on nuke prod at 05:55:00
// and ran again on sun at 05:55:30, both successful, because the only record of
// the success was per-pod memory and sun's pod had just started cold.
//
// The claim is the one artefact both clusters share, so the hold is what closes
// that gap: while the winner keeps the claim, the peer's Claim gets a conflict
// and cannot spawn; once the window passes, the peer may claim.
func TestReleaseAfterSuccessHoldsTheClaimAgainstAPeer(t *testing.T) {
	client := newSuccessHoldRefClient()
	job := finishedJobControl{}
	start := time.Date(2026, 10, 23, 5, 55, 0, 0, time.UTC)
	clockA := &stepClock{now: start}
	clockB := &stepClock{now: start}

	mgrA, err := NewLeaseManager("nuke-prod", client, job, clockA, libtime.NewWaiterDuration())
	if err != nil {
		t.Fatalf("NewLeaseManager(nuke-prod) returned %v", err)
	}
	mgrB, err := NewLeaseManager("sun", client, job, clockB, libtime.NewWaiterDuration())
	if err != nil {
		t.Fatalf("NewLeaseManager(sun) returned %v", err)
	}

	ctx := context.Background()
	const key = "claims/repo/task-1"

	if _, claimErr := mgrA.Claim(ctx, key, "task-1"); claimErr != nil {
		t.Fatalf("cluster A claim returned %v; want success", claimErr)
	}

	// A's Job succeeded at start: the claim is held, not deleted.
	if err := mgrA.ReleaseAfterSuccess(ctx, key, libtime.DateTime(start)); err != nil {
		t.Fatalf("ReleaseAfterSuccess returned %v; want nil (the hold is not an error)", err)
	}
	if got := len(mgrA.HeldClaims()); got != 1 {
		t.Fatalf("A held claims = %d; want 1 — the hold must keep the claim", got)
	}

	// Inside the window the peer cannot take the task over, so it cannot spawn.
	clockB.set(start.Add(30 * time.Second))
	held, err := mgrB.Claim(ctx, key, "task-1")
	if !stderrors.Is(err, ErrLeaseConflict) || held != nil {
		t.Fatalf(
			"peer claim inside the grace window = (%v, %v); want (nil, ErrLeaseConflict)",
			held,
			err,
		)
	}

	// Past the window A lets go — by the hold's expiry or by the fence, both of
	// which delete the ref — and the peer may claim.
	clockA.set(start.Add(SuccessGracePeriod + time.Second))
	if tickErr := mgrA.Tick(ctx); tickErr != nil {
		t.Fatalf("A tick returned %v; want nil", tickErr)
	}
	clockB.set(start.Add(SuccessGracePeriod + 2*time.Second))
	held, err = mgrB.Claim(ctx, key, "task-1")
	if err != nil || held == nil {
		t.Fatalf("peer claim after the window = (%v, %v); want a held claim", held, err)
	}
}

// TestReleaseAfterSuccessReleasesImmediatelyWhenTheWindowHasPassed pins the
// other half: a hold is only ever a delay, never a permanent lease. A Job whose
// completion is already older than the window releases at once, so a redelivered
// terminal Job cannot pin a claim forever.
func TestReleaseAfterSuccessReleasesImmediatelyWhenTheWindowHasPassed(t *testing.T) {
	client := newSuccessHoldRefClient()
	job := finishedJobControl{}
	start := time.Date(2026, 10, 23, 5, 55, 0, 0, time.UTC)
	clock := &stepClock{now: start}

	mgr, err := NewLeaseManager("nuke-prod", client, job, clock, libtime.NewWaiterDuration())
	if err != nil {
		t.Fatalf("NewLeaseManager returned %v", err)
	}

	ctx := context.Background()
	const key = "claims/repo/task-2"

	if _, claimErr := mgr.Claim(ctx, key, "task-2"); claimErr != nil {
		t.Fatalf("claim returned %v; want success", claimErr)
	}

	clock.set(start.Add(SuccessGracePeriod + time.Minute))
	if err := mgr.ReleaseAfterSuccess(ctx, key, libtime.DateTime(start)); err != nil {
		t.Fatalf("ReleaseAfterSuccess returned %v; want nil", err)
	}
	if got := len(mgr.HeldClaims()); got != 0 {
		t.Fatalf("held claims = %d after a release past the window; want 0", got)
	}
}
