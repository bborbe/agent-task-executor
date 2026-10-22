// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"sync"
	"testing"
	"time"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// countingClaimRefClient records how many Deletes it saw.
type countingClaimRefClient struct {
	mu      sync.Mutex
	deletes int
	// onPut runs while a Put is in flight, so a test can observe the handler's
	// state at exactly that moment.
	onPut func()
}

func (c *countingClaimRefClient) List(context.Context, string) ([]claim.ClaimRef, error) {
	return nil, nil
}

func (c *countingClaimRefClient) Put(
	context.Context, string, string, string, string, libtime.DateTime,
) (string, error) {
	if c.onPut != nil {
		c.onPut()
	}
	return "sha-1", nil
}

func (c *countingClaimRefClient) Delete(context.Context, string, string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes++
	return nil
}

// TestReleaseTaskClaimSkipsWhileSpawnPending pins the claim-then-store window:
// between taking a claim and recording the new Job the store names only the
// previous Job, so a terminal event for it must not release the fresh claim.
func TestReleaseTaskClaimSkipsWhileSpawnPending(t *testing.T) {
	ctx := context.Background()
	client := &countingClaimRefClient{}
	h := newGateHandler(pkg.ClaimModeShadow, client)

	manager, err := h.leaseManager(ctx)
	if err != nil {
		t.Fatalf("lease manager: %v", err)
	}
	if _, err := manager.Claim(ctx, "claims/task/task-1", "task-1"); err != nil {
		t.Fatalf("claim: %v", err)
	}

	h.markSpawnPending(lib.TaskIdentifier("task-1"))
	h.ReleaseTaskClaim(ctx, lib.TaskIdentifier("task-1"))
	if client.deletes != 0 {
		t.Fatalf("released %d claim(s) while a spawn was pending, want 0", client.deletes)
	}

	h.clearSpawnPending(lib.TaskIdentifier("task-1"))
	h.ReleaseTaskClaim(ctx, lib.TaskIdentifier("task-1"))
	if client.deletes != 1 {
		t.Fatalf("released %d claim(s) after the spawn window closed, want 1", client.deletes)
	}
}

// TestReleaseBlocksWhileASpawnHoldsTheClaimLock pins the ordering the review
// found: the pending check and the delete must be atomic against the spawn's
// mark+claim, so a release cannot run between them and delete a fresh claim.
func TestReleaseBlocksWhileASpawnHoldsTheClaimLock(t *testing.T) {
	ctx := context.Background()
	client := &countingClaimRefClient{}
	h := newGateHandler(pkg.ClaimModeShadow, client)

	manager, err := h.leaseManager(ctx)
	if err != nil {
		t.Fatalf("lease manager: %v", err)
	}
	if _, err := manager.Claim(ctx, "claims/task/task-1", "task-1"); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Hold exactly what a spawn holds while it takes its claim.
	unlockClaim := h.lockClaimRelease(lib.TaskIdentifier("task-1"))
	h.markSpawnPending(lib.TaskIdentifier("task-1"))

	released := make(chan struct{})
	go func() {
		h.ReleaseTaskClaim(ctx, lib.TaskIdentifier("task-1"))
		close(released)
	}()

	select {
	case <-released:
		t.Fatal("release ran while a spawn held the claim lock")
	case <-time.After(100 * time.Millisecond):
	}

	unlockClaim()
	<-released
	h.clearSpawnPending(lib.TaskIdentifier("task-1"))

	if client.deletes != 0 {
		t.Fatalf("released %d claim(s) across the spawn window, want 0", client.deletes)
	}
}
