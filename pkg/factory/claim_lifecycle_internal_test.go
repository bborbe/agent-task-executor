// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package factory

import (
	"context"
	"testing"
	"time"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// stubLeaseManager records Run calls so a test can assert the factory hands
// back the manager's own loop. Plain testing, not Ginkgo: this repo's Ginkgo
// specs declared in an internal test package never run.
type stubLeaseManager struct {
	runCalls     int
	recoverCalls int
	recoverErr   error
}

func (s *stubLeaseManager) Claim(
	context.Context, string, string,
) (*claim.HeldClaim, error) {
	return nil, nil
}

func (s *stubLeaseManager) Release(context.Context, string) error { return nil }

func (s *stubLeaseManager) Takeover(
	context.Context, string, string,
) (*claim.TakeoverResult, error) {
	return nil, nil
}

func (s *stubLeaseManager) Tick(context.Context) error { return nil }

func (s *stubLeaseManager) Run(context.Context) error {
	s.runCalls++
	return nil
}

func (s *stubLeaseManager) Recover(context.Context) error {
	s.recoverCalls++
	return s.recoverErr
}

func (s *stubLeaseManager) HeldClaims() []claim.HeldClaim { return nil }

func (s *stubLeaseManager) AuditMismatch(context.Context, map[string]bool) {}

// TestCreateClaimLifecycleRunFuncs asserts the factory wires startup recovery
// ahead of the lease manager's own loop, and that both are runnable. The context
// is already cancelled because the recovery func holds until cancellation — it
// must not return while the service is live (see the regression test below).
func TestCreateClaimLifecycleRunFuncs(t *testing.T) {
	manager := &stubLeaseManager{}

	runFuncs := CreateClaimLifecycleRunFuncs(manager)

	if len(runFuncs) != 2 {
		t.Fatalf("run funcs = %d, want 2 (recover, run)", len(runFuncs))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i, runFunc := range runFuncs {
		if err := runFunc(ctx); err != nil {
			t.Fatalf("run func %d: %v", i, err)
		}
	}
	if manager.recoverCalls != 1 {
		t.Fatalf("lease manager Recover calls = %d, want 1", manager.recoverCalls)
	}
	if manager.runCalls != 1 {
		t.Fatalf("lease manager Run calls = %d, want 1", manager.runCalls)
	}
}

// TestCreateClaimRecoverFuncKeepsTheRunGroupOpen is the v0.30.2 regression. The
// recover run func must not return while the service context is live: service.Run
// wraps every run func in run.CancelOnFirstFinishWait, so a recovery step that
// returned once it had recovered cancelled the lease loop, the HTTP server and the
// cron — every executor exited 0 a few seconds after boot.
func TestCreateClaimRecoverFuncKeepsTheRunGroupOpen(t *testing.T) {
	manager := &stubLeaseManager{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- CreateClaimRecoverFunc(manager)(ctx) }()

	select {
	case err := <-done:
		t.Fatalf(
			"recover run func returned %v with a live context; service.Run would cancel every other component",
			err,
		)
	case <-time.After(200 * time.Millisecond):
	}
	if manager.recoverCalls != 1 {
		t.Fatalf("lease manager Recover calls = %d, want 1 before the hold", manager.recoverCalls)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("recover run func returned %v after cancel; want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recover run func did not return after the context was cancelled")
	}
}
