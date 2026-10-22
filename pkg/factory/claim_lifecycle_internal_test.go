// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package factory

import (
	"context"
	"testing"

	"github.com/bborbe/errors"

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
// ahead of the lease manager's own loop, and that both are runnable.
func TestCreateClaimLifecycleRunFuncs(t *testing.T) {
	manager := &stubLeaseManager{}

	runFuncs := CreateClaimLifecycleRunFuncs(manager)

	if len(runFuncs) != 2 {
		t.Fatalf("run funcs = %d, want 2 (recover, run)", len(runFuncs))
	}
	for i, runFunc := range runFuncs {
		if err := runFunc(context.Background()); err != nil {
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

// TestCreateClaimRecoverFuncNeverFailsTheBoot asserts a git-rest outage at
// startup is logged and retried but does not crash-loop the executor: the run
// func returns nil even when every attempt fails.
func TestCreateClaimRecoverFuncNeverFailsTheBoot(t *testing.T) {
	manager := &stubLeaseManager{recoverErr: errors.Errorf(context.Background(), "git-rest down")}

	if err := CreateClaimRecoverFunc(manager)(context.Background()); err != nil {
		t.Fatalf("recover run func returned %v; want nil so the executor still boots", err)
	}
	if manager.recoverCalls < 2 {
		t.Fatalf(
			"lease manager Recover calls = %d, want the retry to run more than once",
			manager.recoverCalls,
		)
	}
}

// TestCreateClaimRecoverFuncStopsOnCancelledContext asserts the retry loop does
// not outlive the service context.
func TestCreateClaimRecoverFuncStopsOnCancelledContext(t *testing.T) {
	manager := &stubLeaseManager{recoverErr: errors.Errorf(context.Background(), "git-rest down")}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := CreateClaimRecoverFunc(manager)(ctx); err != nil {
		t.Fatalf("recover run func returned %v; want nil", err)
	}
	if manager.recoverCalls != 1 {
		t.Fatalf(
			"lease manager Recover calls = %d, want 1 before the context stops the retry",
			manager.recoverCalls,
		)
	}
}
