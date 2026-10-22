// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package factory

import (
	"context"
	"testing"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// stubLeaseManager records Run calls so a test can assert the factory hands
// back the manager's own loop. Plain testing, not Ginkgo: this repo's Ginkgo
// specs declared in an internal test package never run.
type stubLeaseManager struct {
	runCalls int
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

func (s *stubLeaseManager) Recover(context.Context) error { return nil }

func (s *stubLeaseManager) HeldClaims() []claim.HeldClaim { return nil }

func (s *stubLeaseManager) AuditMismatch(context.Context, map[string]bool) {}

// TestCreateClaimLifecycleRunFuncs asserts the factory wires exactly one run
// func and that it is the lease manager's own loop.
func TestCreateClaimLifecycleRunFuncs(t *testing.T) {
	manager := &stubLeaseManager{}

	runFuncs := CreateClaimLifecycleRunFuncs(manager)

	if len(runFuncs) != 1 {
		t.Fatalf("run funcs = %d, want 1", len(runFuncs))
	}
	if err := runFuncs[0](context.Background()); err != nil {
		t.Fatalf("run func: %v", err)
	}
	if manager.runCalls != 1 {
		t.Fatalf("lease manager Run calls = %d, want 1", manager.runCalls)
	}
}
