// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package factory

import (
	"context"
	"sync"
	"testing"
	"time"

	libtime "github.com/bborbe/time"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// stubLeaseManager records Run calls so a test can assert the factory hands
// back the manager's own loop. Plain testing, not Ginkgo: this repo's Ginkgo
// specs declared in an internal test package never run.
//
// The counters are mutex-guarded and read through the accessors because the run
// funcs under test execute on their own goroutines, and recovered is closed on
// the first Recover so a test can synchronize on "recovery has run" instead of
// sleeping and hoping.
type stubLeaseManager struct {
	mu           sync.Mutex
	runCalls     int
	recoverCalls int
	recoverErr   error
	recovered    chan struct{}
	recoverOnce  sync.Once
}

func newStubLeaseManager() *stubLeaseManager {
	return &stubLeaseManager{recovered: make(chan struct{})}
}

func (s *stubLeaseManager) runCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runCalls
}

func (s *stubLeaseManager) recoverCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recoverCalls
}

func (s *stubLeaseManager) Claim(
	context.Context, string, string,
) (*claim.HeldClaim, error) {
	return nil, nil
}

func (s *stubLeaseManager) ClaimPair(
	context.Context, string, string, string,
) (*claim.HeldClaim, error) {
	return nil, nil
}

func (s *stubLeaseManager) Release(context.Context, string) error { return nil }

func (s *stubLeaseManager) ReleaseAfterSuccess(
	context.Context, string, libtime.DateTime,
) error {
	return nil
}

func (s *stubLeaseManager) Takeover(
	context.Context, string, string,
) (*claim.TakeoverResult, error) {
	return nil, nil
}

func (s *stubLeaseManager) TakeoverPair(
	context.Context, string, string, string,
) (*claim.TakeoverResult, error) {
	return nil, nil
}

func (s *stubLeaseManager) Tick(context.Context) error { return nil }

func (s *stubLeaseManager) Run(context.Context) error {
	s.mu.Lock()
	s.runCalls++
	s.mu.Unlock()
	return nil
}

func (s *stubLeaseManager) Recover(context.Context) error {
	s.mu.Lock()
	s.recoverCalls++
	s.mu.Unlock()
	s.recoverOnce.Do(func() { close(s.recovered) })
	return s.recoverErr
}

func (s *stubLeaseManager) HeldClaims() []claim.HeldClaim { return nil }

func (s *stubLeaseManager) AuditMismatch(context.Context, map[string]bool) {}

// TestCreateClaimLifecycleRunFuncs asserts the factory wires startup recovery
// ahead of the lease manager's own loop, and that both are runnable. The context
// is already cancelled because the recovery func holds until cancellation — it
// must not return while the service is live (see the regression test below).
func TestCreateClaimLifecycleRunFuncs(t *testing.T) {
	manager := newStubLeaseManager()

	runFuncs := CreateClaimLifecycleRunFuncs(manager, nil)

	if len(runFuncs) != 2 {
		t.Fatalf("run funcs = %d, want 2 (recover, run)", len(runFuncs))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancel()
	for i, runFunc := range runFuncs {
		if err := runFunc(ctx); err != nil {
			t.Fatalf("run func %d: %v", i, err)
		}
	}
	if got := manager.recoverCount(); got != 1 {
		t.Fatalf("lease manager Recover calls = %d, want 1", got)
	}
	if got := manager.runCount(); got != 1 {
		t.Fatalf("lease manager Run calls = %d, want 1", got)
	}
}

// TestCreateClaimRecoverFuncKeepsTheRunGroupOpen is the v0.30.2 regression. The
// recover run func must not return while the service context is live: service.Run
// wraps every run func in run.CancelOnFirstFinishWait, so a recovery step that
// returned once it had recovered cancelled the lease loop, the HTTP server and the
// cron — every executor exited 0 a few seconds after boot.
func TestCreateClaimRecoverFuncKeepsTheRunGroupOpen(t *testing.T) {
	manager := newStubLeaseManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- CreateClaimRecoverFunc(manager, nil)(ctx) }()

	// Synchronize on Recover having run rather than on a sleep: a fixed delay is
	// too short under load (the counter assertion fails for a correct
	// implementation) and wasted in the happy path.
	select {
	case <-manager.recovered:
	case <-time.After(5 * time.Second):
		t.Fatal("lease manager Recover was never called")
	}

	// Some window is unavoidable: "must not return" is a negative, observable only
	// over an interval. It starts after the deterministic signal above, so it can
	// only make the test stricter — never flakier.
	select {
	case err := <-done:
		t.Fatalf(
			"recover run func returned %v with a live context; service.Run would cancel every other component",
			err,
		)
	case <-time.After(100 * time.Millisecond):
	}
	if got := manager.recoverCount(); got != 1 {
		t.Fatalf("lease manager Recover calls = %d, want 1 before the hold", got)
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
