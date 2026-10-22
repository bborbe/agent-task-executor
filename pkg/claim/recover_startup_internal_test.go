// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	stderrors "errors"
	"testing"
	"time"
)

// recoverStub is a LeaseManager whose only interesting method is Recover.
type recoverStub struct {
	calls     int
	failUntil int
}

func (s *recoverStub) Claim(context.Context, string, string) (*HeldClaim, error) {
	return nil, nil
}

func (s *recoverStub) Release(context.Context, string) error { return nil }

func (s *recoverStub) Takeover(context.Context, string, string) (*TakeoverResult, error) {
	return nil, nil
}

func (s *recoverStub) Tick(context.Context) error { return nil }

func (s *recoverStub) Run(context.Context) error { return nil }

func (s *recoverStub) Recover(context.Context) error {
	s.calls++
	if s.calls <= s.failUntil {
		return stderrors.New("git-rest down")
	}
	return nil
}

func (s *recoverStub) HeldClaims() []HeldClaim { return nil }

func (s *recoverStub) AuditMismatch(context.Context, map[string]bool) {}

// TestRecoverAtStartupRetriesThenSucceeds asserts a transient failure is retried
// and the boot proceeds once recovery lands.
func TestRecoverAtStartupRetriesThenSucceeds(t *testing.T) {
	manager := &recoverStub{failUntil: 2}

	if err := RecoverAtStartup(context.Background(), manager); err != nil {
		t.Fatalf("RecoverAtStartup returned %v; want nil", err)
	}
	if manager.calls != 3 {
		t.Fatalf("Recover calls = %d; want 3 (two failures, then success)", manager.calls)
	}
}

// TestRecoverAtStartupNeverFailsTheBoot asserts an executor that cannot reach
// git-rest still starts: the claims lapse, which is the behaviour before this
// step existed, rather than the pod crash-looping.
func TestRecoverAtStartupNeverFailsTheBoot(t *testing.T) {
	manager := &recoverStub{failUntil: recoverStartupAttempts}

	if err := RecoverAtStartup(context.Background(), manager); err != nil {
		t.Fatalf("RecoverAtStartup returned %v; want nil", err)
	}
	if manager.calls != recoverStartupAttempts {
		t.Fatalf("Recover calls = %d; want %d", manager.calls, recoverStartupAttempts)
	}
}

// TestRecoverAtStartupStopsOnCancelledContext asserts the retry loop does not
// outlive the service context.
func TestRecoverAtStartupStopsOnCancelledContext(t *testing.T) {
	manager := &recoverStub{failUntil: recoverStartupAttempts}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := RecoverAtStartup(ctx, manager); err != nil {
		t.Fatalf("RecoverAtStartup returned %v; want nil", err)
	}
	if manager.calls != 1 {
		t.Fatalf("Recover calls = %d; want 1 before the context stops the retry", manager.calls)
	}
}

// TestRunClaimRecoveryHoldsTheRunGroupOpen is the v0.30.2 regression at the
// claim layer: service.Run cancels every other run func as soon as one returns,
// so recovery completing must not end the run group.
func TestRunClaimRecoveryHoldsTheRunGroupOpen(t *testing.T) {
	manager := &recoverStub{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunClaimRecovery(ctx, manager) }()

	select {
	case err := <-done:
		t.Fatalf(
			"RunClaimRecovery returned %v while the context was live; the service would shut down",
			err,
		)
	case <-time.After(200 * time.Millisecond):
	}
	if manager.calls != 1 {
		t.Fatalf("Recover calls = %d; want 1 before the hold", manager.calls)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunClaimRecovery returned %v after cancel; want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunClaimRecovery did not return after the context was cancelled")
	}
}
