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
