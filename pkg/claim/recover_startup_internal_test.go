// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	stderrors "errors"
	"sync"
	"testing"
	"time"

	libtime "github.com/bborbe/time"
)

// recoverStub is a LeaseManager whose only interesting method is Recover.
//
// Recover runs on the service goroutine in the run-group tests, so the call
// counter is mutex-guarded and read through callCount; and recovered is closed on
// the first call so a test can synchronize on "Recover has run" instead of
// sleeping and hoping.
type recoverStub struct {
	mu        sync.Mutex
	calls     int
	failUntil int
	recovered chan struct{}
	once      sync.Once
}

func newRecoverStub(failUntil int) *recoverStub {
	return &recoverStub{failUntil: failUntil, recovered: make(chan struct{})}
}

// callCount is the synchronized read of the counter.
func (s *recoverStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *recoverStub) Claim(context.Context, string, string) (*HeldClaim, error) {
	return nil, nil
}

func (s *recoverStub) Release(context.Context, string) error { return nil }

func (s *recoverStub) ReleaseAfterSuccess(
	context.Context, string, libtime.DateTime,
) error {
	return nil
}

func (s *recoverStub) Takeover(context.Context, string, string) (*TakeoverResult, error) {
	return nil, nil
}

func (s *recoverStub) Tick(context.Context) error { return nil }

func (s *recoverStub) Run(context.Context) error { return nil }

func (s *recoverStub) Recover(context.Context) error {
	s.mu.Lock()
	s.calls++
	calls := s.calls
	s.mu.Unlock()
	s.once.Do(func() { close(s.recovered) })
	if calls <= s.failUntil {
		return stderrors.New("git-rest down")
	}
	return nil
}

func (s *recoverStub) HeldClaims() []HeldClaim { return nil }

func (s *recoverStub) AuditMismatch(context.Context, map[string]bool) {}

// TestRecoverAtStartupRetriesThenSucceeds asserts a transient failure is retried
// and the boot proceeds once recovery lands.
func TestRecoverAtStartupRetriesThenSucceeds(t *testing.T) {
	manager := newRecoverStub(2)

	if err := RecoverAtStartup(context.Background(), manager, nil); err != nil {
		t.Fatalf("RecoverAtStartup returned %v; want nil", err)
	}
	if got := manager.callCount(); got != 3 {
		t.Fatalf("Recover calls = %d; want 3 (two failures, then success)", got)
	}
}

// TestRecoverAtStartupNeverFailsTheBoot asserts an executor that cannot reach
// git-rest still starts: the claims lapse, which is the behaviour before this
// step existed, rather than the pod crash-looping.
func TestRecoverAtStartupNeverFailsTheBoot(t *testing.T) {
	manager := newRecoverStub(recoverStartupAttempts)

	if err := RecoverAtStartup(context.Background(), manager, nil); err != nil {
		t.Fatalf("RecoverAtStartup returned %v; want nil", err)
	}
	if got := manager.callCount(); got != recoverStartupAttempts {
		t.Fatalf("Recover calls = %d; want %d", got, recoverStartupAttempts)
	}
}

// TestRecoverAtStartupStopsOnCancelledContext asserts the retry loop does not
// outlive the service context.
func TestRecoverAtStartupStopsOnCancelledContext(t *testing.T) {
	manager := newRecoverStub(recoverStartupAttempts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancel()

	if err := RecoverAtStartup(ctx, manager, nil); err != nil {
		t.Fatalf("RecoverAtStartup returned %v; want nil", err)
	}
	if got := manager.callCount(); got != 1 {
		t.Fatalf("Recover calls = %d; want 1 before the context stops the retry", got)
	}
}

// TestRunClaimRecoveryHoldsTheRunGroupOpen is the v0.30.2 regression at the
// claim layer: service.Run cancels every other run func as soon as one returns,
// so recovery completing must not end the run group.
func TestRunClaimRecoveryHoldsTheRunGroupOpen(t *testing.T) {
	manager := newRecoverStub(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- RunClaimRecovery(ctx, manager, nil) }()

	// Synchronize on Recover having run rather than on a sleep: a fixed delay is
	// too short under load (the counter assertion fails for a correct
	// implementation) and wasted in the happy path. Recover returning is also the
	// exact point at which a one-shot implementation takes the run group down.
	select {
	case <-manager.recovered:
	case <-time.After(5 * time.Second):
		t.Fatal("Recover was never called")
	}

	// Some window is unavoidable: "must not return" is a negative, observable only
	// over an interval. It starts after the deterministic signal above, so it can
	// only make the test stricter — never flakier.
	select {
	case err := <-done:
		t.Fatalf(
			"RunClaimRecovery returned %v while the context was live; the service would shut down",
			err,
		)
	case <-time.After(100 * time.Millisecond):
	}
	if got := manager.callCount(); got != 1 {
		t.Fatalf("Recover calls = %d; want 1 before the hold", got)
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

// TestWaitForTaskViewBlocksUntilReady asserts startup recovery does not decide a
// claim's fate against a view that has not replayed. The fence resolves a Job's
// namespace through the task's assignee, so a view that has not caught up reads
// "no task, no Job" for a Job that is still running, and recovery releases its
// claim — the duplicate the claims exist to prevent.
func TestWaitForTaskViewBlocksUntilReady(t *testing.T) {
	previous := recoverStartupViewPollInterval
	recoverStartupViewPollInterval = time.Millisecond
	defer func() { recoverStartupViewPollInterval = previous }()

	replayed := make(chan struct{})
	done := make(chan struct{})
	go func() {
		waitForTaskView(context.Background(), func() bool {
			select {
			case <-replayed:
				return true
			default:
				return false
			}
		})
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("waitForTaskView returned while the task view had not replayed")
	case <-time.After(50 * time.Millisecond):
	}

	close(replayed)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitForTaskView did not return once the view became ready")
	}
}
