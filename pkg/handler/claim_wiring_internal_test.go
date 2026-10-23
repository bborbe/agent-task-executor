// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	stderrors "errors"
	"strings"
	"testing"
	stdtime "time"

	lib "github.com/bborbe/agent"
	base "github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// noopClaimRefClient satisfies claim.ClaimRefClient for construction tests. It
// is defined here rather than taken from the mocks package, which imports
// pkg/handler and so cannot be imported by an internal test of it.
type noopClaimRefClient struct{}

func (noopClaimRefClient) List(context.Context, string) ([]claim.ClaimRef, error) {
	return nil, nil
}

func (noopClaimRefClient) Put(
	context.Context, string, string, string, string, libtime.DateTime,
) (string, error) {
	return "", nil
}

func (noopClaimRefClient) Delete(context.Context, string, string) error { return nil }

func TestLeaseManagerRefusesClaimModeOff(t *testing.T) {
	ctx := context.Background()
	h := &taskEventHandler{
		claimMode: pkg.ClaimModeOff,
		claimRefClientFactory: func() claim.ClaimRefClient {
			panic("factory must not be called in claim mode off")
		},
		claimJobControl: noopJobControl{},
		currentDateTime: libtime.NewCurrentDateTime(),
	}
	manager, err := h.leaseManager(ctx)
	if err == nil {
		t.Fatal("expected an error in claim mode off")
	}
	if !strings.Contains(err.Error(), "off") {
		t.Fatalf("expected error to name mode off, got %q", err.Error())
	}
	if manager != nil {
		t.Fatal("expected no manager in claim mode off")
	}
}

// TestLeaseManagerRefusesUnparsedClaimMode pins the allow-list: a zero-value
// mode is not off, so a check for off alone would construct a manager here and
// fail open.
func TestLeaseManagerRefusesUnparsedClaimMode(t *testing.T) {
	ctx := context.Background()
	h := &taskEventHandler{
		claimMode: "",
		claimRefClientFactory: func() claim.ClaimRefClient {
			panic("factory must not be called for an unparsed claim mode")
		},
		claimJobControl: noopJobControl{},
		currentDateTime: libtime.NewCurrentDateTime(),
	}
	manager, err := h.leaseManager(ctx)
	if err == nil {
		t.Fatal("expected an error for an unparsed claim mode")
	}
	if manager != nil {
		t.Fatal("expected no manager for an unparsed claim mode")
	}
}

func TestLeaseManagerConstructsOnce(t *testing.T) {
	ctx := context.Background()
	calls := 0
	h := &taskEventHandler{
		claimMode: pkg.ClaimModeShadow,
		cluster:   "self",
		claimRefClientFactory: func() claim.ClaimRefClient {
			calls++
			return noopClaimRefClient{}
		},
		claimJobControl: noopJobControl{},
		currentDateTime: libtime.NewCurrentDateTime(),
	}
	first, err := h.leaseManager(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first == nil {
		t.Fatal("expected a manager")
	}
	second, err := h.leaseManager(ctx)
	if err != nil {
		t.Fatalf("unexpected error on second call: %v", err)
	}
	if second != first {
		t.Fatal("expected the same manager instance")
	}
	if calls != 1 {
		t.Fatalf("expected factory to be called once, got %d", calls)
	}
}

// TestLeaseManagerRejectsNilRefClient pins the loud failure: a factory that
// returns nil must be refused here, because a nil client would panic on the
// first claim instead of surfacing a configuration error.
func TestLeaseManagerRejectsNilRefClient(t *testing.T) {
	ctx := context.Background()
	h := &taskEventHandler{
		claimMode: pkg.ClaimModeShadow,
		cluster:   "self",
		claimRefClientFactory: func() claim.ClaimRefClient {
			return nil
		},
		claimJobControl: noopJobControl{},
		currentDateTime: libtime.NewCurrentDateTime(),
	}
	manager, err := h.leaseManager(ctx)
	if err == nil {
		t.Fatal("expected an error for a nil ref client")
	}
	if !strings.Contains(err.Error(), "nil") {
		t.Fatalf("expected error to name nil, got %q", err.Error())
	}
	if manager != nil {
		t.Fatal("expected no manager for a nil ref client")
	}
}

func TestLeaseManagerRejectsEmptyCluster(t *testing.T) {
	ctx := context.Background()
	h := &taskEventHandler{
		claimMode: pkg.ClaimModeShadow,
		cluster:   "",
		claimRefClientFactory: func() claim.ClaimRefClient {
			return noopClaimRefClient{}
		},
		claimJobControl: noopJobControl{},
		currentDateTime: libtime.NewCurrentDateTime(),
	}
	_, err := h.leaseManager(ctx)
	if err == nil {
		t.Fatal("expected an error for an empty cluster")
	}
	if !strings.Contains(err.Error(), "cluster") {
		t.Fatalf("expected error to name cluster, got %q", err.Error())
	}
}

// TestLeaseManagerRefusesNilJobControl asserts a claim-taking mode cannot build
// a manager without a JobControl: the fence and restart recovery must never run
// against a control that cannot delete a Job.
func TestLeaseManagerRefusesNilJobControl(t *testing.T) {
	ctx := context.Background()
	h := &taskEventHandler{
		claimMode: pkg.ClaimModeShadow,
		cluster:   "self",
		claimRefClientFactory: func() claim.ClaimRefClient {
			return noopClaimRefClient{}
		},
		currentDateTime: libtime.NewCurrentDateTime(),
	}
	manager, err := h.leaseManager(ctx)
	if err == nil {
		t.Fatal("expected an error for a nil job control")
	}
	if !strings.Contains(err.Error(), "job control") {
		t.Fatalf("expected error to name job control, got %q", err.Error())
	}
	if manager != nil {
		t.Fatal("expected no manager without a job control")
	}
}

// noopJobControl satisfies claim.JobControl where a test only needs the manager
// to construct.
type noopJobControl struct{}

func (noopJobControl) IsJobRunning(context.Context, string) (bool, error) {
	return false, nil
}

func (noopJobControl) DeleteJob(context.Context, string) error { return nil }

// TestRedriveStreakStoreSharesLeaseManagerClient pins that the streak store and
// the lease manager are built from one claim-ref client instance, inside the
// existing one-time construction.
func TestRedriveStreakStoreSharesLeaseManagerClient(t *testing.T) {
	ctx := context.Background()
	calls := 0
	client := &recordingClaimRefClient{}
	h := &taskEventHandler{
		claimMode: pkg.ClaimModeShadow,
		cluster:   "self",
		claimRefClientFactory: func() claim.ClaimRefClient {
			calls++
			return client
		},
		claimJobControl: noopJobControl{},
		currentDateTime: libtime.NewCurrentDateTime(),
	}

	store, err := h.redriveStreakStore(ctx)
	if err != nil {
		t.Fatalf("unexpected store error: %v", err)
	}
	manager, err := h.ClaimLeaseManager(ctx)
	if err != nil {
		t.Fatalf("unexpected manager error: %v", err)
	}
	storeAgain, err := h.redriveStreakStore(ctx)
	if err != nil {
		t.Fatalf("unexpected store error on second call: %v", err)
	}
	managerAgain, err := h.ClaimLeaseManager(ctx)
	if err != nil {
		t.Fatalf("unexpected manager error on second call: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected the factory to be called once, got %d", calls)
	}
	if store == nil || manager == nil {
		t.Fatal("expected both the store and the manager to be constructed")
	}
	if store != storeAgain {
		t.Fatal("expected the same store instance")
	}
	if manager != managerAgain {
		t.Fatal("expected the same manager instance")
	}

	if _, err := store.Count(ctx, "tid-ac10", "execution"); err != nil {
		t.Fatalf("unexpected store count error: %v", err)
	}
	if _, err := manager.Claim(ctx, "claims/task/tid-ac10", "tid-ac10"); err != nil {
		t.Fatalf("unexpected claim error: %v", err)
	}
	if len(client.listCalls) != 1 {
		t.Fatalf("expected one list call on the shared client, got %d", len(client.listCalls))
	}
	if len(client.putCalls) != 1 {
		t.Fatalf("expected one put call on the shared client, got %d", len(client.putCalls))
	}
}

func testBoolPtr(b bool) *bool { return &b }

// redispatchConfig is the explicit opt-in: a task taken over on this Config
// runs again under the new owner.
func redispatchConfig() *pkg.AgentConfiguration {
	return &pkg.AgentConfiguration{RerunOnLostClaim: testBoolPtr(true)}
}

// parkConfig declares the agent must not run a second time.
func parkConfig() *pkg.AgentConfiguration {
	return &pkg.AgentConfiguration{RerunOnLostClaim: testBoolPtr(false)}
}

// unsetConfig leaves rerunOnLostClaim out, which parks: an unset field is the
// safe default for a Config that predates the field.
func unsetConfig() *pkg.AgentConfiguration {
	return &pkg.AgentConfiguration{}
}

// parkRecordingPublisher satisfies pkg.ResultPublisher for the park branch. It
// records the park writes and ignores every other method. It lives here rather
// than in the mocks package because that package imports pkg/handler, so an
// internal test of handler cannot import it.
type parkRecordingPublisher struct {
	parked []lib.TaskIdentifier
	// failFirst makes the next failFirst park calls fail, so a test can pin the
	// retry and the final-failure path without a second fake.
	failFirst int
}

func (s *parkRecordingPublisher) PublishParkLostClaim(
	_ context.Context,
	task lib.Task,
) error {
	if s.failFirst > 0 {
		s.failFirst--
		return stderrors.New("publish failed")
	}
	s.parked = append(s.parked, task.TaskIdentifier)
	return nil
}

func (s *parkRecordingPublisher) PublishSpawnNotification(
	context.Context, lib.Task, string,
) error {
	return nil
}

func (s *parkRecordingPublisher) PublishFailure(
	context.Context, lib.Task, string, string,
) error {
	return nil
}

func (s *parkRecordingPublisher) PublishClearCurrentJob(
	context.Context, lib.Task, string,
) error {
	return nil
}

func (s *parkRecordingPublisher) PublishIncrementTriggerCount(
	context.Context, lib.Task,
) error {
	return nil
}

func (s *parkRecordingPublisher) PublishSetTriggerScope(
	context.Context, lib.Task, string, int,
) error {
	return nil
}

func (s *parkRecordingPublisher) PublishTypeMismatchFailure(
	context.Context, lib.Task, string,
) error {
	return nil
}

func (s *parkRecordingPublisher) PublishRestoreAssignee(
	context.Context, lib.Task, string,
) error {
	return nil
}

func (s *parkRecordingPublisher) PublishQueueEntry(
	context.Context, lib.Task, string,
) error {
	return nil
}

func (s *parkRecordingPublisher) PublishRaw(
	context.Context, base.CommandOperation, interface{},
) error {
	return nil
}

// parkOnLostClaimHandler builds a handler whose claim client answers with an
// expired lease, so the first Claim conflicts and Takeover succeeds.
func parkOnLostClaimHandler() (*taskEventHandler, *parkRecordingPublisher) {
	client := takeoverClient(takeoverClaimRef(
		"expired-sha",
		stdtime.Now().UTC().Add(-31*stdtime.Second),
	))
	h := newGateHandler(pkg.ClaimModeOn, client)
	publisher := &parkRecordingPublisher{}
	h.resultPublisher = publisher
	return h, publisher
}

const parkedLogLine = "event=parked_lost_claim key=claims/task/tid-gate-1 task=tid-gate-1"

// TestAcquireSpawnClaimParksOnLostClaimWhenRerunDisabled is SC1 for the
// explicit false: a takeover parks the task at human_review and reports false,
// which is the caller's "create no Job" signal.
func TestAcquireSpawnClaimParksOnLostClaimWhenRerunDisabled(t *testing.T) {
	ctx := context.Background()
	h, publisher := parkOnLostClaimHandler()

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, gateTask("", ""), parkConfig())
	})

	if ok {
		t.Fatal("expected no Job for a non-re-runnable agent after a takeover")
	}
	if len(publisher.parked) != 1 {
		t.Fatalf("park writes = %d, want 1", len(publisher.parked))
	}
	if !strings.Contains(out, parkedLogLine) {
		t.Errorf("stderr = %q, want it to contain %q", out, parkedLogLine)
	}
	if strings.Contains(out, "event=claim_acquired") {
		t.Errorf("stderr = %q, want no claim_acquired line for a parked task", out)
	}
}

// TestAcquireSpawnClaimParksOnLostClaimWhenRerunUnset is SC1 for the unset
// field: the flipped default parks, so a Config that predates the field does
// not silently re-run work its author never opted into re-running.
func TestAcquireSpawnClaimParksOnLostClaimWhenRerunUnset(t *testing.T) {
	ctx := context.Background()
	h, publisher := parkOnLostClaimHandler()

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, gateTask("", ""), unsetConfig())
	})

	if ok {
		t.Fatal("expected no Job for an unset rerunOnLostClaim after a takeover")
	}
	if len(publisher.parked) != 1 {
		t.Fatalf("park writes = %d, want 1", len(publisher.parked))
	}
	if !strings.Contains(out, parkedLogLine) {
		t.Errorf("stderr = %q, want it to contain %q", out, parkedLogLine)
	}
}

// TestAcquireSpawnClaimRedispatchesOnLostClaimWhenRerunEnabled is SC2: the
// explicit opt-in still runs the task under the new owner, so the parking
// branch does not capture every Config.
func TestAcquireSpawnClaimRedispatchesOnLostClaimWhenRerunEnabled(t *testing.T) {
	ctx := context.Background()
	h, publisher := parkOnLostClaimHandler()

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, gateTask("", ""), redispatchConfig())
	})

	if !ok {
		t.Fatal("expected the spawn to proceed for rerunOnLostClaim: true")
	}
	if len(publisher.parked) != 0 {
		t.Fatalf("park writes = %d, want 0", len(publisher.parked))
	}
	if !strings.Contains(out, acquiredLogLine) {
		t.Errorf("stderr = %q, want it to contain %q", out, acquiredLogLine)
	}
}

// TestAcquireSpawnClaimDoesNotParkInShadowMode pins shadow's contract: it takes
// claims without ever blocking a dispatch. Parking there would be worse than a
// violation — the task is written to human_review, this cluster keeps the claim,
// and the next pass's Takeover fails against its own live lease, so
// claimFailureDecision proceeds and a Job is created for the parked task.
func TestAcquireSpawnClaimDoesNotParkInShadowMode(t *testing.T) {
	ctx := context.Background()
	h, publisher := parkOnLostClaimHandler()
	h.claimMode = pkg.ClaimModeShadow

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, gateTask("", ""), parkConfig())
	})

	if !ok {
		t.Fatal("expected shadow to proceed with the spawn")
	}
	if len(publisher.parked) != 0 {
		t.Fatalf("park writes = %d, want 0 in shadow mode", len(publisher.parked))
	}
	if strings.Contains(out, "event=parked_lost_claim") {
		t.Errorf("stderr = %q, want no park line in shadow mode", out)
	}
}

// TestAcquireSpawnClaimRetriesTheParkWrite pins the retry: a single failed send
// must not be reported as a failed park, because the phase write is what stops
// the task running again after its claim is released jobless.
func TestAcquireSpawnClaimRetriesTheParkWrite(t *testing.T) {
	ctx := context.Background()
	h, publisher := parkOnLostClaimHandler()
	publisher.failFirst = 1

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, gateTask("", ""), parkConfig())
	})

	if ok {
		t.Fatal("expected no Job for a non-re-runnable agent after a takeover")
	}
	if len(publisher.parked) != 1 {
		t.Fatalf("park writes = %d, want 1 after one retry", len(publisher.parked))
	}
	if !strings.Contains(out, parkedLogLine) {
		t.Errorf("stderr = %q, want the success line once the write lands", out)
	}
}

// TestAcquireSpawnClaimReportsAFailedParkWrite pins the final-failure path: the
// task is still not spawned, but the log must say the park failed rather than
// claim it happened.
func TestAcquireSpawnClaimReportsAFailedParkWrite(t *testing.T) {
	ctx := context.Background()
	h, publisher := parkOnLostClaimHandler()
	publisher.failFirst = parkWriteAttempts

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, gateTask("", ""), parkConfig())
	})

	if ok {
		t.Fatal("expected no Job even when the park write never lands")
	}
	if len(publisher.parked) != 0 {
		t.Fatalf("park writes = %d, want 0", len(publisher.parked))
	}
	if !strings.Contains(out, "event=parked_lost_claim_failed") {
		t.Errorf("stderr = %q, want the failure line", out)
	}
	if strings.Contains(out, parkedLogLine) {
		t.Errorf("stderr = %q, want no success line when the write failed", out)
	}
}
