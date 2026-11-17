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
	corev1 "k8s.io/api/core/v1"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/claim/claimtest"
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
		claimClientFactory: func() claim.ClaimClient {
			return claimtest.NewClient(noopClaimRefClient{}, libtime.NewCurrentDateTime().Now)
		},
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
		claimClientFactory: func() claim.ClaimClient {
			return claimtest.NewClient(noopClaimRefClient{}, libtime.NewCurrentDateTime().Now)
		},
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
		claimClientFactory: func() claim.ClaimClient {
			return claimtest.NewClient(noopClaimRefClient{}, libtime.NewCurrentDateTime().Now)
		},
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
		claimClientFactory: func() claim.ClaimClient {
			return claimtest.NewClient(noopClaimRefClient{}, libtime.NewCurrentDateTime().Now)
		},
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
		claimClientFactory: func() claim.ClaimClient {
			return claimtest.NewClient(noopClaimRefClient{}, libtime.NewCurrentDateTime().Now)
		},
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
		claimClientFactory: func() claim.ClaimClient {
			return claimtest.NewClient(noopClaimRefClient{}, libtime.NewCurrentDateTime().Now)
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
		claimClientFactory: func() claim.ClaimClient {
			return claimtest.NewClient(client, libtime.NewCurrentDateTime().Now)
		},
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
	if len(client.listCalls) != 3 {
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
	// parkedSucceeded records the succeeded-Job parks, which write a different
	// phase for a different cause and must not be conflated with the above.
	parkedSucceeded []lib.Task
	// parkedRedriveCap records the cap-exhausted parks, which stay open: the
	// work did not finish, so they must never be recorded as succeeded.
	parkedRedriveCap []lib.Task
	// failParkSucceeded makes every succeeded-Job park fail, so a spec can pin
	// the log-and-continue branch.
	failParkSucceeded bool
	// failFirst makes the next failFirst park calls fail, so a test can pin the
	// retry and the final-failure path without a second fake.
	failFirst int
}

func (s *parkRecordingPublisher) PublishParkLostClaim(
	_ context.Context,
	task lib.Task,
	_ string,
	_ string,
	_ string,
) error {
	if s.failFirst > 0 {
		s.failFirst--
		return stderrors.New("publish failed")
	}
	s.parked = append(s.parked, task.TaskIdentifier)
	return nil
}

// PublishParkSucceeded records a succeeded-Job park, so a spec can assert both
// that it happened and what it published.
func (s *parkRecordingPublisher) PublishParkSucceeded(
	_ context.Context,
	task lib.Task,
) error {
	if s.failParkSucceeded {
		return stderrors.New("publish failed")
	}
	s.parkedSucceeded = append(s.parkedSucceeded, task)
	return nil
}

// PublishParkRedriveCap records a cap-exhausted park, which must stay open.
func (s *parkRecordingPublisher) PublishParkRedriveCap(
	_ context.Context,
	task lib.Task,
) error {
	if s.failParkSucceeded {
		return stderrors.New("publish failed")
	}
	s.parkedRedriveCap = append(s.parkedRedriveCap, task)
	return nil
}

func (s *parkRecordingPublisher) PublishSpawnNotification(
	context.Context, lib.Task, string, string,
) error {
	return nil
}

func (s *parkRecordingPublisher) PublishFailure(
	context.Context, lib.Task, string, string, *corev1.Pod,
) error {
	return nil
}

func (s *parkRecordingPublisher) PublishStoreMissFailure(
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

func (s *parkRecordingPublisher) PublishTriggerBudgetRefusal(
	context.Context, lib.Task, string, int, int,
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

func (s *parkRecordingPublisher) PublishAbort(
	context.Context, lib.Task, string, int,
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

// ranGateTask builds gateTask plus the evidence that a Job was already created
// for it. A rerun decision applies only to a task that has run, so every spec
// pinning the park branch needs this shape.
func ranGateTask() lib.Task {
	task := gateTask("", "")
	task.Frontmatter["current_job"] = "pr-reviewer-agent-tid-gate-1"
	return task
}

// TestAcquireSpawnClaimParksOnLostClaimWhenRerunDisabled is SC1 for the
// explicit false: a takeover parks the task at human_review and reports false,
// which is the caller's "create no Job" signal.
func TestAcquireSpawnClaimParksOnLostClaimWhenRerunDisabled(t *testing.T) {
	ctx := context.Background()
	h, publisher := parkOnLostClaimHandler()

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, ranGateTask(), parkConfig())
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
		ok = h.acquireSpawnClaim(ctx, ranGateTask(), unsetConfig())
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

// TestAcquireSpawnClaimDoesNotParkATaskThatNeverRan pins the rule that a
// takeover of a task this cluster never ran is not a second run.
//
// rerunOnLostClaim answers "may this agent run a task a previous owner already
// ran". A task with no current_job and no job_started_at carries no evidence of
// any run, so there is nothing to re-run and the guard does not apply: the
// spawn must proceed. Parking it instead writes human_review onto a task that
// no executor ever claimed, and the empty assignee that write leaves makes the
// task permanently unspawnable — measured on a pr-review task parked 40 s after
// creation with no Job, no current_job and no job_started_at.
func TestAcquireSpawnClaimDoesNotParkATaskThatNeverRan(t *testing.T) {
	ctx := context.Background()
	h, publisher := parkOnLostClaimHandler()

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, gateTask("", ""), parkConfig())
	})

	if !ok {
		t.Fatal(
			"expected the spawn to proceed: a task with no current_job and no job_started_at has never run here",
		)
	}
	if len(publisher.parked) != 0 {
		t.Fatalf("park writes = %d, want 0 for a task that never ran", len(publisher.parked))
	}
	if strings.Contains(out, "event=parked_lost_claim") {
		t.Errorf("stderr = %q, want no parked_lost_claim line for a task that never ran", out)
	}
}

// staleJobStartedTask builds the shape every observed lost-claim park carried:
// a job_started_at left behind by a Job that is long gone, and no current_job.
func staleJobStartedTask() lib.Task {
	task := gateTask("", "")
	task.Frontmatter["job_started_at"] = "2026-11-10T22:43:56Z"
	return task
}

// unattributableTakeoverClient models the shape TakeoverPair reports as an empty
// PreviousTaskID: the first ClaimPair read sees a held claim and conflicts, and
// by the time TakeoverPair reads the same key it is free, so the takeover takes
// its create path (pkg/claim/lease_manager.go:827) and displaces nobody.
func unattributableTakeoverClient() *recordingClaimRefClient {
	var keyReads int
	return &recordingClaimRefClient{
		put: func(
			_ context.Context,
			_ string,
			_ string,
			_ string,
			_ string,
			_ libtime.DateTime,
		) (string, error) {
			return "new-sha", nil
		},
		list: func(_ context.Context, prefix string) ([]claim.ClaimRef, error) {
			if prefix != gateClaimKey {
				return nil, nil
			}
			keyReads++
			if keyReads > 1 {
				return nil, nil
			}
			return []claim.ClaimRef{{
				Ref:        "refs/" + gateClaimKey,
				Sha:        "held-sha",
				Cluster:    "other",
				TaskID:     "previous-task",
				LeaseUntil: stdtime.Now().UTC().Add(60 * stdtime.Second).Format(stdtime.RFC3339),
			}}, nil
		},
	}
}

// TestAcquireSpawnClaimDoesNotParkATakeoverWithNoPreviousHolder pins the rule
// that a takeover which displaced nobody is not a lost claim.
//
// TakeoverPair reports an empty PreviousTaskID only on its create path, where no
// listed claim was replaced (pkg/claim/lease_manager.go:827,837) — so there is
// no previous holder to have run the task. The task still carries a
// job_started_at from an earlier dispatch, which is the shape all 9 measured
// parked_lost_claim records had; the guard used to park on that alone, writing
// human_review onto a task whose only claim had just been created here.
func TestAcquireSpawnClaimDoesNotParkATakeoverWithNoPreviousHolder(t *testing.T) {
	ctx := context.Background()
	h := newGateHandler(pkg.ClaimModeOn, unattributableTakeoverClient())
	publisher := &parkRecordingPublisher{}
	h.resultPublisher = publisher

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, staleJobStartedTask(), parkConfig())
	})

	if !ok {
		t.Fatal(
			"expected the spawn to proceed: a takeover that displaced no holder has no prior run to decide about",
		)
	}
	if len(publisher.parked) != 0 {
		t.Fatalf("park writes = %d, want 0 for an unattributable takeover", len(publisher.parked))
	}
	if strings.Contains(out, "event=parked_lost_claim") {
		t.Errorf("stderr = %q, want no parked_lost_claim line for an unattributable takeover", out)
	}
	if !strings.Contains(out, "event=claim_acquired") {
		t.Errorf("stderr = %q, want a claim_acquired line for the takeover create", out)
	}
}

// TestAcquireSpawnClaimParksAnAttributableTakeover is the other arm: a takeover
// that displaced a named holder still parks, so the narrowing does not capture
// the case the guard exists for.
func TestAcquireSpawnClaimParksAnAttributableTakeover(t *testing.T) {
	ctx := context.Background()
	h, publisher := parkOnLostClaimHandler()

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, ranGateTask(), parkConfig())
	})

	if ok {
		t.Fatal("expected no Job when the takeover displaced a named holder")
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
		ok = h.acquireSpawnClaim(ctx, ranGateTask(), parkConfig())
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
		ok = h.acquireSpawnClaim(ctx, ranGateTask(), parkConfig())
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

// TestAcquireSpawnClaimStopsTheParkWriteOnCancellation pins the shutdown path: a
// cancelled pass must stop after the first failed send rather than burn the
// remaining attempts against a dead context and then report a failure that is
// ours, not the broker's.
func TestAcquireSpawnClaimStopsTheParkWriteOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h, publisher := parkOnLostClaimHandler()
	publisher.failFirst = parkWriteAttempts

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, ranGateTask(), parkConfig())
	})

	if ok {
		t.Fatal("expected no Job after a cancelled park attempt")
	}
	if strings.Contains(out, "event=parked_lost_claim_failed") {
		t.Errorf("stderr = %q, want no failed line for a cancelled pass", out)
	}
	if !strings.Contains(out, "event=parked_lost_claim_cancelled") {
		t.Errorf("stderr = %q, want the cancelled line", out)
	}
}
