// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	stderrors "errors"
	"flag"
	"io"
	"os"
	"strings"
	"testing"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/claim/claimtest"
)

// captureInternalStderr runs fn with glog's stderr redirected into a pipe and
// returns everything written. The internal specs run serially.
func captureInternalStderr(fn func()) string {
	_ = flag.Set("logtostderr", "true")
	defer func() { _ = flag.Set("logtostderr", "false") }()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		panic(err)
	}
	os.Stderr = writer
	fn()
	glog.Flush()
	os.Stderr = original
	_ = writer.Close()
	out, _ := io.ReadAll(reader)
	_ = reader.Close()
	return string(out)
}

// stubRedriveReasonStore answers Reasons with a fixed slice and ignores the
// task and phase: these tests classify one record at a time.
type stubRedriveReasonStore struct {
	reasons []string
}

func (s *stubRedriveReasonStore) Record(context.Context, string, string, string) error {
	return nil
}

func (s *stubRedriveReasonStore) Reasons(context.Context, string, string) ([]string, error) {
	return s.reasons, nil
}

func (s *stubRedriveReasonStore) Snapshot(
	context.Context,
) (map[string]map[string][]string, error) {
	return nil, nil
}

func (s *stubRedriveReasonStore) Clear(context.Context, string) error { return nil }

// abortCall records one PublishAbort invocation.
type abortCall struct {
	taskID lib.TaskIdentifier
	reason string
	streak int
}

// abortRecordingPublisher embeds stubResultPublisher for every method it does
// not care about and records the aborts.
type abortRecordingPublisher struct {
	stubResultPublisher
	aborts []abortCall
}

func (p *abortRecordingPublisher) PublishAbort(
	_ context.Context,
	task lib.Task,
	reason string,
	streak int,
) error {
	p.aborts = append(p.aborts, abortCall{
		taskID: task.TaskIdentifier,
		reason: reason,
		streak: streak,
	})
	return nil
}

// newDecisionHandler builds a handler whose streak and reason stores answer the
// given values. The leaseManager warm-up is mandatory: createLeaseManager
// overwrites both fields with real stores on its first call.
func newDecisionHandler(
	t *testing.T,
	count int,
	reasons []string,
	publisher pkg.ResultPublisher,
) (*taskEventHandler, *stubJobSpawner) {
	t.Helper()
	client := &countingClaimRefClient{}
	spawner := &stubJobSpawner{}
	handler := NewTaskEventHandler(
		spawner,
		"dev",
		"prod",
		"self",
		true,
		true,
		nil,
		publisher,
		pkg.NewTaskStore(),
		libtime.NewCurrentDateTime(),
		&fakeTaskView{},
		"v",
		DefaultReconcilePassTimeout,
		DefaultReconcileInterval,
		pkg.ClaimModeShadow,
		func() claim.ClaimRefClient { return client },
		WithClaimClientFactory(func() claim.ClaimClient {
			return claimtest.NewClient(client, libtime.NewCurrentDateTime().Now)
		}),
		WithClaimJobControl(noopJobControl{}),
	)
	h, ok := handler.(*taskEventHandler)
	if !ok {
		t.Fatalf("expected *taskEventHandler, got %T", handler)
	}
	if _, err := h.leaseManager(context.Background()); err != nil {
		t.Fatalf("lease manager: %v", err)
	}
	h.redriveStore = &stubRedriveStreakStore{count: count}
	h.redriveReasons = &stubRedriveReasonStore{reasons: reasons}
	return h, spawner
}

// driveCapped drives the standard eligible task through the funnel.
func driveCapped(h *taskEventHandler) reconcileOutcome {
	task := cappedTestTask()
	return h.driveCandidate(context.Background(), reconcileCandidate{
		task:   task,
		config: &pkg.AgentConfiguration{Assignee: "claude", Image: "img"},
		claim:  deferredClaim{id: task.TaskIdentifier},
	})
}

// TestClassifyRedriveReasons pins every rule and their order. The rules are
// total: each input falls into exactly one class.
func TestClassifyRedriveReasons(t *testing.T) {
	cases := []struct {
		name    string
		reasons []string
		count   int
		want    string
	}{
		{
			name:    "the same broken reason three times is broken",
			reasons: []string{"pod_oom_killed", "pod_oom_killed", "pod_oom_killed"},
			count:   3,
			want:    redriveClassBroken,
		},
		{
			name: "type_mismatch is in the broken set even though no path stores it",
			reasons: []string{
				"type_mismatch", "type_mismatch", "type_mismatch",
			},
			count: 3,
			want:  redriveClassBroken,
		},
		{
			name: "three transient reasons are transient",
			reasons: []string{
				"deadline_exceeded", "deadline_exceeded", "deadline_exceeded",
			},
			count: 3,
			want:  redriveClassTransient,
		},
		{
			name:    "two different reasons are transient even when one is broken",
			reasons: []string{"pod_oom_killed", "pod_crash_no_stdout", "pod_oom_killed"},
			count:   3,
			want:    redriveClassTransient,
		},
		{
			name:    "no reasons at all is unknown",
			reasons: nil,
			count:   3,
			want:    redriveClassUnknown,
		},
		{
			name:    "three unknown literals are unknown",
			reasons: []string{"unknown", "unknown", "unknown"},
			count:   3,
			want:    redriveClassUnknown,
		},
		{
			name: "only the most recent count reasons are classified",
			reasons: []string{
				"pod_oom_killed", "pod_oom_killed", "pod_oom_killed",
				"pod_oom_killed", "pod_oom_killed", "pod_oom_killed",
			},
			count: 3,
			want:  redriveClassBroken,
		},
		{
			// Pins the truncation DIRECTION: six identical values above pass even if
			// the slice were reasons[:window]. Here the oldest reason differs, so
			// only taking the most recent three can classify broken.
			name: "a differing older reason is excluded from the window",
			reasons: []string{
				"deadline_exceeded", "pod_oom_killed", "pod_oom_killed", "pod_oom_killed",
			},
			count: 3,
			want:  redriveClassBroken,
		},
		{
			name:    "a streak below one is unknown",
			reasons: []string{"pod_oom_killed"},
			count:   0,
			want:    redriveClassUnknown,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyRedriveReasons(tc.reasons, tc.count); got != tc.want {
				t.Fatalf("classify(%v, %d) = %q; want %q", tc.reasons, tc.count, got, tc.want)
			}
		})
	}
}

// TestClassifyRedriveReasonsDriftedStreak pins the drift rule: the streak counts
// every successful spawn while a reason is recorded only from PublishFailure, so
// the streak can legitimately exceed the reason count. The classifier must read
// the failures it has rather than refuse — a refusal strands the record, because
// a record that cannot spawn can never earn the missing reason.
func TestClassifyRedriveReasonsDriftedStreak(t *testing.T) {
	cases := []struct {
		name    string
		reasons []string
		count   int
		want    string
	}{
		{
			name:    "one broken reason against a streak of three is transient, not broken",
			reasons: []string{"pod_oom_killed"},
			count:   3,
			want:    redriveClassTransient,
		},
		{
			name:    "one transient reason against a streak of three is transient",
			reasons: []string{"deadline_exceeded"},
			count:   3,
			want:    redriveClassTransient,
		},
		{
			// The all-unknown guard must survive the drift: a window shorter than the
			// streak is transient, but a window that is entirely the literal
			// "unknown" still has no evidence and must stay unknown.
			name:    "one unknown literal against a streak of four is still unknown",
			reasons: []string{"unknown"},
			count:   4,
			want:    redriveClassUnknown,
		},
		{
			// The regression: a streak one ahead of the reason set is normal and
			// must still close a broken record, not strand it as unknown for ever.
			name: "three broken reasons against a streak of four are broken",
			reasons: []string{
				"pod_oom_killed", "pod_oom_killed", "pod_oom_killed",
			},
			count: 4,
			want:  redriveClassBroken,
		},
		{
			name:    "two broken reasons against a streak of four are transient, not broken",
			reasons: []string{"pod_oom_killed", "pod_oom_killed"},
			count:   4,
			want:    redriveClassTransient,
		},
		{
			// Observed live on dev 2026-10-26: streak=4 cap=3 with these three
			// reasons. Differing reasons are transient, so the record earns the
			// bounded extension instead of being refused and abandoned.
			name: "the observed mixed set against a streak of four is transient",
			reasons: []string{
				"deadline_exceeded", "deadline_exceeded", "pod_crash_no_stdout",
			},
			count: 4,
			want:  redriveClassTransient,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyRedriveReasons(tc.reasons, tc.count); got != tc.want {
				t.Fatalf("classify(%v, %d) = %q; want %q", tc.reasons, tc.count, got, tc.want)
			}
		})
	}
}

// TestTransientVocabularyIsTransient pins the one invariant that still matters
// about the transient set now that the classifier no longer branches on it:
// nothing it names may be classified broken, i.e. the two vocabularies stay
// disjoint. Since transient is the default return, a value added to both maps is
// the only way this can fail — and that is exactly the mistake worth catching,
// because it would silently turn a retry into a close.
func TestTransientVocabularyIsTransient(t *testing.T) {
	for reason := range transientRedriveReasons {
		if got := classifyRedriveReasons([]string{reason, reason, reason}, 3); got != redriveClassTransient {
			t.Errorf("classify(%q x3, 3) = %q; want %q", reason, got, redriveClassTransient)
		}
	}
}

// TestBrokenRedriveClosesAndRefuses pins the broken decision: the spawn is
// refused and exactly one abort carries the broken-signature reason.
func TestBrokenRedriveClosesAndRefuses(t *testing.T) {
	publisher := &abortRecordingPublisher{}
	h, spawner := newDecisionHandler(t, 3, []string{
		"pod_oom_killed", "pod_oom_killed", "pod_oom_killed",
	}, publisher)

	outcome := driveCapped(h)

	if outcome != reconcileOutcomeCapped {
		t.Fatalf("outcome = %v; want capped", outcome)
	}
	if spawner.spawned != 0 {
		t.Fatalf("spawned = %d; want 0", spawner.spawned)
	}
	if len(publisher.aborts) != 1 {
		t.Fatalf("aborts = %d; want 1", len(publisher.aborts))
	}
	if publisher.aborts[0].reason != pkg.AbortReasonBrokenSignature {
		t.Fatalf("reason = %q; want %q",
			publisher.aborts[0].reason, pkg.AbortReasonBrokenSignature)
	}
	if publisher.aborts[0].streak != 3 {
		t.Fatalf("streak = %d; want 3", publisher.aborts[0].streak)
	}
}

// TestTransientRedriveBelowBoundSpawns pins the extension: a transient record
// below maxConsecutiveRedrives+transientRedriveBudget is allowed to spawn. The
// assertion is on the spawn itself, not on the absence of an error.
func TestTransientRedriveBelowBoundSpawns(t *testing.T) {
	publisher := &abortRecordingPublisher{}
	h, spawner := newDecisionHandler(t, 3, []string{
		"deadline_exceeded", "deadline_exceeded", "deadline_exceeded",
	}, publisher)

	outcome := driveCapped(h)

	if outcome != reconcileOutcomeRedriven {
		t.Fatalf("outcome = %v; want redriven", outcome)
	}
	if spawner.spawned != 1 {
		t.Fatalf("spawned = %d; want 1", spawner.spawned)
	}
	if len(publisher.aborts) != 0 {
		t.Fatalf("aborts = %d; want 0", len(publisher.aborts))
	}
}

// TestTransientRedriveAtBoundClosesAndRefuses pins the other end of the
// extension: reaching the bound closes the record instead of extending again.
func TestTransientRedriveAtBoundClosesAndRefuses(t *testing.T) {
	reasons := make([]string, 0, maxConsecutiveRedrives+transientRedriveBudget)
	for range maxConsecutiveRedrives + transientRedriveBudget {
		reasons = append(reasons, "deadline_exceeded")
	}
	publisher := &abortRecordingPublisher{}
	h, spawner := newDecisionHandler(
		t,
		maxConsecutiveRedrives+transientRedriveBudget,
		reasons,
		publisher,
	)

	outcome := driveCapped(h)

	if outcome != reconcileOutcomeCapped {
		t.Fatalf("outcome = %v; want capped", outcome)
	}
	if spawner.spawned != 0 {
		t.Fatalf("spawned = %d; want 0", spawner.spawned)
	}
	if len(publisher.aborts) != 1 {
		t.Fatalf("aborts = %d; want 1", len(publisher.aborts))
	}
	if publisher.aborts[0].reason != pkg.AbortReasonTransientExhausted {
		t.Fatalf("reason = %q; want %q",
			publisher.aborts[0].reason, pkg.AbortReasonTransientExhausted)
	}
}

// TestUnknownRedriveRefusesAndClosesNothing pins the safe direction: no
// evidence refuses the spawn but never closes the record.
func TestUnknownRedriveRefusesAndClosesNothing(t *testing.T) {
	publisher := &abortRecordingPublisher{}
	h, spawner := newDecisionHandler(t, 3, nil, publisher)

	outcome := driveCapped(h)

	if outcome != reconcileOutcomeCapped {
		t.Fatalf("outcome = %v; want capped", outcome)
	}
	if spawner.spawned != 0 {
		t.Fatalf("spawned = %d; want 0", spawner.spawned)
	}
	if len(publisher.aborts) != 0 {
		t.Fatalf("aborts = %d; want 0", len(publisher.aborts))
	}
}

// TestPartialEvidenceRetriesAndClosesNothing pins the drift rule: a broken reason
// against a longer streak is transient, not broken. The streak counts every spawn
// while a reason is recorded only from PublishFailure, so a short count is normal
// rather than evidence of a repeated signature. It must never close a record as
// broken — and it must not refuse the spawn either, because a record that cannot
// spawn can never earn the missing reason and is then stranded at the cap for
// ever. It earns the bounded extension instead.
func TestPartialEvidenceRetriesAndClosesNothing(t *testing.T) {
	publisher := &abortRecordingPublisher{}
	h, spawner := newDecisionHandler(t, 3, []string{"pod_oom_killed"}, publisher)

	outcome := driveCapped(h)

	if outcome != reconcileOutcomeRedriven {
		t.Fatalf(
			"outcome = %v; want redriven — partial evidence must not refuse the spawn",
			outcome,
		)
	}
	if spawner.spawned != 1 {
		t.Fatalf("spawned = %d; want 1", spawner.spawned)
	}
	if len(publisher.aborts) != 0 {
		t.Fatalf(
			"aborts = %d; want 0 — partial evidence must never close a record",
			len(publisher.aborts),
		)
	}
}

// failingAbortPublisher reports every abort as failed, so the decision path's
// swallow-and-log branch is exercised.
type failingAbortPublisher struct {
	stubResultPublisher
}

func (p *failingAbortPublisher) PublishAbort(
	context.Context, lib.Task, string, int,
) error {
	return stderrors.New("publish failed")
}

// erroringReasonStore answers Reasons with an error, modelling an unreadable
// store. Everything else is a no-op.
type erroringReasonStore struct {
	clearErr error
}

func (s *erroringReasonStore) Record(context.Context, string, string, string) error {
	return nil
}

func (s *erroringReasonStore) Reasons(context.Context, string, string) ([]string, error) {
	return nil, stderrors.New("reason store down")
}

func (s *erroringReasonStore) Snapshot(
	context.Context,
) (map[string]map[string][]string, error) {
	return nil, nil
}

func (s *erroringReasonStore) Clear(context.Context, string) error { return s.clearErr }

// TestAbortFailureIsLoggedAndStillRefuses pins that a failed close-out write is
// swallowed: the spawn is refused either way and the failure is visible.
func TestAbortFailureIsLoggedAndStillRefuses(t *testing.T) {
	h, spawner := newDecisionHandler(t, 3, []string{
		"pod_oom_killed", "pod_oom_killed", "pod_oom_killed",
	}, &failingAbortPublisher{})

	var outcome reconcileOutcome
	out := captureInternalStderr(func() { outcome = driveCapped(h) })

	if outcome != reconcileOutcomeCapped {
		t.Fatalf("outcome = %v; want capped", outcome)
	}
	if spawner.spawned != 0 {
		t.Fatalf("spawned = %d; want 0", spawner.spawned)
	}
	if !strings.Contains(out, "event=redrive_abort_failed") {
		t.Fatalf("expected a redrive_abort_failed log line, got %q", out)
	}
}

// TestReasonReadFailureRefusesAndClosesNothing pins the failed-read branch: it
// is logged, treated as no evidence, and closes nothing.
func TestReasonReadFailureRefusesAndClosesNothing(t *testing.T) {
	publisher := &abortRecordingPublisher{}
	h, spawner := newDecisionHandler(t, 3, nil, publisher)
	h.redriveReasons = &erroringReasonStore{}

	var outcome reconcileOutcome
	out := captureInternalStderr(func() { outcome = driveCapped(h) })

	if outcome != reconcileOutcomeCapped {
		t.Fatalf("outcome = %v; want capped", outcome)
	}
	if spawner.spawned != 0 {
		t.Fatalf("spawned = %d; want 0", spawner.spawned)
	}
	if len(publisher.aborts) != 0 {
		t.Fatalf("aborts = %d; want 0", len(publisher.aborts))
	}
	if !strings.Contains(out, "event=redrive_reason_read_failed") {
		t.Fatalf("expected a redrive_reason_read_failed log line, got %q", out)
	}
}

// erroringStreakStore answers Count with an error and Clear with a fixed error,
// so recordRedriveSpawn's read and clear failures are both reachable.
type erroringStreakStore struct {
	clearErr error
}

func (s *erroringStreakStore) Count(context.Context, string, string) (int, error) {
	return 0, stderrors.New("streak store down")
}

func (s *erroringStreakStore) Snapshot(
	context.Context,
) (map[string]map[string]int, error) {
	return nil, nil
}

func (s *erroringStreakStore) Record(context.Context, string, string) error { return nil }

func (s *erroringStreakStore) Clear(context.Context, string) error { return s.clearErr }

// The reason-clear and the streak read both moved off the spawn path with the
// streak write itself: they now run inside resultPublisher.recordRedriveFailure,
// and their coverage lives in pkg/result_publisher_test.go. recordRedriveSpawn
// touches only the in-memory floor, so there is nothing left here to fail.

// The clearReasonsForNewStreak specs moved to pkg/result_publisher_test.go with
// the function itself: the reason-clear now runs on the failure path, inside
// recordRedriveFailure, because the streak counts published failures.

// TestClearRedriveStreakSwallowsStoreErrors pins that a terminal clear logs each
// failed store and never blocks the terminal handling.
func TestClearRedriveStreakSwallowsStoreErrors(t *testing.T) {
	h, _ := newDecisionHandler(t, 0, nil, &abortRecordingPublisher{})
	h.redriveStore = &erroringStreakStore{clearErr: stderrors.New("clear failed")}
	h.redriveReasons = &erroringReasonStore{clearErr: stderrors.New("clear failed")}

	out := captureInternalStderr(func() {
		h.clearRedriveStreak(context.Background(), lib.TaskIdentifier("task-capped"))
	})

	if !strings.Contains(out, "event=redrive_streak_write_failed") {
		t.Fatalf("expected a redrive_streak_write_failed log line, got %q", out)
	}
	if !strings.Contains(out, "event=redrive_reason_write_failed") {
		t.Fatalf("expected a redrive_reason_write_failed log line, got %q", out)
	}
}
