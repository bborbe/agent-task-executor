// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"
	"fmt"
	"testing"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// terminalAnswerClient answers every Acquire with the given error, so a test can
// assert how the handler classifies a claim the service will never grant.
type terminalAnswerClient struct {
	acquireErr error
}

func (c *terminalAnswerClient) Acquire(
	context.Context,
	claim.ClaimTarget,
	string,
	string,
	int,
) (*claim.Claim, error) {
	return nil, c.acquireErr
}

func (c *terminalAnswerClient) Renew(
	context.Context,
	claim.ClaimTarget,
	string,
	string,
	string,
	int,
) (*claim.Claim, error) {
	return nil, fmt.Errorf("renew must not be called")
}

func (c *terminalAnswerClient) List(context.Context, string) ([]claim.Claim, error) {
	return nil, nil
}

func (c *terminalAnswerClient) Release(context.Context, claim.ClaimTarget, string) error {
	return nil
}

func (c *terminalAnswerClient) Read(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
	return nil, claim.ErrClaimFree
}

// newTerminalAnswerHandler builds a handler whose claim client answers acquire
// with err, in claim mode on, so a refused claim blocks the spawn.
func newTerminalAnswerHandler(err error) *taskEventHandler {
	return &taskEventHandler{
		claimMode:             pkg.ClaimModeOn,
		cluster:               "self",
		runUntagged:           true,
		currentDateTime:       libtime.NewCurrentDateTime(),
		claimJobControl:       noopJobControl{},
		refusedGates:          make(map[lib.TaskIdentifier]string),
		unclaimable:           make(map[lib.TaskIdentifier]struct{}),
		deferredRespawns:      make(map[lib.TaskIdentifier]deferredEntry),
		deferredEpochs:        make(map[lib.TaskIdentifier]uint64),
		claimClientFactory:    func() claim.ClaimClient { return &terminalAnswerClient{acquireErr: err} },
		claimRefClientFactory: func() claim.ClaimRefClient { return &recordingClaimRefClient{} },
	}
}

// TestIsTerminalClaimAnswer pins the classification the fix rests on: only a 404
// is final. A 5xx is transient, and a 401/403 is a credential fault that must not
// permanently stop live work — both stay retryable.
func TestIsTerminalClaimAnswer(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"404", &claim.StatusError{Action: "acquire", Target: "task tid", Status: 404}, true},
		{"500", &claim.StatusError{Action: "acquire", Target: "task tid", Status: 500}, false},
		{"401", &claim.StatusError{Action: "acquire", Target: "task tid", Status: 401}, false},
		{"403", &claim.StatusError{Action: "acquire", Target: "task tid", Status: 403}, false},
		{"untyped", stderrors.New("boom"), false},
		{
			"wrapped 404",
			fmt.Errorf("claim pair: %w", &claim.StatusError{
				Action: "acquire", Target: "task tid", Status: 404,
			}),
			true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTerminalClaimAnswer(tc.err); got != tc.want {
				t.Errorf("isTerminalClaimAnswer(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestAcquireSpawnClaimTerminal404MarksUnclaimable is the regression test for the
// unbounded claim stream: a claim the service answers 404 must be remembered, so
// the reconcile floor stops candidating the id. Before the fix the spawn was
// refused and nothing was recorded, so the next pass asked again — measured
// 2026-11-16 at 507 attempts over 506 min for one aborted task.
func TestAcquireSpawnClaimTerminal404MarksUnclaimable(t *testing.T) {
	h := newTerminalAnswerHandler(&claim.StatusError{
		Action: "acquire", Target: "task tid-gate-1", Status: 404,
	})

	if h.acquireSpawnClaim(context.Background(), gateTask("", ""), redispatchConfig()) {
		t.Fatal("expected the spawn to be blocked on a terminal claim answer")
	}
	if !h.isUnclaimable("tid-gate-1") {
		t.Fatal("expected the 404 id to be recorded unclaimable")
	}
}

// TestAcquireSpawnClaimServerErrorStaysRetryable guards the scope of the fix: a
// 5xx is transient and must not poison the id.
func TestAcquireSpawnClaimServerErrorStaysRetryable(t *testing.T) {
	h := newTerminalAnswerHandler(&claim.StatusError{
		Action: "acquire", Target: "task tid-gate-1", Status: 500,
	})

	if h.acquireSpawnClaim(context.Background(), gateTask("", ""), redispatchConfig()) {
		t.Fatal("expected the spawn to be blocked in claim mode on")
	}
	if h.isUnclaimable("tid-gate-1") {
		t.Fatal("a 500 must not mark the task unclaimable")
	}
}

// TestReconcileRefusesUnclaimableTask is the other half of the regression: the
// gate must sit above every frontmatter check, because a stale view carries a
// terminal record as in_progress/execution and those checks all read the view.
func TestReconcileRefusesUnclaimableTask(t *testing.T) {
	h := newTerminalAnswerHandler(nil)
	h.recordUnclaimable("tid-gate-1")

	task := lib.Task{
		TaskIdentifier: lib.TaskIdentifier("tid-gate-1"),
		Frontmatter: lib.TaskFrontmatter{
			"cluster":   "self",
			"status":    "in_progress",
			"phase":     "execution",
			"assignee":  "egress-probe",
			"task_type": "probe",
		},
	}

	_, _, ok, err := h.reconcileTask(context.Background(), task)
	if err != nil {
		t.Fatalf("reconcileTask returned an error: %v", err)
	}
	if ok {
		t.Fatal("expected an unclaimable task to be refused by the reconcile floor")
	}
}
