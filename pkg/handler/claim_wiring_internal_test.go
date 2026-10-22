// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"strings"
	"testing"

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
