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

func TestLeaseManagerRefusesClaimModeOff(t *testing.T) {
	ctx := context.Background()
	h := &taskEventHandler{
		claimMode: pkg.ClaimModeOff,
		claimRefClientFactory: func() claim.ClaimRefClient {
			panic("factory must not be called in claim mode off")
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

func TestLeaseManagerConstructsOnce(t *testing.T) {
	ctx := context.Background()
	calls := 0
	h := &taskEventHandler{
		claimMode: pkg.ClaimModeShadow,
		cluster:   "self",
		claimRefClientFactory: func() claim.ClaimRefClient {
			calls++
			return nil
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

func TestLeaseManagerRejectsEmptyCluster(t *testing.T) {
	ctx := context.Background()
	h := &taskEventHandler{
		claimMode: pkg.ClaimModeShadow,
		cluster:   "",
		claimRefClientFactory: func() claim.ClaimRefClient {
			return nil
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

func TestUnmanagedJobControlFailsLoud(t *testing.T) {
	ctx := context.Background()
	var control claim.JobControl = unmanagedJobControl{}

	running, err := control.IsJobRunning(ctx, "task-1")
	if err == nil {
		t.Fatal("expected IsJobRunning to fail")
	}
	if running {
		t.Fatal("expected IsJobRunning to report false")
	}
	if !strings.Contains(err.Error(), "not wired") {
		t.Fatalf("expected not-wired error, got %q", err.Error())
	}

	if err := control.DeleteJob(ctx, "task-1"); err == nil {
		t.Fatal("expected DeleteJob to fail")
	} else if !strings.Contains(err.Error(), "not wired") {
		t.Fatalf("expected not-wired error, got %q", err.Error())
	}
}
