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
	stdtime "time"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

const acquiredLogLine = "event=claim_acquired key=claims/task/tid-gate-1 task=tid-gate-1"

func TestAcquireSpawnClaimLogsAcquiredOnClaim(t *testing.T) {
	ctx := context.Background()
	h := newGateHandler(pkg.ClaimModeOn, &recordingClaimRefClient{})

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, gateTask("", ""))
	})

	if !ok {
		t.Fatal("expected the spawn to proceed after a successful claim")
	}
	if !strings.Contains(out, acquiredLogLine) {
		t.Errorf("stderr = %q, want it to contain %q", out, acquiredLogLine)
	}
}

func TestAcquireSpawnClaimLogsAcquiredOnTakeover(t *testing.T) {
	ctx := context.Background()
	client := takeoverClient(takeoverClaimRef(
		"expired-sha",
		stdtime.Now().UTC().Add(-31*stdtime.Second),
	))
	h := newGateHandler(pkg.ClaimModeOn, client)

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, gateTask("", ""))
	})

	if !ok {
		t.Fatal("expected the spawn to proceed after an expired-lease takeover")
	}
	if !strings.Contains(out, acquiredLogLine) {
		t.Errorf("stderr = %q, want it to contain %q", out, acquiredLogLine)
	}
}

func TestAcquireSpawnClaimDoesNotLogAcquiredWhenBothFail(t *testing.T) {
	ctx := context.Background()
	client := takeoverClient(takeoverClaimRef(
		"live-sha",
		stdtime.Now().UTC().Add(5*stdtime.Minute),
	))
	h := newGateHandler(pkg.ClaimModeOn, client)

	var ok bool
	out := captureGlogStderr(t, func() {
		ok = h.acquireSpawnClaim(ctx, gateTask("", ""))
	})

	if ok {
		t.Fatal("expected a live lease to block the spawn in claim mode on")
	}
	if strings.Contains(out, "event=claim_acquired") {
		t.Errorf("stderr = %q, want no claim_acquired line", out)
	}
}
