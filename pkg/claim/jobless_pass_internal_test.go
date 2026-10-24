// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"testing"
	"time"

	libtime "github.com/bborbe/time"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// ctxAwareJobControl counts IsJobRunning calls and answers with the context
// error when the context is already done.
type ctxAwareJobControl struct {
	calls int
}

func (c *ctxAwareJobControl) IsJobRunning(ctx context.Context, _ string) (bool, error) {
	c.calls++
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}

func (c *ctxAwareJobControl) DeleteJob(context.Context, string) error { return nil }

// TestReleaseJoblessClaimsStopsOnCancelledContext pins that a pass whose
// context is already cancelled makes no Job lookup and counts no error.
func TestReleaseJoblessClaimsStopsOnCancelledContext(t *testing.T) {
	clock := &stepClock{now: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)}
	job := &ctxAwareJobControl{}
	m := &leaseManager{
		cluster:         "self",
		jobControl:      job,
		currentDateTime: clock,
		held: map[string]*heldClaim{
			"claims/task/ctx": {
				key:        "claims/task/ctx",
				taskID:     "t-ctx",
				leaseUntil: libtime.DateTime(clock.get().Add(300 * time.Second)),
			},
		},
	}

	before := testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues("error"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m.releaseJoblessClaims(ctx)

	after := testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues("error"))
	if job.calls != 0 {
		t.Fatalf("IsJobRunning calls = %d; want 0 on a cancelled context", job.calls)
	}
	if after-before != 0 {
		t.Fatalf("error delta = %v; want 0 on a cancelled context", after-before)
	}
	if !m.held["claims/task/ctx"].lastJoblessCheckAt.Time().IsZero() {
		t.Fatalf(
			"lastJoblessCheckAt = %v; want zero on a cancelled context",
			m.held["claims/task/ctx"].lastJoblessCheckAt,
		)
	}
}
