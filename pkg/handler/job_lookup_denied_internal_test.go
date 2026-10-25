// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	stderrors "errors"
	"strings"
	"testing"

	libk8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// deniedHandler returns a handler with only the fields the denial path reads.
// It is built directly rather than through NewTaskEventHandler on purpose: that
// is the shape a future internal test would use, and the path must not panic on
// it (see the nil-map guard).
func deniedHandler() *taskEventHandler {
	return &taskEventHandler{currentDateTime: libtime.NewCurrentDateTime()}
}

func deniedErr(assignee string) *jobLookupDeniedError {
	return &jobLookupDeniedError{
		err:       stderrors.New("token expired"),
		assignee:  assignee,
		namespace: libk8s.Namespace("dev"),
	}
}

// SC2's log arm, positively: a line must actually reach stderr, not merely a
// throttle entry. A regression that records the timestamp and then skips the
// glog call passes every map assertion above and emits nothing, which is the
// silent failure the criterion exists to rule out.
func TestLogJobLookupDeniedEmitsALine(t *testing.T) {
	h := deniedHandler()

	out := captureGlogStderr(t, func() {
		h.logJobLookupDenied(deniedErr("claude"))
	})

	if !strings.Contains(out, "job_lookup_denied") {
		t.Fatalf("no job_lookup_denied line emitted; stderr was: %q", out)
	}
	if !strings.Contains(out, "assignee=claude") {
		t.Fatalf("line does not name the assignee; stderr was: %q", out)
	}
}

// And the negative half of the same arm: a throttled call must emit nothing.
func TestLogJobLookupDeniedThrottledCallEmitsNothing(t *testing.T) {
	h := deniedHandler()
	h.logJobLookupDenied(deniedErr("claude"))

	out := captureGlogStderr(t, func() {
		h.logJobLookupDenied(deniedErr("claude"))
	})

	if strings.Contains(out, "job_lookup_denied") {
		t.Fatalf("a throttled call still logged; stderr was: %q", out)
	}
}

// SC2's log arm: three denials for one assignee/namespace inside the window must
// leave exactly one throttle entry, unmoved by the later calls. The counter is
// deliberately not involved here — it is unthrottled and asserted separately.
func TestLogJobLookupDeniedThrottlesToOnePerPairPerInterval(t *testing.T) {
	h := deniedHandler()
	denied := deniedErr("claude")

	h.logJobLookupDenied(denied)
	first := h.jobLookupDeniedLoggedAt["claude/dev"]

	h.logJobLookupDenied(denied)
	h.logJobLookupDenied(denied)

	if got := len(h.jobLookupDeniedLoggedAt); got != 1 {
		t.Fatalf("throttle entries = %d, want 1", got)
	}
	if got := h.jobLookupDeniedLoggedAt["claude/dev"]; !got.Equal(first) {
		t.Fatalf("timestamp moved on a throttled call: %v then %v", first, got)
	}
}

// The window re-opens: an entry older than jobLookupDeniedInterval is replaced,
// so a standing denial still produces one line per interval rather than one ever.
func TestLogJobLookupDeniedReopensAfterTheInterval(t *testing.T) {
	h := deniedHandler()
	h.logJobLookupDenied(deniedErr("claude"))

	stale := h.currentDateTime.Now().Time().Add(-2 * jobLookupDeniedInterval)
	h.jobLookupDeniedLoggedAt["claude/dev"] = stale

	h.logJobLookupDenied(deniedErr("claude"))

	if got := h.jobLookupDeniedLoggedAt["claude/dev"]; !got.After(stale) {
		t.Fatalf("timestamp did not advance past the interval: still %v", got)
	}
}

// Pairs are throttled independently: one assignee's denial must not silence
// another's, or a fleet-wide credential break would log once total.
func TestLogJobLookupDeniedThrottlesPerPair(t *testing.T) {
	h := deniedHandler()
	h.logJobLookupDenied(deniedErr("claude"))
	h.logJobLookupDenied(deniedErr("trading"))

	if got := len(h.jobLookupDeniedLoggedAt); got != 2 {
		t.Fatalf("throttle entries = %d, want 2 (one per assignee)", got)
	}
}

// The map is initialised lazily, matching bumpSpawnGen: a handler built directly
// in a test carries a nil map, and a panic here would take down the very path
// whose job is to keep the executor alive.
func TestLogJobLookupDeniedSurvivesNilMap(t *testing.T) {
	h := &taskEventHandler{currentDateTime: libtime.NewCurrentDateTime()}
	if h.jobLookupDeniedLoggedAt != nil {
		t.Fatal("precondition: map should start nil")
	}
	h.logJobLookupDenied(deniedErr("claude"))
	if got := len(h.jobLookupDeniedLoggedAt); got != 1 {
		t.Fatalf("throttle entries = %d, want 1", got)
	}
}

// SC2's counter arm needs a series that exists before the first denial, or
// `increase(...[5m]) > 0` is indistinguishable from a metric nobody publishes.
// PreInitJobLookupDenied is that seed, so it is asserted on a fresh pair.
func TestPreInitJobLookupDeniedSeedsAZeroSeries(t *testing.T) {
	const assignee = "probe-seed-assignee"
	const namespace = "probe-seed-namespace"

	before := testutil.CollectAndCount(metrics.JobLookupDeniedTotal)
	metrics.PreInitJobLookupDenied(assignee, namespace)
	after := testutil.CollectAndCount(metrics.JobLookupDeniedTotal)

	if after != before+1 {
		t.Fatalf("series count = %d, want %d (seed must add exactly one)", after, before+1)
	}
	if got := testutil.ToFloat64(
		metrics.JobLookupDeniedTotal.WithLabelValues(assignee, namespace),
	); got != 0 {
		t.Fatalf("seeded series = %v, want 0", got)
	}

	// Idempotent: a second read of the same pair must not add another series.
	metrics.PreInitJobLookupDenied(assignee, namespace)
	if got := testutil.CollectAndCount(metrics.JobLookupDeniedTotal); got != after {
		t.Fatalf("series count after re-seed = %d, want %d", got, after)
	}
}

// A denial is counted once per suppressed read, not once per pass: three reads
// must move the counter by three. This is the assertion a once-per-pass
// increment fails.
func TestHandleJobLookupDeniedCountsOncePerRead(t *testing.T) {
	h := deniedHandler()
	before := testutil.ToFloat64(
		metrics.JobLookupDeniedTotal.WithLabelValues("counter-probe", "dev"),
	)

	for i := 0; i < 3; i++ {
		if !h.handleJobLookupDenied(
			&jobLookupDeniedError{
				assignee:  "counter-probe",
				namespace: libk8s.Namespace("dev"),
			},
		) {
			t.Fatal("handleJobLookupDenied did not recognise a denial")
		}
	}

	after := testutil.ToFloat64(
		metrics.JobLookupDeniedTotal.WithLabelValues("counter-probe", "dev"),
	)
	if after-before != 3 {
		t.Fatalf("counter delta = %v, want 3", after-before)
	}
}

// A non-denial must not be counted as one — the classification is the whole
// point, and a handler that counted every error here would make the metric
// meaningless as an identity signal.
func TestHandleJobLookupDeniedIgnoresOtherErrors(t *testing.T) {
	h := deniedHandler()
	before := testutil.ToFloat64(
		metrics.JobLookupDeniedTotal.WithLabelValues("counter-probe", "dev"),
	)

	if h.handleJobLookupDenied(stderrors.New("not an identity refusal")) {
		t.Fatal("handleJobLookupDenied claimed a plain error was a denial")
	}

	after := testutil.ToFloat64(
		metrics.JobLookupDeniedTotal.WithLabelValues("counter-probe", "dev"),
	)
	if after != before {
		t.Fatalf("counter moved on a non-denial: %v then %v", before, after)
	}
}
