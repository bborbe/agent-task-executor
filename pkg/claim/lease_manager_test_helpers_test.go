// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// claimResults is the closed result set of executor_claim_total.
var claimResults = []string{
	"claimed", "conflict", "renewed", "recovered", "renew_failed",
	"claim_lost", "fenced", "taken_over", "takeover_too_early",
	"released", "already_released", "adopted", "error",
}

// leaseTestT0 is far enough in the future that a fake-clock deadline converted
// to a real context deadline is still in the future for the HTTP client.
var leaseTestT0 = func() libtime.DateTime {
	parsed, err := time.Parse(time.RFC3339, "2099-01-01T00:00:00Z")
	if err != nil {
		panic(err)
	}
	return libtime.DateTime(parsed)
}()

// snapshotClaims reads every series of executor_claim_total.
func snapshotClaims() map[string]float64 {
	snapshot := make(map[string]float64, len(claimResults))
	for _, name := range claimResults {
		snapshot[name] = testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues(name))
	}
	return snapshot
}

// expectClaimDelta asserts the delta for every result series; unlisted series
// must not change.
func expectClaimDelta(before, after map[string]float64, expected map[string]float64) {
	for _, name := range claimResults {
		Expect(after[name]-before[name]).To(
			Equal(expected[name]),
			"executor_claim_total{result=%q} delta",
			name,
		)
	}
}

// newTestClock returns a settable fake clock pinned at t0.
func newTestClock(t0 libtime.DateTime) libtime.CurrentDateTime {
	clock := libtime.NewCurrentDateTime()
	clock.SetNow(t0)
	return clock
}

// advance moves the fake clock by d.
func advance(clock libtime.CurrentDateTime, d libtime.Duration) {
	clock.SetNow(clock.Now().Add(d))
}

// tickAt ticks the manager in TickInterval steps until the clock reaches
// target. It never sleeps.
func tickAt(mgr claim.LeaseManager, clock libtime.CurrentDateTime, target libtime.DateTime) {
	for clock.Now().Before(target) {
		next := clock.Now().Add(claim.TickInterval)
		if next.After(target) {
			next = target
		}
		clock.SetNow(next)
		Expect(mgr.Tick(context.Background())).To(Succeed())
	}
}

// countSuccessfulPuts counts PUT requests the fake answered 200.
func countSuccessfulPuts(requests []fakeRequest) int {
	count := 0
	for _, request := range requests {
		if request.Method == "PUT" && request.Status == 200 {
			count++
		}
	}
	return count
}

// putExpectedSha returns the expected_sha of a recorded PUT request.
func putExpectedSha(request fakeRequest) string {
	var body fakePutBody
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return ""
	}
	return body.ExpectedSha
}

// rfc3339 renders a lease deadline exactly as the ref client does.
func rfc3339(at libtime.DateTime) string {
	return at.Time().UTC().Format(time.RFC3339)
}

// newTestManager builds a manager with the real waiter.
func newTestManager(
	cluster string,
	client claim.ClaimRefClient,
	job claim.JobControl,
	clock libtime.CurrentDateTimeGetter,
) claim.LeaseManager {
	mgr, err := claim.NewLeaseManager(cluster, client, job, clock, libtime.NewWaiterDuration())
	Expect(err).NotTo(HaveOccurred())
	return mgr
}

// deleteCall records one DeleteJob call and the fake-clock time it happened.
type deleteCall struct {
	taskID string
	at     libtime.DateTime
}

// fakeJobControl is a race-safe JobControl fake. The optional callbacks get
// the per-task call index (1-based).
type fakeJobControl struct {
	mu           sync.Mutex
	clock        libtime.CurrentDateTimeGetter
	runningFn    func(taskID string, call int) (bool, error)
	deleteFn     func(taskID string, call int) error
	runningCalls map[string]int
	deleteCalls  map[string]int
	deleted      []deleteCall
}

func newFakeJobControl(clock libtime.CurrentDateTimeGetter) *fakeJobControl {
	return &fakeJobControl{
		clock:        clock,
		runningCalls: map[string]int{},
		deleteCalls:  map[string]int{},
	}
}

func (f *fakeJobControl) IsJobRunning(_ context.Context, taskID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runningCalls[taskID]++
	if f.runningFn == nil {
		return false, nil
	}
	return f.runningFn(taskID, f.runningCalls[taskID])
}

func (f *fakeJobControl) DeleteJob(_ context.Context, taskID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls[taskID]++
	f.deleted = append(f.deleted, deleteCall{taskID: taskID, at: f.clock.Now()})
	if f.deleteFn == nil {
		return nil
	}
	return f.deleteFn(taskID, f.deleteCalls[taskID])
}

// DeleteCallCount returns the number of DeleteJob calls for taskID.
func (f *fakeJobControl) DeleteCallCount(taskID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleteCalls[taskID]
}

// Deleted returns a copy of every DeleteJob call.
func (f *fakeJobControl) Deleted() []deleteCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]deleteCall, len(f.deleted))
	copy(result, f.deleted)
	return result
}

// SetRunning makes IsJobRunning answer from a fixed set of running task ids.
func (f *fakeJobControl) SetRunning(running map[string]bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runningFn = func(taskID string, _ int) (bool, error) {
		return running[taskID], nil
	}
}

// requestsMatching returns the recorded requests whose path contains part.
func requestsMatching(requests []fakeRequest, method, part string) []fakeRequest {
	result := make([]fakeRequest, 0)
	for _, request := range requests {
		if request.Method == method && strings.Contains(request.Path, part) {
			result = append(result, request)
		}
	}
	return result
}
