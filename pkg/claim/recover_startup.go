// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	"time"

	"github.com/golang/glog"
)

// recoverStartupAttempts is how many times startup recovery is tried before it
// gives up and lets the claims lapse.
const recoverStartupAttempts = 5

// recoverStartupRetryDelay is the pause between attempts. Recovery is one list
// call against git-rest, so a handful of one-second retries covers a restarting
// endpoint without holding the boot open.
const recoverStartupRetryDelay = time.Second

// recoverStartupViewWait bounds how long recovery waits for the task view to
// finish replaying before it decides any claim's fate.
//
// The wait is load-bearing for correctness. A claim's Job may live outside the
// executor's namespace, and the fence finds it through the task's assignee —
// read from the view. Deciding against a view that has not caught up reads "no
// task, no Job" for a Job that is running and releases its claim: the duplicate
// the claims exist to prevent. A boot that outlasts the bound proceeds and logs
// it, so a view that never becomes ready (Kafka down) cannot stop recovery.
const recoverStartupViewWait = 60 * time.Second

// recoverStartupViewPollInterval is how often the readiness gate re-checks. A
// var, not a const, so a test can shrink the gate's latency.
var recoverStartupViewPollInterval = 500 * time.Millisecond

// RecoverAtStartup re-adopts, releases or fences the claims this cluster held
// before a restart, retried a bounded number of times.
//
// It exists because the held set is in memory: a replaced pod starts with it
// empty, so without this step nothing renews and nothing fences the Jobs that
// pod was running. Their leases lapse, another cluster's takeover is legitimate,
// and the original Job runs on to its natural end. Every rollout replaces a pod,
// so that is not a rare path.
//
// It never returns an error. A git-rest outage at boot must not crash-loop an
// executor that can still process tasks: the failure is logged and the claims
// stay unheld, which is the behaviour before this step existed.
func RecoverAtStartup(
	ctx context.Context,
	manager LeaseManager,
	viewReady func() bool,
) error {
	waitForTaskView(ctx, viewReady)
	for attempt := 1; attempt <= recoverStartupAttempts; attempt++ {
		err := manager.Recover(ctx)
		if err == nil {
			return nil
		}
		glog.Errorf(
			"claim recovery attempt %d/%d failed: %v",
			attempt,
			recoverStartupAttempts,
			err,
		)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(recoverStartupRetryDelay):
		}
	}
	glog.Errorf(
		"claim recovery failed after %d attempts; claims held before this start stay unheld until their leases lapse",
		recoverStartupAttempts,
	)
	return nil
}

// waitForTaskView blocks until the task view has replayed, the bound expires or
// ctx is done. A nil check function means the caller has no view and every
// namespace lookup is the executor's own, so there is nothing to wait for.
func waitForTaskView(ctx context.Context, viewReady func() bool) {
	if viewReady == nil || viewReady() {
		return
	}
	glog.Infof("claim recovery: waiting up to %s for the task view replay", recoverStartupViewWait)
	deadline := time.NewTimer(recoverStartupViewWait)
	defer deadline.Stop()
	ticker := time.NewTicker(recoverStartupViewPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			glog.Warningf(
				"claim recovery: task view not ready after %s; a claim whose Job lives outside this namespace may be released",
				recoverStartupViewWait,
			)
			return
		case <-ticker.C:
			if viewReady() {
				return
			}
		}
	}
}

// RunClaimRecovery is RecoverAtStartup as a run-group member: it recovers once,
// then holds until the context is cancelled.
//
// The hold is load-bearing, not tidiness. service.Run wraps every run func in
// run.CancelOnFirstFinishWait, which cancels all the others as soon as ANY one
// returns. A recovery step that returned once it had recovered therefore ends
// the whole service a few seconds after boot — v0.30.2 did exactly that on nuke
// dev, nuke prod and sun, exiting 0 with `claim lease tick failed: context
// canceled` while every other component was still healthy. Recovering is a
// startup action, not a component with a lifetime, so it waits on the same
// context everything else waits on.
func RunClaimRecovery(
	ctx context.Context,
	manager LeaseManager,
	viewReady func() bool,
) error {
	_ = RecoverAtStartup(ctx, manager, viewReady) // never returns an error; see above
	<-ctx.Done()
	return nil
}
