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
func RecoverAtStartup(ctx context.Context, manager LeaseManager) error {
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
