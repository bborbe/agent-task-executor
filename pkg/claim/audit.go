// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"

	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// AuditMismatch compares the claims this manager holds with the given set of
// running Job task ids. It adds 1 to executor_claim_mismatch_total for every
// running Job with no held claim and for every held claim with no running Job,
// and logs which case each was. A task present in both is not counted.
func (m *leaseManager) AuditMismatch(ctx context.Context, runningTaskIDs map[string]bool) {
	// HeldClaims takes and releases the manager lock itself; take the snapshot
	// first and iterate it without holding the lock.
	held := m.HeldClaims()
	heldTaskIDs := make(map[string]bool, len(held))
	for _, claim := range held {
		heldTaskIDs[claim.TaskID] = true
	}
	for taskID := range runningTaskIDs {
		if !heldTaskIDs[taskID] {
			metrics.ClaimMismatchTotal.Inc()
			glog.Warningf("claim_mismatch case=job_without_claim task=%s", taskID)
		}
	}
	for _, claim := range held {
		if !runningTaskIDs[claim.TaskID] {
			metrics.ClaimMismatchTotal.Inc()
			glog.Warningf("claim_mismatch case=claim_without_job task=%s", claim.TaskID)
		}
	}
}
