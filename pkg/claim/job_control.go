// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import "context"

//counterfeiter:generate -o ../../mocks/claim_job_control.go --fake-name FakeClaimJobControl . JobControl

// JobControl is the small slice of Kubernetes Job control the lease manager
// needs. It is injected so the lease logic can be tested on a fake clock.
type JobControl interface {
	// IsJobRunning reports whether a non-terminal Job exists for taskID.
	IsJobRunning(ctx context.Context, taskID string) (bool, error)
	// DeleteJob deletes the Job for taskID. Deleting an absent Job is not an
	// error (idempotent).
	DeleteJob(ctx context.Context, taskID string) error
}
