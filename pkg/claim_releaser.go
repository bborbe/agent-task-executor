// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"time"

	lib "github.com/bborbe/agent"
)

// ClaimReleaser releases this executor's held claim for a task, if any. The
// task event handler implements it and the Job watcher is injected with it, so
// a terminal Job can release the claim taken before the Job was created.
type ClaimReleaser interface {
	ReleaseTaskClaim(ctx context.Context, taskID lib.TaskIdentifier)
	// ReleaseTaskClaimAfterSuccess holds the claim until SuccessGracePeriod has
	// elapsed since completedAt, then releases it, and records the success at
	// phase in the shared claim store so the suppression survives a pod restart.
	// A failed Job must call ReleaseTaskClaim instead: a retry belongs to this
	// cluster and delaying its claim release would only delay the retry.
	//
	// phase is the phase the succeeded Job was spawned for, carried on the Job as
	// TaskPhaseAnnotationKey. The hold and the record are the same moment, so they
	// are written through one end point rather than a second parallel path.
	ReleaseTaskClaimAfterSuccess(
		ctx context.Context,
		taskID lib.TaskIdentifier,
		phase string,
		completedAt time.Time,
	)
}
