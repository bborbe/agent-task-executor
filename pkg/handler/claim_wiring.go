// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// ClaimRefClientFactory builds the git-rest claim ref client. It is called at
// most once per process, and only when CLAIM_MODE is shadow or on.
type ClaimRefClientFactory func() claim.ClaimRefClient

// unmanagedJobControl is the JobControl handed to the dispatch-only lease
// manager. Claim and Takeover never call it; Recover and the fence do, and the
// lease-lifecycle spec owns those paths. It fails loud so a future wiring that
// starts them cannot silently run without Job control.
type unmanagedJobControl struct{}

// IsJobRunning reports that claim Job control is not wired.
func (unmanagedJobControl) IsJobRunning(ctx context.Context, taskID string) (bool, error) {
	return false, errors.Errorf(ctx, "claim job control not wired: IsJobRunning(%s)", taskID)
}

// DeleteJob reports that claim Job control is not wired.
func (unmanagedJobControl) DeleteJob(ctx context.Context, taskID string) error {
	return errors.Errorf(ctx, "claim job control not wired: DeleteJob(%s)", taskID)
}

// leaseManager returns this process's single lease manager, constructing it on
// first use. Only the two claim-taking modes construct one, so a zero-value or
// otherwise unparsed mode fails closed instead of building a manager nothing
// asked for.
//
// A construction failure is cached for the process lifetime. That is deliberate:
// the inputs are fixed at startup (the injected factory and the cluster name),
// so a retry would fail identically on every dispatch and only add log noise.
// In `on` the cached error fails closed; in `shadow` it fails open, which is the
// mode's contract.
func (h *taskEventHandler) leaseManager(ctx context.Context) (claim.LeaseManager, error) {
	if !h.claimMode.TakesClaim() {
		return nil, errors.Errorf(
			ctx,
			"claim mode %q takes no claim: no lease manager constructed",
			h.claimMode,
		)
	}
	h.claimManagerOnce.Do(func() {
		h.claimManager, h.claimManagerErr = h.createLeaseManager(ctx)
	})
	return h.claimManager, h.claimManagerErr
}

// createLeaseManager builds the process's lease manager from the injected
// factory. It is called at most once, from leaseManager's sync.Once.
func (h *taskEventHandler) createLeaseManager(
	ctx context.Context,
) (claim.LeaseManager, error) {
	if h.claimRefClientFactory == nil {
		return nil, errors.Errorf(
			ctx,
			"claim ref client factory is nil for claim mode %q",
			h.claimMode,
		)
	}
	refClient := h.claimRefClientFactory()
	manager, err := claim.NewLeaseManager(
		h.cluster,
		refClient,
		unmanagedJobControl{},
		h.currentDateTime,
		libtime.NewWaiterDuration(),
	)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create claim lease manager")
	}
	return manager, nil
}
