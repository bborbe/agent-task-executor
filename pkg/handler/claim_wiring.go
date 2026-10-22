// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"
	"strings"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
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
// first use. It refuses to construct anything in claim mode off.
func (h *taskEventHandler) leaseManager(ctx context.Context) (claim.LeaseManager, error) {
	if h.claimMode == pkg.ClaimModeOff {
		return nil, errors.Errorf(ctx, "claim mode off: no lease manager constructed")
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

// acquireSpawnClaim takes this executor's lease for the task immediately before
// the Job is created. It reports whether the spawn may proceed. Claim failures
// are recorded by the claim package and never returned: the next dispatch pass
// that would have spawned this task retries.
func (h *taskEventHandler) acquireSpawnClaim(ctx context.Context, task lib.Task) bool {
	if h.claimMode == pkg.ClaimModeOff {
		return true
	}
	manager, err := h.leaseManager(ctx)
	if err != nil {
		return h.claimFailureDecision(task, "lease_manager", err)
	}
	owner, repo := claimOwnerRepo(task)
	key, err := claim.DeriveClaimKey(
		ctx,
		string(task.Frontmatter.TaskType()),
		string(task.TaskIdentifier),
		owner,
		repo,
	)
	if err != nil {
		return h.claimFailureDecision(task, "derive_key", err)
	}
	_, claimErr := manager.Claim(ctx, key, string(task.TaskIdentifier))
	if claimErr == nil {
		return true
	}
	if stderrors.Is(claimErr, claim.ErrLeaseConflict) {
		// One takeover attempt against the listed claim. A live lease (including
		// one inside the takeover grace) and a lost compare-and-swap race both
		// leave the key with its current owner; the claim package has already
		// counted conflict / takeover_too_early / taken_over / error.
		_, takeoverErr := manager.Takeover(ctx, key, string(task.TaskIdentifier))
		if takeoverErr == nil {
			return true
		}
		return h.claimFailureDecision(task, "takeover", takeoverErr)
	}
	return h.claimFailureDecision(task, "claim", claimErr)
}

// claimOwnerRepo reads the task's single `repo` frontmatter key, which carries
// "<owner>/<name>". A missing or malformed value yields empty components;
// DeriveClaimKey ignores them for task types that are not repo-keyed and
// rejects them for the ones that are.
func claimOwnerRepo(task lib.Task) (string, string) {
	raw, ok := task.Frontmatter.String("repo")
	if !ok {
		return "", ""
	}
	idx := strings.Index(raw, "/")
	if idx <= 0 || idx == len(raw)-1 {
		return "", ""
	}
	return raw[:idx], raw[idx+1:]
}

// claimFailureDecision logs a claim failure and reports whether the spawn may
// proceed: shadow proceeds, on fails closed. The claim package has already
// incremented executor_claim_total for every request it made; this function
// must not increment anything.
func (h *taskEventHandler) claimFailureDecision(
	task lib.Task,
	stage string,
	err error,
) bool {
	glog.Warningf(
		"event=claim_failed stage=%s mode=%s task=%s error=%v",
		stage, h.claimMode, task.TaskIdentifier, err,
	)
	return h.claimMode == pkg.ClaimModeShadow
}
