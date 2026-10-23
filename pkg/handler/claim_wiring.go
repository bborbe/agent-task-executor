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
	if refClient == nil {
		return nil, errors.Errorf(
			ctx,
			"claim ref client factory returned nil for claim mode %q",
			h.claimMode,
		)
	}
	// The fence and restart recovery act on a Job. A mode that takes a claim must
	// never run them against the loud unmanaged control, so a nil injection is a
	// construction failure rather than a silent no-op.
	jobControl := h.claimJobControl
	if jobControl == nil {
		return nil, errors.Errorf(
			ctx,
			"claim job control is nil for claim mode %q",
			h.claimMode,
		)
	}
	manager, err := claim.NewLeaseManager(
		h.cluster,
		refClient,
		jobControl,
		h.currentDateTime,
		libtime.NewWaiterDuration(),
	)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create claim lease manager")
	}
	// Assigned only after the manager constructed, so a failed construction
	// leaves the field nil rather than half-wiring the handler.
	h.redriveStore = claim.NewRedriveStreakStore(refClient, h.currentDateTime)
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
	// `repo` is the documented source and wins when present.
	if raw, ok := task.Frontmatter.String("repo"); ok {
		if owner, repo, split := splitOwnerRepo(raw); split {
			return owner, repo
		}
	}
	// Every pr-review task the GitHub watcher creates carries `clone_url`
	// instead of `repo` (9,779 of 9,779 in the OpenClaw vault, 2026-10-22), so
	// without this fallback every one of them failed key derivation and, in
	// on mode, would fail closed and never run.
	if raw, ok := task.Frontmatter.String("clone_url"); ok {
		if owner, repo, parsed := ownerRepoFromCloneURL(raw); parsed {
			return owner, repo
		}
	}
	return "", ""
}

// splitOwnerRepo splits "<owner>/<name>", rejecting an empty or partial value.
func splitOwnerRepo(raw string) (string, string, bool) {
	idx := strings.Index(raw, "/")
	if idx <= 0 || idx == len(raw)-1 {
		return "", "", false
	}
	return raw[:idx], raw[idx+1:], true
}

// ownerRepoFromCloneURL extracts owner and repo from an https, ssh:// or
// scp-style (git@host:owner/repo) clone URL, with an optional ".git" suffix and
// trailing slash. Anything else is rejected so the caller keeps failing loudly.
func ownerRepoFromCloneURL(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimSuffix(raw, "/")
	raw = strings.TrimSuffix(raw, ".git")
	var path string
	switch {
	case strings.Contains(raw, "://"):
		rest := raw[strings.Index(raw, "://")+3:]
		if at := strings.Index(rest, "@"); at >= 0 {
			rest = rest[at+1:]
		}
		slash := strings.Index(rest, "/")
		if slash < 0 {
			return "", "", false
		}
		path = rest[slash+1:]
	case strings.HasPrefix(raw, "git@"):
		colon := strings.Index(raw, ":")
		if colon < 0 {
			return "", "", false
		}
		path = raw[colon+1:]
	default:
		return "", "", false
	}
	return splitOwnerRepo(path)
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
