// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"
	"strings"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// parkWriteAttempts bounds the retries of the park's phase write. The write is
// what stops a later jobless release from letting the task run again, so a
// single failed send is worth retrying before it is reported as failed.
const parkWriteAttempts = 3

// parkWriteRetryDelay is the pause between park-write attempts. Short: the send
// is a Kafka publish, and a slow retry would stall the reconcile pass.
const parkWriteRetryDelay = 200 * time.Millisecond

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
	// The lease manager reaches every claim through the typed client. A nil
	// factory, or a factory that builds nothing, is a construction failure: the
	// manager has no fallback, because falling back to the git-rest ref client
	// would put reads on one service and writes on another.
	if h.claimClientFactory == nil {
		return nil, errors.Errorf(
			ctx,
			"claim client factory is nil for claim mode %q",
			h.claimMode,
		)
	}
	claimClient := h.claimClientFactory()
	if claimClient == nil {
		return nil, errors.Errorf(
			ctx,
			"claim client factory returned nil for claim mode %q",
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
		claimClient,
		jobControl,
		h.currentDateTime,
		libtime.NewWaiterDuration(),
	)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create claim lease manager")
	}
	// Assigned only after the manager constructed, so a failed construction
	// leaves the fields nil rather than half-wiring the handler. The two stores
	// share refClient but keep separate ref namespaces.
	h.redriveStore = claim.NewRedriveStreakStore(refClient, h.currentDateTime)
	h.redriveReasons = claim.NewRedriveReasonStore(refClient, h.currentDateTime)
	h.claimSchemeGuard = claim.NewSchemeGuard(refClient, h.cluster, h.currentDateTime)
	return manager, nil
}

// acquireSpawnClaim takes this executor's lease for the task immediately before
// the Job is created. It reports whether the spawn may proceed. Claim failures
// are recorded by the claim package and never returned: the next dispatch pass
// that would have spawned this task retries.
func (h *taskEventHandler) acquireSpawnClaim(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
) bool {
	if h.claimMode == pkg.ClaimModeOff {
		return true
	}
	manager, err := h.leaseManager(ctx)
	if err != nil {
		return h.claimFailureDecision(task, "lease_manager", err)
	}
	// The scheme guard runs before any ref is derived or written. A mismatch
	// means this cluster derives different key strings from its peers, so a claim
	// it takes would exclude nobody: the peer claims its own ref for the same
	// task and both run. Refusing is the whole point — no claim is written, so a
	// matching cluster is free to take the work.
	//
	// claimFailureDecision, not a bare false, is what keeps shadow honest: shadow
	// takes claims without ever blocking a dispatch, so there the refusal skips
	// the write and lets the spawn through, which is exactly its contract.
	if guard := h.claimSchemeGuard; guard != nil {
		if guardErr := guard.Check(ctx); guardErr != nil {
			return h.claimFailureDecision(task, "claim_scheme", guardErr)
		}
	}
	owner, repo := claimOwnerRepo(task)
	primary, secondary, fallback, err := claim.DeriveClaimKeysWithBody(
		ctx,
		string(task.Frontmatter.TaskType()),
		string(task.TaskIdentifier),
		owner,
		repo,
		string(task.Content),
	)
	if err != nil {
		return h.claimFailureDecision(task, "derive_key", err)
	}
	if fallback {
		metrics.ClaimKeyFallbackTotal.Inc()
		glog.Warningf(
			"event=claim_key_fallback task=%s repo=%s/%s",
			task.TaskIdentifier, owner, repo,
		)
	}
	_, claimErr := manager.ClaimPair(ctx, primary, secondary, string(task.TaskIdentifier))
	if claimErr == nil {
		logClaimAcquired(primary, task.TaskIdentifier)
		return true
	}
	if stderrors.Is(claimErr, claim.ErrLeaseConflict) {
		// One takeover attempt against the listed claim. A live lease (including
		// one inside the takeover grace) and a lost compare-and-swap race both
		// leave the key with its current owner; the claim package has already
		// counted conflict / takeover_too_early / taken_over / error.
		_, takeoverErr := manager.TakeoverPair(
			ctx,
			primary,
			secondary,
			string(task.TaskIdentifier),
		)
		if takeoverErr == nil {
			// Another executor owned this task's claim and its lease had lapsed.
			// The Config decides whether that work may run a second time here.
			//
			// Only `on` may park. Shadow's contract is to take claims without
			// ever blocking a dispatch, and parking there would be worse than a
			// violation: the task is written to human_review, this cluster keeps
			// the claim, and the next pass's Takeover fails against its own live
			// lease, so claimFailureDecision proceeds and a Job is created for the
			// task that was just parked.
			if h.claimMode == pkg.ClaimModeOn &&
				config.LostClaimAction() == agentv1.ParkAfterLostClaim {
				h.parkAfterLostClaim(ctx, primary, task)
				return false
			}
			logClaimAcquired(primary, task.TaskIdentifier)
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

// parkAfterLostClaim stops a task whose claim another executor took over, when
// its Config says the agent must not run a second time. The caller returns false
// either way, so no Job is created here.
//
// The phase write is retried, because returning false without it is not
// fail-safe on its own: the takeover left this cluster holding the claim, and
// releaseJoblessClaims drops a held claim whose Job has been absent for
// JoblessGrace (300 s). A task still in a spawnable phase is then claimed fresh
// and runs a second time — exactly what the Config forbids. The success line is
// logged only once the write lands, so the log never reports a park that did
// not happen; a write that never lands is logged as an error and counted.
func (h *taskEventHandler) parkAfterLostClaim(
	ctx context.Context,
	key string,
	task lib.Task,
) {
	var err error
	for attempt := 1; attempt <= parkWriteAttempts; attempt++ {
		if err = h.resultPublisher.PublishParkLostClaim(ctx, task); err == nil {
			glog.Infof("event=parked_lost_claim key=%s task=%s", key, task.TaskIdentifier)
			metrics.ParkedLostClaimTotal.WithLabelValues("parked").Inc()
			return
		}
		if attempt == parkWriteAttempts {
			break
		}
		select {
		case <-ctx.Done():
			// Cancelled — shutdown, or the reconcile pass timeout. Stop rather
			// than burn the remaining attempts against a dead context and then
			// report a failure that is ours, not the broker's. Not counted as
			// failed: the park did not fail, the pass did.
			glog.Warningf(
				"event=parked_lost_claim_cancelled key=%s task=%s error=%v",
				key, task.TaskIdentifier, err,
			)
			return
		case <-time.After(parkWriteRetryDelay):
		}
	}
	glog.Errorf(
		"event=parked_lost_claim_failed key=%s task=%s attempts=%d error=%v",
		key, task.TaskIdentifier, parkWriteAttempts, err,
	)
	metrics.ParkedLostClaimTotal.WithLabelValues("failed").Inc()
}

// logClaimAcquired records one successful spawn claim, whether it was taken by
// a fresh Claim or by a Takeover after a conflict.
func logClaimAcquired(key string, taskID lib.TaskIdentifier) {
	glog.Infof("event=claim_acquired key=%s task=%s", key, taskID)
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
