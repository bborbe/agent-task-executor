// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"
	"sync"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// claimProbeResult is what the shared record says about a task, as one value
// rather than a pair of booleans: "no claim exists" and "no claim can be read"
// are different answers that both look like false, and reading the second as
// the first is exactly the mistake the probe exists to prevent.
type claimProbeResult int

const (
	// claimProbeUnavailable means this deployment keeps no claim to read
	// (CLAIM_MODE=off, or a factory that was never injected). The caller falls
	// back to local evidence; it must not read this as a free claim.
	claimProbeUnavailable claimProbeResult = iota
	// claimProbeHeld means some cluster holds the task's claim right now.
	claimProbeHeld
	// claimProbeFree means the claim is absent or its lease has passed.
	claimProbeFree
)

// claimProbe asks the shared record whether any cluster is running this task.
//
// It is the one record that answers a question this executor cannot answer
// locally: with no store entry, the Job named in current_job may be another
// cluster's, and that cluster's Job is invisible here. Its claim is not — the
// claim lives in the vault's ref store, is written before the Job and released
// when the Job ends (or held for the success grace after it), so a live claim
// says "somewhere is running this task" and its absence says no compliant
// cluster is.
//
// An error means the claim store could not be read. That is not evidence of
// absence — a caller must keep suppressing, exactly as it does for a failed Job
// lookup — and the read is retried on the next pass.
func (h *taskEventHandler) claimProbe(
	ctx context.Context,
	task lib.Task,
) (claimProbeResult, error) {
	if !h.claimMode.TakesClaim() {
		return claimProbeUnavailable, nil
	}
	client, err := h.claimProbeClient(ctx)
	if err != nil {
		// The mode takes a claim, so a client that cannot be built is a fault,
		// not "this deployment keeps no claims": fail closed like every other
		// claim-taking path rather than fall back to local-only evidence.
		return claimProbeUnavailable, err
	}
	target, err := claimTargetFor(ctx, task)
	if err != nil {
		return claimProbeUnavailable, err
	}
	current, err := client.Read(ctx, target)
	if err != nil {
		if stderrors.Is(err, claim.ErrClaimFree) {
			return claimProbeFree, nil
		}
		return claimProbeUnavailable, errors.Wrapf(
			ctx,
			err,
			"read claim for task %s",
			task.TaskIdentifier,
		)
	}
	// An unreadable lease is treated as held: the same direction restart
	// recovery takes, because a claim nobody can read is one nobody has been
	// proven to have dropped.
	if current.LeaseUnreadable {
		return claimProbeHeld, nil
	}
	if h.currentDateTime.Now().Time().Before(current.LeaseUntil.Time()) {
		return claimProbeHeld, nil
	}
	return claimProbeFree, nil
}

// claimTargetFor derives the claim target this task's spawn would take.
//
// It is the same derivation the spawn path uses, so the probe asks about the
// same ref the claiming cluster wrote. A task whose key cannot be derived has no
// claim to read; the caller treats that as "cannot tell" rather than as a free
// claim.
func claimTargetFor(ctx context.Context, task lib.Task) (claim.ClaimTarget, error) {
	owner, repo := claimOwnerRepo(task)
	key, _, _, err := claim.DeriveClaimKeysWithBody(
		ctx,
		string(task.Frontmatter.TaskType()),
		string(task.TaskIdentifier),
		owner,
		repo,
		string(task.Content),
	)
	if err != nil {
		return claim.ClaimTarget{}, errors.Wrapf(
			ctx,
			err,
			"derive claim key for task %s",
			task.TaskIdentifier,
		)
	}
	target, ok := claim.TargetFromKey(key)
	if !ok {
		return claim.ClaimTarget{}, errors.Errorf(
			ctx,
			"claim key %q for task %s is not addressable",
			key,
			task.TaskIdentifier,
		)
	}
	return target, nil
}

// claimProbeState is the probe's lazily built client. It is a field of the
// handler rather than two fields of its own to keep the handler's struct, and
// so the handler file, inside the repo's length budget.
type claimProbeState struct {
	once   sync.Once
	client claim.ClaimClient
}

// claimProbeClient returns the typed claim client the probe reads through,
// building it once from the injected factory.
//
// It is a separate instance from the lease manager's, deliberately: the probe
// runs on dispatch paths that may never take a claim (a task whose store entry
// is empty is precisely one this executor has not spawned), so it cannot wait
// for the manager to be constructed.
func (h *taskEventHandler) claimProbeClient(ctx context.Context) (claim.ClaimClient, error) {
	if h.claimClientFactory == nil {
		return nil, errors.Errorf(
			ctx,
			"claim client factory is nil for claim mode %q",
			h.claimMode,
		)
	}
	h.probeState.once.Do(func() {
		h.probeState.client = h.claimClientFactory()
	})
	if h.probeState.client == nil {
		return nil, errors.Errorf(
			ctx,
			"claim client factory returned nil for claim mode %q",
			h.claimMode,
		)
	}
	return h.probeState.client, nil
}

// countClaimProbeFailure records a claim read that failed or could not be
// derived. The task stays suppressed, so the counter is the only signal that a
// fleet is holding spawns because its claim store is unreadable.
func countClaimProbeFailure(task lib.Task, stage string, err error) {
	glog.Warningf(
		"event=claim_probe_failed stage=%s task=%s error=%v",
		stage,
		task.TaskIdentifier,
		err,
	)
	metrics.TaskEventsTotal.WithLabelValues("claim_probe_failed").Inc()
}
