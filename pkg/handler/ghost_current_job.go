// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// Guard labels for event=ghost_current_job_kept. Closed set.
const (
	ghostGuardStoreEntry          = "store_entry"
	ghostGuardGraceWindow         = "grace_window"
	ghostGuardNamespaceUnresolved = "namespace_unresolved"
	ghostGuardJobActive           = "job_active"
	ghostGuardJobExists           = "job_exists"
	ghostGuardClaimHeld           = "claim_held"
	ghostGuardClaimUnprobeable    = "claim_unprobeable"
	ghostGuardReadFailed          = "read_failed"
	ghostGuardPublishFailed       = "publish_failed"
)

// clearGhostCurrentJobs is the reconcile pass's ghost current_job step.
//
// A task's current_job can name a Job that runs nowhere. The informer has no
// delete handler, a spawned Job carries no ownerReference back to the task, and
// the in-memory store is empty after a restart — so a Job removed before any
// event the executor observed (TTL garbage collection across a restart, the
// weekly cluster reboot, a manual delete) leaves the field set forever. Every
// reader then believes a Job is running, and the ghost-claim regression metric
// stays above 0. This step is the only path that clears the field from evidence
// gathered after the fact rather than from an observed event.
//
// It writes only current_job: "". Status, phase, the claim, the body,
// job_started_at and dispatch are all left alone.
//
// It runs only on passes that complete the drive loop: every early return above
// skips it, so an aborted or gated pass performs no reads and emits no summary.
//
// One residual window is accepted: a task claimed by a cluster whose claim this
// executor cannot read (a vault partition, or a claim written after the probe)
// can still be cleared here. No vault-side compare-and-set exists —
// UpdateFrontmatterCommand carries no precondition — so the window is bounded
// instead by the foreign cluster's held claim (a live claim keeps the guard at
// claim_held), by IsJobActive, and by the foreign job watcher writing the field
// at terminal.
func (h *taskEventHandler) clearGhostCurrentJobs(passCtx context.Context, owned []lib.Task) {
	candidates := make([]lib.Task, 0, len(owned))
	for _, task := range owned {
		if h.isGhostCandidate(task) {
			candidates = append(candidates, task)
		}
	}

	scanned, cleared, kept, unreached := 0, 0, 0, 0
	for i, task := range candidates {
		if h.passDone(passCtx) {
			unreached = len(candidates) - i
			break
		}
		scanned++
		jobName := task.Frontmatter.CurrentJob()
		guard, err := h.ghostCurrentJobGuard(passCtx, task, jobName)
		if guard == "" {
			guard, err = h.publishGhostClear(passCtx, task, jobName)
		}
		if guard == "" {
			cleared++
			glog.Infof(
				"event=ghost_current_job_cleared task=%s job=%s",
				task.TaskIdentifier,
				jobName,
			)
			continue
		}
		kept++
		if err != nil {
			glog.V(2).Infof(
				"event=ghost_current_job_kept task=%s job=%s guard=%s err=%v",
				task.TaskIdentifier,
				jobName,
				guard,
				err,
			)
			continue
		}
		glog.V(2).Infof(
			"event=ghost_current_job_kept task=%s job=%s guard=%s",
			task.TaskIdentifier,
			jobName,
			guard,
		)
	}
	glog.V(2).Infof(
		"event=ghost_current_job_pass scanned=%d cleared=%d kept=%d unreached=%d",
		scanned,
		cleared,
		kept,
		unreached,
	)
}

// isGhostCandidate reports whether the task is one this step may examine: it
// names a Job, is open, is on this executor's stage, and is not tagged for
// another cluster. Non-candidates cost no reads and emit no log line.
//
// The stage rule deliberately ignores stageFilter. It is stricter than
// dispatch's stageSkipResult: a task on another stage belongs to another
// executor's pool whether or not this executor opted out of stage filtering,
// so this step must not touch it either way.
func (h *taskEventHandler) isGhostCandidate(task lib.Task) bool {
	if task.Frontmatter.CurrentJob() == "" {
		return false
	}
	if IsTerminalStatus(task.Frontmatter.Status()) {
		return false
	}
	if task.Frontmatter.Stage() != string(h.branch) {
		return false
	}
	return !h.isForeignCluster(task)
}

// ghostCurrentJobGuard evaluates, in a fixed order, whether the named Job is
// provably gone. It returns ("", nil) when every check passes; otherwise the
// first failing guard label, plus the causing error where one exists.
//
// The order is load-bearing. The local store and the spawn marker are checked
// first because they are the only evidence that does not cost a remote read,
// and because a task this executor is actively spawning must never be cleared
// on a stale remote view. The grace window follows: it is a local clock read
// that mirrors dispatch's respawn gate, and a Job that started moments ago must
// not be cleared on a stale remote view either. The three remote reads that
// follow are the most this step may make: the live, task-id-scoped
// jobSpawner.IsJobActive list, JobExists, and the claim probe.
func (h *taskEventHandler) ghostCurrentJobGuard(
	ctx context.Context,
	task lib.Task,
	jobName string,
) (string, error) {
	if h.hasLocalSpawnState(task.TaskIdentifier) {
		return ghostGuardStoreEntry, nil
	}
	if h.withinGhostGraceWindow(task) {
		return ghostGuardGraceWindow, nil
	}
	config, err := h.resolveConfigQuiet(ctx, task)
	if err != nil {
		return ghostGuardNamespaceUnresolved, err
	}
	if config == nil {
		return ghostGuardNamespaceUnresolved, nil
	}
	namespace, err := h.jobNamespaceForTask(ctx, config, task.TaskIdentifier)
	if err != nil {
		return ghostGuardNamespaceUnresolved, err
	}
	active, err := h.jobSpawner.IsJobActive(ctx, namespace, task.TaskIdentifier)
	if err != nil {
		return ghostGuardReadFailed, errors.Wrapf(
			ctx,
			err,
			"check active job for task %s",
			task.TaskIdentifier,
		)
	}
	if active {
		return ghostGuardJobActive, nil
	}
	exists, err := h.jobSpawner.JobExists(ctx, namespace, jobName)
	if err != nil {
		return ghostGuardReadFailed, errors.Wrapf(
			ctx,
			err,
			"check named job %s for task %s",
			jobName,
			task.TaskIdentifier,
		)
	}
	if exists {
		return ghostGuardJobExists, nil
	}
	result, err := h.claimProbe(ctx, task)
	if err != nil {
		return ghostGuardClaimUnprobeable, err
	}
	switch result {
	case claimProbeFree:
		return "", nil
	case claimProbeHeld:
		return ghostGuardClaimHeld, nil
	default:
		return ghostGuardClaimUnprobeable, nil
	}
}

// withinGhostGraceWindow reports whether job_started_at lies inside
// defaultRespawnGracePeriod of now. It mirrors the time arithmetic of
// withinRespawnGraceWindow but is a pure check: no log line, no metric and no
// grace-window deferral store. A parse error or a zero value counts as elapsed,
// matching dispatch, so a legacy record is still cleared.
func (h *taskEventHandler) withinGhostGraceWindow(task lib.Task) bool {
	jobStartedAt, err := task.Frontmatter.JobStartedAt()
	if err != nil {
		return false
	}
	if jobStartedAt.IsZero() {
		return false
	}
	return h.currentDateTime.Now().Time().Sub(jobStartedAt) < defaultRespawnGracePeriod
}

// hasLocalSpawnState reports whether this executor knows of a spawn for the
// task: a store entry, or a spawn between taking the claim and recording the
// Job.
func (h *taskEventHandler) hasLocalSpawnState(taskID lib.TaskIdentifier) bool {
	if _, ok := h.taskStore.Load(taskID); ok {
		return true
	}
	return h.spawnPendingFor(taskID)
}

// publishGhostClear re-checks the local spawn state under the per-task claim
// lock and then publishes the clear.
//
// The re-check is what makes the concurrent spawn safe. The spawn path calls
// markSpawnPending and only then takes lockClaimRelease for its claim. Either
// the mark came first, and this re-check sees it, or the mark comes later, and
// the spawn's claim and spawn notification are published after the clear. That
// publish order does not fix apply order: commands are keyed by a random
// RequestID, so the controller may apply the later spawn notification before
// the clear. That inversion is the spec's accepted "Apply-order inversion"
// residual. The PublishClearCurrentJob call must happen while the lock is held;
// the snapshot task is passed unchanged.
func (h *taskEventHandler) publishGhostClear(
	ctx context.Context,
	task lib.Task,
	jobName string,
) (string, error) {
	unlock := h.lockClaimRelease(task.TaskIdentifier)
	defer unlock()
	if h.hasLocalSpawnState(task.TaskIdentifier) {
		return ghostGuardStoreEntry, nil
	}
	if err := h.resultPublisher.PublishClearCurrentJob(ctx, task, jobName); err != nil {
		return ghostGuardPublishFailed, err
	}
	return "", nil
}

// resolveConfigQuiet mirrors resolveConfigForReconcile's lookup rules exactly
// but increments no metric and emits no log line of its own. The ghost step must
// not add to the gather's skipped_unknown_assignee accounting.
func (h *taskEventHandler) resolveConfigQuiet(
	ctx context.Context,
	task lib.Task,
) (*pkg.AgentConfiguration, error) {
	if task.Frontmatter.Assignee() != "" {
		resolved, err := h.resolver.Resolve(ctx, string(task.Frontmatter.Assignee()))
		if err != nil {
			if stderrors.Is(err, pkg.ErrConfigNotFound) {
				return nil, nil
			}
			return nil, errors.Wrapf(
				ctx,
				err,
				"resolve agent config for task %s",
				task.TaskIdentifier,
			)
		}
		return &resolved, nil
	}
	taskType := task.Frontmatter.TaskType()
	if taskType == "" {
		return nil, nil
	}
	rawPreviousAssignee, _ := task.Frontmatter.String("previous_assignee")
	previousAssignee, err := h.inTypePreviousAssignee(
		ctx,
		rawPreviousAssignee,
		string(taskType),
	)
	if err != nil {
		return nil, err
	}
	resolved, err := h.resolver.ResolveByTaskType(ctx, string(taskType), previousAssignee)
	if err != nil {
		if stderrors.Is(err, pkg.ErrConfigNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &resolved, nil
}
