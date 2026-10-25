// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
	"github.com/golang/glog"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// jobLookupDeniedInterval is the window in which one assignee/namespace pair
// produces at most one log line. A denied read is a standing condition, not an
// event: with a broken credential every task on the assignee hits it on every
// pass, so an unthrottled log is one line per task per pass and buries the line
// that matters.
const jobLookupDeniedInterval = time.Minute

// jobLookupDeniedError marks an active-job read the API server refused on
// authentication (401) or authorization (403).
//
// It is the read-path sibling of spawnJobError: retrying cannot help until the
// credential or the grant is restored, so the caller suppresses the spawn and
// lets the reconcile floor re-drive the task, instead of returning an error
// that terminates ConsumeMessage and restarts the executor for every assignee
// at once. That is what happened on 2026-10-24 (Sentry NUKE-PROD-CG): a single
// 401 from `list jobs` exited the process.
//
// It carries assignee and namespace because isJobActive resolves the namespace
// internally and the metric needs both, so the counter names which identity
// broke rather than which fleet went quiet.
type jobLookupDeniedError struct {
	err       error
	assignee  string
	namespace libk8s.Namespace
}

func (e *jobLookupDeniedError) Error() string { return e.err.Error() }

func (e *jobLookupDeniedError) Unwrap() error { return e.err }

// isJobLookupDenied reports whether err is a read the API server refused on
// identity grounds. It matches only Unauthorized and Forbidden: a 404 means the
// resource resolved and the grant is fine, a 429 is worth retrying, and a 500
// is a real fault that must stay fatal.
func isJobLookupDenied(err error) bool {
	return apierrors.IsUnauthorized(err) || apierrors.IsForbidden(err)
}

// wrapJobLookupError wraps a failed active-job read. A denial becomes a
// jobLookupDeniedError so the caller can defer instead of failing the message;
// every other error keeps its existing wrapped form and stays fatal.
func wrapJobLookupError(
	ctx context.Context,
	err error,
	assignee string,
	namespace libk8s.Namespace,
	format string,
	args ...any,
) error {
	if !isJobLookupDenied(err) {
		return errors.Wrapf(ctx, err, format, args...)
	}
	return errors.Wrapf(
		ctx,
		&jobLookupDeniedError{err: err, assignee: assignee, namespace: namespace},
		format,
		args...,
	)
}

// assigneeOf returns the Config's assignee, or the empty string for a nil
// config. A nil config is the empty-assignee path, where the active-job read
// falls back to the direct per-task lookup.
func assigneeOf(config *pkg.AgentConfiguration) string {
	if config == nil {
		return ""
	}
	return config.Assignee
}

// suppressForActiveCurrentJob answers the current_job gate. A task whose
// frontmatter names a Job is suppressed while that Job is still active or
// inside its grace window; a refused read defers it instead of failing the
// message.
//
// Extracted from spawnIfNeededClaimed, which is at its cognitive-complexity
// budget: the gate is two nested conditions plus the denial branch, and this
// keeps all three out of that function.
func (h *taskEventHandler) suppressForActiveCurrentJob(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
	claim *deferredClaim,
) (bool, error) {
	currentJob := task.Frontmatter.CurrentJob()
	if currentJob == "" {
		return false, nil
	}
	suppress, err := h.checkActiveCurrentJob(ctx, task, currentJob, config, claim)
	if err == nil {
		return suppress, nil
	}
	if h.handleJobLookupDenied(err) {
		return true, nil
	}
	metrics.TaskEventsTotal.WithLabelValues("error").Inc()
	return false, err
}

// withinRespawnGraceWindow reports whether the task is still inside the window
// after job_started_at during which a respawn is suppressed, storing the
// grace-window deferral when it is.
//
// A missing or unparseable job_started_at counts as elapsed, preserving the
// behaviour of legacy tasks. Extracted from checkActiveCurrentJob so that
// function stays inside its length budget.
func (h *taskEventHandler) withinRespawnGraceWindow(
	task lib.Task,
	config *pkg.AgentConfiguration,
	currentJob string,
	claim *deferredClaim,
) bool {
	jobStartedAt, parseErr := task.Frontmatter.JobStartedAt()
	if parseErr != nil {
		glog.Warningf(
			"task %s: failed to parse job_started_at: %v; treating grace period as elapsed",
			task.TaskIdentifier, parseErr,
		)
		return false
	}
	if jobStartedAt.IsZero() {
		return false
	}
	elapsed := h.currentDateTime.Now().Time().Sub(jobStartedAt)
	if elapsed >= defaultRespawnGracePeriod {
		return false
	}
	glog.Infof(
		"event=respawn_grace_window task=%s current_job=%s elapsed=%.0fs",
		task.TaskIdentifier, currentJob, elapsed.Seconds(),
	)
	metrics.TaskEventsTotal.WithLabelValues("respawn_grace_window").Inc()
	h.storeGraceWindowDeferral(
		task,
		config,
		jobStartedAt.Add(defaultRespawnGracePeriod),
		claim,
	)
	return true
}

// currentJobFinished reports whether the Job named in current_job has certainly
// ended, classifying a failed read.
//
// The JobExists inside jobProvablyFinished is the second active-job read in
// checkActiveCurrentJob, and it can be refused where the first was granted — a
// `get jobs` 403 beside a `list jobs` grant. Classifying it here is what keeps
// that asymmetry from exiting ConsumeMessage. Extracted so the gate stays inside
// its length budget.
func (h *taskEventHandler) currentJobFinished(
	ctx context.Context,
	task lib.Task,
	currentJob string,
	namespace libk8s.Namespace,
	assignee string,
) (bool, error) {
	finished, err := h.jobProvablyFinished(ctx, task, currentJob, namespace)
	if err != nil {
		return false, wrapJobLookupError(
			ctx,
			err,
			assignee,
			namespace,
			"check whether current_job is finished",
		)
	}
	return finished, nil
}

// handleJobLookupDenied reports a denied active-job read and returns true when
// err was one. The task is deliberately not re-parked: the reconcile floor
// re-drives any eligible task with no active Job within its interval, which is
// what makes the deferral safe rather than lossy.
func (h *taskEventHandler) handleJobLookupDenied(err error) bool {
	var denied *jobLookupDeniedError
	if !stderrors.As(err, &denied) {
		return false
	}
	metrics.JobLookupDeniedTotal.WithLabelValues(
		denied.assignee,
		denied.namespace.String(),
	).Inc()
	h.logJobLookupDenied(denied)
	return true
}

// logJobLookupDenied emits at most one line per assignee/namespace per
// jobLookupDeniedInterval. The state is per handler instance, which matches the
// process that holds the credential.
func (h *taskEventHandler) logJobLookupDenied(denied *jobLookupDeniedError) {
	key := denied.assignee + "/" + denied.namespace.String()
	now := h.currentDateTime.Now().Time()
	h.jobLookupDeniedMu.Lock()
	// Lazily initialised, like bumpSpawnGen: internal tests build
	// &taskEventHandler{...} directly, and a nil map would panic on the path
	// whose whole job is to keep the executor alive.
	if h.jobLookupDeniedLoggedAt == nil {
		h.jobLookupDeniedLoggedAt = make(map[string]time.Time)
	}
	last, logged := h.jobLookupDeniedLoggedAt[key]
	if logged && now.Sub(last) < jobLookupDeniedInterval {
		h.jobLookupDeniedMu.Unlock()
		return
	}
	h.jobLookupDeniedLoggedAt[key] = now
	h.jobLookupDeniedMu.Unlock()
	glog.Warningf(
		"event=job_lookup_denied assignee=%s namespace=%s throttled_to_one_line_per=%s err=%v",
		denied.assignee,
		denied.namespace.String(),
		jobLookupDeniedInterval,
		denied.err,
	)
}
