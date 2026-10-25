// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// deferIfAtConcurrencyCap reports whether the agent is already running its
// effective capacity — the smaller positive value of MaxConcurrentJobs and the
// pod quota of the agent's priority class — and if so defers the respawn.
// Deferring rather than skipping is the whole point: task publication is
// edge-triggered on vault file changes, so a skipped spawn would never be
// re-driven and the task would sit at in_progress forever, indistinguishable
// from work in flight.
func (h *taskEventHandler) deferIfAtConcurrencyCap(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
	claim *deferredClaim,
) (bool, error) {
	// Uncapped and limiter-less: no namespace resolution and no active-Job list.
	if config == nil || (h.podQuotaLimiter == nil && config.MaxConcurrentJobs <= 0) {
		return false, nil
	}
	namespace, err := h.jobNamespaceForTask(ctx, config, task.TaskIdentifier)
	if err != nil {
		// Not counted here: the caller counts every error it propagates, and an
		// increment on both sides would count this failure twice.
		return false, err
	}
	quota, quotaBounded := h.podQuotaFor(ctx, namespace, config)
	capacity, capacityBounded := effectiveConcurrencyCap(
		config.MaxConcurrentJobs,
		quota,
		quotaBounded,
	)
	// Only an unbounded zero is "uncapped". A bounded zero is a quota that admits
	// no pod, and it must defer rather than admit.
	if capacity <= 0 && !capacityBounded {
		return false, nil
	}
	activeSet, err := h.activeJobsFor(ctx, namespace, config.Assignee)
	if err != nil {
		// The caller counts the error, not this branch: a refused read is
		// suppressed rather than propagated, and counting it here would make the
		// same denial visible on this path and invisible on isJobActive's.
		return false, wrapJobLookupError(
			ctx,
			err,
			config.Assignee,
			namespace,
			"count active jobs for assignee %s",
			config.Assignee,
		)
	}
	running := len(activeSet)
	if running < capacity {
		// Log the ADMIT decision, not just the deferral. Without this, an
		// over-cap spawn leaves no trace and overshoot can only be found by
		// counting Jobs in the cluster by hand — which is how the 2026-08-15
		// race went unnoticed until 17 Jobs existed against a cap of 1.
		// Same V(1) rationale as the deferral log below.
		glog.V(1).Infof(
			"event=concurrency_admit task=%s assignee=%s running=%d cap=%d max=%d quota=%d",
			task.TaskIdentifier, config.Assignee, running, capacity,
			config.MaxConcurrentJobs, quota,
		)
		return false, nil
	}
	// Cheap pre-check before the side effects below: an evicted claim must not
	// publish a queued_at or count a deferral. The authoritative check is at the
	// store, under the same lock as the write.
	h.deferredMu.Lock()
	invalid := h.claimInvalidatedLocked(claim)
	h.deferredMu.Unlock()
	if invalid {
		h.recordClaimInvalidated(task)
		return true, nil
	}
	// V(1), not V(0): a capped deferral is an expected steady state and repeats
	// once per retry interval per waiting task, so V(0) would be a constant
	// stream during any backlog. V(1) is inside the deployed verbosity, so it
	// stays visible to an operator looking — unlike the V(3) skip that hid the
	// wedge fixed in #12.
	glog.V(1).Infof(
		"event=concurrency_cap task=%s assignee=%s running=%d cap=%d max=%d quota=%d retry_after=%s",
		task.TaskIdentifier, config.Assignee, running, capacity,
		config.MaxConcurrentJobs, quota, concurrencyCapRetryDelay,
	)
	metrics.TaskEventsTotal.WithLabelValues("deferred_concurrency_cap").Inc()
	// The bare counter above is kept for continuity; this one carries the
	// assignee so an alert can exclude an agent that is capped on purpose.
	metrics.DeferredConcurrencyCapTotal.WithLabelValues(config.Assignee).Inc()
	task = h.stampQueueEntry(ctx, task)
	h.deferredMu.Lock()
	if h.claimInvalidatedLocked(claim) {
		h.deferredMu.Unlock()
		h.recordClaimInvalidated(task)
		// The deferral ends here without re-parking, so the record stamped above
		// must not outlive it.
		h.clearQueuedAtStampIfNotDeferred(task.TaskIdentifier)
		return true, nil
	}
	// A claimed replay stores only when nothing is parked: a concurrent fresh
	// event may have parked a newer snapshot under the same id.
	if claim != nil {
		if _, exists := h.deferredRespawns[task.TaskIdentifier]; exists {
			h.deferredMu.Unlock()
			// Nothing is parked by this call. The existing entry keeps its own
			// stamp, so this is a no-op when the task is parked and a clear when
			// it is not.
			h.clearQueuedAtStampIfNotDeferred(task.TaskIdentifier)
			return true, nil
		}
	}
	h.deferredRespawns[task.TaskIdentifier] = deferredEntry{
		task:       task,
		config:     *config,
		retryAfter: h.currentDateTime.Now().Time().Add(concurrencyCapRetryDelay),
	}
	h.deferredMu.Unlock()
	return true, nil
}
