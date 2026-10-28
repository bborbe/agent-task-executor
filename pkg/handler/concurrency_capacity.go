// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	libk8s "github.com/bborbe/k8s"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// effectiveConcurrencyCap returns the admission bound and whether a quota bounds
// it at all. Without a quota bound the declared cap is the only bound, and 0
// means uncapped. With a quota bound the result is authoritative and may be 0 —
// a quota admitting no pod at all — which must defer every spawn rather than
// fall back to the declared cap.
func effectiveConcurrencyCap(
	maxConcurrentJobs int,
	podQuota int,
	quotaBounded bool,
) (int, bool) {
	if !quotaBounded {
		if maxConcurrentJobs < 0 {
			return 0, false
		}
		return maxConcurrentJobs, false
	}
	if podQuota < 0 {
		podQuota = 0
	}
	if maxConcurrentJobs <= 0 {
		return podQuota, true
	}
	if podQuota < maxConcurrentJobs {
		return podQuota, true
	}
	return maxConcurrentJobs, true
}

// quotaBound names why a pod quota does or does not bound admission. It exists so
// a refused read can never render as a full quota: without it a failed read, an
// absent quota and a genuine pods: 0 all reach the admission line as quota=0, and
// a queue behind a broken read reads exactly like a queue behind a real cap. That
// conflation is what made a 2026-10-26 investigation misread a queue as a defect
// for two days.
type quotaBound int

const (
	// quotaUnbounded means nothing bounded the read: no limiter, no priority
	// class on the Config, or no quota scoping the class. The declared cap alone
	// bounds admission.
	quotaUnbounded quotaBound = iota
	// quotaBounded means the read succeeded and its limit is authoritative,
	// including a genuine pods: 0 that admits nothing at all.
	quotaBounded
	// quotaReadFailed means the read was refused or errored, so admission fell
	// back to the declared cap. This is the state that must never be silent.
	quotaReadFailed
)

// String renders the state for the admission and deferral lines, so a log reader
// can tell a broken read from a real zero without inferring it from cap.
func (q quotaBound) String() string {
	switch q {
	case quotaBounded:
		return "ok"
	case quotaReadFailed:
		return "failed"
	default:
		return "none"
	}
}

// bounds reports whether the quota is authoritative for admission.
func (q quotaBound) bounds() bool {
	return q == quotaBounded
}

// podQuotaFor returns the pod quota that bounds admission for config, and why it
// does or does not bound it. A failed read is reported as quotaReadFailed rather
// than folded into "no bound", so the caller can say which it was; it still never
// blocks the spawn on its own.
func (h *taskEventHandler) podQuotaFor(
	ctx context.Context,
	namespace libk8s.Namespace,
	config *pkg.AgentConfiguration,
) (int, quotaBound) {
	if h.podQuotaLimiter == nil || config.PriorityClassName == "" {
		return 0, quotaUnbounded
	}
	quota, bounded, err := h.podQuotaLimiter.PodQuota(
		ctx,
		namespace,
		config.PriorityClassName,
	)
	if err != nil {
		// The fail-open path is observable, not just logged: while the executor
		// runs without the resourcequotas grant, every spawn-eligible admission
		// re-reads and fails, and admission silently runs without the quota
		// bound. The counter is what an operator can alert on; the log line alone
		// is invisible to Prometheus.
		metrics.TaskEventsTotal.WithLabelValues("pod_quota_read_failed").Inc()
		glog.Warningf(
			"event=pod_quota_read_failed namespace=%s assignee=%s priority_class=%s err=%v",
			namespace, config.Assignee, config.PriorityClassName, err,
		)
		return 0, quotaReadFailed
	}
	state := quotaUnbounded
	if bounded {
		state = quotaBounded
	}
	if quota < 0 {
		return 0, state
	}
	return quota, state
}
