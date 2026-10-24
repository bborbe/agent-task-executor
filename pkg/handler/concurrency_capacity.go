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

// podQuotaFor returns the pod quota that bounds admission for config, and whether
// it bounds it at all. It reports no bound when no limiter is attached, the
// Config names no priority class, or the read fails — a failed read logs exactly
// one warning and must never block the spawn on its own.
func (h *taskEventHandler) podQuotaFor(
	ctx context.Context,
	namespace libk8s.Namespace,
	config *pkg.AgentConfiguration,
) (int, bool) {
	if h.podQuotaLimiter == nil || config.PriorityClassName == "" {
		return 0, false
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
		return 0, false
	}
	if quota < 0 {
		return 0, bounded
	}
	return quota, bounded
}
