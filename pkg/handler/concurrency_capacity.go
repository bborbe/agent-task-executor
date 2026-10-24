// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	libk8s "github.com/bborbe/k8s"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// effectiveConcurrencyCap returns the admission bound: the smaller positive
// value of the declared cap and the pod quota. Zero means uncapped. A zero
// declared cap does NOT disable the quota bound; a zero quota does not disable
// the declared cap.
func effectiveConcurrencyCap(maxConcurrentJobs int, podQuota int) int {
	if maxConcurrentJobs < 0 {
		maxConcurrentJobs = 0
	}
	if podQuota < 0 {
		podQuota = 0
	}
	switch {
	case maxConcurrentJobs == 0:
		return podQuota
	case podQuota == 0:
		return maxConcurrentJobs
	case podQuota < maxConcurrentJobs:
		return podQuota
	default:
		return maxConcurrentJobs
	}
}

// podQuotaFor returns the pod quota that bounds admission for config, or 0 when
// no limiter is attached, the Config names no priority class, or the read fails.
// A failed read logs exactly one warning and must never block the spawn on its
// own.
func (h *taskEventHandler) podQuotaFor(
	ctx context.Context,
	namespace libk8s.Namespace,
	config *pkg.AgentConfiguration,
) int {
	if h.podQuotaLimiter == nil || config.PriorityClassName == "" {
		return 0
	}
	quota, err := h.podQuotaLimiter.PodQuota(ctx, namespace, config.PriorityClassName)
	if err != nil {
		glog.Warningf(
			"event=pod_quota_read_failed namespace=%s assignee=%s priority_class=%s err=%v",
			namespace, config.Assignee, config.PriorityClassName, err,
		)
		return 0
	}
	if quota < 0 {
		return 0
	}
	return quota
}
