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

// rejectInvalidJobNamespace reports whether the Config's Job namespace cannot
// be resolved. Such a Config is logged at ERROR and skipped before any gate or
// claim, so the task stays untouched until a human corrects it.
func (h *taskEventHandler) rejectInvalidJobNamespace(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
) bool {
	if config == nil {
		return false
	}
	if _, err := pkg.ResolveJobNamespace(ctx, h.namespace, config.JobNamespace); err != nil {
		glog.Errorf(
			"event=job_namespace_rejected task=%s config=%s jobNamespace=%q: %v",
			task.TaskIdentifier, config.Assignee, config.JobNamespace, err,
		)
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return true
	}
	return false
}
