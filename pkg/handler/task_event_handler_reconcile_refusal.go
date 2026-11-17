// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	lib "github.com/bborbe/agent"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// refuseReconcile records that the reconcile eligibility filter refused task at
// gate and returns the filter's standard refusal tuple.
//
// The counter moves on every refusal, but the log line is emitted only when the
// task's gate differs from the last one recorded for it. Without that rule the
// reconcile loop would emit one line per task per tick — the same volume the
// filter was silent to avoid — while the change itself is the actionable event:
// a task that moved from status_not_triggered to terminal_phase tells an
// operator something happened, a task that stayed put does not.
//
// V(2) is the level the deployed executors run at, the same reason
// logPinOutsidePool uses it: the neighbouring stage skip is V(3) and invisible.
func (h *taskEventHandler) refuseReconcile(
	task lib.Task,
	gate string,
) (lib.Task, *pkg.AgentConfiguration, bool, error) {
	metrics.ReconcileRefusedTotal.WithLabelValues(gate).Inc()
	if h.recordReconcileRefusal(task.TaskIdentifier, gate) {
		glog.V(2).Infof(
			"event=reconcile_refused task=%s gate=%s task_type=%s phase=%s status=%s stage=%s cluster=%s",
			task.TaskIdentifier,
			gate,
			task.Frontmatter.TaskType(),
			reconcileRefusalPhase(task),
			task.Frontmatter.Status(),
			task.Frontmatter.Stage(),
			taskCluster(task),
		)
	}
	return task, nil, false, nil
}

// recordReconcileRefusal stores gate as the task's last refusing gate and
// reports whether it differs from the previously recorded one.
func (h *taskEventHandler) recordReconcileRefusal(
	id lib.TaskIdentifier,
	gate string,
) bool {
	h.refusedMu.Lock()
	defer h.refusedMu.Unlock()
	changed := h.refusedGates[id] != gate
	h.refusedGates[id] = gate
	return changed
}

// clearReconcileRefusal forgets the gate recorded for id, so a task that became
// a candidate and is later refused again logs that refusal afresh.
func (h *taskEventHandler) clearReconcileRefusal(id lib.TaskIdentifier) {
	h.refusedMu.Lock()
	defer h.refusedMu.Unlock()
	delete(h.refusedGates, id)
}

// reconcileRefusalPhase renders the task's optional phase for the refusal line,
// leaving it empty when the frontmatter carries none.
func reconcileRefusalPhase(task lib.Task) string {
	if phase := task.Frontmatter.Phase(); phase != nil {
		return string(*phase)
	}
	return ""
}
