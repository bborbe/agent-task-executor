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

// finishSpawn records a Job that was just created. The spawn notification is
// published BEFORE the trigger-counter write: a counter write that cannot be
// recorded is a real fault and still propagates, but it must not leave a running
// Job with an empty current_job on the record, which reads as unclaimed and has
// nothing that retries it. The Job is stored before either write, so the
// in-memory store knows the Job exists even when both fail.
func (h *taskEventHandler) finishSpawn(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
	jobName string,
	sessionID string,
) error {
	h.bumpSpawnGen(config.Assignee)
	h.storeSpawnedJob(task, jobName)

	if err := h.resultPublisher.PublishSpawnNotification(ctx, task, jobName, sessionID); err != nil {
		// Log but don't fail — job is already spawned, spawn notification is best-effort
		glog.Warningf("publish spawn notification for task %s failed (job %s still running): %v",
			task.TaskIdentifier, jobName, err)
	}

	if err := h.recordTriggerSpawn(ctx, task); err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		glog.Warningf("event=trigger_write_failed task=%s job=%s err=%v",
			task.TaskIdentifier, jobName, err)
		return err
	}

	glog.V(2).Infof(
		"spawned job for task %s (assignee=%s image=%s session_id=%s job=%s)",
		task.TaskIdentifier, task.Frontmatter.Assignee(), config.Image, sessionID, jobName,
	)
	metrics.TaskEventsTotal.WithLabelValues("spawned").Inc()
	metrics.JobsSpawnedTotal.WithLabelValues(h.spawnedNamespace(ctx, config)).Inc()
	h.recordTaskRerun(task)
	h.recordRedriveSpawn(task)
	return nil
}
