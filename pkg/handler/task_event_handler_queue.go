// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// The queued_at stamp lives here rather than in task_event_handler.go, which is
// at the 2000-line lint ceiling. It is the deferral-path's own concern and
// shares nothing with the spawn decision beyond the handler receiver.

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/golang/glog"
)

// stampQueueEntry returns the task carrying a queued_at value, writing that
// value to the vault the first time the task is deferred behind the concurrency
// cap. queued_at is the moment the FIRST wait began: it is written once and never
// moved by a later re-deferral, so the field can be read as a queue-entry time.
//
// Four cases, in order:
//
//   - the task already carries queued_at (a re-deferred in-memory entry) — return
//     it unchanged and publish nothing.
//   - the task view already carries queued_at — return a copy carrying it. The
//     view is the authority here because a duplicate Kafka event can arrive after
//     the publish but before the controller has written the field; re-stamping
//     would move the timestamp forward.
//   - this executor already published queued_at for the task — return a copy
//     carrying that value and publish nothing. This is the case a missing task
//     file depends on: both checks above read state only a SUCCESSFUL vault write
//     updates, so for a task whose file was deleted (or whose write keeps failing)
//     neither ever becomes true and the deferral would stamp a fresh value on
//     every retry — once per concurrencyCapRetryDelay, indefinitely. Measured on
//     nukedev 2026-10-23: one update-frontmatter command per minute for a task id
//     with no file anywhere, each one a not-found at the controller.
//   - none of the above — stamp the injected clock's current UTC time, record it,
//     publish it, and return a copy carrying it.
//
// A failed publish is deliberately not retried. The returned copy still carries
// the value, so the next re-deferral sees it and publishes nothing: that wait
// reports no queue age rather than publishing a later, wrong one.
func (h *taskEventHandler) stampQueueEntry(ctx context.Context, task lib.Task) lib.Task {
	if v, ok := task.Frontmatter.String("queued_at"); ok && v != "" {
		return task
	}
	if v := h.queuedAtFromView(task.TaskIdentifier); v != "" {
		return taskWithFrontmatterValue(task, "queued_at", v)
	}
	if v := h.queuedAtStamp(task.TaskIdentifier); v != "" {
		return taskWithFrontmatterValue(task, "queued_at", v)
	}
	queuedAt := h.currentDateTime.Now().UTC().Format("2006-01-02T15:04:05Z07:00")
	h.recordQueuedAtStamp(task.TaskIdentifier, queuedAt)
	if err := h.resultPublisher.PublishQueueEntry(ctx, task, queuedAt); err != nil {
		glog.Warningf(
			"event=queued_at_publish_failed task=%s queued_at=%s err=%v",
			task.TaskIdentifier, queuedAt, err,
		)
	} else {
		glog.V(1).Infof(
			"event=queued_at_written task=%s queued_at=%s",
			task.TaskIdentifier, queuedAt,
		)
	}
	return taskWithFrontmatterValue(task, "queued_at", queuedAt)
}
