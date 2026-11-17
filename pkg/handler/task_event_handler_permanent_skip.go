// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	"github.com/golang/glog"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
	"github.com/bborbe/agent-task-executor/pkg/spawner"
)

// classifySpawnError is the consumer's last step after a spawn attempt. A
// deterministic validation failure (e.g. Kubernetes rejecting the Job name as a
// non-RFC-1123 label) can never succeed on redelivery: the same message produces
// the same error. Returning it would crash-loop the consumer and wedge every
// agent in the namespace behind one message, so it is recorded durably and
// skipped instead. Everything else — a transient API or vault error — keeps the
// offset unmarked and surfaces, so it is retried rather than silently dropped.
func (h *taskEventHandler) classifySpawnError(
	ctx context.Context,
	task lib.Task,
	msg *sarama.ConsumerMessage,
	err error,
) error {
	if isPermanentSpawnError(err) {
		return h.skipPermanentMessage(ctx, task, msg, err)
	}
	return err
}

// skipPermanentMessage records a provably unprocessable message durably and
// returns nil so the offset advances past it. It deliberately returns an error
// when the durable record cannot be written: skipping without a record is the
// silent loss SC2 forbids, so the consumer must keep failing loudly instead.
func (h *taskEventHandler) skipPermanentMessage(
	ctx context.Context,
	task lib.Task,
	msg *sarama.ConsumerMessage,
	cause error,
) error {
	if h.deadLetterPublisher == nil {
		glog.Errorf(
			"event=event_permanent_error_no_dead_letter task=%s err=%v: surfacing instead of skipping",
			task.TaskIdentifier,
			cause,
		)
		return cause
	}
	if err := h.deadLetterPublisher.Publish(ctx, msg, cause); err != nil {
		return errors.Wrapf(
			ctx, err, "publish dead letter for task %s", task.TaskIdentifier,
		)
	}
	glog.Warningf(
		"event=event_permanent_skip task=%s topic=%s partition=%d offset=%d err=%v: skipped after dead letter",
		task.TaskIdentifier,
		msg.Topic,
		msg.Partition,
		msg.Offset,
		cause,
	)
	metrics.TaskEventsTotal.WithLabelValues("skipped_permanent").Inc()
	metrics.DeadLetterTotal.WithLabelValues(msg.Topic).Inc()
	return nil
}

// isPermanentSpawnError reports whether err is a deterministic validation
// failure that redelivery cannot fix. It is intentionally narrow: only errors
// Kubernetes classifies as Invalid or BadRequest, plus the executor's own
// pre-flight refusals that depend on nothing but the task body, qualify — so a
// transient API, network or vault error is never mistaken for a permanent one.
func isPermanentSpawnError(err error) bool {
	if err == nil {
		return false
	}
	// An oversized TASK_CONTENT is decided entirely by the task body, which
	// redelivery does not change. Without this arm the refusal surfaces as a
	// consumer error: the offset stays unmarked, the executor restarts, and the
	// identical message is redelivered on every restart.
	if errors.Is(err, spawner.ErrTaskContentTooLarge) {
		return true
	}
	return apierrors.IsInvalid(err) || apierrors.IsBadRequest(err)
}
