// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/IBM/sarama"
	taskcmd "github.com/bborbe/agent/command/task"
	"github.com/bborbe/cqrs/base"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	"github.com/prometheus/client_golang/prometheus/testutil"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// capturingAbortSyncProducer records every command the real result publisher
// sends, so a spec can read the emission instead of asserting that a method was
// called on a fake.
type capturingAbortSyncProducer struct {
	messages []*sarama.ProducerMessage
}

func (c *capturingAbortSyncProducer) SendMessage(
	_ context.Context,
	msg *sarama.ProducerMessage,
) (int32, int64, error) {
	c.messages = append(c.messages, msg)
	return 0, 0, nil
}

func (c *capturingAbortSyncProducer) SendMessages(
	_ context.Context,
	msgs []*sarama.ProducerMessage,
) error {
	c.messages = append(c.messages, msgs...)
	return nil
}

func (c *capturingAbortSyncProducer) Close() error { return nil }

var _ libkafka.SyncProducer = &capturingAbortSyncProducer{}

// decodeAbortUpdateFrontmatterCommand extracts the operation and the
// UpdateFrontmatterCommand from a captured message.
func decodeAbortUpdateFrontmatterCommand(
	t *testing.T,
	msg *sarama.ProducerMessage,
) (base.CommandOperation, taskcmd.UpdateFrontmatterCommand) {
	t.Helper()
	raw, err := msg.Value.Encode()
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}
	var command base.Command
	if err := json.Unmarshal(raw, &command); err != nil {
		t.Fatalf("unmarshal command: %v", err)
	}
	dataBytes, err := json.Marshal(command.Data)
	if err != nil {
		t.Fatalf("marshal command data: %v", err)
	}
	var cmd taskcmd.UpdateFrontmatterCommand
	if err := json.Unmarshal(dataBytes, &cmd); err != nil {
		t.Fatalf("unmarshal update command: %v", err)
	}
	return command.Operation, cmd
}

// TestRedriveAbortedPathEmitsAbortedRecordAndCounter is the step-level proof
// behind the RedriveAborted alert rule
// (bborbe/nuke agent/alerts/redrive-aborted-alert.yaml — metadata.name:
// redrive-aborted, spec.name: RedriveAborted, expression:
// 'sum by (vault, reason) (increase(executor_redrive_aborted_total[1h])) > 0').
//
// The rule's subject is the reason label on executor_redrive_aborted_total. This
// spec drives that value from the handler's entry point — redriveStreakCapped,
// reached through driveCandidate on the reconcile funnel — through the REAL
// result publisher rather than a fake, and asserts both emissions the rule
// depends on: the counter series it selects on, and the UpdateFrontmatterCommand
// that closes the record so it is not stranded.
//
// The seam is the point. The handler's abort decision and the publisher's
// close-out write are each covered on their own (redrive_classifier_internal_test
// and result_publisher_test); neither proves the two are wired together, so a
// change that dropped the PublishAbort call, or emitted a different reason, would
// leave every existing spec green while the alert's counter stayed at zero.
func TestRedriveAbortedPathEmitsAbortedRecordAndCounter(t *testing.T) {
	producer := &capturingAbortSyncProducer{}
	currentDateTime := libtime.NewCurrentDateTime()
	publisher := pkg.NewResultPublisher(
		producer,
		base.TopicPrefix("develop"),
		currentDateTime,
		nil,
		nil,
	)
	// Three identical broken signatures at the cap: the class the alert's
	// broken-signature reason series counts.
	brokenReason := pkg.ZombieReasonPodOOMKilled.String()
	h, spawner := newDecisionHandler(t, maxConsecutiveRedrives, []string{
		brokenReason, brokenReason, brokenReason,
	}, publisher)

	abortedBefore := testutil.ToFloat64(
		metrics.RedriveAbortedTotal.WithLabelValues(
			metrics.RedriveAbortReasonBrokenSignature,
		),
	)
	transientBefore := testutil.ToFloat64(
		metrics.RedriveAbortedTotal.WithLabelValues(
			metrics.RedriveAbortReasonTransientExhausted,
		),
	)
	cappedBefore := testutil.ToFloat64(
		metrics.RedriveCappedTotal.WithLabelValues("", "execution"),
	)

	outcome := driveCapped(h)

	if outcome != reconcileOutcomeCapped {
		t.Fatalf("outcome = %v; want capped", outcome)
	}
	if spawner.spawned != 0 {
		t.Fatalf("spawned = %d; want 0", spawner.spawned)
	}

	// The emission: exactly one command, carrying the frozen close-out pair
	// (status + aborted_reason) plus gate_successor, which vault-cli requires
	// before it will accept an aborted status.
	assertAbortedCloseOutCommand(t, producer)
	// The counter the alert rule selects on.
	assertAbortedCounters(t, abortedBefore, transientBefore, cappedBefore)
}

// assertAbortedCloseOutCommand pins the record emission: exactly one command
// carrying the frozen close-out pair (status + aborted_reason) plus
// gate_successor, which vault-cli requires before it will accept an aborted
// status.
func assertAbortedCloseOutCommand(
	t *testing.T,
	producer *capturingAbortSyncProducer,
) {
	t.Helper()
	if len(producer.messages) != 1 {
		t.Fatalf("emitted messages = %d; want 1", len(producer.messages))
	}
	operation, cmd := decodeAbortUpdateFrontmatterCommand(t, producer.messages[0])
	if string(operation) != string(taskcmd.UpdateFrontmatterCommandOperation) {
		t.Fatalf("operation = %q; want %q",
			operation, taskcmd.UpdateFrontmatterCommandOperation)
	}
	if got := cmd.Updates["status"]; got != "aborted" {
		t.Fatalf("status = %v; want aborted", got)
	}
	if got := cmd.Updates["aborted_reason"]; got != metrics.RedriveAbortReasonBrokenSignature {
		t.Fatalf("aborted_reason = %v; want %s",
			got, metrics.RedriveAbortReasonBrokenSignature)
	}
	if got := cmd.Updates["gate_successor"]; got != "none" {
		t.Fatalf("gate_successor = %v; want none", got)
	}
	if got, ok := cmd.Updates["current_job"]; !ok || got != "" {
		t.Fatalf("current_job = %v (present=%v); want empty string", got, ok)
	}
}

// assertAbortedCounters pins the counter the alert rule selects on: the matching
// reason series moves, every other reason series stays put, and the refusal
// itself is still counted — the dashboard trend, which deliberately carries no
// alert rule.
func assertAbortedCounters(
	t *testing.T,
	abortedBefore, transientBefore, cappedBefore float64,
) {
	t.Helper()
	abortedAfter := testutil.ToFloat64(
		metrics.RedriveAbortedTotal.WithLabelValues(
			metrics.RedriveAbortReasonBrokenSignature,
		),
	)
	if abortedAfter != abortedBefore+1 {
		t.Fatalf("executor_redrive_aborted_total{reason=%q} = %v; want %v",
			metrics.RedriveAbortReasonBrokenSignature, abortedAfter, abortedBefore+1)
	}
	if got := testutil.ToFloat64(
		metrics.RedriveAbortedTotal.WithLabelValues(
			metrics.RedriveAbortReasonTransientExhausted,
		),
	); got != transientBefore {
		t.Fatalf("executor_redrive_aborted_total{reason=%q} = %v; want unchanged %v",
			metrics.RedriveAbortReasonTransientExhausted, got, transientBefore)
	}
	if got := testutil.ToFloat64(
		metrics.RedriveCappedTotal.WithLabelValues("", "execution"),
	); got != cappedBefore+1 {
		t.Fatalf("executor_redrive_capped_total{task_type=%q,phase=%q} = %v; want %v",
			"", "execution", got, cappedBefore+1)
	}
}
