// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"
	stderrors "errors"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// invalidJobNameError is the shape Kubernetes returns when it rejects a Job
// name as a non-RFC-1123 label — the deterministic class this task skips.
func invalidJobNameError() error {
	return apierrors.NewInvalid(
		schema.GroupKind{Group: "batch", Kind: "Job"},
		"sentry-collector-agent-EE07BA49-20261104071910",
		field.ErrorList{
			field.Invalid(
				field.NewPath("metadata", "name"),
				"sentry-collector-agent-EE07BA49-20261104071910",
				"a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-'",
			),
		},
	)
}

var _ = Describe("Permanent message skip", func() {
	const pastGrace = "2026-05-17T09:40:00Z"

	var (
		ctx         context.Context
		fakeSpawner *mocks.FakeJobSpawner
		fakeView    *mocks.FakeTaskView
		deadLetter  *mocks.FakeDeadLetterPublisher
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeSpawner = new(mocks.FakeJobSpawner)
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeView = &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		deadLetter = new(mocks.FakeDeadLetterPublisher)
	})

	build := func(opts ...handler.HandlerOption) handler.TaskEventHandler {
		fakeResolver := &mocks.FakeConfigResolver{}
		fakeResolver.ResolveReturns(pkg.AgentConfiguration{
			Assignee: "claude",
			Image:    "my-image:latest",
			Trigger: &agentv1.Trigger{
				Statuses: domain.TaskStatuses{domain.TaskStatusInProgress},
				Phases:   domain.TaskPhases{domain.TaskPhaseExecution},
			},
		}, nil)
		currentDateTime := libtime.NewCurrentDateTime()
		currentDateTime.SetNow(libtimetest.ParseDateTime(pastGrace))
		return handler.NewTaskEventHandler(
			fakeSpawner,
			"dev",
			base.Branch("prod"),
			"",
			true,
			true,
			fakeResolver,
			&mocks.FakeResultPublisher{},
			pkg.NewTaskStore(),
			currentDateTime,
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeOff,
			nil,
			opts...,
		)
	}

	task := func(id string) lib.Task {
		return lib.Task{
			TaskIdentifier: lib.TaskIdentifier(id),
			Vault:          testTaskVault,
			Frontmatter: lib.TaskFrontmatter{
				"status":        "in_progress",
				"phase":         string(domain.TaskPhaseExecution),
				"trigger_scope": string(domain.TaskPhaseExecution) + ":",
				"assignee":      "claude",
				"stage":         "prod",
			},
		}
	}

	message := func(t lib.Task) *sarama.ConsumerMessage {
		value, err := json.Marshal(t)
		Expect(err).ToNot(HaveOccurred())
		return &sarama.ConsumerMessage{
			Topic:     "develop-agent-task-v1-event",
			Partition: 3,
			Offset:    42,
			Value:     value,
		}
	}

	// SC1: the poison message is recorded durably and does not stop the
	// consumer; the valid message behind it is handled and the offset advances
	// past both (both calls return nil).
	It("records the poison message durably and still handles the valid one after it", func() {
		fakeSpawner.SpawnJobReturnsOnCall(0, "", invalidJobNameError())
		fakeSpawner.SpawnJobReturnsOnCall(1, "job-valid", nil)
		h := build(handler.WithDeadLetterPublisher(deadLetter))

		poison := message(task("tid-poison"))
		Expect(h.ConsumeMessage(ctx, poison)).To(Succeed())

		Expect(deadLetter.PublishCallCount()).To(Equal(1))
		_, capturedMsg, capturedCause := deadLetter.PublishArgsForCall(0)
		Expect(capturedMsg).To(BeIdenticalTo(poison))
		Expect(capturedCause.Error()).To(ContainSubstring("RFC 1123"))

		Expect(h.ConsumeMessage(ctx, message(task("tid-valid")))).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(2))
		Expect(
			deadLetter.PublishCallCount(),
		).To(Equal(1), "the valid message must not be dead-lettered")
	})

	// SC3: a failure on a valid message is not swallowed. The error surfaces and
	// the offset is not advanced, because ConsumeMessage returns non-nil.
	It("surfaces a transient spawn failure instead of dead-lettering it", func() {
		fakeSpawner.SpawnJobReturns("", stderrors.New("kube api unavailable"))
		h := build(handler.WithDeadLetterPublisher(deadLetter))

		err := h.ConsumeMessage(ctx, message(task("tid-transient")))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("kube api unavailable"))
		Expect(deadLetter.PublishCallCount()).To(BeZero())
	})

	// A permanent failure with nowhere to record it must NOT be skipped: that
	// would drop the message with no evidence, which is what SC2 forbids.
	It("surfaces a permanent failure when no dead-letter publisher is configured", func() {
		fakeSpawner.SpawnJobReturns("", invalidJobNameError())
		h := build()

		Expect(h.ConsumeMessage(ctx, message(task("tid-no-sink")))).NotTo(Succeed())
	})

	// If the durable record cannot be written, the message must not be skipped:
	// the consumer keeps failing loudly rather than losing it silently.
	It("surfaces the failure when the dead-letter publish itself fails", func() {
		fakeSpawner.SpawnJobReturns("", invalidJobNameError())
		deadLetter.PublishReturns(stderrors.New("kafka unavailable"))
		h := build(handler.WithDeadLetterPublisher(deadLetter))

		Expect(h.ConsumeMessage(ctx, message(task("tid-dlq-down")))).NotTo(Succeed())
	})

	It("counts the permanent skip", func() {
		fakeSpawner.SpawnJobReturns("", invalidJobNameError())
		h := build(handler.WithDeadLetterPublisher(deadLetter))

		before := testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("skipped_permanent"),
		)
		dlqBefore := testutil.ToFloat64(
			metrics.DeadLetterTotal.WithLabelValues("develop-agent-task-v1-event"),
		)
		Expect(h.ConsumeMessage(ctx, message(task("tid-metric")))).To(Succeed())
		Expect(
			testutil.ToFloat64(
				metrics.TaskEventsTotal.WithLabelValues("skipped_permanent"),
			) - before,
		).To(Equal(float64(1)))
		Expect(
			testutil.ToFloat64(
				metrics.DeadLetterTotal.WithLabelValues("develop-agent-task-v1-event"),
			) - dlqBefore,
		).To(Equal(float64(1)))
	})
})
