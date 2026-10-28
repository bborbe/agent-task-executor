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
	taskcmd "github.com/bborbe/agent/command/task"
	"github.com/bborbe/cqrs/base"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// buildTriggerRefusalMsg stamps the vault the handler owns and encodes the task.
// The stamp is load-bearing: an unstamped task is dropped as foreign before the
// budget gate ever runs.
func buildTriggerRefusalMsg(task lib.Task) *sarama.ConsumerMessage {
	if task.Vault == "" {
		task.Vault = testTaskVault
	}
	value, err := json.Marshal(task)
	Expect(err).To(BeNil())
	return &sarama.ConsumerMessage{Value: value}
}

// capturingSyncProducer implements libkafka.SyncProducer and records sent
// messages. Copied from pkg/result_publisher_test.go — a different package, so
// it cannot be imported.
type capturingSyncProducer struct {
	messages []*sarama.ProducerMessage
}

func (c *capturingSyncProducer) SendMessage(
	_ context.Context,
	msg *sarama.ProducerMessage,
) (int32, int64, error) {
	c.messages = append(c.messages, msg)
	return 0, 0, nil
}

func (c *capturingSyncProducer) SendMessages(
	_ context.Context,
	msgs []*sarama.ProducerMessage,
) error {
	c.messages = append(c.messages, msgs...)
	return nil
}

func (c *capturingSyncProducer) Close() error { return nil }

var _ libkafka.SyncProducer = &capturingSyncProducer{}

// decodeUpdateFrontmatterCommand extracts the operation and
// UpdateFrontmatterCommand from a captured message.
func decodeUpdateFrontmatterCommand(
	msg *sarama.ProducerMessage,
) (base.CommandOperation, taskcmd.UpdateFrontmatterCommand) {
	raw, err := msg.Value.Encode()
	Expect(err).NotTo(HaveOccurred())

	var command base.Command
	Expect(json.Unmarshal(raw, &command)).To(Succeed())

	dataBytes, err := json.Marshal(command.Data)
	Expect(err).NotTo(HaveOccurred())

	var cmd taskcmd.UpdateFrontmatterCommand
	Expect(json.Unmarshal(dataBytes, &cmd)).To(Succeed())

	return command.Operation, cmd
}

var _ = Describe("Trigger budget refusal write", func() {
	It(
		"publishes one trigger budget refusal carrying the four keys and a ## Trigger Budget section",
		func() {
			ctx := context.Background()
			producer := &capturingSyncProducer{}
			currentDateTime := libtime.NewCurrentDateTime()
			currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-28T11:00:00Z"))

			fakeSpawner := new(mocks.FakeJobSpawner)
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeResolver := &mocks.FakeConfigResolver{}
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"},
				nil,
			)
			fakeView := &mocks.FakeTaskView{}
			fakeView.ReadyReturns(true)
			fakeView.SnapshotReturns(nil)
			fakeView.GetReturns(lib.Task{}, false)

			publisher := pkg.NewResultPublisher(
				producer,
				base.TopicPrefix("develop"),
				currentDateTime,
				nil,
			)
			h := handler.NewTaskEventHandler(
				fakeSpawner,
				"dev",
				base.Branch("prod"),
				"",
				true,
				true,
				fakeResolver,
				publisher,
				pkg.NewTaskStore(),
				currentDateTime,
				fakeView,
				testTaskVault,
				handler.DefaultReconcilePassTimeout,
				handler.DefaultReconcileInterval,
				pkg.ClaimModeOff,
				nil,
			)

			buildMsg := func(task lib.Task) *sarama.ConsumerMessage {
				if task.Vault == "" {
					task.Vault = testTaskVault
				}
				value, err := json.Marshal(task)
				Expect(err).To(BeNil())
				return &sarama.ConsumerMessage{Value: value}
			}

			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-trigger-refusal"),
				Vault:          testTaskVault,
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         "planning",
					"trigger_scope": "planning:4963ffa6",
					"ref":           "4963ffa6aaaaaaaaaaaaaaaa",
					"trigger_count": 4,
					"max_triggers":  3,
					"assignee":      "claude",
					"stage":         "prod",
				},
			}

			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

			Expect(producer.messages).To(HaveLen(1))
			operation, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(
				string(operation),
			).To(Equal(string(taskcmd.UpdateFrontmatterCommandOperation)))
			// No frontmatter keys: vault-api's typedTaskFields allowlist refuses
			// the whole command when any key is outside it, and the
			// trigger_refusal_* names are not in it. The numbers live in the body.
			Expect(cmd.Updates).To(BeEmpty())

			Expect(cmd.Body).NotTo(BeNil())
			Expect(cmd.Body.Heading).To(Equal("## Trigger Budget"))
			Expect(cmd.Body.Heading).NotTo(Equal("## Failure"))
			Expect(cmd.Body.Section).To(ContainSubstring("trigger_count 4 >= max_triggers 3"))
			Expect(cmd.Body.Section).To(ContainSubstring("planning:4963ffa6"))
		},
	)

	// The consumer path must swallow a refusal-write failure rather than return
	// it: ConsumeMessage's error cancels every run func, restarting the process
	// for every assignee at once. Without this spec a regression that made the
	// branch fatal would still pass CI, because every other spec asserts the
	// success path.
	It("keeps a failed refusal write non-fatal on the consumer path", func() {
		ctx := context.Background()
		fakeSpawner := new(mocks.FakeJobSpawner)
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeResolver := &mocks.FakeConfigResolver{}
		fakeResolver.ResolveReturns(
			pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"},
			nil,
		)
		fakeView := &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		fakeResultPublisher := new(mocks.FakeResultPublisher)
		fakeResultPublisher.PublishTriggerBudgetRefusalReturns(
			stderrors.New("kafka unavailable"),
		)

		h := handler.NewTaskEventHandler(
			fakeSpawner,
			"dev",
			base.Branch("prod"),
			"",
			true,
			true,
			fakeResolver,
			fakeResultPublisher,
			pkg.NewTaskStore(),
			libtime.NewCurrentDateTime(),
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeOff,
			nil,
		)

		task := lib.Task{
			TaskIdentifier: lib.TaskIdentifier("test-task-refusal-write-failure"),
			Vault:          testTaskVault,
			Frontmatter: lib.TaskFrontmatter{
				"status":        "in_progress",
				"phase":         "planning",
				"trigger_scope": "planning:4963ffa6",
				"ref":           "4963ffa6aaaaaaaaaaaaaaaa",
				"trigger_count": 4,
				"max_triggers":  3,
				"assignee":      "claude",
				"stage":         "prod",
			},
		}

		before := testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("event_refusal_write_failed"),
		)

		Expect(h.ConsumeMessage(ctx, buildTriggerRefusalMsg(task))).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		Expect(fakeResultPublisher.PublishTriggerBudgetRefusalCallCount()).To(Equal(1))
		Expect(
			testutil.ToFloat64(
				metrics.TaskEventsTotal.WithLabelValues("event_refusal_write_failed"),
			) - before,
		).To(Equal(float64(1)))
	})
})
