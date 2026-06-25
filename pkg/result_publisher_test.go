// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent/lib"
	taskcmd "github.com/bborbe/agent/lib/command/task"
	"github.com/bborbe/cqrs/base"
	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg"
)

// capturingSyncProducer implements libkafka.SyncProducer and records sent messages.
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

// failingSyncProducer implements libkafka.SyncProducer and always returns an error.
type failingSyncProducer struct {
	err error
}

func (f *failingSyncProducer) SendMessage(
	_ context.Context,
	_ *sarama.ProducerMessage,
) (int32, int64, error) {
	return 0, 0, f.err
}

func (f *failingSyncProducer) SendMessages(
	_ context.Context,
	_ []*sarama.ProducerMessage,
) error {
	return f.err
}

func (f *failingSyncProducer) Close() error { return nil }

var _ libkafka.SyncProducer = &failingSyncProducer{}

// partialFailingSyncProducer succeeds for the first `successCount` sends, then
// returns `err` on every subsequent send. Captures all attempted messages
// (including the ones that failed) so tests can assert exact send counts.
type partialFailingSyncProducer struct {
	successCount int
	calls        int
	err          error
	messages     []*sarama.ProducerMessage
}

func (p *partialFailingSyncProducer) SendMessage(
	_ context.Context,
	msg *sarama.ProducerMessage,
) (int32, int64, error) {
	p.calls++
	p.messages = append(p.messages, msg)
	if p.calls > p.successCount {
		return 0, 0, p.err
	}
	return 0, 0, nil
}

func (p *partialFailingSyncProducer) SendMessages(
	_ context.Context,
	msgs []*sarama.ProducerMessage,
) error {
	p.calls++
	p.messages = append(p.messages, msgs...)
	if p.calls > p.successCount {
		return p.err
	}
	return nil
}

func (p *partialFailingSyncProducer) Close() error { return nil }

var _ libkafka.SyncProducer = &partialFailingSyncProducer{}

// decodeUpdateFrontmatterCommand extracts the operation and UpdateFrontmatterCommand from a captured message.
func decodeUpdateFrontmatterCommand(
	msg *sarama.ProducerMessage,
) (base.CommandOperation, taskcmd.UpdateFrontmatterCommand) {
	raw, err := msg.Value.Encode()
	Expect(err).NotTo(HaveOccurred())

	var command base.Command
	Expect(json.Unmarshal(raw, &command)).To(Succeed())

	// Re-marshal the Event data and unmarshal into UpdateFrontmatterCommand.
	dataBytes, err := json.Marshal(command.Data)
	Expect(err).NotTo(HaveOccurred())

	var cmd taskcmd.UpdateFrontmatterCommand
	Expect(json.Unmarshal(dataBytes, &cmd)).To(Succeed())

	return command.Operation, cmd
}

// decodeIncrementFrontmatterCommand extracts the operation and IncrementFrontmatterCommand from a captured message.
func decodeIncrementFrontmatterCommand(
	msg *sarama.ProducerMessage,
) (base.CommandOperation, taskcmd.IncrementFrontmatterCommand) {
	raw, err := msg.Value.Encode()
	Expect(err).NotTo(HaveOccurred())

	var command base.Command
	Expect(json.Unmarshal(raw, &command)).To(Succeed())

	dataBytes, err := json.Marshal(command.Data)
	Expect(err).NotTo(HaveOccurred())

	var cmd taskcmd.IncrementFrontmatterCommand
	Expect(json.Unmarshal(dataBytes, &cmd)).To(Succeed())

	return command.Operation, cmd
}

var _ = Describe("ResultPublisher", func() {
	var (
		ctx             context.Context
		publisher       pkg.ResultPublisher
		currentDateTime libtime.CurrentDateTime
		producer        *capturingSyncProducer
	)

	BeforeEach(func() {
		ctx = context.Background()
		currentDateTime = libtime.NewCurrentDateTime()
		currentDateTime.SetNow(libtimetest.ParseDateTime("2026-04-18T12:00:00Z"))
		producer = &capturingSyncProducer{}
		publisher = pkg.NewResultPublisher(
			producer,
			base.Branch("prod"),
			currentDateTime,
		)
	})

	Describe("PublishSpawnNotification", func() {
		It("sends exactly three keys via UpdateFrontmatterCommand", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-1"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         "ai_review",
					"assignee":      "claude",
					"trigger_count": 1,
				},
				Content: lib.TaskContent("do the work"),
			}
			err := publisher.PublishSpawnNotification(ctx, task, "claude-20260418120000")
			Expect(err).NotTo(HaveOccurred())

			Expect(producer.messages).To(HaveLen(1))
			operation, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])

			Expect(string(operation)).To(Equal(string(taskcmd.UpdateFrontmatterCommandOperation)))
			Expect(cmd.Updates).To(HaveLen(3))

			Expect(cmd.Updates["spawn_notification"]).To(Equal(true))
			Expect(cmd.Updates["current_job"]).To(Equal("claude-20260418120000"))
			Expect(cmd.Updates["job_started_at"]).To(Equal("2026-04-18T12:00:00Z"))

			_, hasTriggerCount := cmd.Updates["trigger_count"]
			Expect(hasTriggerCount).To(BeFalse(), "trigger_count must not be in spawn notification")
			_, hasStatus := cmd.Updates["status"]
			Expect(hasStatus).To(BeFalse(), "status must not be in spawn notification")
			_, hasPhase := cmd.Updates["phase"]
			Expect(hasPhase).To(BeFalse(), "phase must not be in spawn notification")
		})
	})

	Describe("PublishFailure", func() {
		It(
			"publishes two commands: UpdateFrontmatterCommand clearing current_job with ## Failure body, then IncrementFrontmatterCommand bumping trigger_count",
			func() {
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-2"),
					Frontmatter: lib.TaskFrontmatter{
						"status":        "in_progress",
						"phase":         "ai_review",
						"assignee":      "claude",
						"trigger_count": 2,
					},
					Content: lib.TaskContent("do the work"),
				}
				err := publisher.PublishFailure(
					ctx,
					task,
					"claude-20260418120000",
					"pod OOM killed",
				)
				Expect(err).NotTo(HaveOccurred())

				Expect(producer.messages).To(HaveLen(2))

				// First message: UpdateFrontmatterCommand
				operation, updateCmd := decodeUpdateFrontmatterCommand(producer.messages[0])
				Expect(
					string(operation),
				).To(Equal(string(taskcmd.UpdateFrontmatterCommandOperation)))
				Expect(updateCmd.Updates).To(HaveLen(1))
				Expect(updateCmd.Updates["current_job"]).To(Equal(""))

				_, hasStatus := updateCmd.Updates["status"]
				Expect(hasStatus).To(BeFalse(), "status must not be in failure update")
				_, hasPhase := updateCmd.Updates["phase"]
				Expect(hasPhase).To(BeFalse(), "phase must not be in failure update")
				_, hasAssignee := updateCmd.Updates["assignee"]
				Expect(hasAssignee).To(BeFalse(), "assignee must not be in failure update")
				_, hasPreviousAssignee := updateCmd.Updates["previous_assignee"]
				Expect(
					hasPreviousAssignee,
				).To(BeFalse(), "previous_assignee must not be in failure update")
				_, hasTriggerCount := updateCmd.Updates["trigger_count"]
				Expect(hasTriggerCount).To(BeFalse(), "trigger_count must not be in failure update")

				Expect(updateCmd.Body).NotTo(BeNil())
				Expect(updateCmd.Body.Heading).To(Equal("## Failure"))
				Expect(updateCmd.Body.Section).To(ContainSubstring("2026-04-18T12:00:00Z"))
				Expect(updateCmd.Body.Section).To(ContainSubstring("claude-20260418120000"))
				Expect(updateCmd.Body.Section).To(ContainSubstring("pod OOM killed"))

				// Second message: IncrementFrontmatterCommand
				incOperation, incCmd := decodeIncrementFrontmatterCommand(producer.messages[1])
				Expect(
					string(incOperation),
				).To(Equal(string(taskcmd.IncrementFrontmatterCommandOperation)))
				Expect(string(incCmd.TaskIdentifier)).To(Equal("test-task-2"))
				Expect(incCmd.Field).To(Equal("trigger_count"))
				Expect(incCmd.Delta).To(Equal(1))
			},
		)
	})

	Describe("PublishFailure dedupe", func() {
		It("suppresses a second call with the same job name", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-dedupe"),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
				},
				Content: lib.TaskContent("do the work"),
			}

			err := publisher.PublishFailure(ctx, task, "claude-20260418120000", "pod OOM killed")
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.messages).To(HaveLen(2))

			err = publisher.PublishFailure(ctx, task, "claude-20260418120000", "pod OOM killed")
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.messages).To(HaveLen(2), "second call should be deduped")
		})

		It(
			"does NOT record dedupe when increment publish fails, so next cycle retries both messages",
			func() {
				partialProducer := &partialFailingSyncProducer{
					successCount: 1, // first send (update) succeeds, second (increment) fails
					err:          errors.New(context.Background(), "kafka: leader not available"),
				}
				partialPublisher := pkg.NewResultPublisher(
					partialProducer,
					base.Branch("prod"),
					currentDateTime,
				)

				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-partial"),
					Frontmatter: lib.TaskFrontmatter{
						"status": "in_progress",
					},
					Content: lib.TaskContent("do the work"),
				}

				// First call: update commits, increment fails — caller sees the increment error.
				err := partialPublisher.PublishFailure(
					ctx,
					task,
					"claude-20260418120000",
					"pod OOM killed",
				)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("trigger_count increment"))
				Expect(
					partialProducer.messages,
				).To(HaveLen(2), "both update and increment were attempted")

				// Second call with the same jobName: dedupe must NOT suppress it,
				// because the increment failed last time. The publish is attempted
				// again — verified by the producer recording at least one more
				// message (this producer's state means the update also fails on
				// the retry, but the key invariant is: not deduped to zero sends).
				err = partialPublisher.PublishFailure(
					ctx,
					task,
					"claude-20260418120000",
					"pod OOM killed",
				)
				Expect(err).To(HaveOccurred())
				Expect(
					len(partialProducer.messages),
				).To(BeNumerically(">", 2), "second call must re-attempt publishing (not deduped)")
			},
		)

		It(
			"allows re-send after dedupeTTL expires",
			func() {
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-ttl"),
					Frontmatter: lib.TaskFrontmatter{
						"status": "in_progress",
					},
					Content: lib.TaskContent("do the work"),
				}

				err := publisher.PublishFailure(
					ctx,
					task,
					"claude-20260418120000",
					"pod OOM killed",
				)
				Expect(err).NotTo(HaveOccurred())
				Expect(producer.messages).To(HaveLen(2))

				// Advance past dedupeTTL (3600s).
				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-04-18T13:00:01Z"))

				err = publisher.PublishFailure(
					ctx,
					task,
					"claude-20260418120000",
					"pod OOM killed",
				)
				Expect(err).NotTo(HaveOccurred())
				Expect(
					producer.messages,
				).To(HaveLen(4), "second call after TTL expiry must publish both messages again")
			},
		)

		It(
			"does NOT record dedupe when the first (update) publish fails and does not attempt the increment",
			func() {
				// successCount: 0 — first send (update) fails immediately.
				partialProducer := &partialFailingSyncProducer{
					successCount: 0,
					err:          errors.New(context.Background(), "kafka: leader not available"),
				}
				partialPublisher := pkg.NewResultPublisher(
					partialProducer,
					base.Branch("prod"),
					currentDateTime,
				)

				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-first-fail"),
					Frontmatter: lib.TaskFrontmatter{
						"status": "in_progress",
					},
					Content: lib.TaskContent("do the work"),
				}

				err := partialPublisher.PublishFailure(
					ctx,
					task,
					"claude-20260418120000",
					"pod OOM killed",
				)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("zombie failure update"))
				Expect(
					partialProducer.messages,
				).To(HaveLen(1), "only the update was attempted; increment must not run after update fails")

				// Verify dedupe was NOT recorded: second call attempts publishing again.
				err = partialPublisher.PublishFailure(
					ctx,
					task,
					"claude-20260418120000",
					"pod OOM killed",
				)
				Expect(err).To(HaveOccurred())
				Expect(
					partialProducer.messages,
				).To(HaveLen(2), "second call re-attempts the update (dedupe was not recorded)")
			},
		)
	})

	Describe("PublishTypeMismatchFailure", func() {
		It(
			"publishes assignee='', previous_assignee=<prior>, current_job='' and Assignee bullet in body",
			func() {
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-3"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "in_progress",
						"phase":    "planning",
						"assignee": "agent-pr-reviewer",
					},
				}
				err := publisher.PublishTypeMismatchFailure(
					ctx,
					task,
					`task_type "healthcheck" not in effective set [pr-review] of agent "agent-pr-reviewer"`,
				)
				Expect(err).NotTo(HaveOccurred())

				Expect(producer.messages).To(HaveLen(1))
				operation, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])

				Expect(
					string(operation),
				).To(Equal(string(taskcmd.UpdateFrontmatterCommandOperation)))
				Expect(cmd.Updates).To(HaveLen(3))
				Expect(cmd.Updates["assignee"]).To(Equal(""))
				Expect(cmd.Updates["previous_assignee"]).To(Equal("agent-pr-reviewer"))
				Expect(cmd.Updates["current_job"]).To(Equal(""))

				_, hasStatus := cmd.Updates["status"]
				Expect(hasStatus).To(BeFalse(), "status must not be in type mismatch update")
				_, hasPhase := cmd.Updates["phase"]
				Expect(hasPhase).To(BeFalse(), "phase must not be in type mismatch update")
				_, hasTriggerCount := cmd.Updates["trigger_count"]
				Expect(
					hasTriggerCount,
				).To(BeFalse(), "trigger_count must not be in type mismatch update")

				Expect(cmd.Body).NotTo(BeNil())
				Expect(cmd.Body.Heading).To(Equal("## Failure"))
				Expect(cmd.Body.Section).To(ContainSubstring("2026-04-18T12:00:00Z"))
				Expect(cmd.Body.Section).To(ContainSubstring("agent-pr-reviewer"))
				Expect(cmd.Body.Section).To(ContainSubstring("healthcheck"))
			},
		)

		It(
			"omits previous_assignee when prior assignee is empty",
			func() {
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-empty-assignee"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "in_progress",
						"phase":    "planning",
						"assignee": "",
					},
				}
				err := publisher.PublishTypeMismatchFailure(
					ctx,
					task,
					"reason=type_mismatch",
				)
				Expect(err).NotTo(HaveOccurred())

				Expect(producer.messages).To(HaveLen(1))
				_, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])

				Expect(cmd.Updates).To(HaveLen(2))
				Expect(cmd.Updates["assignee"]).To(Equal(""))
				Expect(cmd.Updates["current_job"]).To(Equal(""))

				_, hasPreviousAssignee := cmd.Updates["previous_assignee"]
				Expect(
					hasPreviousAssignee,
				).To(BeFalse(), "previous_assignee must be omitted when prior assignee is empty")

				Expect(cmd.Body).NotTo(BeNil())
				Expect(cmd.Body.Section).To(ContainSubstring("reason=type_mismatch"))
			},
		)
	})

	Describe("PublishIncrementTriggerCount", func() {
		It("sends IncrementFrontmatterCommand with trigger_count and delta 1", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-4"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         "planning",
					"trigger_count": 0,
				},
			}
			err := publisher.PublishIncrementTriggerCount(ctx, task)
			Expect(err).NotTo(HaveOccurred())

			Expect(producer.messages).To(HaveLen(1))
			operation, cmd := decodeIncrementFrontmatterCommand(producer.messages[0])

			Expect(
				string(operation),
			).To(Equal(string(taskcmd.IncrementFrontmatterCommandOperation)))
			Expect(string(cmd.TaskIdentifier)).To(Equal("test-task-4"))
			Expect(cmd.Field).To(Equal("trigger_count"))
			Expect(cmd.Delta).To(Equal(1))
		})

		It("returns error when sender fails", func() {
			failingProducer := &failingSyncProducer{
				err: errors.New(context.Background(), "kafka: leader not available"),
			}
			failingPublisher := pkg.NewResultPublisher(
				failingProducer,
				base.Branch("prod"),
				currentDateTime,
			)

			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-5"),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
				},
			}
			err := failingPublisher.PublishIncrementTriggerCount(ctx, task)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("kafka: leader not available"))
		})

		It("handles empty task identifier gracefully", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier(""),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
				},
			}
			err := publisher.PublishIncrementTriggerCount(ctx, task)
			Expect(err).NotTo(HaveOccurred())

			Expect(producer.messages).To(HaveLen(1))
			_, cmd := decodeIncrementFrontmatterCommand(producer.messages[0])

			Expect(string(cmd.TaskIdentifier)).To(Equal(""))
			Expect(cmd.Field).To(Equal("trigger_count"))
			Expect(cmd.Delta).To(Equal(1))
		})
	})

	Describe("PublishRaw", func() {
		It("returns wrapped error when base.ParseEvent fails", func() {
			// Pass an invalid JSON string to cause ParseEvent to fail
			invalidJSON := "{not valid json"
			err := publisher.PublishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, invalidJSON)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("parse event for operation"))
			Expect(err.Error()).To(ContainSubstring("update-frontmatter"))
		})
	})
})
