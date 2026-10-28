// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"flag"
	"io"
	"os"
	"strings"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	taskcmd "github.com/bborbe/agent/command/task"
	"github.com/bborbe/cqrs/base"
	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/mocks"
	"github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// captureGlogStderr runs fn with glog writing at V(2) to stderr and returns
// everything written. crashFailureUpdates logs at V(2), so the verbosity flag
// must be raised here too: without it the line is filtered out and an assertion
// would pass vacuously against an empty string. The suite runs serially, so the
// global os.Stderr swap cannot race another spec's logging.
func captureGlogStderr(fn func()) string {
	GinkgoHelper()
	Expect(flag.Set("logtostderr", "true")).To(Succeed())
	Expect(flag.Set("v", "2")).To(Succeed())
	defer func() {
		// Deliberately not Expect: this runs during the panic unwinding that
		// follows a failed spec assertion, and a second failure here would
		// replace the real one with "Test Panicked".
		_ = flag.Set("logtostderr", "false")
		_ = flag.Set("v", "0")
	}()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	Expect(err).ToNot(HaveOccurred())
	os.Stderr = writer
	// Deferred, not sequential: fn() calls PublishFailure with Expect inside it,
	// and a failed Expect panics. A sequential restore would then be skipped and
	// the pipe would stay installed as the process stderr for the rest of the
	// suite, so the failure diagnostics are exactly what disappears.
	defer func() { os.Stderr = original }()

	fn()
	glog.Flush()

	Expect(writer.Close()).To(Succeed())
	out, err := io.ReadAll(reader)
	Expect(err).ToNot(HaveOccurred())
	Expect(reader.Close()).To(Succeed())
	return string(out)
}

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
			base.TopicPrefix("develop"),
			currentDateTime,
			nil,
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

	Describe("PublishQueueEntry", func() {
		It("sends exactly one queued_at key via UpdateFrontmatterCommand", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-queued"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    "execution",
					"assignee": "claude",
				},
			}
			err := publisher.PublishQueueEntry(ctx, task, "2026-05-17T09:34:00Z")
			Expect(err).NotTo(HaveOccurred())

			Expect(producer.messages).To(HaveLen(1))
			operation, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])

			Expect(string(operation)).To(
				Equal(string(taskcmd.UpdateFrontmatterCommandOperation)),
			)
			Expect(cmd.Updates).To(HaveLen(1))
			Expect(cmd.Updates["queued_at"]).To(Equal("2026-05-17T09:34:00Z"))
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

	Describe("PublishStoreMissFailure", func() {
		// A failure section under a terminal status reads as a status change that
		// never happened, so this path states its own reason for existing. The
		// four clauses are the ones a later reader needs, and each is asserted
		// separately so dropping one fails here rather than in the field.
		It("states why a record that was already terminal carries a failure", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-3"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "completed",
					"phase":    "done",
					"assignee": "github-update-go-agent",
				},
				Content: lib.TaskContent("update go"),
			}
			err := publisher.PublishStoreMissFailure(
				ctx,
				task,
				"github-update-go-agent-ad12e9fb-20261028065801",
				"pod_crash_no_stdout",
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.messages).To(HaveLen(2))

			_, updateCmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(updateCmd.Body.Heading).To(Equal("## Failure"))
			section := updateCmd.Body.Section
			Expect(section).To(ContainSubstring("pod_crash_no_stdout"))
			Expect(section).To(
				ContainSubstring("had already reached its terminal status"),
				"the entry must say the record was already terminal",
			)
			Expect(section).To(
				ContainSubstring("The Job crashed before producing any output"),
				"the entry must say the job crashed",
			)
			Expect(section).To(
				ContainSubstring("would otherwise be silent"),
				"the entry must say why it exists",
			)
			Expect(section).To(
				ContainSubstring("The status above is unchanged"),
				"the entry must say the status did not change",
			)

			// The note is scoped to this path: the store-hit failure must not
			// claim the record was already terminal.
			err = publisher.PublishFailure(
				ctx,
				task,
				"github-update-go-agent-00000000000000",
				"pod_crash_no_stdout",
			)
			Expect(err).NotTo(HaveOccurred())
			_, hitCmd := decodeUpdateFrontmatterCommand(producer.messages[2])
			Expect(hitCmd.Body.Section).NotTo(ContainSubstring("The status above is unchanged"))
		})

		// The full note is true only of the crash class. A deadline_exceeded or
		// pod_oom_killed Job reaches this path too and may well have produced
		// output, and a store evicted by a restart can leave the record still
		// in_progress — so the note must not assert a crash or a terminal record
		// for those reasons.
		It("claims only what the path guarantees for a non-crash reason", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-4"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    "planning",
					"assignee": "github-update-go-agent",
				},
				Content: lib.TaskContent("update go"),
			}
			err := publisher.PublishStoreMissFailure(
				ctx,
				task,
				"github-update-go-agent-e009d201-20261028140856",
				"deadline_exceeded",
			)
			Expect(err).NotTo(HaveOccurred())

			_, updateCmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			section := updateCmd.Body.Section
			Expect(section).To(ContainSubstring("deadline_exceeded"))
			Expect(section).To(
				ContainSubstring("was not in the executor's in-memory store"),
				"the entry must say why it exists",
			)
			Expect(section).To(
				ContainSubstring("The status above is unchanged"),
				"the entry must say the status did not change",
			)
			Expect(section).NotTo(
				ContainSubstring("had already reached its terminal status"),
				"a non-crash store miss must not claim the record was already terminal",
			)
			Expect(section).NotTo(
				ContainSubstring("crashed before producing any output"),
				"a non-crash store miss must not claim the Job produced no output",
			)
		})
	})

	Describe("PublishFailure trigger cap", func() {
		// The cap is armed only for the crash class, and only when the key is
		// absent. Both halves are load-bearing: arming it for every class would
		// silently cap the seven classes the deliverer does reach, and clobbering
		// a present value would halve a deliberate operator budget.

		It("arms max_triggers=3 for the pod_crash_no_stdout class", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-crash"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         "planning",
					"assignee":      "pr-reviewer-agent",
					"trigger_count": 1,
				},
				Content: lib.TaskContent("do the work"),
			}
			err := publisher.PublishFailure(
				ctx,
				task,
				"claude-20260418120000",
				"pod_crash_no_stdout",
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.messages).To(HaveLen(2))

			_, updateCmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(updateCmd.Updates).To(HaveLen(2))
			Expect(updateCmd.Updates["current_job"]).To(Equal(""))
			// BeNumerically, not Equal(3): the assertion reads the command back
			// through its JSON encoding, where every number decodes as float64.
			Expect(updateCmd.Updates["max_triggers"]).To(BeNumerically("==", 3))

			_, hasAssignee := updateCmd.Updates["assignee"]
			Expect(hasAssignee).To(BeFalse(), "assignee is the controller's to clear, not ours")
		})

		It("arms the cap for a suffixed crash reason, proving the parser gate not string equality",
			func() {
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-crash-suffixed"),
					Frontmatter: lib.TaskFrontmatter{
						"status": "in_progress",
					},
					Content: lib.TaskContent("do the work"),
				}
				err := publisher.PublishFailure(
					ctx,
					task,
					"claude-20260418120000",
					"pod_crash_no_stdout (Error/1)",
				)
				Expect(err).NotTo(HaveOccurred())

				_, updateCmd := decodeUpdateFrontmatterCommand(producer.messages[0])
				Expect(updateCmd.Updates).To(HaveLen(2))
				Expect(updateCmd.Updates["max_triggers"]).To(BeNumerically("==", 3))
			},
		)

		DescribeTable("leaves every other class uncapped",
			func(reason string) {
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-other-class"),
					Frontmatter: lib.TaskFrontmatter{
						"status": "in_progress",
					},
					Content: lib.TaskContent("do the work"),
				}
				err := publisher.PublishFailure(ctx, task, "claude-20260418120000", reason)
				Expect(err).NotTo(HaveOccurred())

				_, updateCmd := decodeUpdateFrontmatterCommand(producer.messages[0])
				Expect(updateCmd.Updates).To(HaveLen(1))
				Expect(updateCmd.Updates["current_job"]).To(Equal(""))
				_, hasMax := updateCmd.Updates["max_triggers"]
				Expect(hasMax).To(BeFalse(), "reason %q must not arm the cap", reason)
			},
			Entry("pod_oom_killed", "pod_oom_killed"),
			Entry("pod_oom_killed with suffix", "pod_oom_killed (OOMKilled/137)"),
			Entry("pod_evicted", "pod_evicted"),
			Entry("image_pull_backoff", "image_pull_backoff"),
			Entry("executor_watch_lost", "executor_watch_lost"),
			Entry("type_mismatch", "type_mismatch"),
			Entry("unknown reason", "something nobody classified"),
			Entry("empty reason", ""),
		)

		It("preserves an operator-set max_triggers instead of clobbering it", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-operator-cap"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"max_triggers":  6,
					"trigger_count": 1,
				},
				Content: lib.TaskContent("do the work"),
			}
			err := publisher.PublishFailure(
				ctx,
				task,
				"claude-20260418120000",
				"pod_crash_no_stdout",
			)
			Expect(err).NotTo(HaveOccurred())

			_, updateCmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(updateCmd.Updates).To(HaveLen(1))
			_, hasMax := updateCmd.Updates["max_triggers"]
			Expect(hasMax).To(BeFalse(), "a deliberate budget of 6 must not become 3")
		})

		It("leaves max_triggers alone when the key is present but unknown (nil)", func() {
			// The job watcher publishes a failure for a task that has left its
			// store with max_triggers present but nil: the real value is unknown,
			// and writing the default could lower an operator-set cap.
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-store-miss"),
				Frontmatter:    lib.TaskFrontmatter{"max_triggers": nil},
			}
			err := publisher.PublishFailure(
				ctx,
				task,
				"claude-20260418120000",
				"pod_crash_no_stdout (ContainerStatusUnknown/137)",
			)
			Expect(err).NotTo(HaveOccurred())

			_, updateCmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(updateCmd.Updates).To(HaveLen(1))
			Expect(updateCmd.Updates["current_job"]).To(Equal(""))
		})

		// A record that reaches the controller without max_triggers is
		// indistinguishable from one whose class never matched, so the log line
		// has to carry the inputs the decision tested rather than its verdict.
		// These two specs are what pin that: the early return names the cached
		// value it saw, and the arming path names the absence it acted on.
		It("logs the values it tested when it returns early on an already-present max_triggers",
			func() {
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-log-early-return"),
					Frontmatter: lib.TaskFrontmatter{
						"status":       "in_progress",
						"max_triggers": 6,
					},
					Content: lib.TaskContent("do the work"),
				}

				out := captureGlogStderr(func() {
					err := publisher.PublishFailure(
						ctx, task, "pr-reviewer-agent-1234", "pod_crash_no_stdout",
					)
					Expect(err).NotTo(HaveOccurred())
				})

				Expect(out).To(ContainSubstring("event=crash_arm_decision task="))
				Expect(out).To(ContainSubstring("decision=max_triggers_already_present"))
				Expect(out).To(ContainSubstring("task=test-task-log-early-return"))
				Expect(out).To(ContainSubstring("job=pr-reviewer-agent-1234"))
				Expect(out).To(ContainSubstring(`reason="pod_crash_no_stdout"`))
				Expect(out).To(ContainSubstring(`parsed="pod_crash_no_stdout"`))
				Expect(out).To(ContainSubstring("parsed_ok=true"))
				Expect(out).To(ContainSubstring("cached_max_triggers=6"))
				// Exactly once, so a doubled log call or a second emitting path
				// cannot pass on presence alone.
				Expect(strings.Count(out, "event=crash_arm_decision ")).To(Equal(1))
			},
		)

		It("logs the tested values on the arming path too", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-log-arm"),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
				},
				Content: lib.TaskContent("do the work"),
			}

			out := captureGlogStderr(func() {
				err := publisher.PublishFailure(
					ctx, task, "pr-reviewer-agent-5678", "pod_crash_no_stdout",
				)
				Expect(err).NotTo(HaveOccurred())
			})

			Expect(out).To(ContainSubstring("event=crash_arm_decision task="))
			Expect(out).To(ContainSubstring("decision=arm cached_max_triggers=<absent>"))
			Expect(out).To(ContainSubstring("task=test-task-log-arm"))
			// Leading space anchors the field boundary: without it this is also a
			// substring of "cached_max_triggers=3" and would silently start
			// accepting the early-return branch if the line above were removed.
			Expect(out).To(ContainSubstring(" max_triggers=3"))
			Expect(strings.Count(out, "event=crash_arm_decision ")).To(Equal(1))
		})

		It("logs a rejected class with its parsed value so a miss is not silent", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-log-other-class"),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
				},
				Content: lib.TaskContent("do the work"),
			}

			out := captureGlogStderr(func() {
				err := publisher.PublishFailure(
					ctx, task, "pr-reviewer-agent-9999", "pod_oom_killed (OOMKilled/137)",
				)
				Expect(err).NotTo(HaveOccurred())
			})

			Expect(out).To(ContainSubstring("event=crash_arm_decision task="))
			Expect(out).To(ContainSubstring("decision=not_crash_class"))
			// Both the raw and the parsed form, which is the distinction this
			// spec exists to pin: the suffixed reason must classify, and the line
			// must still show what actually arrived.
			Expect(out).To(ContainSubstring(`reason="pod_oom_killed (OOMKilled/137)"`))
			Expect(out).To(ContainSubstring(`parsed="pod_oom_killed"`))
			Expect(out).To(ContainSubstring("parsed_ok=true"))
			Expect(strings.Count(out, "event=crash_arm_decision ")).To(Equal(1))
		})
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
					base.TopicPrefix("develop"),
					currentDateTime,
					nil,
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
					base.TopicPrefix("develop"),
					currentDateTime,
					nil,
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

	Describe("PublishClearCurrentJob", func() {
		It(
			"publishes exactly one UpdateFrontmatterCommand clearing current_job, no body, no other keys",
			func() {
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-clear"),
					Frontmatter: lib.TaskFrontmatter{
						"status":        "in_progress",
						"phase":         "ai_review",
						"assignee":      "claude",
						"current_job":   "claude-20260418120000",
						"trigger_count": 2,
					},
					Content: lib.TaskContent("do the work"),
				}
				err := publisher.PublishClearCurrentJob(ctx, task, "claude-20260418120000")
				Expect(err).NotTo(HaveOccurred())

				Expect(producer.messages).To(HaveLen(1))
				operation, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])

				Expect(
					string(operation),
				).To(Equal(string(taskcmd.UpdateFrontmatterCommandOperation)))
				Expect(string(cmd.TaskIdentifier)).To(Equal("test-task-clear"))
				Expect(cmd.Updates).To(HaveLen(1))
				Expect(cmd.Updates["current_job"]).To(Equal(""))

				_, hasStatus := cmd.Updates["status"]
				Expect(hasStatus).To(BeFalse(), "status must not be in clear-current-job update")
				_, hasPhase := cmd.Updates["phase"]
				Expect(hasPhase).To(BeFalse(), "phase must not be in clear-current-job update")
				_, hasTriggerCount := cmd.Updates["trigger_count"]
				Expect(
					hasTriggerCount,
				).To(BeFalse(), "trigger_count must not be in clear-current-job update")
				Expect(cmd.Body).To(BeNil(), "success-path clear must not append a body section")
			},
		)

		It("suppresses a second call with the same job name (dedupe)", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-clear-dedupe"),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
				},
				Content: lib.TaskContent("do the work"),
			}

			err := publisher.PublishClearCurrentJob(ctx, task, "claude-20260418120000")
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.messages).To(HaveLen(1))

			err = publisher.PublishClearCurrentJob(ctx, task, "claude-20260418120000")
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.messages).To(HaveLen(1), "second call should be deduped")
		})

		It("returns error when the sender fails and does NOT record dedupe", func() {
			failingProducer := &partialFailingSyncProducer{
				successCount: 0,
				err:          errors.New(context.Background(), "kafka: leader not available"),
			}
			failingPublisher := pkg.NewResultPublisher(
				failingProducer,
				base.TopicPrefix("develop"),
				currentDateTime,
				nil,
			)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-clear-fail"),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
				},
				Content: lib.TaskContent("do the work"),
			}

			err := failingPublisher.PublishClearCurrentJob(ctx, task, "claude-20260418120000")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("current_job clear"))

			// Dedupe must NOT be recorded on failure — the informer re-delivery
			// retries the clear until it lands.
			err = failingPublisher.PublishClearCurrentJob(ctx, task, "claude-20260418120000")
			Expect(err).To(HaveOccurred())
			Expect(
				failingProducer.messages,
			).To(HaveLen(2), "second call re-attempts the clear (dedupe was not recorded)")
		})
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

	Describe("PublishParkSucceeded", func() {
		It(
			"writes exactly the phase key with human_review and nothing else",
			func() {
				// The park's whole effect is this one key: human_review is what
				// takes the record out of the executor's drive set. A wrong key
				// or value would leave the loop running, and every watcher-side
				// spec asserts only that the publisher was called.
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-parked"),
					Frontmatter: lib.TaskFrontmatter{
						"status":       "in_progress",
						"phase":        "execution",
						"assignee":     "claude",
						"current_job":  "job-1",
						"target_vault": "work",
					},
				}
				err := publisher.PublishParkSucceeded(ctx, task)
				Expect(err).NotTo(HaveOccurred())

				Expect(producer.messages).To(HaveLen(1))
				operation, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])

				Expect(
					string(operation),
				).To(Equal(string(taskcmd.UpdateFrontmatterCommandOperation)))
				Expect(cmd.TaskIdentifier).To(Equal(lib.TaskIdentifier("test-task-parked")))
				Expect(cmd.TargetVault).To(Equal("work"))

				Expect(cmd.Updates).To(HaveLen(1))
				Expect(cmd.Updates["phase"]).To(Equal("human_review"))

				for _, key := range []string{
					"status",
					"assignee",
					"current_job",
				} {
					_, hasKey := cmd.Updates[key]
					Expect(hasKey).To(BeFalse(), key+" must not be in the park update")
				}
				Expect(cmd.Body).To(BeNil())
				Expect(cmd.Validate(ctx)).To(Succeed())
			},
		)
	})

	Describe("PublishRestoreAssignee", func() {
		It(
			"writes exactly one assignee key and no counter key via UpdateFrontmatterCommand",
			func() {
				// Spec 009 AC 7: the re-drive writes no counter key. The parked
				// fixture carries the keys a parked task really has — an empty
				// assignee, a previous_assignee left by the failure path, and a
				// trigger_count — so a restore that echoed any of them would show
				// up here rather than passing on a minimal fixture.
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-parked"),
					Frontmatter: lib.TaskFrontmatter{
						"status":            "in_progress",
						"phase":             "planning",
						"assignee":          "",
						"previous_assignee": "stale-agent",
						"task_type":         "github-update-go",
						"target_vault":      "work",
						"trigger_count":     0,
						"trigger_scope":     "planning:",
					},
				}
				err := publisher.PublishRestoreAssignee(ctx, task, "correct-agent")
				Expect(err).NotTo(HaveOccurred())

				Expect(producer.messages).To(HaveLen(1))
				operation, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])

				Expect(
					string(operation),
				).To(Equal(string(taskcmd.UpdateFrontmatterCommandOperation)))
				Expect(cmd.TaskIdentifier).To(Equal(lib.TaskIdentifier("test-task-parked")))
				Expect(cmd.TargetVault).To(Equal("work"))

				Expect(cmd.Updates).To(HaveLen(1))
				Expect(cmd.Updates["assignee"]).To(Equal("correct-agent"))

				for _, key := range []string{
					"trigger_count",
					"trigger_scope",
					"current_job",
					"status",
					"phase",
					"previous_assignee",
				} {
					_, hasKey := cmd.Updates[key]
					Expect(hasKey).To(BeFalse(), key+" must not be in the restore update")
				}
				Expect(cmd.Body).To(BeNil())

				// The emitted command must satisfy the lib slug contract the
				// controller enforces, exactly as every other update here does.
				Expect(cmd.Validate(ctx)).To(Succeed())
			},
		)

		It("returns error when sender fails", func() {
			failingProducer := &failingSyncProducer{
				err: errors.New(context.Background(), "kafka: leader not available"),
			}
			failingPublisher := pkg.NewResultPublisher(
				failingProducer,
				base.TopicPrefix("develop"),
				currentDateTime,
				nil,
			)

			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-parked-fail"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"assignee": "",
				},
			}
			err := failingPublisher.PublishRestoreAssignee(ctx, task, "correct-agent")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("kafka: leader not available"))
		})
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
				base.TopicPrefix("develop"),
				currentDateTime,
				nil,
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

	Describe("TargetVault stamping", func() {
		It(
			"stamps UpdateFrontmatterCommand and IncrementFrontmatterCommand with the task's target_vault",
			func() {
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-vault"),
					Frontmatter: lib.TaskFrontmatter{
						"status":        "in_progress",
						"target_vault":  "personal",
						"trigger_count": 0,
					},
				}
				err := publisher.PublishFailure(
					ctx,
					task,
					"claude-20260418120000",
					"pod OOM killed",
				)
				Expect(err).NotTo(HaveOccurred())

				Expect(producer.messages).To(HaveLen(2))
				_, updateCmd := decodeUpdateFrontmatterCommand(producer.messages[0])
				Expect(updateCmd.TargetVault).To(Equal("personal"))
				_, incrementCmd := decodeIncrementFrontmatterCommand(producer.messages[1])
				Expect(incrementCmd.TargetVault).To(Equal("personal"))
			},
		)

		It("leaves TargetVault off the wire when the task has no target_vault", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-no-vault"),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
				},
			}
			err := publisher.PublishFailure(ctx, task, "claude-20260418120000", "pod OOM killed")
			Expect(err).NotTo(HaveOccurred())

			Expect(producer.messages).To(HaveLen(2))
			_, updateCmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(updateCmd.TargetVault).To(Equal(""))
			_, incrementCmd := decodeIncrementFrontmatterCommand(producer.messages[1])
			Expect(incrementCmd.TargetVault).To(Equal(""))

			// omitempty keeps the field off the wire entirely when empty.
			raw, err := producer.messages[0].Value.Encode()
			Expect(err).NotTo(HaveOccurred())
			var command base.Command
			Expect(json.Unmarshal(raw, &command)).To(Succeed())
			dataBytes, err := json.Marshal(command.Data)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(dataBytes)).NotTo(ContainSubstring("targetVault"))
		})

		It("stamped commands satisfy the lib slug contract", func() {
			// The publish path itself never calls Validate — the slug rule is
			// enforced downstream by the controller. Pin the contract here so a
			// malformed vault slug in a task's frontmatter cannot ride through
			// silently: what we publish must be something the consumer accepts.
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-vault-contract"),
				Frontmatter: lib.TaskFrontmatter{
					"status":       "in_progress",
					"target_vault": "personal",
				},
			}
			err := publisher.PublishSpawnNotification(ctx, task, "claude-20260418120000")
			Expect(err).NotTo(HaveOccurred())

			_, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(cmd.Validate(ctx)).To(Succeed())
		})

		It("stamps all other command constructions with the task's target_vault", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-vault-all"),
				Frontmatter: lib.TaskFrontmatter{
					"status":       "in_progress",
					"assignee":     "agent-pr-reviewer",
					"target_vault": "work",
				},
			}

			// PublishSpawnNotification
			err := publisher.PublishSpawnNotification(ctx, task, "claude-20260418120000")
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.messages).To(HaveLen(1))
			_, spawnCmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(spawnCmd.TargetVault).To(Equal("work"))

			// PublishIncrementTriggerCount
			err = publisher.PublishIncrementTriggerCount(ctx, task)
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.messages).To(HaveLen(2))
			_, incCmd := decodeIncrementFrontmatterCommand(producer.messages[1])
			Expect(incCmd.TargetVault).To(Equal("work"))

			// PublishSetTriggerScope
			err = publisher.PublishSetTriggerScope(ctx, task, "ai_review:abc12345", 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.messages).To(HaveLen(3))
			_, scopeCmd := decodeUpdateFrontmatterCommand(producer.messages[2])
			Expect(scopeCmd.TargetVault).To(Equal("work"))

			// PublishTypeMismatchFailure
			err = publisher.PublishTypeMismatchFailure(
				ctx,
				task,
				`task_type "healthcheck" not in effective set [pr-review]`,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.messages).To(HaveLen(4))
			_, mismatchCmd := decodeUpdateFrontmatterCommand(producer.messages[3])
			Expect(mismatchCmd.TargetVault).To(Equal("work"))
		})

		It("stamps empty TargetVault when target_vault holds a non-string value", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-nonstring-vault"),
				Frontmatter: lib.TaskFrontmatter{
					"status":       "in_progress",
					"target_vault": 42,
				},
			}
			err := publisher.PublishSpawnNotification(ctx, task, "claude-20260418120000")
			Expect(err).NotTo(HaveOccurred())

			Expect(producer.messages).To(HaveLen(1))
			_, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(cmd.TargetVault).To(Equal(""))
		})
	})

	Describe("PublishAbort", func() {
		abortTask := func() lib.Task {
			return lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-abort"),
				Frontmatter: lib.TaskFrontmatter{
					"status":       "in_progress",
					"phase":        "execution",
					"task_type":    "update-go",
					"target_vault": "work",
				},
			}
		}

		It("writes exactly the four close-out keys and no fifth", func() {
			err := publisher.PublishAbort(
				ctx,
				abortTask(),
				pkg.AbortReasonBrokenSignature,
				3,
			)
			Expect(err).NotTo(HaveOccurred())

			Expect(producer.messages).To(HaveLen(1))
			operation, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(
				string(operation),
			).To(Equal(string(taskcmd.UpdateFrontmatterCommandOperation)))
			Expect(cmd.Updates).To(HaveLen(4))
			Expect(cmd.Updates["status"]).To(Equal("aborted"))
			Expect(cmd.Updates["aborted_reason"]).To(Equal(pkg.AbortReasonBrokenSignature))
			Expect(cmd.Updates["gate_successor"]).To(Equal("none"))
			Expect(cmd.Updates["current_job"]).To(Equal(""))
			Expect(cmd.Body).To(BeNil())
			Expect(cmd.TargetVault).To(Equal("work"))

			// The emitted command must satisfy the command contract.
			Expect(cmd.Validate(ctx)).To(Succeed())

			// And the close-out guard the write exists to satisfy must accept the
			// same values. NewTaskFrontmatter returns a value and SetStatus has a
			// pointer receiver, so the call cannot be chained.
			fm := domain.NewTaskFrontmatter(map[string]any{
				"status":         "aborted",
				"aborted_reason": pkg.AbortReasonBrokenSignature,
				"gate_successor": "none",
			})
			Expect(fm.SetStatus(domain.TaskStatusAborted)).To(Succeed())

			// Without gate_successor the same values are rejected: that is why the
			// key is not optional.
			withoutGate := domain.NewTaskFrontmatter(map[string]any{
				"status":         "aborted",
				"aborted_reason": pkg.AbortReasonBrokenSignature,
			})
			Expect(withoutGate.SetStatus(domain.TaskStatusAborted)).NotTo(Succeed())
		})

		It("publishes nothing when the task is already aborted", func() {
			task := abortTask()
			task.Frontmatter["status"] = "aborted"

			Expect(publisher.PublishAbort(ctx, task, pkg.AbortReasonBrokenSignature, 3)).
				To(Succeed())
			Expect(producer.messages).To(BeEmpty())
		})

		It("dedupes a second abort for the same task", func() {
			task := abortTask()
			Expect(publisher.PublishAbort(ctx, task, pkg.AbortReasonBrokenSignature, 3)).
				To(Succeed())
			Expect(publisher.PublishAbort(ctx, task, pkg.AbortReasonBrokenSignature, 3)).
				To(Succeed())

			Expect(producer.messages).To(HaveLen(1))
		})

		It("does not record dedupe when the send fails, so the next event retries", func() {
			partialProducer := &partialFailingSyncProducer{
				successCount: 0,
				err:          errors.New(context.Background(), "kafka: leader not available"),
			}
			failingPublisher := pkg.NewResultPublisher(
				partialProducer,
				base.TopicPrefix("develop"),
				currentDateTime,
				nil,
			)
			task := abortTask()

			Expect(
				failingPublisher.PublishAbort(ctx, task, pkg.AbortReasonBrokenSignature, 3),
			).NotTo(Succeed())
			Expect(
				failingPublisher.PublishAbort(ctx, task, pkg.AbortReasonBrokenSignature, 3),
			).NotTo(Succeed())

			// Both attempts reached the producer: a failed send is retried, not
			// suppressed for the TTL.
			Expect(partialProducer.messages).To(HaveLen(2))
		})

		It("increments only the matching abort reason series", func() {
			brokenBefore := testutil.ToFloat64(
				metrics.RedriveAbortedTotal.WithLabelValues(
					metrics.RedriveAbortReasonBrokenSignature,
				),
			)
			transientBefore := testutil.ToFloat64(
				metrics.RedriveAbortedTotal.WithLabelValues(
					metrics.RedriveAbortReasonTransientExhausted,
				),
			)

			Expect(
				publisher.PublishAbort(ctx, abortTask(), pkg.AbortReasonBrokenSignature, 3),
			).To(Succeed())

			Expect(testutil.ToFloat64(
				metrics.RedriveAbortedTotal.WithLabelValues(
					metrics.RedriveAbortReasonBrokenSignature,
				),
			)).To(Equal(brokenBefore + 1))
			Expect(testutil.ToFloat64(
				metrics.RedriveAbortedTotal.WithLabelValues(
					metrics.RedriveAbortReasonTransientExhausted,
				),
			)).To(Equal(transientBefore))
		})
	})

	Describe("PublishTriggerBudgetRefusal", func() {
		refusalTask := func() lib.Task {
			return lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-refusal"),
				Frontmatter: lib.TaskFrontmatter{
					"status":       "in_progress",
					"phase":        "planning",
					"target_vault": "work",
				},
			}
		}

		It("writes the four trigger_refusal keys and no fifth", func() {
			err := publisher.PublishTriggerBudgetRefusal(
				ctx,
				refusalTask(),
				"planning:4963ffa6",
				4,
				3,
			)
			Expect(err).NotTo(HaveOccurred())

			Expect(producer.messages).To(HaveLen(1))
			operation, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(
				string(operation),
			).To(Equal(string(taskcmd.UpdateFrontmatterCommandOperation)))
			// No frontmatter keys at all: vault-api's typedTaskFields allowlist
			// refuses the WHOLE command when any named key is outside it, and it
			// does not carry the trigger_refusal_* names — a command with them is
			// discarded body section included. Pinned here so a future
			// "helpfully" restored key cannot silently reintroduce that.
			Expect(cmd.Updates).To(BeEmpty())
			Expect(cmd.TargetVault).To(Equal("work"))
		})

		It("writes a Trigger Budget section, never a Failure section", func() {
			err := publisher.PublishTriggerBudgetRefusal(
				ctx,
				refusalTask(),
				"planning:4963ffa6",
				4,
				3,
			)
			Expect(err).NotTo(HaveOccurred())

			_, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(cmd.Body).NotTo(BeNil())
			Expect(cmd.Body.Heading).To(Equal("## Trigger Budget"))
			Expect(cmd.Body.Heading).NotTo(Equal("## Failure"))
			Expect(cmd.Body.Section).To(HavePrefix("## Trigger Budget"))
			Expect(cmd.Body.Section).To(ContainSubstring("trigger_count 4 >= max_triggers 3"))
			Expect(cmd.Body.Section).To(ContainSubstring("planning:4963ffa6"))
			Expect(cmd.Body.Section).To(HaveSuffix("\n"))
		})

		It("does not touch the failure section or ownership keys", func() {
			err := publisher.PublishTriggerBudgetRefusal(
				ctx,
				refusalTask(),
				"planning:4963ffa6",
				4,
				3,
			)
			Expect(err).NotTo(HaveOccurred())

			_, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])
			Expect(cmd.Body.Heading).NotTo(Equal("## Failure"))
			for _, key := range []string{
				"status",
				"phase",
				"assignee",
				"current_job",
				"trigger_count",
				"trigger_scope",
				"max_triggers",
			} {
				_, ok := cmd.Updates[key]
				Expect(ok).To(BeFalse(), "key %q must not be written", key)
			}
		})

		It("publishes once for an identical refusal triple", func() {
			task := refusalTask()
			Expect(
				publisher.PublishTriggerBudgetRefusal(ctx, task, "planning:4963ffa6", 4, 3),
			).To(Succeed())
			Expect(
				publisher.PublishTriggerBudgetRefusal(ctx, task, "planning:4963ffa6", 4, 3),
			).To(Succeed())

			Expect(producer.messages).To(HaveLen(1))
		})

		It("publishes again when the refusal triple changes", func() {
			task := refusalTask()
			Expect(
				publisher.PublishTriggerBudgetRefusal(ctx, task, "planning:4963ffa6", 4, 3),
			).To(Succeed())
			Expect(
				publisher.PublishTriggerBudgetRefusal(ctx, task, "execution:bbbbbbbb", 4, 3),
			).To(Succeed())
			Expect(
				publisher.PublishTriggerBudgetRefusal(ctx, task, "execution:bbbbbbbb", 5, 3),
			).To(Succeed())

			Expect(producer.messages).To(HaveLen(3))
		})

		It("does not remember a failed publish, so the next tick retries", func() {
			partialProducer := &partialFailingSyncProducer{
				successCount: 0,
				err:          errors.New(context.Background(), "kafka unavailable"),
			}
			failingPublisher := pkg.NewResultPublisher(
				partialProducer,
				base.TopicPrefix("develop"),
				currentDateTime,
				nil,
			)
			task := refusalTask()

			Expect(
				failingPublisher.PublishTriggerBudgetRefusal(
					ctx, task, "planning:4963ffa6", 4, 3,
				),
			).NotTo(Succeed())
			Expect(
				failingPublisher.PublishTriggerBudgetRefusal(
					ctx, task, "planning:4963ffa6", 4, 3,
				),
			).NotTo(Succeed())

			Expect(partialProducer.messages).To(HaveLen(2))
		})
	})

	Describe("PublishFailure reason recording", func() {
		reasonPublisher := func(client claim.ClaimRefClient) pkg.ResultPublisher {
			return pkg.NewResultPublisher(
				producer,
				base.TopicPrefix("develop"),
				currentDateTime,
				func() claim.ClaimRefClient { return client },
			)
		}

		It("records the reason for the task's current phase", func() {
			client := new(mocks.FakeClaimRefClient)
			p := reasonPublisher(client)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-reason"),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
					"phase":  "execution",
				},
			}

			Expect(p.PublishFailure(ctx, task, "job-reason-1", "pod_oom_killed")).To(Succeed())

			// Two writes: the streak ref (recordRedriveFailure) lands first, then
			// the reason ref — the streak counts published failures, and the
			// reason is the evidence that failure is classified from.
			Expect(client.PutCallCount()).To(Equal(2))
			_, key, _, cluster, taskID, _ := client.PutArgsForCall(1)
			Expect(key).To(Equal(
				"claims/redrive-reason/test-task-reason/execution/pod_oom_killed/1",
			))
			Expect(cluster).To(Equal(claim.RedriveStreakCluster))
			Expect(taskID).To(Equal("test-task-reason"))
		})

		It("normalises the formatted backoff reason to its vocabulary value", func() {
			client := new(mocks.FakeClaimRefClient)
			p := reasonPublisher(client)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-formatted"),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
					"phase":  "execution",
				},
			}

			Expect(
				p.PublishFailure(ctx, task, "job-reason-2", "pod_oom_killed (OOMKilled/137)"),
			).To(Succeed())

			_, key, _, _, _, _ := client.PutArgsForCall(1)
			Expect(key).To(Equal(
				"claims/redrive-reason/test-task-formatted/execution/pod_oom_killed/1",
			))
		})

		It("records an unparseable reason as unknown", func() {
			client := new(mocks.FakeClaimRefClient)
			p := reasonPublisher(client)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-unparsed"),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
					"phase":  "execution",
				},
			}

			Expect(p.PublishFailure(ctx, task, "job-reason-3", "pod OOM killed")).To(Succeed())

			_, key, _, _, _, _ := client.PutArgsForCall(1)
			Expect(key).To(Equal(
				"claims/redrive-reason/test-task-unparsed/execution/unknown/1",
			))
		})

		It("records nothing on a deduped repeat", func() {
			client := new(mocks.FakeClaimRefClient)
			p := reasonPublisher(client)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-deduped-reason"),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
					"phase":  "execution",
				},
			}

			Expect(p.PublishFailure(ctx, task, "job-reason-4", "pod_oom_killed")).To(Succeed())
			Expect(p.PublishFailure(ctx, task, "job-reason-4", "pod_oom_killed")).To(Succeed())

			// One streak ref + one reason ref; the deduped repeat writes neither.
			Expect(client.PutCallCount()).To(Equal(2))
		})
	})

	// recordRedriveFailure is the ONLY writer of the re-drive streak, and it is
	// unexported, so its branches are exercised through PublishFailure — the one
	// call site. The streak ref is always the first Put: it is written before the
	// reason, so the new streak's evidence starts empty and gains exactly the
	// reason being published.
	Describe("recordRedriveFailure", func() {
		streakPublisher := func(client claim.ClaimRefClient) pkg.ResultPublisher {
			return pkg.NewResultPublisher(
				producer,
				base.TopicPrefix("develop"),
				currentDateTime,
				func() claim.ClaimRefClient { return client },
			)
		}
		failingTask := func(id string) lib.Task {
			return lib.Task{
				TaskIdentifier: lib.TaskIdentifier(id),
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
					"phase":  "execution",
				},
			}
		}
		// listByTaskPrefix serves refs per list prefix. The stores list
		// "<prefix><taskID>" with no trailing separator, so the key is exact.
		listByTaskPrefix := func(
			byPrefix map[string][]claim.ClaimRef,
		) func(context.Context, string) ([]claim.ClaimRef, error) {
			return func(_ context.Context, prefix string) ([]claim.ClaimRef, error) {
				return byPrefix[prefix], nil
			}
		}

		It("records the streak even when the count cannot be read", func() {
			client := new(mocks.FakeClaimRefClient)
			client.ListReturns(nil, nil)
			// The first List is Count's; a read failure must not skip the write.
			client.ListReturnsOnCall(0, nil, stderrors.New("list boom"))
			p := streakPublisher(client)

			Expect(
				p.PublishFailure(
					ctx,
					failingTask("test-task-streak-read-fail"),
					"job-srf-1",
					"pod_oom_killed",
				),
			).To(Succeed())

			// The count is unknown, so the new-streak clear is skipped: no Delete.
			// The streak and the reason are still recorded.
			Expect(client.DeleteCallCount()).To(Equal(0))
			Expect(client.PutCallCount()).To(Equal(2))
			_, key, _, _, _, _ := client.PutArgsForCall(0)
			Expect(key).To(Equal(
				"claims/redrive/test-task-streak-read-fail/execution/1",
			))
		})

		It("clears the previous streak's reasons when a new streak starts", func() {
			client := new(mocks.FakeClaimRefClient)
			client.ListCalls(listByTaskPrefix(map[string][]claim.ClaimRef{
				"claims/redrive-reason/test-task-new-streak": {
					{
						Ref: "refs/claims/redrive-reason/test-task-new-streak" +
							"/execution/pod_oom_killed/1",
					},
				},
			}))
			p := streakPublisher(client)

			Expect(
				p.PublishFailure(
					ctx,
					failingTask("test-task-new-streak"),
					"job-ns-1",
					"pod_oom_killed",
				),
			).To(Succeed())

			// A reason set left by an earlier streak would otherwise satisfy the
			// classifier's window on partial evidence and could close the record
			// as broken.
			Expect(client.DeleteCallCount()).To(Equal(1))
			_, key, _ := client.DeleteArgsForCall(0)
			Expect(key).To(Equal(
				"claims/redrive-reason/test-task-new-streak/execution/pod_oom_killed/1",
			))
			_, streakKey, _, _, _, _ := client.PutArgsForCall(0)
			Expect(streakKey).To(Equal(
				"claims/redrive/test-task-new-streak/execution/1",
			))
		})

		It("keeps the previous reasons when the streak is already running", func() {
			client := new(mocks.FakeClaimRefClient)
			client.ListCalls(listByTaskPrefix(map[string][]claim.ClaimRef{
				"claims/redrive/test-task-running-streak": {
					{Ref: "refs/claims/redrive/test-task-running-streak/execution/1"},
				},
				"claims/redrive-reason/test-task-running-streak": {
					{
						Ref: "refs/claims/redrive-reason/test-task-running-streak" +
							"/execution/pod_oom_killed/1",
					},
				},
			}))
			p := streakPublisher(client)

			Expect(
				p.PublishFailure(
					ctx,
					failingTask("test-task-running-streak"),
					"job-rs-1",
					"pod_oom_killed",
				),
			).To(Succeed())

			// Mid-streak the evidence must accumulate, so nothing is cleared.
			Expect(client.DeleteCallCount()).To(Equal(0))
			_, streakKey, _, _, _, _ := client.PutArgsForCall(0)
			Expect(streakKey).To(Equal(
				"claims/redrive/test-task-running-streak/execution/2",
			))
		})
	})
})
