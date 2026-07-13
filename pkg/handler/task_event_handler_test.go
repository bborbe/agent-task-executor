// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6@v6.12.2 -generate

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

func TestHandler(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Handler Suite")
}

var _ = Describe("TaskEventHandler", func() {
	var (
		ctx                 context.Context
		fakeSpawner         *mocks.FakeJobSpawner
		fakeResolver        *mocks.FakeConfigResolver
		fakeResultPublisher *mocks.FakeResultPublisher
		taskStore           *pkg.TaskStore
		currentDateTime     libtime.CurrentDateTime
		h                   handler.TaskEventHandler
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeSpawner = new(mocks.FakeJobSpawner)
		fakeResolver = &mocks.FakeConfigResolver{}
		fakeResolver.ResolveReturns(
			pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"},
			nil,
		)
		fakeResultPublisher = &mocks.FakeResultPublisher{}
		taskStore = pkg.NewTaskStore()
		currentDateTime = libtime.NewCurrentDateTime()
		h = handler.NewTaskEventHandler(
			fakeSpawner,
			base.Branch("prod"),
			fakeResolver,
			fakeResultPublisher,
			taskStore,
			currentDateTime,
		)
	})

	buildMsg := func(task lib.Task) *sarama.ConsumerMessage {
		value, err := json.Marshal(task)
		Expect(err).To(BeNil())
		return &sarama.ConsumerMessage{Value: value}
	}

	Describe("ConsumeMessage", func() {
		It("skips empty message", func() {
			err := h.ConsumeMessage(ctx, &sarama.ConsumerMessage{Value: []byte{}})
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("skips malformed JSON without error", func() {
			err := h.ConsumeMessage(ctx, &sarama.ConsumerMessage{Value: []byte("not-json")})
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("skips task with empty TaskIdentifier", func() {
			task := lib.Task{
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("skips task with status != in_progress", func() {
			task := lib.Task{
				TaskIdentifier: "tid-1",
				Frontmatter: lib.TaskFrontmatter{
					"status":   "todo",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("skips task with nil phase", func() {
			task := lib.Task{
				TaskIdentifier: "tid-2",
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("skips task with phase todo", func() {
			task := lib.Task{
				TaskIdentifier: "tid-3",
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseTodo),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("skips task with phase human_review", func() {
			task := lib.Task{
				TaskIdentifier: "tid-4",
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseHumanReview),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("skips task with empty assignee", func() {
			task := lib.Task{
				TaskIdentifier: "tid-5",
				Frontmatter: lib.TaskFrontmatter{
					"status": "in_progress",
					"phase":  string(domain.TaskPhaseExecution),
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("skips unknown assignee without error", func() {
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{},
				errors.Wrapf(ctx, pkg.ErrConfigNotFound, "find assignee"),
			)
			task := lib.Task{
				TaskIdentifier: "tid-6",
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "unknown-agent",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("returns wrapped error when resolver fails with non-NotFound", func() {
			fakeResolver.ResolveReturns(pkg.AgentConfiguration{}, errors.Errorf(ctx, "boom"))
			task := lib.Task{
				TaskIdentifier: "tid-6b",
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "some-agent",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).NotTo(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("skips task when active job exists", func() {
			fakeSpawner.IsJobActiveReturns(true, nil)
			task := lib.Task{
				TaskIdentifier: "tid-7",
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("spawns job when no active job exists", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-8"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
				},
				Content: lib.TaskContent("do the work"),
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			_, spawnedTask, config := fakeSpawner.SpawnJobArgsForCall(0)
			Expect(string(spawnedTask.TaskIdentifier)).To(Equal("tid-8"))
			Expect(config.Image).To(Equal("my-image:latest"))
		})

		It("returns error when IsJobActive fails", func() {
			fakeSpawner.IsJobActiveReturns(false, errors.Errorf(ctx, "k8s unavailable"))
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-9"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).NotTo(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("returns error when SpawnJob fails", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("", errors.Errorf(ctx, "k8s unavailable"))
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-10"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).NotTo(BeNil())
		})

		It("accepts task with phase planning", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-11"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhasePlanning),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		It("accepts task with phase ai_review", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-12"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseAIReview),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		It("publishes spawn notification after successful spawn", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("claude-20260418120000", nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-uuid-1234"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseAIReview),
					"assignee": "claude",
					"stage":    "prod",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeResultPublisher.PublishSpawnNotificationCallCount()).To(Equal(1))
			_, calledTask, calledJobName := fakeResultPublisher.PublishSpawnNotificationArgsForCall(
				0,
			)
			Expect(string(calledTask.TaskIdentifier)).To(Equal("test-task-uuid-1234"))
			Expect(calledJobName).To(Equal("claude-20260418120000"))
		})

		It(
			"returns nil when PublishSpawnNotification fails but SpawnJob succeeds (best-effort)",
			func() {
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeSpawner.SpawnJobReturns("claude-20260418120000", nil)
				fakeResultPublisher.PublishSpawnNotificationReturns(
					errors.Errorf(ctx, "kafka unavailable"),
				)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-uuid-1234"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "in_progress",
						"phase":    string(domain.TaskPhaseAIReview),
						"assignee": "claude",
						"stage":    "prod",
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				// Spawn must have been called (job is running), but handler returns nil
				// because the notification failure is best-effort only.
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			},
		)

		It("stores task in taskStore after successful spawn", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("claude-20260418120000", nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-uuid-1234"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseAIReview),
					"assignee": "claude",
					"stage":    "prod",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			_, ok := taskStore.Load(lib.TaskIdentifier("test-task-uuid-1234"))
			Expect(ok).To(BeTrue())
		})

		It(
			"publishes increment trigger_count before spawning job (retry_count bump no longer called)",
			func() {
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeSpawner.SpawnJobReturns("claude-20260418120000", nil)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-uuid-1234"),
					Frontmatter: lib.TaskFrontmatter{
						"status":        "in_progress",
						"phase":         string(domain.TaskPhaseAIReview),
						"assignee":      "claude",
						"stage":         "prod",
						"trigger_count": 1,
						"max_triggers":  3,
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(1))
				_, calledTask := fakeResultPublisher.PublishIncrementTriggerCountArgsForCall(0)
				Expect(string(calledTask.TaskIdentifier)).To(Equal("test-task-uuid-1234"))
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			},
		)

		It("does not spawn job when PublishIncrementTriggerCount fails", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeResultPublisher.PublishIncrementTriggerCountReturns(
				errors.New(ctx, "kafka unavailable"),
			)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-uuid-1234"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseAIReview),
					"assignee": "claude",
					"stage":    "prod",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(HaveOccurred())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("skips spawn when trigger_count >= max_triggers (cap reached)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-cap-1"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         string(domain.TaskPhaseAIReview),
					"assignee":      "claude",
					"stage":         "prod",
					"trigger_count": 3,
					"max_triggers":  3,
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(0))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("publishes increment and spawns when below cap (happy path)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("claude-job-1", nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-cap-2"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         string(domain.TaskPhaseAIReview),
					"assignee":      "claude",
					"stage":         "prod",
					"trigger_count": 1,
					"max_triggers":  3,
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(1))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		It(
			"blocks spawn when PublishIncrementTriggerCount fails (publish-failure scenario)",
			func() {
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeResultPublisher.PublishIncrementTriggerCountReturns(
					errors.New(ctx, "kafka down"),
				)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-cap-3"),
					Frontmatter: lib.TaskFrontmatter{
						"status":        "in_progress",
						"phase":         string(domain.TaskPhaseAIReview),
						"assignee":      "claude",
						"stage":         "prod",
						"trigger_count": 0,
						"max_triggers":  3,
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(HaveOccurred())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			},
		)

		It("skips spawn when max_triggers=0 (zero-cap edge case)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-cap-4"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         string(domain.TaskPhaseAIReview),
					"assignee":      "claude",
					"stage":         "prod",
					"trigger_count": 0,
					"max_triggers":  0,
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(0))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("publishes increment once even when SpawnJob fails (over-count documented)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("", errors.New(ctx, "k8s create failed"))
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-cap-5"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         string(domain.TaskPhaseAIReview),
					"assignee":      "claude",
					"stage":         "prod",
					"trigger_count": 1,
					"max_triggers":  3,
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(HaveOccurred())
			Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(1))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		It("skips spawn when current_job in frontmatter and K8s job is active", func() {
			fakeSpawner.IsJobActiveReturns(true, nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-uuid-1234"),
				Frontmatter: lib.TaskFrontmatter{
					"status":      "in_progress",
					"phase":       string(domain.TaskPhaseAIReview),
					"assignee":    "claude",
					"stage":       "prod",
					"current_job": "claude-20260418000000",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("spawns job when stage is absent (defaults to prod) and executor is prod", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-stage-1"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		It("skips task with stage=dev when executor branch is prod", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-stage-2"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
					"stage":    "dev",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("spawns job with stage=dev when executor branch is dev", func() {
			localSpawner := new(mocks.FakeJobSpawner)
			localSpawner.IsJobActiveReturns(false, nil)
			localResolver := &mocks.FakeConfigResolver{}
			localResolver.ResolveReturns(
				pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"},
				nil,
			)
			localHandler := handler.NewTaskEventHandler(
				localSpawner,
				base.Branch("dev"),
				localResolver,
				&mocks.FakeResultPublisher{},
				pkg.NewTaskStore(),
				libtime.NewCurrentDateTime(),
			)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-stage-3"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
					"stage":    "dev",
				},
			}
			err := localHandler.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(localSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		It("removes task from taskStore when event has status=completed", func() {
			taskStore.Store(lib.TaskIdentifier("test-task-uuid-1234"), lib.Task{
				TaskIdentifier: "test-task-uuid-1234",
			})
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-uuid-1234"),
				Frontmatter: lib.TaskFrontmatter{
					"status": "completed",
					"phase":  "done",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			_, ok := taskStore.Load(lib.TaskIdentifier("test-task-uuid-1234"))
			Expect(ok).To(BeFalse())
		})

		It("skips task with absent stage (defaults to prod) when executor branch is dev", func() {
			localSpawner := new(mocks.FakeJobSpawner)
			localResolver := &mocks.FakeConfigResolver{}
			localResolver.ResolveReturns(
				pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"},
				nil,
			)
			localHandler := handler.NewTaskEventHandler(
				localSpawner,
				base.Branch("dev"),
				localResolver,
				&mocks.FakeResultPublisher{},
				pkg.NewTaskStore(),
				libtime.NewCurrentDateTime(),
			)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-stage-4"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
				},
			}
			err := localHandler.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(localSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It("spawns job when Trigger == nil (default phases and statuses apply)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest", Trigger: nil},
				nil,
			)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-trigger-1"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		It("spawns job when Config has Trigger.Phases=[todo] and event phase=todo", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{
					Assignee: "claude",
					Image:    "my-image:latest",
					Trigger:  &agentv1.Trigger{Phases: domain.TaskPhases{domain.TaskPhaseTodo}},
				},
				nil,
			)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-trigger-2"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseTodo),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		It(
			"spawns job when Config has Trigger.Statuses=[completed] and event status=completed",
			func() {
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee: "claude",
						Image:    "my-image:latest",
						Trigger: &agentv1.Trigger{
							Statuses: domain.TaskStatuses{domain.TaskStatusCompleted},
						},
					},
					nil,
				)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-trigger-3"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "completed",
						"phase":    string(domain.TaskPhaseExecution),
						"assignee": "claude",
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			},
		)

		It(
			"does not spawn when trigger includes done phase (terminal gate suppresses before allowlist)",
			func() {
				// done is a terminal phase — the gate fires before the allowlist check,
				// so even a custom trigger that includes done cannot cause a spawn.
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee: "claude",
						Image:    "my-image:latest",
						Trigger: &agentv1.Trigger{
							Phases:   domain.TaskPhases{domain.TaskPhaseDone},
							Statuses: domain.TaskStatuses{domain.TaskStatusCompleted},
						},
					},
					nil,
				)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-trigger-4a"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "completed",
						"phase":    string(domain.TaskPhaseDone),
						"assignee": "claude",
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			},
		)

		It(
			"does not spawn when combined trigger does not match event (non-matching event)",
			func() {
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee: "claude",
						Image:    "my-image:latest",
						Trigger: &agentv1.Trigger{
							Phases:   domain.TaskPhases{domain.TaskPhaseDone},
							Statuses: domain.TaskStatuses{domain.TaskStatusCompleted},
						},
					},
					nil,
				)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-trigger-4b"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "in_progress",
						"phase":    string(domain.TaskPhasePlanning),
						"assignee": "claude",
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			},
		)

		It(
			"increments skipped_status and does not spawn when phase matches but status does not",
			func() {
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee: "claude",
						Image:    "my-image:latest",
						Trigger: &agentv1.Trigger{
							Phases:   domain.TaskPhases{domain.TaskPhaseExecution},
							Statuses: domain.TaskStatuses{domain.TaskStatusCompleted},
						},
					},
					nil,
				)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-trigger-5"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "in_progress",
						"phase":    string(domain.TaskPhaseExecution),
						"assignee": "claude",
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			},
		)

		It(
			"increments skipped_phase and does not spawn when status matches but phase does not",
			func() {
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee: "claude",
						Image:    "my-image:latest",
						Trigger: &agentv1.Trigger{
							Phases:   domain.TaskPhases{domain.TaskPhaseDone},
							Statuses: domain.TaskStatuses{domain.TaskStatusInProgress},
						},
					},
					nil,
				)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-trigger-6"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "in_progress",
						"phase":    string(domain.TaskPhaseExecution),
						"assignee": "claude",
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			},
		)

		It("spawns job when Trigger has empty phase and status lists (defaults apply)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{
					Assignee: "claude",
					Image:    "my-image:latest",
					Trigger:  &agentv1.Trigger{},
				},
				nil,
			)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-trigger-7"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		// --- Type filter behavior matrix (spec 028) ---

		It(
			"spawns job when singular-only TaskType matches task_type (singular-only match)",
			func() {
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee:  "agent-pr-reviewer",
						Image:     "my-image:latest",
						TaskType:  "pr-review",
						TaskTypes: nil,
					},
					nil,
				)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-type-1"),
					Frontmatter: lib.TaskFrontmatter{
						"status":    "in_progress",
						"phase":     string(domain.TaskPhasePlanning),
						"stage":     "prod",
						"assignee":  "agent-pr-reviewer",
						"task_type": "pr-review",
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeResultPublisher.PublishTypeMismatchFailureCallCount()).To(Equal(0))
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			},
		)

		It("spawns job when list-only TaskTypes matches task_type (list-only match)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{
					Assignee:  "agent-pr-reviewer",
					Image:     "my-image:latest",
					TaskType:  "",
					TaskTypes: []string{"healthcheck"},
				},
				nil,
			)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-type-2"),
				Frontmatter: lib.TaskFrontmatter{
					"status":    "in_progress",
					"phase":     string(domain.TaskPhasePlanning),
					"stage":     "prod",
					"assignee":  "agent-pr-reviewer",
					"task_type": "healthcheck",
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeResultPublisher.PublishTypeMismatchFailureCallCount()).To(Equal(0))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		It(
			"spawns job when task_type matches via TaskTypes list when both singular and list are set (overlap match)",
			func() {
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee:  "agent-pr-reviewer",
						Image:     "my-image:latest",
						TaskType:  "pr-review",
						TaskTypes: []string{"healthcheck"},
					},
					nil,
				)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-type-3"),
					Frontmatter: lib.TaskFrontmatter{
						"status":    "in_progress",
						"phase":     string(domain.TaskPhasePlanning),
						"stage":     "prod",
						"assignee":  "agent-pr-reviewer",
						"task_type": "healthcheck",
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeResultPublisher.PublishTypeMismatchFailureCallCount()).To(Equal(0))
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			},
		)

		It(
			"publishes type mismatch failure and does not spawn when task_type is not in effective set (mismatch)",
			func() {
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee:  "agent-pr-reviewer",
						Image:     "my-image:latest",
						TaskType:  "pr-review",
						TaskTypes: []string{"healthcheck"},
					},
					nil,
				)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-type-4"),
					Frontmatter: lib.TaskFrontmatter{
						"status":    "in_progress",
						"phase":     string(domain.TaskPhasePlanning),
						"stage":     "prod",
						"assignee":  "agent-pr-reviewer",
						"task_type": "code-review",
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeResultPublisher.PublishTypeMismatchFailureCallCount()).To(Equal(1))
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
				_, _, reason := fakeResultPublisher.PublishTypeMismatchFailureArgsForCall(0)
				Expect(reason).To(ContainSubstring("code-review"))
			},
		)

		It(
			"publishes type mismatch failure and does not spawn when task_type key is absent from frontmatter (missing task_type)",
			func() {
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee:  "agent-pr-reviewer",
						Image:     "my-image:latest",
						TaskType:  "pr-review",
						TaskTypes: nil,
					},
					nil,
				)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-type-5"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "in_progress",
						"phase":    string(domain.TaskPhasePlanning),
						"stage":    "prod",
						"assignee": "agent-pr-reviewer",
						// task_type key intentionally absent
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeResultPublisher.PublishTypeMismatchFailureCallCount()).To(Equal(1))
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
				_, _, reason := fakeResultPublisher.PublishTypeMismatchFailureArgsForCall(0)
				Expect(reason).To(ContainSubstring("no task_type"))
			},
		)

		Describe("terminal phase gate", func() {
			// DescribeTable covers the 5 regression rows from spec 035.
			// Rows 2 and 3 (terminal phases) use a custom trigger that includes
			// human_review/done in its Phases — so WITHOUT the gate the second
			// event would spawn (count > 0). The gate MUST fire to keep count=0.
			DescribeTable("phase/status combinations",
				func(
					status string,
					phase domain.TaskPhase,
					customTriggerPhases domain.TaskPhases,
					expectSpawn int,
					expectSuppress float64,
				) {
					if len(customTriggerPhases) > 0 {
						fakeResolver.ResolveReturns(
							pkg.AgentConfiguration{
								Assignee: "claude",
								Image:    "my-image:latest",
								Trigger: &agentv1.Trigger{
									Phases:   customTriggerPhases,
									Statuses: domain.TaskStatuses{domain.TaskStatusInProgress},
								},
							},
							nil,
						)
					}
					fakeSpawner.IsJobActiveReturns(false, nil)
					fakeSpawner.SpawnJobReturns("job-1", nil)

					before := testutil.ToFloat64(
						metrics.TaskEventsTotal.WithLabelValues("spawn_suppressed_terminal_phase"),
					)
					task := lib.Task{
						TaskIdentifier: lib.TaskIdentifier("tid-gate-table"),
						Frontmatter: lib.TaskFrontmatter{
							"status":   status,
							"phase":    string(phase),
							"assignee": "claude",
						},
					}
					err := h.ConsumeMessage(ctx, buildMsg(task))
					Expect(err).To(BeNil())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(expectSpawn))
					after := testutil.ToFloat64(
						metrics.TaskEventsTotal.WithLabelValues("spawn_suppressed_terminal_phase"),
					)
					Expect(after - before).To(Equal(expectSuppress))
				},
				Entry(
					"status=in_progress phase=in_progress => spawn",
					"in_progress",
					domain.TaskPhaseExecution,
					domain.TaskPhases(nil),
					1,
					float64(0),
				),
				Entry(
					"status=in_progress phase=human_review => no spawn",
					// Custom trigger includes human_review — without the gate this would spawn.
					"in_progress", domain.TaskPhaseHumanReview,
					domain.TaskPhases{domain.TaskPhaseExecution, domain.TaskPhaseHumanReview},
					0, float64(1),
				),
				Entry(
					"status=in_progress phase=done => no spawn",
					// Custom trigger includes done — without the gate this would spawn.
					"in_progress", domain.TaskPhaseDone,
					domain.TaskPhases{domain.TaskPhaseExecution, domain.TaskPhaseDone},
					0, float64(1),
				),
				Entry(
					"status=completed phase=in_progress => no spawn",
					// Filtered by status check, not terminal gate.
					"completed", domain.TaskPhaseExecution, domain.TaskPhases(nil), 0, float64(0),
				),
			)

			It("sequential events in_progress->human_review => exactly 1 spawn total", func() {
				// Custom trigger includes human_review in its Phases.
				// Without the terminal gate, the second event (phase=human_review)
				// would also spawn because human_review IS in the trigger → total count=2.
				// The gate MUST fire on the second event to keep count=1.
				// If IsTerminal() is removed, this test fails on the Equal(1) assertion.
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee: "claude",
						Image:    "my-image:latest",
						Trigger: &agentv1.Trigger{
							Phases: domain.TaskPhases{
								domain.TaskPhaseExecution,
								domain.TaskPhaseHumanReview,
							},
							Statuses: domain.TaskStatuses{domain.TaskStatusInProgress},
						},
					},
					nil,
				)
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeSpawner.SpawnJobReturns("job-seq-1", nil)

				// Event 1: phase=in_progress → spawns (legitimate spawn)
				event1 := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("22fda7e7"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "in_progress",
						"phase":    string(domain.TaskPhaseExecution),
						"assignee": "claude",
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(event1))
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))

				// Event 2: phase=human_review (terminal) → gate suppresses.
				// The metric delta proves the gate fired, not the allowlist.
				before := testutil.ToFloat64(
					metrics.TaskEventsTotal.WithLabelValues("spawn_suppressed_terminal_phase"),
				)
				event2 := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("22fda7e7"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "in_progress",
						"phase":    string(domain.TaskPhaseHumanReview),
						"assignee": "claude",
					},
				}
				err = h.ConsumeMessage(ctx, buildMsg(event2))
				Expect(err).To(BeNil())
				// Total spawn count must remain 1 — the terminal gate prevented the second spawn.
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
				after := testutil.ToFloat64(
					metrics.TaskEventsTotal.WithLabelValues("spawn_suppressed_terminal_phase"),
				)
				Expect(after - before).To(Equal(float64(1)))
			})

			It(
				"emits unknown_phase metric+log on enum drift (phase outside vault-cli v0.64.0 set)",
				func() {
					// Guards Desired Behavior #8 from spec 035: a phase value not in the
					// knownPhases map increments the unknown_phase metric and falls through
					// to the allowlist's skipped_phase path (no spawn).
					before := testutil.ToFloat64(
						metrics.TaskEventsTotal.WithLabelValues("unknown_phase"),
					)
					task := lib.Task{
						TaskIdentifier: lib.TaskIdentifier("tid-unknown-phase-035"),
						Frontmatter: lib.TaskFrontmatter{
							"status":   "in_progress",
							"phase":    "future_enum_value_not_in_v0.64.0",
							"assignee": "claude",
						},
					}
					err := h.ConsumeMessage(ctx, buildMsg(task))
					Expect(err).To(BeNil())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
					after := testutil.ToFloat64(
						metrics.TaskEventsTotal.WithLabelValues("unknown_phase"),
					)
					Expect(after - before).To(Equal(float64(1)))
				},
			)

			It(
				"does not emit spawn_suppressed on nil phase (parse-error / missing phase path)",
				func() {
					// Guards Failure Modes row 4 from spec 035: a task with missing/unparseable
					// phase must NOT emit spawn_suppressed_terminal_phase — it takes the
					// existing skipped_phase path.
					before := testutil.ToFloat64(
						metrics.TaskEventsTotal.WithLabelValues("spawn_suppressed_terminal_phase"),
					)
					task := lib.Task{
						TaskIdentifier: lib.TaskIdentifier("tid-nil-phase-035"),
						Frontmatter: lib.TaskFrontmatter{
							"status":   "in_progress",
							"assignee": "claude",
							// phase intentionally absent → Phase() returns nil
						},
					}
					err := h.ConsumeMessage(ctx, buildMsg(task))
					Expect(err).To(BeNil())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
					after := testutil.ToFloat64(
						metrics.TaskEventsTotal.WithLabelValues("spawn_suppressed_terminal_phase"),
					)
					Expect(after - before).To(Equal(float64(0)))
				},
			)
		})

		Describe("EvalDeferredRespawns (spec 037)", func() {
			const (
				anchorTime      = "2026-05-17T09:34:00Z" // T+0 (pod 1 start)
				insideGrace     = "2026-05-17T09:34:59Z" // T+59s (suppression event time)
				graceExpiredM1  = "2026-05-17T09:38:59Z" // T+299s (1s before grace expiry)
				graceExpiredR   = "2026-05-17T09:39:30Z" // T+330s (within R=60s)
				graceExpiredMax = "2026-05-17T09:40:00Z" // T+360s (= grace + 60s)
			)

			buildGraceTask := func(phase domain.TaskPhase, triggerCount, maxTriggers int) lib.Task {
				return lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-deferred-037"),
					Frontmatter: lib.TaskFrontmatter{
						"status":         "in_progress",
						"phase":          string(phase),
						"assignee":       "claude",
						"stage":          "prod",
						"current_job":    "pr-reviewer-agent-cbe79223-20260517093325",
						"job_started_at": anchorTime,
						"trigger_count":  triggerCount,
						"max_triggers":   maxTriggers,
					},
				}
			}

			BeforeEach(func() {
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeSpawner.SpawnJobReturns("job-deferred-1", nil)
			})

			It("deferred re-eval fires after grace expiry without a second Kafka event", func() {
				// Step 1: suppression event arrives inside grace window — no spawn
				currentDateTime.SetNow(libtimetest.ParseDateTime(insideGrace))
				task := buildGraceTask(domain.TaskPhaseExecution, 0, 3)
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

				// Step 2: no further Kafka event; advance clock to T+330s (within R=60s)
				currentDateTime.SetNow(libtimetest.ParseDateTime(graceExpiredR))

				before := testutil.ToFloat64(
					metrics.TaskEventsTotal.WithLabelValues("respawn_after_grace_window"),
				)
				err = h.EvalDeferredRespawns(ctx)
				Expect(err).To(BeNil())

				// Deferred eval must have spawned once
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
				after := testutil.ToFloat64(
					metrics.TaskEventsTotal.WithLabelValues("respawn_after_grace_window"),
				)
				Expect(after - before).To(Equal(float64(1)))
			})

			It("deferred re-eval bound: no spawn before grace+R, spawn at grace+60s", func() {
				// Suppress inside grace window
				currentDateTime.SetNow(libtimetest.ParseDateTime(insideGrace))
				task := buildGraceTask(domain.TaskPhaseExecution, 0, 3)
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())

				// At grace-1s: evaluation fires but retryAfter not yet reached → no spawn
				currentDateTime.SetNow(libtimetest.ParseDateTime(graceExpiredM1))
				err = h.EvalDeferredRespawns(ctx)
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

				// At grace+60s: retryAfter reached → spawn
				currentDateTime.SetNow(libtimetest.ParseDateTime(graceExpiredMax))
				err = h.EvalDeferredRespawns(ctx)
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			})

			It(
				"deferred re-eval is idempotent when an event-driven spawn occurs during grace",
				func() {
					// Step 1: suppress inside grace window → deferred entry created
					currentDateTime.SetNow(libtimetest.ParseDateTime(insideGrace))
					task := buildGraceTask(domain.TaskPhaseExecution, 0, 3)
					err := h.ConsumeMessage(ctx, buildMsg(task))
					Expect(err).To(BeNil())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

					// Step 2: a fresh event-driven spawn occurs (new pod is now active)
					fakeSpawner.IsJobActiveReturns(
						true,
						nil,
					) // new pod active — simulates event-driven spawn

					// Step 3: advance clock past grace and eval — deferred check finds active job → no duplicate
					currentDateTime.SetNow(libtimetest.ParseDateTime(graceExpiredR))
					before := testutil.ToFloat64(
						metrics.TaskEventsTotal.WithLabelValues("respawn_after_grace_window"),
					)
					err = h.EvalDeferredRespawns(ctx)
					Expect(err).To(BeNil())
					// No duplicate spawn: active job suppresses it
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
					after := testutil.ToFloat64(
						metrics.TaskEventsTotal.WithLabelValues("respawn_after_grace_window"),
					)
					// Spec 037 AC #6: metric increments only when the eval results in a spawn.
					// Here the deferred eval no-ops (active job), so the delta MUST be 0.
					Expect(after - before).To(Equal(float64(0)))
				},
			)

			It(
				"terminal-status event cancels a pending deferred respawn (path C, dev 2026-07-13)",
				func() {
					// Step 1: suppress inside grace window → deferred entry created
					currentDateTime.SetNow(libtimetest.ParseDateTime(insideGrace))
					task := buildGraceTask(domain.TaskPhaseExecution, 0, 3)
					err := h.ConsumeMessage(ctx, buildMsg(task))
					Expect(err).To(BeNil())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

					// Step 2: the job completes and the agent publishes status=completed.
					// This terminal event must clear the deferred entry even though the
					// status filter skips it before the terminal-phase gate. Without the
					// removeDeferredEntry call in the terminal-status block, the entry
					// survives and respawns a job for an already-done task.
					completed := lib.Task{
						TaskIdentifier: lib.TaskIdentifier("tid-deferred-037"),
						Frontmatter: lib.TaskFrontmatter{
							"status":   "completed",
							"phase":    string(domain.TaskPhaseDone),
							"assignee": "claude",
							"stage":    "prod",
						},
					}
					Expect(h.ConsumeMessage(ctx, buildMsg(completed))).To(BeNil())

					// Step 3: advance past grace expiry and eval — the deferred entry was
					// cleared by the terminal event, so no respawn fires.
					currentDateTime.SetNow(libtimetest.ParseDateTime(graceExpiredR))
					err = h.EvalDeferredRespawns(ctx)
					Expect(err).To(BeNil())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
				},
			)

			It(
				"startup seed: stuck task in taskStore is re-evaluated after restart (AC #5)",
				func() {
					// Simulate the post-restart state: a fresh handler with an empty
					// deferredRespawns map but a taskStore that already holds the stuck task.
					// The zero-value config stored by the seed is acceptable here because
					// fakeSpawner does not inspect config fields.
					stuck := buildGraceTask(domain.TaskPhaseExecution, 0, 3)
					restartStore := pkg.NewTaskStore()
					restartStore.Store(stuck.TaskIdentifier, stuck)

					freshHandler := handler.NewTaskEventHandler(
						fakeSpawner,
						base.Branch("prod"),
						fakeResolver,
						fakeResultPublisher,
						restartStore,
						currentDateTime,
					)

					// Clock is past grace expiry — simulating the executor coming back up
					// long after the original suppression event.
					currentDateTime.SetNow(libtimetest.ParseDateTime(graceExpiredMax))

					before := fakeSpawner.SpawnJobCallCount()

					// Drive only the startup path: run the loop in a short-lived context
					// so the goroutine returns after the initial seed + immediate eval.
					shortCtx, cancel := context.WithCancel(ctx)
					done := make(chan error, 1)
					go func() { done <- freshHandler.RunDeferredRespawnLoop(shortCtx) }()
					// Allow the initial eval to run. The first eval runs synchronously
					// before the ticker starts, so cancelling after the spawn is observed is safe.
					Eventually(func() int {
						return fakeSpawner.SpawnJobCallCount()
					}).Should(BeNumerically(">=", before+1))
					cancel()
					Expect(<-done).To(BeNil())

					// Exactly one spawn from the seeded entry.
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(before + 1))
				},
			)

			It(
				"RunDeferredRespawnLoop returns error when initial evalDeferredRespawns fails",
				func() {
					// Seed a task into the store so seedDeferredRespawnsFromStore picks it up.
					// Set clock to graceExpiredR so the entry is immediately ready for eval.
					stuck := buildGraceTask(domain.TaskPhaseExecution, 0, 3)
					restartStore := pkg.NewTaskStore()
					restartStore.Store(stuck.TaskIdentifier, stuck)

					fakeResultPublisher.PublishIncrementTriggerCountReturns(
						errors.Errorf(ctx, "publish failed"),
					)

					freshHandler := handler.NewTaskEventHandler(
						fakeSpawner,
						base.Branch("prod"),
						fakeResolver,
						fakeResultPublisher,
						restartStore,
						currentDateTime,
					)

					currentDateTime.SetNow(libtimetest.ParseDateTime(graceExpiredR))

					done := make(chan error, 1)
					go func() { done <- freshHandler.RunDeferredRespawnLoop(ctx) }()
					err := <-done
					Expect(err).NotTo(BeNil())
					Expect(err.Error()).To(ContainSubstring("publish failed"))
				},
			)

			It(
				"RunDeferredRespawnLoop returns error when evalDeferredRespawns fails on ticker tick",
				func() {
					// Add two entries via ConsumeMessage at insideGrace (retryAfter = graceExpiredR).
					// The ConsumeMessage calls also trigger evalDeferredRespawns, but at insideGrace
					// the entries are not yet ready, so those evals succeed with nothing to do.
					// When RunDeferredRespawnLoop runs, seedDeferredRespawnsFromStore re-adds the
					// entries (they're still in taskStore). The initial eval finds nothing ready.
					// The first ticker tick at graceExpiredR finds both entries ready and processes
					// them. We configure PublishIncrementTriggerCount to error on its second
					// call (first was during ConsumeMessage, second is during the tick eval).
					currentDateTime.SetNow(libtimetest.ParseDateTime(insideGrace))

					taskA := buildGraceTask(domain.TaskPhaseExecution, 0, 3)
					taskA.TaskIdentifier = lib.TaskIdentifier("tid-deferred-tick-a")
					taskB := buildGraceTask(domain.TaskPhaseExecution, 0, 3)
					taskB.TaskIdentifier = lib.TaskIdentifier("tid-deferred-tick-b")

					err := h.ConsumeMessage(ctx, buildMsg(taskA))
					Expect(err).To(BeNil())
					err = h.ConsumeMessage(ctx, buildMsg(taskB))
					Expect(err).To(BeNil())

					fakeResultPublisher.PublishIncrementTriggerCountReturnsOnCall(
						1,
						errors.Errorf(ctx, "tick eval failed"),
					)

					// Advance clock to graceExpiredR so entries are ready when ticker fires.
					currentDateTime.SetNow(libtimetest.ParseDateTime(graceExpiredR))

					done := make(chan error, 1)
					go func() { done <- h.RunDeferredRespawnLoop(ctx) }()
					err = <-done
					Expect(err).NotTo(BeNil())
					Expect(err.Error()).To(ContainSubstring("tick eval failed"))
				},
			)

			It("deferred re-eval respects trigger cap", func() {
				// task with trigger_count == max_triggers — will hit skipped_trigger_cap in spawnIfNeeded
				currentDateTime.SetNow(libtimetest.ParseDateTime(insideGrace))
				task := buildGraceTask(domain.TaskPhaseExecution, 3, 3)
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

				currentDateTime.SetNow(libtimetest.ParseDateTime(graceExpiredR))
				beforeCap := testutil.ToFloat64(
					metrics.TaskEventsTotal.WithLabelValues("skipped_trigger_cap"),
				)
				err = h.EvalDeferredRespawns(ctx)
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
				afterCap := testutil.ToFloat64(
					metrics.TaskEventsTotal.WithLabelValues("skipped_trigger_cap"),
				)
				Expect(afterCap - beforeCap).To(Equal(float64(1)))
			})

			It("deferred re-eval entry is removed when a terminal-phase event arrives", func() {
				// Step 1: suppress inside grace → deferred entry created
				currentDateTime.SetNow(libtimetest.ParseDateTime(insideGrace))
				task := buildGraceTask(domain.TaskPhaseExecution, 0, 3)
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

				// Step 2: terminal-phase event arrives (spec 035 gate fires + removes deferred entry)
				// Use a custom trigger that includes human_review in its Phases so WITHOUT the gate it would spawn.
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee: "claude",
						Image:    "my-image:latest",
						Trigger: &agentv1.Trigger{
							Phases: domain.TaskPhases{
								domain.TaskPhaseExecution,
								domain.TaskPhaseHumanReview,
							},
							Statuses: domain.TaskStatuses{domain.TaskStatusInProgress},
						},
					},
					nil,
				)
				terminalTask := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-deferred-037"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "in_progress",
						"phase":    string(domain.TaskPhaseHumanReview),
						"assignee": "claude",
						"stage":    "prod",
					},
				}
				err = h.ConsumeMessage(ctx, buildMsg(terminalTask))
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

				// Step 3: advance clock past grace and eval — entry was removed by step 2 → no spawn
				currentDateTime.SetNow(libtimetest.ParseDateTime(graceExpiredR))
				err = h.EvalDeferredRespawns(ctx)
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			})
		})

		Describe("grace window (spec 036)", func() {
			BeforeEach(func() {
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeSpawner.SpawnJobReturns("job-grace-1", nil)
			})

			It("treats malformed job_started_at as elapsed and spawns", func() {
				// Malformed job_started_at (not parseable as time.RFC3339) must be treated
				// as elapsed — the grace window is bypassed and spawn proceeds.
				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-05-16T20:19:26Z"))
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-grace-parse-err"),
					Frontmatter: lib.TaskFrontmatter{
						"status":         "in_progress",
						"phase":          string(domain.TaskPhaseExecution),
						"assignee":       "claude",
						"current_job":    "pod-A",
						"job_started_at": "not-a-valid-timestamp",
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			})

			DescribeTable("grace-window decision matrix",
				func(
					currentJob string,
					jobStartedAt string,
					nowAt string,
					expectSpawn int,
					expectSuppress float64,
				) {
					currentDateTime.SetNow(libtimetest.ParseDateTime(nowAt))
					fm := lib.TaskFrontmatter{
						"status":   "in_progress",
						"phase":    string(domain.TaskPhaseExecution),
						"assignee": "claude",
					}
					if currentJob != "" {
						fm["current_job"] = currentJob
					}
					if jobStartedAt != "" {
						fm["job_started_at"] = jobStartedAt
					}
					task := lib.Task{
						TaskIdentifier: lib.TaskIdentifier("tid-grace-table"),
						Frontmatter:    fm,
					}
					before := testutil.ToFloat64(
						metrics.TaskEventsTotal.WithLabelValues("respawn_grace_window"),
					)
					err := h.ConsumeMessage(ctx, buildMsg(task))
					Expect(err).To(BeNil())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(expectSpawn))
					after := testutil.ToFloat64(
						metrics.TaskEventsTotal.WithLabelValues("respawn_grace_window"),
					)
					Expect(after - before).To(Equal(expectSuppress))
				},
				Entry(
					"current_job set, job inactive, within grace => suppress",
					"pod-A", "2026-05-16T20:19:16Z", "2026-05-16T20:19:26Z", // T+10s
					0, float64(1),
				),
				Entry(
					"current_job set, job inactive, past grace => spawn",
					"pod-A", "2026-05-16T20:19:16Z", "2026-05-16T20:24:26Z", // T+310s
					1, float64(0),
				),
				Entry(
					"current_job empty, job inactive => spawn (no grace check)",
					"", "", "2026-05-16T20:19:26Z",
					1, float64(0),
				),
				Entry(
					"current_job set, job inactive, job_started_at absent (legacy) => spawn",
					"pod-legacy", "", "2026-05-16T20:19:26Z",
					1, float64(0),
				),
			)
		})
	})
})
