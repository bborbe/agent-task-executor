// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6@v6.12.2 -generate

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
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
		fakeGitRestClient   *mocks.FakeGitRestClient
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
		fakeGitRestClient = &mocks.FakeGitRestClient{}
		fakeGitRestClient.IsReadyReturns(true, nil)
		taskStore = pkg.NewTaskStore()
		currentDateTime = libtime.NewCurrentDateTime()
		h = handler.NewTaskEventHandler(
			fakeSpawner,
			base.Branch("prod"),
			fakeResolver,
			fakeResultPublisher,
			taskStore,
			currentDateTime,
			fakeGitRestClient,
			"24 Tasks/*.md",
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
			Expect(testutil.ToFloat64(
				metrics.SkippedUnknownAssigneeTotal.WithLabelValues("unknown-agent"),
			)).To(Equal(float64(1)))
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

		It("defers instead of spawning when the assignee is at MaxConcurrentJobs", func() {
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{
					Assignee:          "claude",
					Image:             "my-image:latest",
					MaxConcurrentJobs: 4,
				},
				nil,
			)
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.CountActiveJobsReturns(4, nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-cap-at-limit"),
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

		It("spawns when the assignee is below MaxConcurrentJobs", func() {
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{
					Assignee:          "claude",
					Image:             "my-image:latest",
					MaxConcurrentJobs: 4,
				},
				nil,
			)
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.CountActiveJobsReturns(3, nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-cap-below"),
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

		It("admits exactly MaxConcurrentJobs when spawns arrive concurrently", func() {
			// Regression for the check-then-act race fixed by the per-assignee
			// spawn lock. In prod on 2026-08-15 a cap of 1 admitted 17 Jobs from
			// 36 concurrent releases, because every caller read the same live Job
			// count before any of them had created its Job.
			//
			// The fake counts like the real cluster: CountActiveJobs reports the
			// Jobs actually created so far. Without the lock the count is stale
			// for every goroutine and this spawns far more than the cap.
			const concurrent = 10
			var spawned atomic.Int64
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{
					Assignee:          "claude",
					Image:             "my-image:latest",
					MaxConcurrentJobs: 1,
				},
				nil,
			)
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.CountActiveJobsStub = func(_ context.Context, _ string) (int, error) {
				// Widen the count→spawn window deliberately. The real race is a
				// sub-millisecond gap between a live API List and the Job CREATE;
				// without this delay the unlocked code wins the race often enough
				// that the test passes by luck. Under the lock these sleeps
				// serialize (10 * 20ms) instead of overlapping, so the delay
				// cannot mask a regression — it only makes one visible.
				time.Sleep(20 * time.Millisecond)
				return int(spawned.Load()), nil
			}
			fakeSpawner.SpawnJobStub = func(
				_ context.Context,
				_ lib.Task,
				_ pkg.AgentConfiguration,
			) (string, error) {
				spawned.Add(1)
				return "job-name", nil
			}

			var wg sync.WaitGroup
			for i := range concurrent {
				wg.Add(1)
				go func(i int) {
					defer GinkgoRecover()
					defer wg.Done()
					task := lib.Task{
						TaskIdentifier: lib.TaskIdentifier(
							fmt.Sprintf("tid-cap-concurrent-%d", i),
						),
						Frontmatter: lib.TaskFrontmatter{
							"status":   "in_progress",
							"phase":    string(domain.TaskPhaseExecution),
							"assignee": "claude",
						},
					}
					Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())
				}(i)
			}
			wg.Wait()

			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		It("does not count active jobs when MaxConcurrentJobs is unset", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-cap-unset"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
				},
			}

			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.CountActiveJobsCallCount()).To(Equal(0))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
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
						"trigger_scope": "ai_review:",
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
					"status": "in_progress",
					"phase":  string(domain.TaskPhaseAIReview),
					// Matches triggerScope() for this phase with no ref, so the task
					// takes the same-scope increment path rather than scope adoption.
					"trigger_scope": "ai_review:",
					"assignee":      "claude",
					"stage":         "prod",
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
					"trigger_scope": "ai_review:",
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

		It("does not skip spawn when max_triggers is absent (recurring task)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("sentry-collector-job-1", nil)
			// The regression guard for the 2026-08-27 Daily Sentry Triage incident,
			// where the lib default-3 fallback stripped assignee on the 3rd trigger
			// and silently killed the re-dispatch loop. trigger_count 5 would trip
			// that fallback; absent max_triggers means no cap, so the recurring
			// daily trigger keeps re-dispatching.
			//
			// Scoping must NOT change this. A recurring task is not repo-backed: it
			// carries no ref and sits at a stable phase, so its scope is constant and
			// would never earn a reset. The default-engage gate keys on ref presence,
			// so this task (no ref, no max_triggers) stays uncapped — the v0.7.1
			// regression guard.
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-cap-absent"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         string(domain.TaskPhasePlanning),
					"assignee":      "sentry-collector-agent",
					"stage":         "prod",
					"trigger_count": 5,
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			// No trigger_scope on disk, so this task adopts the current scope rather
			// than incrementing. Adoption carries the existing count forward (5+1),
			// never resetting it — a task already at cap must not gain a free budget
			// on the first event after deploy.
			Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(0))
			Expect(fakeResultPublisher.PublishSetTriggerScopeCallCount()).To(Equal(1))
			_, _, scope, count := fakeResultPublisher.PublishSetTriggerScopeArgsForCall(0)
			Expect(scope).To(Equal("planning:"))
			Expect(count).To(Equal(6))
		})

		It("resets the budget when the ref changes (new commit earns a retry)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("github-update-go-job-1", nil)
			// At cap under the old scope, but the target repo moved to a new commit.
			// This is the operator fixing a broken gate and pushing: the failure is
			// no longer the same failure, so the task earns a fresh budget.
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-scope-newref"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         string(domain.TaskPhaseAIReview),
					"trigger_scope": "ai_review:d0d96f7a",
					"ref":           "9c4e1b77aaaaaaaaaaaaaaaa",
					"assignee":      "github-update-go-agent",
					"stage":         "prod",
					"trigger_count": 3,
					"max_triggers":  3,
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			Expect(fakeResultPublisher.PublishSetTriggerScopeCallCount()).To(Equal(1))
			_, _, scope, count := fakeResultPublisher.PublishSetTriggerScopeArgsForCall(0)
			Expect(scope).To(Equal("ai_review:9c4e1b77"))
			Expect(count).To(Equal(1))
		})

		It("keeps capping when the ref is unchanged (deterministic gate failure)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			// The incident shape: a gate broken at one commit, retried forever. Same
			// phase, same ref, so the scope never moves and the budget stays spent.
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-scope-sameref"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         string(domain.TaskPhaseAIReview),
					"trigger_scope": "ai_review:d0d96f7a",
					"ref":           "d0d96f7abbbbbbbbbbbbbbbb",
					"assignee":      "github-update-go-agent",
					"stage":         "prod",
					"trigger_count": 3,
					"max_triggers":  3,
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			Expect(fakeResultPublisher.PublishSetTriggerScopeCallCount()).To(Equal(0))
			Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(0))
		})

		It(
			"skips spawn for a repo-backed task at the default cap (ref present, max_triggers absent)",
			func() {
				fakeSpawner.IsJobActiveReturns(false, nil)
				// The bug shape: the github-update-go watcher emits a ref but never
				// max_triggers, so the cap never engaged and a deterministically failing
				// planning gate respawned unbounded. ref presence now default-engages the
				// cap at the lib default of 3.
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-cap-repo-atcap"),
					Frontmatter: lib.TaskFrontmatter{
						"status":        "in_progress",
						"phase":         string(domain.TaskPhasePlanning),
						"trigger_scope": "planning:314c4b32",
						"ref":           "314c4b32aaaaaaaaaaaaaaaa",
						"assignee":      "github-update-go-agent",
						"stage":         "prod",
						"trigger_count": 3,
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(0))
				Expect(fakeResultPublisher.PublishSetTriggerScopeCallCount()).To(Equal(0))
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			},
		)

		It(
			"spawns and increments for a repo-backed task below the default cap (ref present, max_triggers absent)",
			func() {
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeSpawner.SpawnJobReturns("github-update-go-job-1", nil)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-cap-repo-below"),
					Frontmatter: lib.TaskFrontmatter{
						"status":        "in_progress",
						"phase":         string(domain.TaskPhasePlanning),
						"trigger_scope": "planning:314c4b32",
						"ref":           "314c4b32aaaaaaaaaaaaaaaa",
						"assignee":      "github-update-go-agent",
						"stage":         "prod",
						"trigger_count": 2,
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(1))
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			},
		)

		It(
			"uses an explicit max_triggers above the default for a repo-backed task (ref present)",
			func() {
				fakeSpawner.IsJobActiveReturns(false, nil)
				fakeSpawner.SpawnJobReturns("github-update-go-job-2", nil)
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("test-task-cap-repo-override"),
					Frontmatter: lib.TaskFrontmatter{
						"status":        "in_progress",
						"phase":         string(domain.TaskPhasePlanning),
						"trigger_scope": "planning:314c4b32",
						"ref":           "314c4b32aaaaaaaaaaaaaaaa",
						"assignee":      "github-update-go-agent",
						"stage":         "prod",
						"trigger_count": 4,
						"max_triggers":  5,
					},
				}
				err := h.ConsumeMessage(ctx, buildMsg(task))
				Expect(err).To(BeNil())
				Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(1))
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			},
		)

		It("does not hand a task already at cap a free budget on scope adoption", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("claude-job-adopt", nil)
			// The migration case: a task in flight before trigger_scope existed, and
			// already at cap. An ABSENT scope must not read as a CHANGED scope — if
			// it did, this task would reset to a fresh budget on the first event
			// after deploy and resume looping for another full round.
			//
			// It never reaches scope adoption: absent means not-changed, so the cap
			// check fires first and the spawn is skipped outright. No publish at all.
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-scope-adopt-atcap"),
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
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			Expect(fakeResultPublisher.PublishSetTriggerScopeCallCount()).To(Equal(0))
			Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(0))
		})

		It("adopts the scope and carries the count forward when below cap", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("claude-job-adopt-ok", nil)
			// Same migration case, but with budget left. The task adopts the current
			// scope and the count moves 1 -> 2 rather than resetting to 1, so the
			// pre-existing attempts are not forgotten.
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-scope-adopt-below"),
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
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			Expect(fakeResultPublisher.PublishSetTriggerScopeCallCount()).To(Equal(1))
			_, _, scope, count := fakeResultPublisher.PublishSetTriggerScopeArgsForCall(0)
			Expect(scope).To(Equal("ai_review:"))
			Expect(count).To(Equal(2))
		})

		It("adopts to exactly the cap, spawning now and capping on the next event", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("claude-job-adopt-boundary", nil)
			// Boundary between the two adoption specs above: 2 -> 3 with max_triggers 3.
			// This spawn is allowed (the cap is checked against the count BEFORE the
			// write, 2 < 3), and adoption lands the task exactly at the cap so the
			// next event is skipped. Guards the off-by-one in both directions: adopting
			// to 4 would skip a legitimate spawn, adopting to 2 would grant an extra.
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-scope-adopt-boundary"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         string(domain.TaskPhaseAIReview),
					"assignee":      "claude",
					"stage":         "prod",
					"trigger_count": 2,
					"max_triggers":  3,
				},
			}
			err := h.ConsumeMessage(ctx, buildMsg(task))
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			Expect(fakeResultPublisher.PublishSetTriggerScopeCallCount()).To(Equal(1))
			_, _, scope, count := fakeResultPublisher.PublishSetTriggerScopeArgsForCall(0)
			Expect(scope).To(Equal("ai_review:"))
			Expect(count).To(Equal(3))
		})

		It("publishes increment and spawns when below cap (happy path)", func() {
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("claude-job-1", nil)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-cap-2"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         string(domain.TaskPhaseAIReview),
					"trigger_scope": "ai_review:",
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
						"trigger_scope": "ai_review:",
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
					"trigger_scope": "ai_review:",
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
					"trigger_scope": "ai_review:",
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
				fakeGitRestClient,
				"24 Tasks/*.md",
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
				fakeGitRestClient,
				"24 Tasks/*.md",
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

	})
})
