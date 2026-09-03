// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
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
	"gopkg.in/yaml.v3"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

var _ = Describe("TaskEventHandler reconcile loop", func() {
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

	// renderTaskFile serializes a task into the vault markdown shape the
	// reconcile loop parses (frontmatter delimited by "---", body after).
	renderTaskFile := func(task lib.Task) string {
		fm := make(map[string]any, len(task.Frontmatter)+1)
		for k, v := range task.Frontmatter {
			fm[k] = v
		}
		fm["task_identifier"] = string(task.TaskIdentifier)
		out, err := yaml.Marshal(fm)
		Expect(err).To(BeNil())
		return "---\n" + string(out) + "---\n# Task body\n"
	}

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

	// eligibleTask builds a task that passes every reconcile eligibility gate:
	// status in_progress, phase in the default trigger set, assignee set, stage
	// matching the executor branch, no current_job key (the shape a concurrency-cap
	// deferral leaves in the vault).
	eligibleTask := func(phase domain.TaskPhase) lib.Task {
		return lib.Task{
			TaskIdentifier: "tid-a",
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(phase),
				"assignee": "claude",
				"stage":    "prod",
			},
		}
	}

	It("re-drives an eligible task from the vault (AC 1)", func() {
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(eligibleTask(domain.TaskPhasePlanning))),
			nil,
		)
		fakeSpawner.IsJobActiveReturns(false, nil)

		before := testutil.ToFloat64(metrics.ReconcileRedrivenTotal)
		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(testutil.ToFloat64(metrics.ReconcileRedrivenTotal)).To(Equal(before + 1))
	})

	It("recovers a deferred task after a restart with an empty task store (AC 2)", func() {
		// Simulate the post-restart wipe: a fresh empty store, and a vault task in
		// the shape deferIfAtConcurrencyCap leaves behind — status in_progress,
		// phase execution, no current_job key — with no Kafka event driving it.
		task := eligibleTask(domain.TaskPhaseExecution)
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
		fakeGitRestClient.GetReturns([]byte(renderTaskFile(task)), nil)
		fakeSpawner.IsJobActiveReturns(false, nil)

		Expect(taskStore.Snapshot()).To(BeEmpty())

		before := testutil.ToFloat64(metrics.ReconcileRedrivenTotal)
		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(testutil.ToFloat64(metrics.ReconcileRedrivenTotal)).To(Equal(before + 1))
	})

	DescribeTable("skips ineligible tasks — SpawnJob not called (AC 3)",
		func(task lib.Task, setup func()) {
			if setup != nil {
				setup()
			}
			fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
			fakeGitRestClient.GetReturns([]byte(renderTaskFile(task)), nil)
			err := h.ReconcileOnce(ctx)
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		},
		Entry("status todo (not in_progress)", lib.Task{
			TaskIdentifier: "tid-a",
			Frontmatter: lib.TaskFrontmatter{
				"status":   "todo",
				"phase":    string(domain.TaskPhaseExecution),
				"assignee": "claude",
				"stage":    "prod",
			},
		}, nil),
		Entry("phase todo (not in default trigger set)", lib.Task{
			TaskIdentifier: "tid-a",
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    "todo",
				"assignee": "claude",
				"stage":    "prod",
			},
		}, nil),
		Entry("phase human_review (terminal)", lib.Task{
			TaskIdentifier: "tid-a",
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(domain.TaskPhaseHumanReview),
				"assignee": "claude",
				"stage":    "prod",
			},
		}, nil),
		Entry("empty assignee (assignee key absent)", lib.Task{
			TaskIdentifier: "tid-a",
			Frontmatter: lib.TaskFrontmatter{
				"status": "in_progress",
				"phase":  string(domain.TaskPhaseExecution),
				"stage":  "prod",
			},
		}, nil),
		Entry("unresolvable config", lib.Task{
			TaskIdentifier: "tid-a",
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(domain.TaskPhaseExecution),
				"assignee": "claude",
				"stage":    "prod",
			},
		}, func() {
			fakeResolver.ResolveReturns(pkg.AgentConfiguration{}, pkg.ErrConfigNotFound)
		}),
		Entry("live Job already active", lib.Task{
			TaskIdentifier: "tid-a",
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(domain.TaskPhaseExecution),
				"assignee": "claude",
				"stage":    "prod",
			},
		}, func() {
			fakeSpawner.IsJobActiveReturns(true, nil)
		}),
		Entry("stage mismatch (stage dev, executor branch prod)", lib.Task{
			TaskIdentifier: "tid-a",
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(domain.TaskPhaseExecution),
				"assignee": "claude",
				"stage":    "dev",
			},
		}, nil),
	)

	It("respects the concurrency cap and defers instead of spawning (AC 4)", func() {
		fakeResolver.ResolveReturns(
			pkg.AgentConfiguration{
				Assignee:          "claude",
				Image:             "my-image:latest",
				MaxConcurrentJobs: 1,
			},
			nil,
		)
		fakeSpawner.CountActiveJobsReturns(1, nil) // at cap
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(eligibleTask(domain.TaskPhaseExecution))),
			nil,
		)

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("skips the pass when git-rest is not ready (failure mode)", func() {
		fakeGitRestClient.IsReadyReturns(false, nil)
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeGitRestClient.ListCallCount()).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("skips the pass when the readiness check errors (failure mode)", func() {
		fakeGitRestClient.IsReadyReturns(false, errors.Errorf(ctx, "boom"))
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeGitRestClient.ListCallCount()).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("skips the pass when List errors (failure mode)", func() {
		fakeGitRestClient.IsReadyReturns(true, nil)
		fakeGitRestClient.ListReturns(nil, errors.Errorf(ctx, "rate limited"))

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("skips the file when Get errors and returns nil (failure mode)", func() {
		fakeGitRestClient.IsReadyReturns(true, nil)
		fakeGitRestClient.ListReturns([]string{"24 Tasks/bad.md"}, nil)
		fakeGitRestClient.GetReturns(nil, errors.Errorf(ctx, "boom"))

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("skips a file whose frontmatter is unparseable YAML", func() {
		fakeGitRestClient.ListReturns([]string{"24 Tasks/bad.md"}, nil)
		fakeGitRestClient.GetReturns([]byte("---\n[unclosed\n---\n# Task body\n"), nil)

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("skips a file with no task_identifier", func() {
		fakeGitRestClient.ListReturns([]string{"24 Tasks/notes.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(
				"---\nstatus: in_progress\nphase: execution\nassignee: claude\nstage: prod\n---\n# Task body\n",
			),
			nil,
		)

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("skips a file whose frontmatter is never closed", func() {
		fakeGitRestClient.ListReturns([]string{"24 Tasks/bad.md"}, nil)
		fakeGitRestClient.GetReturns([]byte("---\nstatus: in_progress\n# Task body\n"), nil)

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("parses CRLF task files and preserves the markdown body verbatim", func() {
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(
				"---\r\nstatus: in_progress\r\nphase: execution\r\nassignee: claude\r\nstage: prod\r\ntask_identifier: tid-a\r\n---\r\n# Task body\r\n",
			),
			nil,
		)
		fakeSpawner.IsJobActiveReturns(false, nil)

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		_, spawnedTask, _ := fakeSpawner.SpawnJobArgsForCall(0)
		Expect(string(spawnedTask.Content)).To(Equal("# Task body\r\n"))
	})

	It("runs one pass immediately at startup before any ticker fire (DB 1)", func() {
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(eligibleTask(domain.TaskPhaseExecution))),
			nil,
		)

		shortCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- h.RunReconcileLoop(shortCtx) }()
		// The interval is 60s, so only the startup pass can run in this test; the
		// startup pass runs synchronously before the ticker starts.
		Eventually(fakeGitRestClient.ListCallCount).Should(BeNumerically(">=", 1))
		cancel()
		Expect(<-done).To(BeNil())
	})

	It(
		"aborts a pass when the parent context is cancelled (DB 7 / slow-vault failure mode)",
		func() {
			fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
			fakeGitRestClient.GetReturns(
				[]byte(renderTaskFile(eligibleTask(domain.TaskPhaseExecution))),
				nil,
			)

			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			err := h.ReconcileOnce(cancelCtx)
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		},
	)

	It(
		"races the reconcile path against the Kafka event path for an uncapped task and spawns exactly one job (AC 5)",
		func() {
			// Spec 005 AC 5: with the per-assignee lock taken unconditionally, a
			// reconcile tick racing the event path for the SAME uncapped task results
			// in exactly one Job. Without the unconditional lock both goroutines read
			// IsJobActive=false before either has created its Job, then both spawn.
			// The fake is stateful like the real cluster: IsJobActive reports whether a
			// Job has been created, and the sleep widens the read->spawn window so the
			// unlocked race is deterministic rather than luck-of-the-timing.
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{
					Assignee:          "claude",
					Image:             "my-image:latest",
					MaxConcurrentJobs: 0,
				},
				nil,
			)
			var spawned atomic.Int64
			fakeSpawner.CountActiveJobsReturns(0, nil)
			fakeSpawner.SpawnJobStub = func(
				_ context.Context,
				_ lib.Task,
				_ pkg.AgentConfiguration,
			) (string, error) {
				spawned.Add(1)
				return "job-name", nil
			}
			fakeSpawner.IsJobActiveStub = func(
				_ context.Context,
				_ lib.TaskIdentifier,
			) (bool, error) {
				time.Sleep(20 * time.Millisecond)
				return spawned.Load() > 0, nil
			}
			fakeGitRestClient.IsReadyReturns(true, nil)

			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-double-spawn"),
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhaseExecution),
					"assignee": "claude",
					"stage":    "prod",
				},
			}
			fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-double-spawn.md"}, nil)
			fakeGitRestClient.GetReturns([]byte(renderTaskFile(task)), nil)

			// NOTE: buildMsg in task_event_handler_test.go is a Describe-scoped
			// closure (not package-level) and is NOT visible from this file. Define a
			// local equivalent so the regression spec is self-contained.
			buildMsg := func(t lib.Task) *sarama.ConsumerMessage {
				value, err := json.Marshal(t)
				Expect(err).To(BeNil())
				return &sarama.ConsumerMessage{Value: value}
			}

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())
			}()
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				Expect(h.ReconcileOnce(ctx)).To(BeNil())
			}()
			wg.Wait()

			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		},
	)

	It(
		"seeds deferred entries with a resolved config so an at-cap task is deferred, not spawned with an empty config (MF-1)",
		func() {
			// MF-1 regression: seedDeferredRespawnsFromStore used to seed
			// zero-valued configs; a seeded entry past its grace window was then
			// evaluated by evalDeferredRespawns → spawnIfNeeded, which proceeded
			// to SpawnJob with an empty (zero-valued) AgentConfiguration. With the
			// config resolved at seed time, an at-cap assignee's seeded task is
			// deferred instead of spawned.
			past := currentDateTime.Now().Time().Add(-10 * time.Minute).Format(time.RFC3339)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-seed-mf1"),
				Frontmatter: lib.TaskFrontmatter{
					"status":         "in_progress",
					"phase":          string(domain.TaskPhaseExecution),
					"assignee":       "claude",
					"current_job":    "job-seed-mf1",
					"job_started_at": past,
				},
			}
			taskStore.Store(task.TaskIdentifier, task)
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{
					Assignee:          "claude",
					Image:             "my-image:latest",
					MaxConcurrentJobs: 1,
				},
				nil,
			)
			fakeSpawner.CountActiveJobsReturns(1, nil) // at cap
			fakeSpawner.IsJobActiveReturns(false, nil) // prior job is gone

			loopCtx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				done <- h.RunDeferredRespawnLoop(loopCtx)
			}()
			time.Sleep(100 * time.Millisecond) // let the startup seed + initial eval run
			cancel()
			Eventually(done).Should(Receive(BeNil()))

			// At cap → deferred, not spawned (a zero-valued config would have spawned).
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			Expect(fakeResolver.ResolveCallCount()).To(BeNumerically(">=", 1))
		},
	)
})
