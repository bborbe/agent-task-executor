// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
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

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
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
		fakeView            *mocks.FakeTaskView
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
		fakeView = &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		taskStore = pkg.NewTaskStore()
		currentDateTime = libtime.NewCurrentDateTime()
		h = handler.NewTaskEventHandler(
			fakeSpawner,
			base.Branch("prod"),
			"",
			true,
			true,
			fakeResolver,
			fakeResultPublisher,
			taskStore,
			currentDateTime,
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
		)
	})

	buildMsg := func(t lib.Task) *sarama.ConsumerMessage {
		if t.Vault == "" {
			t.Vault = testTaskVault
		}
		value, err := json.Marshal(t)
		Expect(err).To(BeNil())
		return &sarama.ConsumerMessage{Value: value}
	}

	// eligibleTask builds a task that passes every reconcile eligibility gate:
	// status in_progress, phase in the default trigger set, assignee set, stage
	// matching the executor branch, no current_job key (the shape a concurrency-cap
	// deferral leaves in the vault), and stamped with this executor's vault.
	eligibleTask := func(phase domain.TaskPhase) lib.Task {
		return lib.Task{
			TaskIdentifier: "tid-a",
			Vault:          testTaskVault,
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(phase),
				"assignee": "claude",
				"stage":    "prod",
			},
		}
	}

	// narrowedTriggerConfig is a Config whose trigger.phases lists only
	// `execution` — the shape the build-fix Config was narrowed to on 2026-09-24
	// to match the single phase its lane binary registers.
	narrowedTriggerConfig := func() pkg.AgentConfiguration {
		return pkg.AgentConfiguration{
			Assignee: "claude",
			Image:    "my-image:latest",
			Trigger: &agentv1.Trigger{
				Phases:   domain.TaskPhases{domain.TaskPhaseExecution},
				Statuses: domain.TaskStatuses{domain.TaskStatusInProgress},
			},
		}
	}

	It("skips an untagged task without resolving Config when RUN_UNTAGGED is off", func() {
		untaggedResolver := &mocks.FakeConfigResolver{}
		untaggedHandler := handler.NewTaskEventHandler(
			fakeSpawner,
			base.Branch("prod"),
			"sun",
			true,
			false,
			untaggedResolver,
			fakeResultPublisher,
			taskStore,
			currentDateTime,
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
		)
		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhasePlanning)})
		fakeSpawner.IsJobActiveReturns(false, nil)

		err := untaggedHandler.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		Expect(untaggedResolver.ResolveCallCount()).To(Equal(0))
	})

	It("re-drives an eligible task from the view (AC 1)", func() {
		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhasePlanning)})
		fakeSpawner.IsJobActiveReturns(false, nil)

		before := testutil.ToFloat64(metrics.ReconcileRedrivenTotal)
		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(testutil.ToFloat64(metrics.ReconcileRedrivenTotal)).To(Equal(before + 1))
	})

	Describe("a Job that already succeeded but whose result has not reached the vault", func() {
		// Measured nuke dev 2026-10-21: task a055d019's claude-headless Job
		// finished in ~20 s, the file still read `phase: execution` with no
		// current_job, and every 60 s reconcile tick spawned it again.
		var now time.Time

		BeforeEach(func() {
			now = time.Date(2026, 10, 21, 5, 56, 23, 0, time.UTC)
			currentDateTime.SetNow(libtime.DateTime(now))
			fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})
		})

		It("is not re-driven while the success is inside the grace window", func() {
			taskStore.MarkSucceeded("tid-a", "execution", now.Add(-20*time.Second))

			Expect(h.ReconcileOnce(ctx)).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		})

		It(
			"is not re-driven when the success is stamped slightly in the future (clock skew)",
			func() {
				taskStore.MarkSucceeded("tid-a", "execution", now.Add(2*time.Second))

				Expect(h.ReconcileOnce(ctx)).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			},
		)

		It("is spawned at once when the agent advanced the task to its next phase", func() {
			// The success was for planning; the view now reads execution, so the
			// Job for the next phase must not wait out the grace window.
			taskStore.MarkSucceeded("tid-a", "planning", now.Add(-20*time.Second))

			Expect(h.ReconcileOnce(ctx)).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		It("is re-driven once the grace window has passed", func() {
			taskStore.MarkSucceeded("tid-a", "execution", now.Add(-10*time.Minute))

			Expect(h.ReconcileOnce(ctx)).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		})

		It("still re-drives a task whose Job failed, up to the redrive cap", func() {
			// No success is recorded for a failed Job, so the guard stays out of
			// the way and the existing streak cap (maxConsecutiveRedrives = 3)
			// is what stops it.
			for range 5 {
				Expect(h.ReconcileOnce(ctx)).To(BeNil())
			}
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(3))
		})
	})

	It(
		"honours the Config trigger phases — a phase the Config does not list is not re-driven",
		func() {
			// Regression (2026-09-24): reconcileTask gated on the HARDCODED
			// defaultTriggerPhases instead of the Config's trigger.phases, so a Config
			// that narrowed its phase list was ignored on this path and a task sitting
			// at a dropped phase was re-driven every reconcile tick (60s) forever —
			// while the Kafka path (parseAndFilter) honoured the same Config. That
			// asymmetry is what made the build-fix lane's ~1/min spawn loop survive a
			// Config narrowing that should have stopped it.
			fakeResolver.ResolveReturns(narrowedTriggerConfig(), nil)
			fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhasePlanning)})
			fakeSpawner.IsJobActiveReturns(false, nil)

			err := h.ReconcileOnce(ctx)
			Expect(err).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		},
	)

	It("still re-drives a phase the Config does list", func() {
		// The discriminating half: the narrowed Config must not stop the phases it
		// DOES list, or the fix would trade a loop for a dead lane.
		fakeResolver.ResolveReturns(narrowedTriggerConfig(), nil)
		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})
		fakeSpawner.IsJobActiveReturns(false, nil)

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("recovers a deferred task after a restart with an empty task store (AC 2)", func() {
		// Simulate the post-restart wipe: a fresh empty store, and a view task in
		// the shape deferIfAtConcurrencyCap leaves behind — status in_progress,
		// phase execution, no current_job key — with no Kafka event driving it.
		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})
		fakeSpawner.IsJobActiveReturns(false, nil)

		Expect(taskStore.Snapshot()).To(BeEmpty())

		before := testutil.ToFloat64(metrics.ReconcileRedrivenTotal)
		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(testutil.ToFloat64(metrics.ReconcileRedrivenTotal)).To(Equal(before + 1))
	})

	It(
		"caps consecutive re-drives of a stuck task and stops spawning (regression: unbounded 60s re-drive)",
		func() {
			// Observed on dev 2026-10-13 and again 2026-10-14: a parked task
			// carrying neither `ref` nor `max_triggers` produced a new Job every
			// 15-30 min indefinitely. The trigger budget cannot bound it — with
			// neither field set, applyTriggerBudget's cap branch is disarmed by
			// design (arming it re-creates the 2026-08-27 Daily Sentry Triage
			// incident) — and the agent pods rewrite the frontmatter from their
			// run-start snapshot on every completion, so trigger_scope is clobbered
			// back, scopeChanged is true on every spawn, and trigger_count is reset
			// to 1 each time.
			fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("job-1", nil)

			before := testutil.ToFloat64(metrics.RedriveCappedTotal)

			// Five passes is two past the cap: a pre-fix build spawns on all five.
			for i := 0; i < 5; i++ {
				Expect(h.ReconcileOnce(ctx)).To(Succeed())
			}

			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(3))
			Expect(testutil.ToFloat64(metrics.RedriveCappedTotal)).To(Equal(before + 2))
		},
	)

	It("resets the streak when the task advances to a new phase", func() {
		// The cap bounds a STREAK, not a task. A task that moved phase has
		// advanced, and the executor must serve the new phase — otherwise the cap
		// would strand a lane that legitimately progresses, which is the
		// over-capping regression spec 006 guards.
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeSpawner.SpawnJobReturns("job-1", nil)

		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})
		for i := 0; i < 3; i++ {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		}
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(3))

		// At the cap, and still at the same phase: the next spawn is skipped.
		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(3))

		// The task advances — the phase changed, so the streak starts over.
		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseAIReview)})
		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(4))
	})

	It(
		"counts a Kafka-path spawn toward the same streak (SC1/SC2 regression)",
		func() {
			// The regression this test exists for. v0.18.8 counted only the
			// reconcile floor's OWN spawns, so a Kafka-path spawn landing between
			// two floor spawns cleared the streak before it could reach the cap —
			// measured on the dev fixture 2026-10-14 as four Jobs in 70 min with
			// capped=0 on all 68 reconcile passes, and the floor responsible for
			// only two of the four. Counting every spawn, at the one funnel all
			// three paths share, is what makes the bound hold.
			task := eligibleTask(domain.TaskPhaseExecution)
			fakeView.SnapshotReturns([]lib.Task{task})
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("job-1", nil)

			// Two floor re-drives, then a Kafka-path spawn.
			for i := 0; i < 2; i++ {
				Expect(h.ReconcileOnce(ctx)).To(Succeed())
			}
			Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(Succeed())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(3))

			// The streak now stands at 3 across BOTH paths, so the floor is capped.
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(3))

			// And so is the Kafka path: the cap lives in spawnIfNeeded, not at
			// either call site, so no path can bypass it.
			Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(Succeed())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(3))
		},
	)

	DescribeTable("skips ineligible tasks — SpawnJob not called (AC 3)",
		func(task lib.Task, setup func()) {
			if setup != nil {
				setup()
			}
			task.Vault = testTaskVault
			fakeView.SnapshotReturns([]lib.Task{task})
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
			fakeSpawner.ListActiveJobsReturns(map[lib.TaskIdentifier]bool{"tid-a": true}, nil)
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
		fakeSpawner.ListActiveJobsReturns(activeJobs(1), nil) // at cap
		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("admits a never-attempted child ahead of an older task that already ran", func() {
		// Prod 2026-10-21: 67 eligible children against maxConcurrentJobs 1, and
		// both admissions in a 20-minute window were the first two files in
		// filename order — so a child delegated today (named 20261021-…) sorted
		// last and was never reached. The pass must admit the never-attempted
		// child, not whichever task the snapshot happens to yield first.
		fakeResolver.ResolveReturns(
			pkg.AgentConfiguration{
				Assignee:          "claude",
				Image:             "my-image:latest",
				MaxConcurrentJobs: 1,
			},
			nil,
		)
		older := eligibleTask(domain.TaskPhaseExecution)
		older.TaskIdentifier = "tid-older"
		older.Frontmatter["job_started_at"] = "2026-10-21T08:00:00Z"
		newer := eligibleTask(domain.TaskPhasePlanning)
		newer.TaskIdentifier = "tid-newer"

		// Older first, exactly as the snapshot map may yield it.
		fakeView.SnapshotReturns([]lib.Task{older, newer})
		fakeSpawner.IsJobActiveReturns(false, nil)
		running := 0
		fakeSpawner.ListActiveJobsStub = func(
			context.Context, string,
		) (map[lib.TaskIdentifier]bool, error) {
			return activeJobs(running), nil
		}
		fakeSpawner.SpawnJobStub = func(
			context.Context, lib.Task, pkg.AgentConfiguration,
		) (string, error) {
			running = 1
			return "job-newer", nil
		}

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		_, spawnedTask, _ := fakeSpawner.SpawnJobArgsForCall(0)
		Expect(spawnedTask.TaskIdentifier).To(Equal(lib.TaskIdentifier("tid-newer")))
	})

	It("counts the pass as ok when it completes (SC4)", func() {
		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhasePlanning)})
		fakeSpawner.IsJobActiveReturns(false, nil)

		before := testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok"))
		Expect(h.ReconcileOnce(ctx)).To(BeNil())
		Expect(
			testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok")),
		).To(Equal(before + 1))
	})

	It("counts an empty ready view as an ok pass with evaluated=0", func() {
		fakeView.SnapshotReturns(nil)

		before := testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok"))
		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(BeNil())
		})
		Expect(
			testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok")),
		).To(Equal(before + 1))
		Expect(out).To(ContainSubstring("event=reconcile evaluated=0"))
	})

	It("runs one pass immediately at startup before any ticker fire (DB 1)", func() {
		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})

		shortCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- h.RunReconcileLoop(shortCtx) }()
		// The interval is 60s, so only the startup pass can run in this test; the
		// startup pass runs synchronously before the ticker starts.
		Eventually(fakeView.SnapshotCallCount).Should(BeNumerically(">=", 1))
		cancel()
		Expect(<-done).To(BeNil())
	})

	It(
		"aborts a pass when the parent context is cancelled (DB 7 / slow-vault failure mode)",
		func() {
			fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})

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
			fakeSpawner.SpawnJobStub = func(
				_ context.Context,
				_ lib.Task,
				_ pkg.AgentConfiguration,
			) (string, error) {
				spawned.Add(1)
				return "job-name", nil
			}
			fakeSpawner.ListActiveJobsStub = func(
				_ context.Context,
				_ string,
			) (map[lib.TaskIdentifier]bool, error) {
				time.Sleep(20 * time.Millisecond)
				if spawned.Load() > 0 {
					return map[lib.TaskIdentifier]bool{"tid-double-spawn": true}, nil
				}
				return map[lib.TaskIdentifier]bool{}, nil
			}

			task := eligibleTask(domain.TaskPhaseExecution)
			task.TaskIdentifier = "tid-double-spawn"
			fakeView.SnapshotReturns([]lib.Task{task})

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
				Vault:          testTaskVault,
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
			fakeSpawner.ListActiveJobsReturns(activeJobs(1), nil) // at cap

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

	// Spec 009: the parked-task recovery path. A task an agent parked — its own
	// assignee cleared, status still in_progress, at a trigger phase, no
	// current_job — is invisible to every spawn gate because they all read
	// assignee. Its declared task_type still identifies exactly one Config, so the
	// floor resolves that Config and writes its assignee back. It does NOT spawn:
	// the write changes the file, the controller publishes TaskUpdated for it, and
	// that event drives the normal spawn path with every existing guard in place.

	// parkedTask builds the view shape of a parked task. taskType is what still
	// routes it.
	parkedTask := func(phase domain.TaskPhase, taskType string) lib.Task {
		return lib.Task{
			TaskIdentifier: lib.TaskIdentifier("tid-parked"),
			Vault:          testTaskVault,
			Frontmatter: lib.TaskFrontmatter{
				"status":    "in_progress",
				"phase":     string(phase),
				"assignee":  "",
				"stage":     "prod",
				"task_type": taskType,
			},
		}
	}

	// restoreConfig is the Config a parked task's declared type resolves to. Its
	// assignee is deliberately distinct from the fake's Resolve default ("claude")
	// so a spec cannot pass by reading the wrong resolution.
	restoreConfig := pkg.AgentConfiguration{
		Assignee: "resolved-agent",
		Image:    "my-image:latest",
	}

	It("restores a parked task's routing value with no operator write (AC 1)", func() {
		fakeResolver.ResolveByTaskTypeReturns(restoreConfig, nil)
		fakeView.SnapshotReturns([]lib.Task{
			parkedTask(domain.TaskPhasePlanning, "github-update-go"),
		})

		before := testutil.ToFloat64(metrics.ReconcileRedrivenTotal)
		Expect(h.ReconcileOnce(ctx)).To(Succeed())

		// Exactly one frontmatter update, naming that task.
		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(1))
		_, publishedTask, assignee := fakeResultPublisher.PublishRestoreAssigneeArgsForCall(0)
		Expect(publishedTask.TaskIdentifier).To(Equal(lib.TaskIdentifier("tid-parked")))
		Expect(assignee).To(Equal("resolved-agent"))

		// The declared task_type is what resolved it — not the assignee, which is
		// empty, and not previous_assignee, which this fixture does not carry.
		Expect(fakeResolver.ResolveByTaskTypeCallCount()).To(Equal(1))
		_, resolvedType, preferAssignee := fakeResolver.ResolveByTaskTypeArgsForCall(0)
		Expect(resolvedType).To(Equal("github-update-go"))
		Expect(preferAssignee).To(BeEmpty())

		// The restore is the whole action: no Job, and the pass counts it redriven.
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		Expect(testutil.ToFloat64(metrics.ReconcileRedrivenTotal)).To(Equal(before + 1))
	})

	It("writes the resolved Config's assignee, never previous_assignee (AC 2)", func() {
		fakeResolver.ResolveByTaskTypeReturns(restoreConfig, nil)
		task := parkedTask(domain.TaskPhasePlanning, "github-update-go")
		task.Frontmatter["previous_assignee"] = "stale-agent"
		fakeView.SnapshotReturns([]lib.Task{task})

		Expect(h.ReconcileOnce(ctx)).To(Succeed())

		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(1))
		_, _, assignee := fakeResultPublisher.PublishRestoreAssigneeArgsForCall(0)
		Expect(assignee).To(Equal("resolved-agent"))

		// stale-agent appears in no published command at all. previous_assignee is
		// only a tie-break among Configs that already declare the task_type, never
		// the sole routing source: routing on it alone is the substitution the
		// operator runbook records a full night lost to (2026-09-13). The restore
		// always writes the assignee of the Config the resolver returned.
		for i := 0; i < fakeResultPublisher.PublishRestoreAssigneeCallCount(); i++ {
			_, _, published := fakeResultPublisher.PublishRestoreAssigneeArgsForCall(i)
			Expect(published).NotTo(Equal("stale-agent"))
		}
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("publishes nothing when the declared task_type matches no Config (AC 3)", func() {
		fakeResolver.ResolveByTaskTypeReturns(pkg.AgentConfiguration{}, pkg.ErrConfigNotFound)
		fakeView.SnapshotReturns([]lib.Task{
			parkedTask(domain.TaskPhasePlanning, "no-such-type"),
		})

		before := testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("skipped_unknown_assignee"),
		)
		Expect(h.ReconcileOnce(ctx)).To(Succeed())

		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		// The skip reuses the existing unknown-assignee label rather than adding a
		// metric: the task carries no routing information this executor can use.
		Expect(
			testutil.ToFloat64(
				metrics.TaskEventsTotal.WithLabelValues("skipped_unknown_assignee"),
			),
		).To(Equal(before + 1))
	})

	It("does not rewrite a task that already carries an assignee (AC 4)", func() {
		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})
		fakeSpawner.IsJobActiveReturns(false, nil)

		Expect(h.ReconcileOnce(ctx)).To(Succeed())

		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(0))
		// The by-type fallback is not consulted at all for a routable task.
		Expect(fakeResolver.ResolveByTaskTypeCallCount()).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	DescribeTable("does not re-drive a terminal task (AC 5)",
		func(status string) {
			task := parkedTask(domain.TaskPhaseExecution, "github-update-go")
			task.Frontmatter["status"] = status
			fakeResolver.ResolveByTaskTypeReturns(restoreConfig, nil)
			fakeView.SnapshotReturns([]lib.Task{task})

			Expect(h.ReconcileOnce(ctx)).To(Succeed())
			Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(0))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		},
		Entry("status completed", "completed"),
		Entry("status aborted", "aborted"),
	)

	It("does not re-drive a parked task outside its Config's trigger set (AC 6)", func() {
		fakeResolver.ResolveByTaskTypeReturns(
			pkg.AgentConfiguration{
				Assignee: "resolved-agent",
				Image:    "my-image:latest",
				Trigger: &agentv1.Trigger{
					Phases:   domain.TaskPhases{domain.TaskPhaseExecution},
					Statuses: domain.TaskStatuses{domain.TaskStatusInProgress},
				},
			},
			nil,
		)
		fakeView.SnapshotReturns([]lib.Task{
			parkedTask(domain.TaskPhaseHumanReview, "github-update-go"),
		})

		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("writes no counter key on the restore path (AC 7)", func() {
		fakeResolver.ResolveByTaskTypeReturns(restoreConfig, nil)
		fakeView.SnapshotReturns([]lib.Task{
			parkedTask(domain.TaskPhaseExecution, "github-update-go"),
		})

		Expect(h.ReconcileOnce(ctx)).To(Succeed())

		// The restore is the only command emitted. Neither counter-writing command
		// is sent, and nothing else touches the task. The Updates map itself — a
		// single assignee key — is pinned in result_publisher_test.go, which can
		// decode the emitted command; the fake here records only call arguments.
		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(1))
		Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(0))
		Expect(fakeResultPublisher.PublishSetTriggerScopeCallCount()).To(Equal(0))
		Expect(fakeResultPublisher.PublishSpawnNotificationCallCount()).To(Equal(0))
		Expect(fakeResultPublisher.PublishClearCurrentJobCallCount()).To(Equal(0))
	})

	It(
		"leaves a parked task untouched when its declared type is ambiguous (failure mode 2)",
		func() {
			// Two Configs declaring the same task_type: the executor does not guess.
			// resolveConfigForReconcile returns the error unchanged and gatherCandidate's
			// resolve_error branch — which names the task and its task_type — turns it
			// into a skip, so the ambiguity is diagnosable and the pass still completes.
			fakeResolver.ResolveByTaskTypeReturns(pkg.AgentConfiguration{}, pkg.ErrConfigAmbiguous)
			fakeView.SnapshotReturns([]lib.Task{
				parkedTask(domain.TaskPhaseExecution, "shared-type"),
			})

			Expect(h.ReconcileOnce(ctx)).To(Succeed())
			Expect(fakeResolver.ResolveByTaskTypeCallCount()).To(Equal(1))
			Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(0))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		},
	)

	It("passes previous_assignee to the task_type lookup as the tie-break", func() {
		// On sun two Configs declared github-update-go and every parked task of
		// that type was skipped as ambiguous. previous_assignee, written at
		// escalation, is handed to the resolver only as a tie-break among the
		// Configs that already declare the task_type, never as the sole routing
		// source; the resolver decides, and the restore writes its Config's assignee.
		fakeResolver.ResolveByTaskTypeReturns(
			pkg.AgentConfiguration{Assignee: "tie-break-agent", Image: "my-image:latest"},
			nil,
		)
		task := parkedTask(domain.TaskPhaseExecution, "github-update-go")
		task.Frontmatter["previous_assignee"] = "tie-break-agent"
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-parked.md"}, nil)
		fakeGitRestClient.GetReturns([]byte(renderTaskFile(task)), nil)

		Expect(h.ReconcileOnce(ctx)).To(Succeed())

		Expect(fakeResolver.ResolveByTaskTypeCallCount()).To(Equal(1))
		_, resolvedType, preferAssignee := fakeResolver.ResolveByTaskTypeArgsForCall(0)
		Expect(resolvedType).To(Equal("github-update-go"))
		Expect(preferAssignee).To(Equal("tie-break-agent"))
		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(1))
		_, _, assignee := fakeResultPublisher.PublishRestoreAssigneeArgsForCall(0)
		Expect(assignee).To(Equal("tie-break-agent"))
	})

	It(
		"still skips a parked task whose task_type matches no Config despite previous_assignee",
		func() {
			fakeResolver.ResolveByTaskTypeReturns(pkg.AgentConfiguration{}, pkg.ErrConfigNotFound)
			task := parkedTask(domain.TaskPhaseExecution, "no-such-type")
			task.Frontmatter["previous_assignee"] = "tie-break-agent"
			fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-parked.md"}, nil)
			fakeGitRestClient.GetReturns([]byte(renderTaskFile(task)), nil)

			Expect(h.ReconcileOnce(ctx)).To(Succeed())

			// previous_assignee never routes on its own: no type match, no restore.
			Expect(fakeResolver.ResolveByTaskTypeCallCount()).To(Equal(1))
			_, _, preferAssignee := fakeResolver.ResolveByTaskTypeArgsForCall(0)
			Expect(preferAssignee).To(Equal("tie-break-agent"))
			Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(0))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		},
	)

	It("still skips a parked task that stays ambiguous despite previous_assignee", func() {
		fakeResolver.ResolveByTaskTypeReturns(pkg.AgentConfiguration{}, pkg.ErrConfigAmbiguous)
		task := parkedTask(domain.TaskPhaseExecution, "shared-type")
		task.Frontmatter["previous_assignee"] = "tie-break-agent"
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-parked.md"}, nil)
		fakeGitRestClient.GetReturns([]byte(renderTaskFile(task)), nil)

		// The ambiguity propagates to reconcileOne's resolve_error skip as before:
		// the pass still completes and nothing touches the task.
		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeResolver.ResolveByTaskTypeCallCount()).To(Equal(1))
		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("leaves a parked task with no declared type untouched (no routing information)", func() {
		task := parkedTask(domain.TaskPhaseExecution, "")
		delete(task.Frontmatter, "task_type")
		fakeView.SnapshotReturns([]lib.Task{task})

		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		// No lookup is attempted without a type to look up.
		Expect(fakeResolver.ResolveByTaskTypeCallCount()).To(Equal(0))
	})

	It("skips the task when the restore publish fails and does not fail the pass", func() {
		fakeResolver.ResolveByTaskTypeReturns(restoreConfig, nil)
		fakeResultPublisher.PublishRestoreAssigneeReturns(errors.Errorf(ctx, "kafka: boom"))
		fakeView.SnapshotReturns([]lib.Task{
			parkedTask(domain.TaskPhaseExecution, "github-update-go"),
		})

		// A publish failure is a skip, not a pass failure: the next tick retries.
		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	Describe("configurable reconcile timing", func() {
		newTimedHandler := func(passTimeout, interval time.Duration) handler.TaskEventHandler {
			return handler.NewTaskEventHandler(
				fakeSpawner,
				base.Branch("prod"),
				"",
				true,
				true,
				fakeResolver,
				fakeResultPublisher,
				taskStore,
				currentDateTime,
				fakeView,
				testTaskVault,
				passTimeout,
				interval,
			)
		}

		It("bounds the pass by the configured timeout, not the default", func() {
			fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})
			var remaining time.Duration
			fakeSpawner.ListActiveJobsStub = func(
				listCtx context.Context,
				_ string,
			) (map[lib.TaskIdentifier]bool, error) {
				deadline, ok := listCtx.Deadline()
				Expect(ok).To(BeTrue())
				remaining = time.Until(deadline)
				return nil, nil
			}
			Expect(newTimedHandler(150*time.Second, time.Minute).ReconcileOnce(ctx)).To(Succeed())
			Expect(remaining).To(BeNumerically(">", handler.DefaultReconcilePassTimeout))
			Expect(remaining).To(BeNumerically("~", 150*time.Second, time.Second))
		})

		It("ticks at the configured interval", func() {
			var passes atomic.Int32
			fakeView.SnapshotStub = func() []lib.Task {
				passes.Add(1)
				return nil
			}
			loopCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- newTimedHandler(time.Second, 10*time.Millisecond).RunReconcileLoop(loopCtx)
			}()
			Eventually(passes.Load, time.Second, 5*time.Millisecond).Should(BeNumerically(">=", 3))
			cancel()
			Eventually(done).Should(Receive(BeNil()))
		})
	})

	It("does not serve the event path the active-Job snapshot a running pass froze", func() {
		// Regression for sun 2026-10-21: a reconcile pass ran for minutes, and
		// while it ran the Kafka consumer and the deferred cap retries were
		// answered from the pass's per-assignee cache. A snapshot taken at 6
		// stayed at 6 while Jobs completed, so the executor logged
		// `concurrency_cap running=6 cap=6` with only 4 Jobs active.
		var liveClaude atomic.Int64
		liveClaude.Store(6)
		codexListed := make(chan struct{})
		releaseCodex := make(chan struct{})
		var holdCodex sync.Once
		fakeResolver.ResolveStub = func(_ context.Context, assignee string) (pkg.AgentConfiguration, error) {
			return pkg.AgentConfiguration{
				Assignee:          assignee,
				Image:             "my-image:latest",
				MaxConcurrentJobs: 6,
			}, nil
		}
		fakeSpawner.ListActiveJobsStub = func(
			_ context.Context,
			assignee string,
		) (map[lib.TaskIdentifier]bool, error) {
			if assignee == "codex" {
				// Hold the pass open after it has cached claude's count. Only the
				// first codex lookup blocks; a refetch after a spawn returns at once.
				holdCodex.Do(func() {
					close(codexListed)
					<-releaseCodex
				})
				return activeJobs(0), nil
			}
			return activeJobs(int(liveClaude.Load())), nil
		}
		fakeSpawner.SpawnJobReturns("job-name", nil)

		claudeTask := eligibleTask(domain.TaskPhaseExecution)
		codexTask := eligibleTask(domain.TaskPhaseExecution)
		codexTask.TaskIdentifier = "tid-codex"
		codexTask.Frontmatter["assignee"] = "codex"
		fakeView.SnapshotReturns([]lib.Task{claudeTask, codexTask})

		passDone := make(chan error, 1)
		go func() { passDone <- h.ReconcileOnce(ctx) }()
		Eventually(codexListed).Should(BeClosed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0)) // claude deferred at 6/6

		// Two of claude's Jobs complete while the pass is still running.
		liveClaude.Store(4)
		eventTask := eligibleTask(domain.TaskPhaseExecution)
		eventTask.TaskIdentifier = "tid-event"
		Expect(h.ConsumeMessage(ctx, buildMsg(eventTask))).To(Succeed())

		close(releaseCodex)
		Eventually(passDone).Should(Receive(BeNil()))
		spawned := make([]lib.TaskIdentifier, 0, fakeSpawner.SpawnJobCallCount())
		for i := range fakeSpawner.SpawnJobCallCount() {
			_, spawnedTask, _ := fakeSpawner.SpawnJobArgsForCall(i)
			spawned = append(spawned, spawnedTask.TaskIdentifier)
		}
		// The event path read the live 4/6 and admitted; the frozen 6/6 would defer it.
		Expect(spawned).To(ContainElement(lib.TaskIdentifier("tid-event")))
		Expect(spawned).NotTo(ContainElement(lib.TaskIdentifier("tid-a")))
	})

	It("lists active Jobs once per assignee for a whole reconcile pass", func() {
		// The pass cache exists so a pass costs one `list jobs` per assignee, not
		// two per task (2026-10-19, client-go's 5 QPS limiter). Scoping it to the
		// pass context must not disable it.
		fakeResolver.ResolveReturns(
			pkg.AgentConfiguration{
				Assignee:          "claude",
				Image:             "my-image:latest",
				MaxConcurrentJobs: 1,
			},
			nil,
		)
		fakeSpawner.ListActiveJobsReturns(activeJobs(1), nil) // at cap: every candidate defers
		first := eligibleTask(domain.TaskPhaseExecution)
		second := eligibleTask(domain.TaskPhaseExecution)
		second.TaskIdentifier = "tid-b"
		fakeView.SnapshotReturns([]lib.Task{first, second})

		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		Expect(fakeSpawner.ListActiveJobsCallCount()).To(Equal(1))
	})

	It("counts a Job the event path spawned mid-pass against the pass's cap check", func() {
		// The pass cache must never read too low: a Job created on another path
		// while the pass runs has to count, or the pass admits past the cap.
		var mu sync.Mutex
		liveClaude := map[lib.TaskIdentifier]bool{"tid-x": true}
		codexListed := make(chan struct{})
		releaseCodex := make(chan struct{})
		var holdCodex sync.Once
		fakeResolver.ResolveStub = func(_ context.Context, assignee string) (pkg.AgentConfiguration, error) {
			return pkg.AgentConfiguration{
				Assignee:          assignee,
				Image:             "my-image:latest",
				MaxConcurrentJobs: 2,
			}, nil
		}
		fakeSpawner.ListActiveJobsStub = func(
			_ context.Context,
			assignee string,
		) (map[lib.TaskIdentifier]bool, error) {
			if assignee == "codex" {
				holdCodex.Do(func() {
					close(codexListed)
					<-releaseCodex
				})
				return activeJobs(0), nil
			}
			mu.Lock()
			defer mu.Unlock()
			snapshot := make(map[lib.TaskIdentifier]bool, len(liveClaude))
			for id := range liveClaude {
				snapshot[id] = true
			}
			return snapshot, nil
		}
		fakeSpawner.SpawnJobStub = func(
			_ context.Context,
			task lib.Task,
			config pkg.AgentConfiguration,
		) (string, error) {
			if config.Assignee == "claude" {
				mu.Lock()
				liveClaude[task.TaskIdentifier] = true
				mu.Unlock()
			}
			return "job-name", nil
		}

		running := eligibleTask(
			domain.TaskPhaseExecution,
		) // its Job is active: caches claude at 1/2
		running.TaskIdentifier = "tid-x"
		codexTask := eligibleTask(domain.TaskPhaseExecution)
		codexTask.TaskIdentifier = "tid-codex"
		codexTask.Frontmatter["assignee"] = "codex"
		late := eligibleTask(domain.TaskPhaseExecution)
		late.TaskIdentifier = "tid-c"
		fakeView.SnapshotReturns([]lib.Task{running, codexTask, late})

		passDone := make(chan error, 1)
		go func() { passDone <- h.ReconcileOnce(ctx) }()
		Eventually(codexListed).Should(BeClosed())

		// The event path fills the second slot while the pass is held open.
		eventTask := eligibleTask(domain.TaskPhaseExecution)
		eventTask.TaskIdentifier = "tid-event"
		Expect(h.ConsumeMessage(ctx, buildMsg(eventTask))).To(Succeed())

		close(releaseCodex)
		Eventually(passDone).Should(Receive(BeNil()))
		mu.Lock()
		defer mu.Unlock()
		Expect(liveClaude).To(HaveKey(lib.TaskIdentifier("tid-event")))
		Expect(
			liveClaude,
		).NotTo(HaveKey(lib.TaskIdentifier("tid-c")))
		// 2/2: the pass must defer it
	})

	It(
		"re-reads an assignee's active Jobs once the pass cache entry is older than its TTL",
		func() {
			// A pass on sun runs for minutes and, at the cap, never spawns to refresh
			// itself. Without a TTL it refuses with free slots for its whole lifetime.
			start := time.Date(2026, 10, 21, 11, 0, 0, 0, time.UTC)
			currentDateTime.SetNow(libtime.DateTime(start))
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{
					Assignee:          "claude",
					Image:             "my-image:latest",
					MaxConcurrentJobs: 1,
				},
				nil,
			)
			fakeSpawner.ListActiveJobsStub = func(_ context.Context, _ string) (map[lib.TaskIdentifier]bool, error) {
				if fakeSpawner.ListActiveJobsCallCount() == 1 {
					// The other Job finishes while the pass is still running.
					currentDateTime.SetNow(libtime.DateTime(start.Add(11 * time.Second)))
					return map[lib.TaskIdentifier]bool{"tid-other": true}, nil
				}
				return activeJobs(0), nil
			}
			fakeSpawner.SpawnJobReturns("job-name", nil)
			fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})

			Expect(h.ReconcileOnce(ctx)).To(Succeed())
			Expect(fakeSpawner.ListActiveJobsCallCount()).To(Equal(2))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		},
	)

	It("keeps other assignees' pass cache entries when one assignee spawns", func() {
		fakeResolver.ResolveStub = func(_ context.Context, assignee string) (pkg.AgentConfiguration, error) {
			return pkg.AgentConfiguration{
				Assignee:          assignee,
				Image:             "my-image:latest",
				MaxConcurrentJobs: 2,
			}, nil
		}
		var claudeLists atomic.Int64
		fakeSpawner.ListActiveJobsStub = func(_ context.Context, assignee string) (map[lib.TaskIdentifier]bool, error) {
			if assignee == "claude" {
				claudeLists.Add(1)
				return map[lib.TaskIdentifier]bool{"tid-x": true}, nil
			}
			return activeJobs(0), nil
		}
		fakeSpawner.SpawnJobReturns("job-name", nil)
		running := eligibleTask(domain.TaskPhaseExecution)
		running.TaskIdentifier = "tid-x"
		codexTask := eligibleTask(domain.TaskPhaseExecution)
		codexTask.TaskIdentifier = "tid-codex"
		codexTask.Frontmatter["assignee"] = "codex"
		late := eligibleTask(domain.TaskPhaseExecution)
		late.TaskIdentifier = "tid-c"
		fakeView.SnapshotReturns([]lib.Task{running, codexTask, late})

		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(2)) // codex, then tid-c at 1/2
		Expect(claudeLists.Load()).To(Equal(int64(1)))       // the codex spawn left claude cached
	})

	Describe("vault ownership from the view", func() {
		It("AC4: a not-ready view drives nothing and counts view_not_ready", func() {
			fakeView.ReadyReturns(false)
			fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})

			beforeNotReady := testutil.ToFloat64(
				metrics.ReconcilePassesTotal.WithLabelValues("view_not_ready"),
			)
			beforeOK := testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok"))
			out := captureStderr(func() {
				Expect(h.ReconcileOnce(ctx)).To(Succeed())
			})
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			Expect(
				testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("view_not_ready")),
			).To(Equal(beforeNotReady + 1))
			Expect(
				testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok")),
			).To(Equal(beforeOK))
			Expect(out).To(ContainSubstring("event=reconcile_view_not_ready task_vault=openclaw"))
		})

		It("AC5: re-drives an eligible owned task without a Kafka event", func() {
			task := eligibleTask(domain.TaskPhaseExecution)
			task.TaskIdentifier = "tid-a"
			fakeView.SnapshotReturns([]lib.Task{task})
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.SpawnJobReturns("job-1", nil)

			before := testutil.ToFloat64(metrics.ReconcileRedrivenTotal)
			out := captureStderr(func() {
				Expect(h.ReconcileOnce(ctx)).To(Succeed())
			})
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			Expect(testutil.ToFloat64(metrics.ReconcileRedrivenTotal)).To(Equal(before + 1))
			Expect(out).To(ContainSubstring("event=reconcile_redrive task=tid-a"))
		})

		It("AC6: reconcile drives only the task stamped with this executor's vault", func() {
			owned := eligibleTask(domain.TaskPhaseExecution)
			owned.TaskIdentifier = "tid-owned"
			foreign := eligibleTask(domain.TaskPhaseExecution)
			foreign.TaskIdentifier = "tid-foreign"
			foreign.Vault = "personal"
			unstamped := eligibleTask(domain.TaskPhaseExecution)
			unstamped.TaskIdentifier = "tid-unstamped"
			unstamped.Vault = ""
			fakeView.SnapshotReturns([]lib.Task{owned, foreign, unstamped})
			fakeSpawner.IsJobActiveReturns(false, nil)

			Expect(h.ReconcileOnce(ctx)).To(Succeed())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			_, spawnedTask, _ := fakeSpawner.SpawnJobArgsForCall(0)
			Expect(spawnedTask.TaskIdentifier).To(Equal(lib.TaskIdentifier("tid-owned")))
		})

		It("AC6: the live path skips tasks from other vaults", func() {
			liveSpawner := new(mocks.FakeJobSpawner)
			liveSpawner.IsJobActiveReturns(false, nil)
			liveHandler := handler.NewTaskEventHandler(
				liveSpawner,
				base.Branch("prod"),
				"",
				true,
				true,
				fakeResolver,
				fakeResultPublisher,
				pkg.NewTaskStore(),
				currentDateTime,
				fakeView,
				testTaskVault,
				handler.DefaultReconcilePassTimeout,
				handler.DefaultReconcileInterval,
			)
			owned := eligibleTask(domain.TaskPhaseExecution)
			owned.TaskIdentifier = "tid-owned"
			foreign := eligibleTask(domain.TaskPhaseExecution)
			foreign.TaskIdentifier = "tid-foreign"
			foreign.Vault = "personal"
			unstamped := eligibleTask(domain.TaskPhaseExecution)
			unstamped.TaskIdentifier = "tid-unstamped"
			unstamped.Vault = ""

			// Marshal directly rather than through buildMsg, which would stamp the
			// empty vault on the third task.
			before := testutil.ToFloat64(
				metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_vault"),
			)
			for _, task := range []lib.Task{owned, foreign, unstamped} {
				value, err := json.Marshal(task)
				Expect(err).To(BeNil())
				Expect(
					liveHandler.ConsumeMessage(ctx, &sarama.ConsumerMessage{Value: value}),
				).To(Succeed())
			}
			Expect(liveSpawner.SpawnJobCallCount()).To(Equal(1))
			Expect(
				testutil.ToFloat64(
					metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_vault"),
				),
			).To(Equal(before + 2))
		})

		It("AC7: fails loudly when no task belongs to this executor's vault", func() {
			foreignA := eligibleTask(domain.TaskPhaseExecution)
			foreignA.Vault = "trading"
			foreignB := eligibleTask(domain.TaskPhaseExecution)
			foreignB.Vault = "personal"
			foreignC := eligibleTask(domain.TaskPhaseExecution)
			foreignC.Vault = "trading"
			unstamped := eligibleTask(domain.TaskPhaseExecution)
			unstamped.Vault = ""
			fakeView.SnapshotReturns([]lib.Task{foreignA, foreignB, foreignC, unstamped})

			beforeMismatch := testutil.ToFloat64(
				metrics.ReconcilePassesTotal.WithLabelValues("task_vault_mismatch"),
			)
			beforeOK := testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok"))
			out := captureStderr(func() {
				Expect(h.ReconcileOnce(ctx)).To(Succeed())
			})
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			Expect(
				testutil.ToFloat64(
					metrics.ReconcilePassesTotal.WithLabelValues("task_vault_mismatch"),
				),
			).To(Equal(beforeMismatch + 1))
			Expect(
				testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok")),
			).To(Equal(beforeOK))
			Expect(out).To(ContainSubstring(
				"event=task_vault_mismatch task_vault=openclaw seen=personal,trading",
			))
		})

		It("AC7: a snapshot containing one owned task passes the vault gate", func() {
			foreign := eligibleTask(domain.TaskPhaseExecution)
			foreign.TaskIdentifier = "tid-foreign"
			foreign.Vault = "trading"
			owned := eligibleTask(domain.TaskPhaseExecution)
			owned.TaskIdentifier = "tid-owned"
			fakeView.SnapshotReturns([]lib.Task{foreign, owned})
			fakeSpawner.IsJobActiveReturns(false, nil)

			beforeOK := testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok"))
			out := captureStderr(func() {
				Expect(h.ReconcileOnce(ctx)).To(Succeed())
			})
			Expect(out).NotTo(ContainSubstring("task_vault_mismatch"))
			Expect(
				testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok")),
			).To(Equal(beforeOK + 1))
		})

		It("AC7a: fails loudly when every task in the view is unstamped", func() {
			tasks := make([]lib.Task, 0, 3)
			for i := 0; i < 3; i++ {
				task := eligibleTask(domain.TaskPhaseExecution)
				task.TaskIdentifier = lib.TaskIdentifier(fmt.Sprintf("tid-%d", i))
				task.Vault = ""
				tasks = append(tasks, task)
			}
			fakeView.SnapshotReturns(tasks)

			before := testutil.ToFloat64(
				metrics.ReconcilePassesTotal.WithLabelValues("task_vault_unstamped"),
			)
			beforeOK := testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok"))
			out := captureStderr(func() {
				Expect(h.ReconcileOnce(ctx)).To(Succeed())
			})
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			Expect(
				testutil.ToFloat64(
					metrics.ReconcilePassesTotal.WithLabelValues("task_vault_unstamped"),
				),
			).To(Equal(before + 1))
			Expect(
				testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok")),
			).To(Equal(beforeOK))
			Expect(out).To(ContainSubstring("event=task_vault_unstamped tasks=3"))
		})

		It("AC11: scans a 15,000-task snapshot in under a second", func() {
			tasks := make([]lib.Task, 0, 15000)
			for i := 0; i < 15000; i++ {
				task := eligibleTask(domain.TaskPhaseExecution)
				task.TaskIdentifier = lib.TaskIdentifier(fmt.Sprintf("tid-%d", i))
				if i < 100 {
					tasks = append(tasks, task)
					continue
				}
				if i%2 == 0 {
					task.Vault = "other"
				} else {
					task.Frontmatter["status"] = "done"
				}
				tasks = append(tasks, task)
			}
			fakeView.SnapshotReturns(tasks)
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"},
				nil,
			)
			fakeSpawner.ListActiveJobsReturns(map[lib.TaskIdentifier]bool{}, nil)
			fakeSpawner.SpawnJobReturns("job", nil)

			beforeOK := testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok"))
			beforeAborted := testutil.ToFloat64(
				metrics.ReconcilePassesTotal.WithLabelValues("aborted"),
			)
			start := time.Now()
			out := captureStderr(func() {
				Expect(h.ReconcileOnce(ctx)).To(Succeed())
			})
			elapsed := time.Since(start)
			GinkgoWriter.Printf("reconcile pass over 15000 tasks took %s\n", elapsed)
			Expect(elapsed).To(BeNumerically("<", time.Second))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(100))
			Expect(
				testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok")),
			).To(Equal(beforeOK + 1))
			Expect(
				testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("aborted")),
			).To(Equal(beforeAborted))
			Expect(out).To(ContainSubstring("event=reconcile evaluated=100"))
			Expect(out).To(ContainSubstring(`task_vault="openclaw"`))
			Expect(out).NotTo(ContainSubstring("event=reconcile_aborted"))
		})
	})

	Describe("queued_at from the view (AC10)", func() {
		const viewQueuedAt = "2026-10-21T10:00:00Z"

		BeforeEach(func() {
			fakeResolver.ResolveReturns(
				pkg.AgentConfiguration{
					Assignee:          "claude",
					Image:             "my-image:latest",
					MaxConcurrentJobs: 1,
				},
				nil,
			)
			currentDateTime.SetNow(libtimetestParse("2026-10-21T10:00:00Z"))
			fakeSpawner.IsJobActiveReturns(false, nil)
			fakeSpawner.ListActiveJobsReturns(
				map[lib.TaskIdentifier]bool{"other-task": true},
				nil,
			)
		})

		It("takes queued_at from the view when the event carries none", func() {
			fakeView.GetReturns(lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-queued"),
				Vault:          testTaskVault,
				Frontmatter:    lib.TaskFrontmatter{"queued_at": viewQueuedAt},
			}, true)
			task := eligibleTask(domain.TaskPhaseExecution)
			task.TaskIdentifier = "tid-queued"

			Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(Succeed())
			Expect(fakeResultPublisher.PublishQueueEntryCallCount()).To(Equal(0))

			currentDateTime.SetNow(libtimetestParse("2026-10-21T10:01:01Z"))
			fakeSpawner.ListActiveJobsReturns(map[lib.TaskIdentifier]bool{}, nil)
			Expect(h.EvalDeferredRespawns(ctx)).To(Succeed())

			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
			_, spawnedTask, _ := fakeSpawner.SpawnJobArgsForCall(0)
			queuedAt, ok := spawnedTask.Frontmatter.String("queued_at")
			Expect(ok).To(BeTrue())
			Expect(queuedAt).To(Equal(viewQueuedAt))
		})

		It("publishes a fresh queued_at when the view has no entry", func() {
			fakeView.GetReturns(lib.Task{}, false)
			task := eligibleTask(domain.TaskPhaseExecution)
			task.TaskIdentifier = "tid-queued"

			Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(Succeed())
			Expect(fakeResultPublisher.PublishQueueEntryCallCount()).To(Equal(1))
		})
	})
})

// libtimetestParse parses an RFC3339 instant for the specs above.
func libtimetestParse(value string) libtime.DateTime {
	parsed, err := time.Parse(time.RFC3339, value)
	Expect(err).To(BeNil())
	return libtime.DateTime(parsed)
}
