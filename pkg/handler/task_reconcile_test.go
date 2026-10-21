// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"
	"strings"
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
			"",
			true,
			true,
			fakeResolver,
			fakeResultPublisher,
			taskStore,
			currentDateTime,
			fakeGitRestClient,
			"24 Tasks/*.md",
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
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
			fakeGitRestClient,
			"24 Tasks/*.md",
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
		)
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(eligibleTask(domain.TaskPhasePlanning))),
			nil,
		)
		fakeSpawner.IsJobActiveReturns(false, nil)

		err := untaggedHandler.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		Expect(untaggedResolver.ResolveCallCount()).To(Equal(0))
	})

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
			fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
			fakeGitRestClient.GetReturns(
				[]byte(renderTaskFile(eligibleTask(domain.TaskPhasePlanning))),
				nil,
			)
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
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(eligibleTask(domain.TaskPhaseExecution))),
			nil,
		)
		fakeSpawner.IsJobActiveReturns(false, nil)

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
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
			fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
			fakeGitRestClient.GetReturns(
				[]byte(renderTaskFile(eligibleTask(domain.TaskPhaseExecution))),
				nil,
			)
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
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeSpawner.SpawnJobReturns("job-1", nil)

		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(eligibleTask(domain.TaskPhaseExecution))),
			nil,
		)
		for i := 0; i < 3; i++ {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		}
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(3))

		// At the cap, and still at the same phase: the next spawn is skipped.
		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(3))

		// The task advances — the phase changed, so the streak starts over.
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(eligibleTask(domain.TaskPhaseAIReview))),
			nil,
		)
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
			//
			// NOTE: buildMsg elsewhere in this package is a Describe-scoped
			// closure, so define a local equivalent rather than reaching for one.
			buildMsg := func(t lib.Task) *sarama.ConsumerMessage {
				value, err := json.Marshal(t)
				Expect(err).To(BeNil())
				return &sarama.ConsumerMessage{Value: value}
			}

			task := eligibleTask(domain.TaskPhaseExecution)
			fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
			fakeGitRestClient.GetReturns([]byte(renderTaskFile(task)), nil)
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
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(eligibleTask(domain.TaskPhaseExecution))),
			nil,
		)

		err := h.ReconcileOnce(ctx)
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("admits a never-attempted child ahead of an older task that already ran", func() {
		// Prod 2026-10-21: 67 eligible children against maxConcurrentJobs 1, and
		// both admissions in a 20-minute window were the first two files in
		// filename order — so a child delegated today (named 20261021-…) sorted
		// last and was never reached. The pass must admit the never-attempted
		// child, not whichever path git-rest happens to list first.
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

		files := map[string][]byte{
			"24 Tasks/20261011-081237-older.md": []byte(renderTaskFile(older)),
			"24 Tasks/20261021-070305-newer.md": []byte(renderTaskFile(newer)),
		}
		// Older first, exactly as git-rest returns this vault: lexicographic.
		fakeGitRestClient.ListReturns([]string{
			"24 Tasks/20261011-081237-older.md",
			"24 Tasks/20261021-070305-newer.md",
		}, nil)
		fakeGitRestClient.GetCalls(func(_ context.Context, path string) ([]byte, error) {
			return files[path], nil
		})
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

	// SC4: a sustained reconcile failure must be alertable, not a warn line
	// repeating once a minute forever. The backstop was dead for months because
	// `reconcile_list_failed` was a bare glog.Warningf with no metric behind it.
	// These three assert the pass outcome is counted, including the `ok` series —
	// an alert on rate(ok)==0 is what catches "the backstop never succeeds",
	// which a failure-only counter cannot express.
	It("counts the pass as list_failed when List errors (SC4)", func() {
		fakeGitRestClient.IsReadyReturns(true, nil)
		fakeGitRestClient.ListReturns(nil, errors.Errorf(ctx, "rate limited"))

		before := testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("list_failed"))
		Expect(h.ReconcileOnce(ctx)).To(BeNil())
		Expect(
			testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("list_failed")),
		).To(Equal(before + 1))
	})

	It("counts the pass as ok when it completes (SC4)", func() {
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(eligibleTask(domain.TaskPhasePlanning))),
			nil,
		)
		fakeSpawner.IsJobActiveReturns(false, nil)

		before := testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok"))
		Expect(h.ReconcileOnce(ctx)).To(BeNil())
		Expect(
			testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("ok")),
		).To(Equal(before + 1))
	})

	It("counts the pass as vault_unavailable when readiness fails (SC4)", func() {
		fakeGitRestClient.IsReadyReturns(false, errors.Errorf(ctx, "boom"))

		before := testutil.ToFloat64(
			metrics.ReconcilePassesTotal.WithLabelValues("vault_unavailable"),
		)
		Expect(h.ReconcileOnce(ctx)).To(BeNil())
		Expect(
			testutil.ToFloat64(metrics.ReconcilePassesTotal.WithLabelValues("vault_unavailable")),
		).To(Equal(before + 1))
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

	// ownedTask builds an eligible task with the given identifier. The reconcile
	// pass is what teaches an instance which identifiers live in its vault.
	ownedTask := func(id string) lib.Task {
		return lib.Task{
			TaskIdentifier: lib.TaskIdentifier(id),
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(domain.TaskPhaseExecution),
				"assignee": "claude",
				"stage":    "prod",
			},
		}
	}

	It(
		"does not spawn a task event for a file outside its own vault",
		func() {
			// A task event carries no vault — lib.Task is identifier + frontmatter +
			// content — and the Kafka consumer group is shared across every executor
			// deployment, so an event reaches whichever instance owns the partition
			// rather than the instance whose vault holds the file. Measured on dev
			// 2026-10-14: one OpenClaw fixture was spawned by three different pods
			// (openclaw 18:19/18:33/18:48, trading 19:03/19:49, personal 19:34), each
			// starting its own consecutive-re-drive streak at zero, so the loop
			// produced >=7 Jobs against a cap of 3. The instance that owns the file
			// drives it through its own floor, so skipping here drops a duplicate,
			// not the only driver.
			buildMsg := func(t lib.Task) *sarama.ConsumerMessage {
				value, err := json.Marshal(t)
				Expect(err).To(BeNil())
				return &sarama.ConsumerMessage{Value: value}
			}
			fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-mine.md"}, nil)
			fakeGitRestClient.GetReturns(
				[]byte(renderTaskFile(ownedTask("tid-mine"))),
				nil,
			)
			fakeSpawner.IsJobActiveReturns(false, nil)

			// The pass spawns the one task it owns and publishes that as the set.
			Expect(h.ReconcileOnce(ctx)).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))

			Expect(h.ConsumeMessage(ctx, buildMsg(ownedTask("tid-foreign")))).To(Succeed())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		},
	)

	It(
		"owns a task filed after the last completed pass even when later passes abort",
		func() {
			// Measured on prod 2026-10-20: the OpenClaw vault outgrew one pass's
			// budget (~14.9k files), every pass after 13:14Z aborted, ownership froze
			// at that pass, and each pr-review task filed afterwards was skipped as
			// foreign. New paths are visited first and owned without waiting for the
			// pass to complete.
			buildMsg := func(t lib.Task) *sarama.ConsumerMessage {
				value, err := json.Marshal(t)
				Expect(err).To(BeNil())
				return &sarama.ConsumerMessage{Value: value}
			}
			fakeGitRestClient.ListReturns([]string{"tasks/old-a.md", "tasks/old-b.md"}, nil)
			fakeGitRestClient.GetStub = func(_ context.Context, relPath string) ([]byte, error) {
				return []byte(
					renderTaskFile(
						ownedTask(
							"tid-" + strings.TrimSuffix(
								strings.TrimPrefix(relPath, "tasks/"),
								".md",
							),
						),
					),
				), nil
			}
			fakeSpawner.IsJobActiveReturns(true, nil)
			Expect(h.ReconcileOnce(ctx)).To(BeNil())

			passCtx, cancelPass := context.WithCancel(ctx)
			defer cancelPass()
			fakeGitRestClient.ListReturns(
				[]string{"tasks/old-a.md", "tasks/old-b.md", "tasks/new.md"},
				nil,
			)
			var visited []string
			fakeGitRestClient.GetStub = func(_ context.Context, relPath string) ([]byte, error) {
				visited = append(visited, relPath)
				if relPath == "tasks/old-a.md" {
					cancelPass() // budget runs out mid-pass
				}
				return []byte(
					renderTaskFile(
						ownedTask(
							"tid-" + strings.TrimSuffix(
								strings.TrimPrefix(relPath, "tasks/"),
								".md",
							),
						),
					),
				), nil
			}
			Expect(h.ReconcileOnce(passCtx)).To(BeNil())
			Expect(visited).To(HaveLen(2))
			Expect(visited[0]).To(Equal("tasks/new.md"))

			fakeSpawner.IsJobActiveReturns(false, nil)
			before := fakeSpawner.SpawnJobCallCount()
			Expect(h.ConsumeMessage(ctx, buildMsg(ownedTask("tid-new")))).To(Succeed())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(before + 1))
		},
	)

	It(
		"still spawns a task event for a file inside its own vault",
		func() {
			buildMsg := func(t lib.Task) *sarama.ConsumerMessage {
				value, err := json.Marshal(t)
				Expect(err).To(BeNil())
				return &sarama.ConsumerMessage{Value: value}
			}
			fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-mine.md"}, nil)
			fakeGitRestClient.GetReturns(
				[]byte(renderTaskFile(ownedTask("tid-mine"))),
				nil,
			)
			fakeSpawner.IsJobActiveReturns(false, nil)

			Expect(h.ReconcileOnce(ctx)).To(BeNil())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))

			// Same task, second driver: the floor's spawn must not have made the
			// instance deaf to the event path for its own vault.
			Expect(h.ConsumeMessage(ctx, buildMsg(ownedTask("tid-mine")))).To(Succeed())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(2))
		},
	)

	It(
		"spawns an event before the first reconcile pass completes (fail-open window)",
		func() {
			// A freshly started instance has not scanned yet. Dropping every event in
			// that window would leave each task to be picked up only by the next tick;
			// the window is one pass (<=60s) and the floor covers whatever it misses.
			buildMsg := func(t lib.Task) *sarama.ConsumerMessage {
				value, err := json.Marshal(t)
				Expect(err).To(BeNil())
				return &sarama.ConsumerMessage{Value: value}
			}
			fakeSpawner.IsJobActiveReturns(false, nil)

			Expect(h.ConsumeMessage(ctx, buildMsg(ownedTask("tid-any")))).To(Succeed())
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		},
	)

	// Spec 009: the parked-task recovery path. A task an agent parked — its own
	// assignee cleared, status still in_progress, at a trigger phase, no
	// current_job — is invisible to every spawn gate because they all read
	// assignee. Its declared task_type still identifies exactly one Config, so the
	// floor resolves that Config and writes its assignee back. It does NOT spawn:
	// the write changes the file, the controller publishes TaskUpdated for it, and
	// that event drives the normal spawn path with every existing guard in place.

	// parkedTask builds the vault shape of a parked task. taskType is what still
	// routes it.
	parkedTask := func(phase domain.TaskPhase, taskType string) lib.Task {
		return lib.Task{
			TaskIdentifier: lib.TaskIdentifier("tid-parked"),
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
		Assignee: "correct-agent",
		Image:    "my-image:latest",
	}

	It("restores a parked task's routing value with no operator write (AC 1)", func() {
		fakeResolver.ResolveByTaskTypeReturns(restoreConfig, nil)
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-parked.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(parkedTask(domain.TaskPhasePlanning, "github-update-go"))),
			nil,
		)

		before := testutil.ToFloat64(metrics.ReconcileRedrivenTotal)
		Expect(h.ReconcileOnce(ctx)).To(Succeed())

		// Exactly one frontmatter update, naming that task.
		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(1))
		_, publishedTask, assignee := fakeResultPublisher.PublishRestoreAssigneeArgsForCall(0)
		Expect(publishedTask.TaskIdentifier).To(Equal(lib.TaskIdentifier("tid-parked")))
		Expect(assignee).To(Equal("correct-agent"))

		// The declared task_type is what resolved it — not the assignee, which is
		// empty, and not previous_assignee, which this fixture does not carry.
		Expect(fakeResolver.ResolveByTaskTypeCallCount()).To(Equal(1))
		_, resolvedType := fakeResolver.ResolveByTaskTypeArgsForCall(0)
		Expect(resolvedType).To(Equal("github-update-go"))

		// The restore is the whole action: no Job, and the pass counts it redriven.
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		Expect(testutil.ToFloat64(metrics.ReconcileRedrivenTotal)).To(Equal(before + 1))
	})

	It("writes the resolved Config's assignee, never previous_assignee (AC 2)", func() {
		fakeResolver.ResolveByTaskTypeReturns(restoreConfig, nil)
		task := parkedTask(domain.TaskPhasePlanning, "github-update-go")
		task.Frontmatter["previous_assignee"] = "stale-agent"
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-parked.md"}, nil)
		fakeGitRestClient.GetReturns([]byte(renderTaskFile(task)), nil)

		Expect(h.ReconcileOnce(ctx)).To(Succeed())

		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(1))
		_, _, assignee := fakeResultPublisher.PublishRestoreAssigneeArgsForCall(0)
		Expect(assignee).To(Equal("correct-agent"))

		// stale-agent appears in no published command at all. Reading
		// previous_assignee as a routing source is the substitution the operator
		// runbook records a full night lost to (2026-09-13).
		for i := 0; i < fakeResultPublisher.PublishRestoreAssigneeCallCount(); i++ {
			_, _, published := fakeResultPublisher.PublishRestoreAssigneeArgsForCall(i)
			Expect(published).NotTo(Equal("stale-agent"))
		}
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("publishes nothing when the declared task_type matches no Config (AC 3)", func() {
		fakeResolver.ResolveByTaskTypeReturns(pkg.AgentConfiguration{}, pkg.ErrConfigNotFound)
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-parked.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(parkedTask(domain.TaskPhasePlanning, "no-such-type"))),
			nil,
		)

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
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(eligibleTask(domain.TaskPhaseExecution))),
			nil,
		)
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
			fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-parked.md"}, nil)
			fakeGitRestClient.GetReturns([]byte(renderTaskFile(task)), nil)

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
				Assignee: "correct-agent",
				Image:    "my-image:latest",
				Trigger: &agentv1.Trigger{
					Phases:   domain.TaskPhases{domain.TaskPhaseExecution},
					Statuses: domain.TaskStatuses{domain.TaskStatusInProgress},
				},
			},
			nil,
		)
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-parked.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(parkedTask(domain.TaskPhaseHumanReview, "github-update-go"))),
			nil,
		)

		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("writes no counter key on the restore path (AC 7)", func() {
		fakeResolver.ResolveByTaskTypeReturns(restoreConfig, nil)
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-parked.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(parkedTask(domain.TaskPhaseExecution, "github-update-go"))),
			nil,
		)

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
			// resolveConfigForReconcile returns the error unchanged and reconcileOne's
			// resolve_error branch — which names the task and its task_type — turns it
			// into a skip, so the ambiguity is diagnosable and the pass still completes.
			fakeResolver.ResolveByTaskTypeReturns(pkg.AgentConfiguration{}, pkg.ErrConfigAmbiguous)
			fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-parked.md"}, nil)
			fakeGitRestClient.GetReturns(
				[]byte(renderTaskFile(parkedTask(domain.TaskPhaseExecution, "shared-type"))),
				nil,
			)

			Expect(h.ReconcileOnce(ctx)).To(Succeed())
			Expect(fakeResolver.ResolveByTaskTypeCallCount()).To(Equal(1))
			Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(0))
			Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		},
	)

	It("leaves a parked task with no declared type untouched (no routing information)", func() {
		task := parkedTask(domain.TaskPhaseExecution, "")
		delete(task.Frontmatter, "task_type")
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-parked.md"}, nil)
		fakeGitRestClient.GetReturns([]byte(renderTaskFile(task)), nil)

		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeResultPublisher.PublishRestoreAssigneeCallCount()).To(Equal(0))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		// No lookup is attempted without a type to look up.
		Expect(fakeResolver.ResolveByTaskTypeCallCount()).To(Equal(0))
	})

	It("skips the file when the restore publish fails and does not fail the pass", func() {
		fakeResolver.ResolveByTaskTypeReturns(restoreConfig, nil)
		fakeResultPublisher.PublishRestoreAssigneeReturns(errors.Errorf(ctx, "kafka: boom"))
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-parked.md"}, nil)
		fakeGitRestClient.GetReturns(
			[]byte(renderTaskFile(parkedTask(domain.TaskPhaseExecution, "github-update-go"))),
			nil,
		)

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
				fakeGitRestClient,
				"24 Tasks/*.md",
				passTimeout,
				interval,
			)
		}

		It("bounds the pass by the configured timeout, not the default", func() {
			var remaining time.Duration
			fakeGitRestClient.ListStub = func(listCtx context.Context, _ string) ([]string, error) {
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
			fakeGitRestClient.ListStub = func(context.Context, string) ([]string, error) {
				passes.Add(1)
				return nil, nil
			}
			loopCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- newTimedHandler(time.Second, 10*time.Millisecond).RunReconcileLoop(loopCtx) }()
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
				// Hold the pass open after it has cached claude's count.
				close(codexListed)
				<-releaseCodex
				return activeJobs(0), nil
			}
			return activeJobs(int(liveClaude.Load())), nil
		}
		fakeSpawner.SpawnJobReturns("job-name", nil)

		claudeTask := eligibleTask(domain.TaskPhaseExecution)
		codexTask := eligibleTask(domain.TaskPhaseExecution)
		codexTask.TaskIdentifier = "tid-codex"
		codexTask.Frontmatter["assignee"] = "codex"
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md", "24 Tasks/tid-codex.md"}, nil)
		fakeGitRestClient.GetStub = func(_ context.Context, relPath string) ([]byte, error) {
			if strings.HasSuffix(relPath, "tid-codex.md") {
				return []byte(renderTaskFile(codexTask)), nil
			}
			return []byte(renderTaskFile(claudeTask)), nil
		}

		passDone := make(chan error, 1)
		go func() { passDone <- h.ReconcileOnce(ctx) }()
		Eventually(codexListed).Should(BeClosed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0)) // claude deferred at 6/6

		// Two of claude's Jobs complete while the pass is still running.
		liveClaude.Store(4)
		eventTask := lib.Task{
			TaskIdentifier: "tid-event",
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(domain.TaskPhaseExecution),
				"assignee": "claude",
				"stage":    "prod",
			},
		}
		value, err := json.Marshal(eventTask)
		Expect(err).To(BeNil())
		Expect(h.ConsumeMessage(ctx, &sarama.ConsumerMessage{Value: value})).To(Succeed())

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
			pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest", MaxConcurrentJobs: 1},
			nil,
		)
		fakeSpawner.ListActiveJobsReturns(activeJobs(1), nil) // at cap: every candidate defers
		first := eligibleTask(domain.TaskPhaseExecution)
		second := eligibleTask(domain.TaskPhaseExecution)
		second.TaskIdentifier = "tid-b"
		fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-a.md", "24 Tasks/tid-b.md"}, nil)
		fakeGitRestClient.GetStub = func(_ context.Context, relPath string) ([]byte, error) {
			if strings.HasSuffix(relPath, "tid-b.md") {
				return []byte(renderTaskFile(second)), nil
			}
			return []byte(renderTaskFile(first)), nil
		}

		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
		Expect(fakeSpawner.ListActiveJobsCallCount()).To(Equal(1))
	})
})
