// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6@v6.12.2 -generate

import (
	"context"
	"encoding/json"

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
						"status": "in_progress",
						"phase":  string(phase),
						// Pre-set to match triggerScope() for this phase (no ref), so these
						// deferred-respawn specs stay on the same-scope increment path.
						// Without it every task here would take the scope-adoption branch
						// and publish SetTriggerScope instead — which silently defeats the
						// specs that inject an error on PublishIncrementTriggerCount and
						// block on the loop returning it. These tests are about grace-window
						// and publish-failure behaviour, not about scoping.
						"trigger_scope":  string(phase) + ":",
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
						fakeGitRestClient,
						"24 Tasks/*.md",
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
						fakeGitRestClient,
						"24 Tasks/*.md",
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
