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

// buildQueueTask is the minimal eligible task the admission-ordering specs defer
// behind the concurrency cap. It carries no current_job, so spawnIfNeeded takes
// the cap path rather than the grace-window path.
func buildQueueTask(id lib.TaskIdentifier, jobStartedAt string) lib.Task {
	fm := lib.TaskFrontmatter{
		"status":   "in_progress",
		"phase":    string(domain.TaskPhasePlanning),
		"assignee": "claude",
		"stage":    "prod",
	}
	if jobStartedAt != "" {
		fm["job_started_at"] = jobStartedAt
	}
	return lib.Task{TaskIdentifier: id, Vault: testTaskVault, Frontmatter: fm}
}

var _ = Describe("TaskEventHandler", func() {
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
			pkg.ClaimModeOff,
			nil,
		)
	})

	buildMsg := func(task lib.Task) *sarama.ConsumerMessage {
		if task.Vault == "" {
			task.Vault = testTaskVault
		}
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
					Vault:          testTaskVault,
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

			It("admits a never-attempted task ahead of an already-attempted one", func() {
				// evalDeferredRespawns collects the due entries by ranging a Go map,
				// so without a tie-break the order is randomized per tick and the same
				// task ids recur in the concurrency_cap log for hours while others are
				// dispatched (prod, 2026-10-21). A task that has never been attempted
				// must win the freed slot over one that has already had a turn.
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee:          "claude",
						Image:             "my-image:latest",
						MaxConcurrentJobs: 1,
					},
					nil,
				)
				running := 1 // at cap, so both events defer instead of spawning
				fakeSpawner.ListActiveJobsStub = func(
					context.Context, string,
				) (map[lib.TaskIdentifier]bool, error) {
					return activeJobs(running), nil
				}
				fakeSpawner.SpawnJobStub = func(
					context.Context, lib.Task, pkg.AgentConfiguration,
				) (string, error) {
					running = 1
					return "job-admitted", nil
				}

				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))
				attempted := buildQueueTask("tid-attempted", "2026-10-21T07:00:00Z")
				fresh := buildQueueTask("tid-fresh", "")
				Expect(h.ConsumeMessage(ctx, buildMsg(attempted))).To(BeNil())
				Expect(h.ConsumeMessage(ctx, buildMsg(fresh))).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

				// The slot frees and both deferred entries come due in the same tick.
				running = 0
				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:01:30Z"))
				Expect(h.EvalDeferredRespawns(ctx)).To(Succeed())

				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
				_, spawnedTask, _ := fakeSpawner.SpawnJobArgsForCall(0)
				Expect(spawnedTask.TaskIdentifier).To(Equal(lib.TaskIdentifier("tid-fresh")))
			})

			// buildRoutingTask is the minimal eligible task the cluster-routing specs
			// defer; cluster is written only when non-empty so the same helper builds
			// the untagged and the tagged variants.
			buildRoutingTask := func(id lib.TaskIdentifier, cluster string) lib.Task {
				task := buildQueueTask(id, "")
				if cluster != "" {
					task.Frontmatter["cluster"] = cluster
				}
				return task
			}

			// newRoutingHandler builds a handler for the given cluster/runUntagged
			// pair, sharing the suite's fakes and clock so the deferred map is driven
			// through the real ConsumeMessage -> EvalDeferredRespawns path.
			newRoutingHandler := func(cluster string, runUntagged bool) handler.TaskEventHandler {
				return handler.NewTaskEventHandler(
					fakeSpawner,
					base.Branch("prod"),
					cluster,
					true,
					runUntagged,
					fakeResolver,
					fakeResultPublisher,
					pkg.NewTaskStore(),
					currentDateTime,
					fakeView,
					testTaskVault,
					handler.DefaultReconcilePassTimeout,
					handler.DefaultReconcileInterval,
					pkg.ClaimModeOff,
					nil,
				)
			}

			// setupCapDeferral installs a single-slot cap with one job already in
			// flight and returns a setter the specs use to free the slot.
			setupCapDeferral := func() func(int) {
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee:          "claude",
						Image:             "my-image:latest",
						MaxConcurrentJobs: 1,
					},
					nil,
				)
				running := 1
				fakeSpawner.ListActiveJobsStub = func(
					context.Context, string,
				) (map[lib.TaskIdentifier]bool, error) {
					return activeJobs(running), nil
				}
				fakeSpawner.SpawnJobStub = func(
					context.Context, lib.Task, pkg.AgentConfiguration,
				) (string, error) {
					running = 1
					return "job-routed", nil
				}
				return func(n int) { running = n }
			}

			// foreignRetaggedTask is the view snapshot of tid-retagged after the
			// retag to cluster sun; the reconcile pass must read it as foreign.
			foreignRetaggedTask := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("tid-retagged"),
				Vault:          testTaskVault,
				Frontmatter: lib.TaskFrontmatter{
					"status":   "in_progress",
					"phase":    string(domain.TaskPhasePlanning),
					"assignee": "claude",
					"stage":    "prod",
					"cluster":  "sun",
				},
			}

			It(
				"does not spawn a deferred task that a later event retagged to another cluster",
				func() {
					// Regression for prod 2026-10-21: a task deferred at the cap while
					// untagged was retagged cluster: sun, yet the parked copy still held
					// the pre-retag snapshot and spawned on nukeprod once a slot freed.
					setRunning := setupCapDeferral()
					routingHandler := newRoutingHandler("nukeprod", true)

					currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))
					Expect(
						routingHandler.ConsumeMessage(
							ctx,
							buildMsg(buildRoutingTask("tid-retagged", "")),
						),
					).
						To(Succeed())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

					Expect(
						routingHandler.ConsumeMessage(
							ctx,
							buildMsg(buildRoutingTask("tid-retagged", "sun")),
						),
					).
						To(Succeed())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

					setRunning(0)
					currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:01:30Z"))
					Expect(routingHandler.EvalDeferredRespawns(ctx)).To(Succeed())
					currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:02:30Z"))
					Expect(routingHandler.EvalDeferredRespawns(ctx)).To(Succeed())

					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
				},
			)

			It("spawns a deferred task exactly once when no later event retags it", func() {
				// Positive control: proves the regression spec above cannot pass
				// vacuously because the deferred path was never exercised.
				setRunning := setupCapDeferral()
				routingHandler := newRoutingHandler("nukeprod", true)

				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))
				Expect(
					routingHandler.ConsumeMessage(
						ctx,
						buildMsg(buildRoutingTask("tid-not-retagged", "")),
					),
				).
					To(Succeed())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

				setRunning(0)
				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:01:30Z"))
				Expect(routingHandler.EvalDeferredRespawns(ctx)).To(Succeed())

				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
				_, spawnedTask, _ := fakeSpawner.SpawnJobArgsForCall(0)
				Expect(spawnedTask.TaskIdentifier).To(Equal(lib.TaskIdentifier("tid-not-retagged")))
			})

			It("drops a deferred entry when a reconcile pass reads the task as foreign", func() {
				// Backstop: a reconcile-driven cap deferral can store the pre-retag
				// snapshot after the Kafka retag event was already consumed.
				setRunning := setupCapDeferral()

				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))
				Expect(h.ConsumeMessage(ctx, buildMsg(buildRoutingTask("tid-retagged", "")))).
					To(Succeed())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

				fakeView.SnapshotReturns([]lib.Task{foreignRetaggedTask})
				Expect(h.ReconcileOnce(ctx)).To(Succeed())

				setRunning(0)
				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:01:30Z"))
				Expect(h.EvalDeferredRespawns(ctx)).To(Succeed())

				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			})

			It(
				"does not spawn a deferred task whose later event drops its cluster tag "+
					"on a cluster that refuses untagged tasks",
				func() {
					setRunning := setupCapDeferral()
					routingHandler := newRoutingHandler("nukeprod", false)

					currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))
					tagged := buildRoutingTask("tid-untagged", "nukeprod")
					Expect(routingHandler.ConsumeMessage(ctx, buildMsg(tagged))).To(Succeed())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

					untagged := buildRoutingTask("tid-untagged", "")
					Expect(routingHandler.ConsumeMessage(ctx, buildMsg(untagged))).To(Succeed())

					setRunning(0)
					currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:01:30Z"))
					Expect(routingHandler.EvalDeferredRespawns(ctx)).To(Succeed())

					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
				},
			)

			// capConfig is the single-slot configuration every claim spec parks a
			// task behind.
			capConfig := pkg.AgentConfiguration{
				Assignee:          "claude",
				Image:             "my-image:latest",
				MaxConcurrentJobs: 1,
			}

			// claimInvalidatedCount reads the claim-invalidation counter.
			claimInvalidatedCount := func() float64 {
				return testutil.ToFloat64(
					metrics.TaskEventsTotal.WithLabelValues("deferred_claim_invalidated"),
				)
			}

			// installCapSpawner makes the spawner report `running` active jobs for
			// claude. The returned setter frees or fills the slot.
			installCapSpawner := func(running *int) {
				fakeSpawner.ListActiveJobsStub = func(
					context.Context, string,
				) (map[lib.TaskIdentifier]bool, error) {
					return activeJobs(*running), nil
				}
				fakeSpawner.SpawnJobStub = func(
					context.Context, lib.Task, pkg.AgentConfiguration,
				) (string, error) {
					*running = 1
					return "job-claim", nil
				}
			}

			It("does not spawn or re-defer a replay whose entry was evicted mid-replay", func() {
				// This spec fails on the pre-fix code. There, evalDeferredRespawns
				// deleted the entry and released deferredMu, then spawned the stale
				// snapshot: the retag eviction below found nothing to remove and the
				// pre-retag task ran on nukeprod. The eviction is forced into that
				// exact gap by the ListActiveJobs stub, which runs inside
				// spawnIfNeeded after the collection and before the claim re-check.
				fakeResolver.ResolveReturns(capConfig, nil)
				routingHandler := newRoutingHandler("nukeprod", true)

				running := 1
				armed := false
				evicted := false
				installCapSpawner(&running)
				// Not IsJobActiveStub: buildQueueTask carries no current_job, so the
				// replay reaches isJobActive -> ListActiveJobs.
				listStub := fakeSpawner.ListActiveJobsStub
				fakeSpawner.ListActiveJobsStub = func(
					ctx context.Context, assignee string,
				) (map[lib.TaskIdentifier]bool, error) {
					if armed && !evicted {
						evicted = true
						Expect(
							routingHandler.ConsumeMessage(
								ctx,
								buildMsg(buildRoutingTask("tid-replay", "sun")),
							),
						).To(Succeed())
					}
					return listStub(ctx, assignee)
				}

				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))
				Expect(
					routingHandler.ConsumeMessage(
						ctx,
						buildMsg(buildRoutingTask("tid-replay", "")),
					),
				).To(Succeed())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

				running = 0
				armed = true
				before := claimInvalidatedCount()
				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:01:30Z"))
				Expect(routingHandler.EvalDeferredRespawns(ctx)).To(Succeed())

				Expect(evicted).To(BeTrue())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
				Expect(claimInvalidatedCount() - before).To(Equal(float64(1)))

				// No re-deferral: a second tick past the retry delay spawns nothing.
				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:02:30Z"))
				Expect(routingHandler.EvalDeferredRespawns(ctx)).To(Succeed())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			})

			It(
				"does not re-defer a replay whose entry was evicted while the agent stays at cap",
				func() {
					// This spec fails on the pre-fix code: the replay reached the cap
					// branch and re-parked the stale snapshot, then spawned it once the
					// slot freed. Keeping the agent at the cap during the replay tick is
					// what forces that store; without it the spec also passes on the old
					// code.
					fakeResolver.ResolveReturns(capConfig, nil)
					routingHandler := newRoutingHandler("nukeprod", true)

					running := 1
					armed := false
					evicted := false
					installCapSpawner(&running)
					listStub := fakeSpawner.ListActiveJobsStub
					fakeSpawner.ListActiveJobsStub = func(
						ctx context.Context, assignee string,
					) (map[lib.TaskIdentifier]bool, error) {
						if armed && !evicted {
							evicted = true
							Expect(
								routingHandler.ConsumeMessage(
									ctx,
									buildMsg(buildRoutingTask("tid-replay-cap", "sun")),
								),
							).To(Succeed())
						}
						return listStub(ctx, assignee)
					}

					currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))
					Expect(
						routingHandler.ConsumeMessage(
							ctx,
							buildMsg(buildRoutingTask("tid-replay-cap", "")),
						),
					).To(Succeed())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

					armed = true
					before := claimInvalidatedCount()
					currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:01:30Z"))
					Expect(routingHandler.EvalDeferredRespawns(ctx)).To(Succeed())

					Expect(evicted).To(BeTrue())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
					Expect(claimInvalidatedCount() - before).To(Equal(float64(1)))

					// Free the slot and tick twice past the retry delay: the evicted
					// snapshot must not have been re-parked.
					running = 0
					currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:02:30Z"))
					Expect(routingHandler.EvalDeferredRespawns(ctx)).To(Succeed())
					currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:03:30Z"))
					Expect(routingHandler.EvalDeferredRespawns(ctx)).To(Succeed())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
				},
			)

			It("spawns an un-evicted replay exactly once", func() {
				// Positive control: the eviction in the two specs above targets a
				// different task id, so the replay's claim stays valid and it spawns
				// exactly once. Proves those specs are not passing because the replay
				// path was never exercised.
				fakeResolver.ResolveReturns(capConfig, nil)
				routingHandler := newRoutingHandler("nukeprod", true)

				running := 1
				armed := false
				evictedOther := false
				installCapSpawner(&running)
				listStub := fakeSpawner.ListActiveJobsStub
				fakeSpawner.ListActiveJobsStub = func(
					ctx context.Context, assignee string,
				) (map[lib.TaskIdentifier]bool, error) {
					if armed && !evictedOther {
						evictedOther = true
						Expect(
							routingHandler.ConsumeMessage(
								ctx,
								buildMsg(buildRoutingTask("tid-other", "sun")),
							),
						).To(Succeed())
					}
					return listStub(ctx, assignee)
				}

				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))
				Expect(
					routingHandler.ConsumeMessage(
						ctx,
						buildMsg(buildRoutingTask("tid-replay-control", "")),
					),
				).To(Succeed())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

				running = 0
				armed = true
				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:01:30Z"))
				Expect(routingHandler.EvalDeferredRespawns(ctx)).To(Succeed())

				Expect(evictedOther).To(BeTrue())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
				_, spawnedTask, _ := fakeSpawner.SpawnJobArgsForCall(0)
				Expect(spawnedTask.TaskIdentifier).
					To(Equal(lib.TaskIdentifier("tid-replay-control")))
			})

			DescribeTable("every filter skip drops a parked deferred entry",
				func(first, second lib.Task, configure func()) {
					fakeResolver.ResolveReturns(capConfig, nil)
					if configure != nil {
						configure()
					}
					routingHandler := newRoutingHandler("nukeprod", true)
					running := 1
					installCapSpawner(&running)

					currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))
					beforeCap := testutil.ToFloat64(
						metrics.TaskEventsTotal.WithLabelValues("deferred_concurrency_cap"),
					)
					Expect(routingHandler.ConsumeMessage(ctx, buildMsg(first))).To(Succeed())
					// The first event must actually have parked the task, otherwise
					// the second event's eviction would be vacuous.
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
					Expect(
						testutil.ToFloat64(
							metrics.TaskEventsTotal.WithLabelValues("deferred_concurrency_cap"),
						) - beforeCap,
					).To(Equal(float64(1)))

					Expect(routingHandler.ConsumeMessage(ctx, buildMsg(second))).To(Succeed())

					running = 0
					currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:01:30Z"))
					Expect(routingHandler.EvalDeferredRespawns(ctx)).To(Succeed())
					Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
				},
				Entry("type mismatch",
					func() lib.Task {
						t := buildQueueTask("tid-skip-type", "")
						t.Frontmatter["task_type"] = "x"
						return t
					}(),
					func() lib.Task {
						t := buildQueueTask("tid-skip-type", "")
						t.Frontmatter["task_type"] = "y"
						return t
					}(),
					func() {
						cfg := capConfig
						cfg.TaskType = "x"
						fakeResolver.ResolveReturns(cfg, nil)
					},
				),
				Entry("status",
					buildQueueTask("tid-skip-status", ""),
					func() lib.Task {
						t := buildQueueTask("tid-skip-status", "")
						t.Frontmatter["status"] = "hold"
						return t
					}(),
					nil,
				),
				Entry("non-trigger phase",
					buildQueueTask("tid-skip-phase", ""),
					func() lib.Task {
						t := buildQueueTask("tid-skip-phase", "")
						t.Frontmatter["phase"] = string(domain.TaskPhaseTodo)
						return t
					}(),
					nil,
				),
				Entry("stage",
					buildQueueTask("tid-skip-stage", ""),
					func() lib.Task {
						t := buildQueueTask("tid-skip-stage", "")
						t.Frontmatter["stage"] = "dev"
						return t
					}(),
					nil,
				),
				Entry("empty assignee",
					buildQueueTask("tid-skip-assignee", ""),
					func() lib.Task {
						t := buildQueueTask("tid-skip-assignee", "")
						t.Frontmatter["assignee"] = ""
						return t
					}(),
					nil,
				),
				Entry("resolveConfig skip",
					buildQueueTask("tid-skip-unknown", ""),
					func() lib.Task {
						t := buildQueueTask("tid-skip-unknown", "")
						t.Frontmatter["assignee"] = "unknown-agent-skip"
						return t
					}(),
					func() {
						fakeResolver.ResolveStub = func(
							_ context.Context, assignee string,
						) (pkg.AgentConfiguration, error) {
							if assignee == "unknown-agent-skip" {
								return pkg.AgentConfiguration{}, pkg.ErrConfigNotFound
							}
							return capConfig, nil
						}
					},
				),
				Entry("foreign cluster",
					buildQueueTask("tid-skip-cluster", ""),
					buildRoutingTask("tid-skip-cluster", "sun"),
					nil,
				),
				Entry("terminal phase",
					buildQueueTask("tid-skip-terminal", ""),
					func() lib.Task {
						t := buildQueueTask("tid-skip-terminal", "")
						t.Frontmatter["phase"] = string(domain.TaskPhaseHumanReview)
						return t
					}(),
					nil,
				),
			)

			It("does not spawn a reconcile drive whose entry was evicted mid-drive", func() {
				// Fails on the pre-fix code: gather captures the candidate, the retag
				// eviction between gather and drive found nothing to remove, and the
				// drive spawned the stale snapshot. The first ListActiveJobs call in
				// a pass is the drive's isJobActive (gather makes none), so the stub
				// lands the eviction exactly in that gap.
				fakeResolver.ResolveReturns(capConfig, nil)
				routingHandler := newRoutingHandler("nukeprod", true)
				running := 0
				evicted := false
				fakeSpawner.ListActiveJobsStub = func(
					ctx context.Context, _ string,
				) (map[lib.TaskIdentifier]bool, error) {
					if !evicted {
						evicted = true
						Expect(
							routingHandler.ConsumeMessage(
								ctx,
								buildMsg(buildRoutingTask("tid-reconcile", "sun")),
							),
						).To(Succeed())
					}
					return activeJobs(running), nil
				}
				fakeSpawner.SpawnJobStub = func(
					context.Context, lib.Task, pkg.AgentConfiguration,
				) (string, error) {
					return "job-reconcile", nil
				}
				fakeView.SnapshotReturns([]lib.Task{buildQueueTask("tid-reconcile", "")})

				before := claimInvalidatedCount()
				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))
				Expect(routingHandler.ReconcileOnce(ctx)).To(Succeed())

				Expect(evicted).To(BeTrue())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
				Expect(claimInvalidatedCount() - before).To(Equal(float64(1)))
			})

			It("does not re-defer a reconcile drive evicted while the agent stays at cap", func() {
				// Fails on the pre-fix code: the drive re-parked the reconcile snapshot
				// at the cap, then spawned it after the slot freed. Keeping the agent at
				// the cap forces the store.
				fakeResolver.ResolveReturns(capConfig, nil)
				routingHandler := newRoutingHandler("nukeprod", true)
				running := 1
				evicted := false
				fakeSpawner.ListActiveJobsStub = func(
					ctx context.Context, _ string,
				) (map[lib.TaskIdentifier]bool, error) {
					if !evicted {
						evicted = true
						Expect(
							routingHandler.ConsumeMessage(
								ctx,
								buildMsg(buildRoutingTask("tid-reconcile", "sun")),
							),
						).To(Succeed())
					}
					return activeJobs(running), nil
				}
				fakeSpawner.SpawnJobStub = func(
					context.Context, lib.Task, pkg.AgentConfiguration,
				) (string, error) {
					return "job-reconcile", nil
				}
				fakeView.SnapshotReturns([]lib.Task{buildQueueTask("tid-reconcile", "")})

				before := claimInvalidatedCount()
				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))
				Expect(routingHandler.ReconcileOnce(ctx)).To(Succeed())
				Expect(evicted).To(BeTrue())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
				Expect(claimInvalidatedCount() - before).To(Equal(float64(1)))

				running = 0
				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:01:30Z"))
				Expect(routingHandler.EvalDeferredRespawns(ctx)).To(Succeed())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
			})

			It("orders already-attempted tasks by queue-entry time, not last attempt", func() {
				// queued_at is when the FIRST wait began, so it is the fair key among
				// tasks that have all had a turn. job_started_at is deliberately
				// inverted here: this spec fails if the comparator falls back to it.
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee:          "claude",
						Image:             "my-image:latest",
						MaxConcurrentJobs: 1,
					},
					nil,
				)
				running := 1
				fakeSpawner.ListActiveJobsStub = func(
					context.Context, string,
				) (map[lib.TaskIdentifier]bool, error) {
					return activeJobs(running), nil
				}
				fakeSpawner.SpawnJobStub = func(
					context.Context, lib.Task, pkg.AgentConfiguration,
				) (string, error) {
					running = 1
					return "job-admitted", nil
				}

				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))
				early := buildQueueTask("tid-early", "2026-10-21T07:55:00Z")
				early.Frontmatter["queued_at"] = "2026-10-21T06:00:00Z"
				late := buildQueueTask("tid-late", "2026-10-21T06:30:00Z")
				late.Frontmatter["queued_at"] = "2026-10-21T07:00:00Z"
				Expect(h.ConsumeMessage(ctx, buildMsg(late))).To(BeNil())
				Expect(h.ConsumeMessage(ctx, buildMsg(early))).To(BeNil())
				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

				running = 0
				currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:01:30Z"))
				Expect(h.EvalDeferredRespawns(ctx)).To(Succeed())

				Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
				_, spawnedTask, _ := fakeSpawner.SpawnJobArgsForCall(0)
				Expect(spawnedTask.TaskIdentifier).To(Equal(lib.TaskIdentifier("tid-early")))
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
						"",
						true,
						true,
						fakeResolver,
						fakeResultPublisher,
						restartStore,
						currentDateTime,
						fakeView,
						testTaskVault,
						handler.DefaultReconcilePassTimeout,
						handler.DefaultReconcileInterval,
						pkg.ClaimModeOff,
						nil,
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
						"",
						true,
						true,
						fakeResolver,
						fakeResultPublisher,
						restartStore,
						currentDateTime,
						fakeView,
						testTaskVault,
						handler.DefaultReconcilePassTimeout,
						handler.DefaultReconcileInterval,
						pkg.ClaimModeOff,
						nil,
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

		Describe("queued_at on cap deferral", func() {
			const (
				queuedAtValue = "2026-05-17T09:34:00Z"
				// concurrencyCapRetryDelay is unexported and this test lives in
				// package handler_test, so the post-retry instant is hardcoded.
				afterRetryDelay = "2026-05-17T09:35:01Z"
			)

			var payload lib.Task

			BeforeEach(func() {
				fakeResolver.ResolveReturns(
					pkg.AgentConfiguration{
						Assignee:          "claude",
						Image:             "my-image:latest",
						MaxConcurrentJobs: 1,
					},
					nil,
				)
				currentDateTime.SetNow(libtimetest.ParseDateTime(queuedAtValue))
				// The cap is counted through ListActiveJobs (activeJobsFor). The
				// map must carry a DIFFERENT identifier: isJobActive returns early
				// when the task's own id is active.
				fakeSpawner.ListActiveJobsReturns(
					map[lib.TaskIdentifier]bool{"other-task": true},
					nil,
				)
				fakeSpawner.IsJobActiveReturns(false, nil)
				payload = lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-queued"),
					Frontmatter: lib.TaskFrontmatter{
						"status":   "in_progress",
						"phase":    string(domain.TaskPhaseExecution),
						"assignee": "claude",
						"stage":    "prod",
					},
				}
			})

			It("publishes the fixed clock value when neither event nor vault carries it", func() {
				Expect(h.ConsumeMessage(ctx, buildMsg(payload))).To(Succeed())

				Expect(fakeResultPublisher.PublishQueueEntryCallCount()).To(Equal(1))
				_, _, queuedAt := fakeResultPublisher.PublishQueueEntryArgsForCall(0)
				Expect(queuedAt).To(Equal("2026-05-17T09:34:00Z"))
			})

			It("does not publish when the view already carries queued_at", func() {
				fakeView.GetReturns(lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-queued"),
					Vault:          testTaskVault,
					Frontmatter: lib.TaskFrontmatter{
						"queued_at": queuedAtValue,
					},
				}, true)

				before := testutil.ToFloat64(
					metrics.DeferredConcurrencyCapTotal.WithLabelValues("claude"),
				)
				Expect(h.ConsumeMessage(ctx, buildMsg(payload))).To(Succeed())
				after := testutil.ToFloat64(
					metrics.DeferredConcurrencyCapTotal.WithLabelValues("claude"),
				)
				// Proves the deferral actually happened rather than the payload
				// being dropped as foreign before reaching queuedAtFromVault.
				Expect(after - before).To(Equal(float64(1)))
				Expect(fakeResultPublisher.PublishQueueEntryCallCount()).To(Equal(0))
			})

			It("does not re-publish on a second deferral of the same task", func() {
				Expect(h.ConsumeMessage(ctx, buildMsg(payload))).To(Succeed())
				Expect(fakeResultPublisher.PublishQueueEntryCallCount()).To(Equal(1))

				// Drive the in-memory deferred entry, not a second ConsumeMessage:
				// a repeat payload carries no queued_at and would re-stamp.
				currentDateTime.SetNow(libtimetest.ParseDateTime(afterRetryDelay))
				Expect(h.EvalDeferredRespawns(ctx)).To(Succeed())
				Expect(fakeResultPublisher.PublishQueueEntryCallCount()).To(Equal(1))
			})

			DescribeTable("queuedAtFromView miss branches still publish once",
				func(setup func(), id lib.TaskIdentifier) {
					setup()
					task := payload
					task.TaskIdentifier = id
					Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(Succeed())
					Expect(fakeResultPublisher.PublishQueueEntryCallCount()).To(Equal(1))
				},
				Entry(
					"unknown id",
					func() {},
					lib.TaskIdentifier("tid-unknown"),
				),
				Entry(
					"view task carries no queued_at",
					func() {
						fakeView.GetReturns(lib.Task{
							TaskIdentifier: lib.TaskIdentifier("tid-queued"),
							Vault:          testTaskVault,
						}, true)
					},
					lib.TaskIdentifier("tid-queued"),
				),
			)

			It("returns nil on publish failure and does not retry the next deferral", func() {
				fakeResultPublisher.PublishQueueEntryReturns(
					errors.Errorf(ctx, "publish failed"),
				)
				Expect(h.ConsumeMessage(ctx, buildMsg(payload))).To(Succeed())
				Expect(fakeResultPublisher.PublishQueueEntryCallCount()).To(Equal(1))

				currentDateTime.SetNow(libtimetest.ParseDateTime(afterRetryDelay))
				Expect(h.EvalDeferredRespawns(ctx)).To(Succeed())
				Expect(fakeResultPublisher.PublishQueueEntryCallCount()).To(Equal(1))
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
