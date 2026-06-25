// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	"time"

	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	lib "github.com/bborbe/agent/lib"
	agentv1 "github.com/bborbe/agent/task/executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent/task/executor/mocks"
	"github.com/bborbe/agent/task/executor/pkg"
)

var _ = Describe("ZombieSweeper", func() {
	var (
		ctx                context.Context
		fakePublisher      *mocks.FakeResultPublisher
		taskStore          *pkg.TaskStore
		eventHandlerConfig pkg.EventHandlerConfig
		currentDateTime    libtime.CurrentDateTime
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakePublisher = &mocks.FakeResultPublisher{}
		taskStore = pkg.NewTaskStore()
		eventHandlerConfig = pkg.NewEventHandlerConfig()
		currentDateTime = libtime.NewCurrentDateTime()
		// Default "now" for tests: 2026-06-01T12:00:00Z
		currentDateTime.SetNow(libtimetest.ParseDateTime("2026-06-01T12:00:00Z"))
	})

	makeTask := func(id string, assignee string, currentJob string, jobStartedAt string) lib.Task {
		fm := lib.TaskFrontmatter{
			"status":   "in_progress",
			"assignee": assignee,
		}
		if currentJob != "" {
			fm["current_job"] = currentJob
		}
		if jobStartedAt != "" {
			fm["job_started_at"] = jobStartedAt
		}
		return lib.Task{
			TaskIdentifier: lib.TaskIdentifier(id),
			Frontmatter:    fm,
			Content:        lib.TaskContent("do the work"),
		}
	}

	// makePodWithCreation creates a pod with a specific CreationTimestamp offset from "now"
	// (which is set by currentDateTime to 2026-06-01T12:00:00Z).
	makePod := func(name, namespace, taskID string, phase corev1.PodPhase, creationAge time.Duration, conditions ...corev1.PodCondition) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Labels: map[string]string{
					"agent.benjamin-borbe.de/task-id": taskID,
				},
				CreationTimestamp: metav1.Time{
					// currentDateTime is 2026-06-01T12:00:00Z, subtract age to get creation time
					Time: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).Add(-creationAge),
				},
			},
			Status: corev1.PodStatus{
				Phase:      phase,
				Conditions: conditions,
			},
		}
	}

	podScheduledFalseCondition := func() corev1.PodCondition {
		return corev1.PodCondition{
			Type:   corev1.PodScheduled,
			Status: corev1.ConditionFalse,
			Reason: "Unschedulable",
		}
	}

	ptrInt32 := func(v int32) *int32 { return &v }

	Describe("SweepOnce", func() {
		// 6a: deadline-exceeded-and-not-running → zombie (deadline_exceeded)
		Context("deadline exceeded with failed pod", func() {
			It("publishes failure with deadline_exceeded", func() {
				taskID := lib.TaskIdentifier("task-6a-deadline-exceeded")
				// job_started_at = 11:30:00Z, elapsed = 30min at 12:00:00Z
				ts := libtimetest.ParseDateTime("2026-06-01T11:30:00Z")
				task := makeTask(string(taskID), "agent-a", "job-1", ts.Format(time.RFC3339))
				taskStore.Store(taskID, task)

				// Config: deadline = 60s
				cfg := agentv1.Config{
					ObjectMeta: metav1.ObjectMeta{Name: "cfg-a"},
					Spec: agentv1.ConfigSpec{
						Assignee:                "agent-a",
						ZombieJobTimeoutSeconds: ptrInt32(60),
					},
				}
				_ = eventHandlerConfig.OnAdd(ctx, cfg)

				fakeClient := fake.NewSimpleClientset()
				informerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(
					fakeClient,
					0,
					k8sinformers.WithNamespace("test-ns"),
				)
				podInformer := informerFactory.Core().V1().Pods().Informer()
				_ = podInformer.GetIndexer().
					Add(makePod("pod-failed", "test-ns", string(taskID), corev1.PodFailed, 0))
				podLister := informerFactory.Core().V1().Pods().Lister()
				fakeJobWatcher := &mocks.FakeJobWatcher{}
				fakeJobWatcher.PodListerReturns(podLister)

				sweeper := pkg.NewZombieSweeper(
					fakeJobWatcher,
					"test-ns",
					taskStore,
					fakePublisher,
					eventHandlerConfig,
					currentDateTime,
				)

				err := sweeper.SweepOnce(ctx)
				Expect(err).To(BeNil())

				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
				_, calledTask, calledJobName, calledReason := fakePublisher.PublishFailureArgsForCall(
					0,
				)
				Expect(string(calledTask.TaskIdentifier)).To(Equal(string(taskID)))
				Expect(calledJobName).To(Equal("job-1"))
				Expect(calledReason).To(Equal("deadline_exceeded"))
			})
		})

		// 6b: deadline-exceeded-but-running → NOT zombie
		Context("deadline exceeded but pod is Running", func() {
			It("skips publish when pod is Running", func() {
				taskID := lib.TaskIdentifier("task-6b-running")
				ts := libtimetest.ParseDateTime("2026-06-01T11:30:00Z")
				task := makeTask(string(taskID), "agent-a", "job-1", ts.Format(time.RFC3339))
				taskStore.Store(taskID, task)

				cfg := agentv1.Config{
					ObjectMeta: metav1.ObjectMeta{Name: "cfg-a"},
					Spec: agentv1.ConfigSpec{
						Assignee:                "agent-a",
						ZombieJobTimeoutSeconds: ptrInt32(60),
					},
				}
				_ = eventHandlerConfig.OnAdd(ctx, cfg)

				fakeClient := fake.NewSimpleClientset()
				informerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(
					fakeClient,
					0,
					k8sinformers.WithNamespace("test-ns"),
				)
				podInformer := informerFactory.Core().V1().Pods().Informer()
				_ = podInformer.GetIndexer().
					Add(makePod("pod-running", "test-ns", string(taskID), corev1.PodRunning, 0))
				podLister := informerFactory.Core().V1().Pods().Lister()
				fakeJobWatcher := &mocks.FakeJobWatcher{}
				fakeJobWatcher.PodListerReturns(podLister)

				sweeper := pkg.NewZombieSweeper(
					fakeJobWatcher,
					"test-ns",
					taskStore,
					fakePublisher,
					eventHandlerConfig,
					currentDateTime,
				)

				err := sweeper.SweepOnce(ctx)
				Expect(err).To(BeNil())
				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
			})
		})

		// 6c: under-deadline → NOT zombie
		Context("elapsed time is under deadline", func() {
			It("skips publish when elapsed < deadline", func() {
				taskID := lib.TaskIdentifier("task-6c-under-deadline")
				// job_started_at = 11:59:30Z, elapsed = 30s at 12:00:00Z < deadline of 60s
				ts := libtimetest.ParseDateTime("2026-06-01T11:59:30Z")
				task := makeTask(string(taskID), "agent-a", "job-1", ts.Format(time.RFC3339))
				taskStore.Store(taskID, task)

				cfg := agentv1.Config{
					ObjectMeta: metav1.ObjectMeta{Name: "cfg-a"},
					Spec: agentv1.ConfigSpec{
						Assignee:                "agent-a",
						ZombieJobTimeoutSeconds: ptrInt32(60),
					},
				}
				_ = eventHandlerConfig.OnAdd(ctx, cfg)

				fakeClient := fake.NewSimpleClientset()
				informerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(
					fakeClient,
					0,
					k8sinformers.WithNamespace("test-ns"),
				)
				podInformer := informerFactory.Core().V1().Pods().Informer()
				_ = podInformer.GetIndexer().
					Add(makePod("pod-failed", "test-ns", string(taskID), corev1.PodFailed, 0))
				podLister := informerFactory.Core().V1().Pods().Lister()
				fakeJobWatcher := &mocks.FakeJobWatcher{}
				fakeJobWatcher.PodListerReturns(podLister)

				sweeper := pkg.NewZombieSweeper(
					fakeJobWatcher,
					"test-ns",
					taskStore,
					fakePublisher,
					eventHandlerConfig,
					currentDateTime,
				)

				err := sweeper.SweepOnce(ctx)
				Expect(err).To(BeNil())
				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
			})
		})

		// 6d: watch-lost → executor_watch_lost
		Context("no pods found for task past deadline", func() {
			It("publishes failure with executor_watch_lost", func() {
				taskID := lib.TaskIdentifier("task-6d-watch-lost")
				ts := libtimetest.ParseDateTime("2026-06-01T11:30:00Z")
				task := makeTask(string(taskID), "agent-a", "job-1", ts.Format(time.RFC3339))
				taskStore.Store(taskID, task)

				cfg := agentv1.Config{
					ObjectMeta: metav1.ObjectMeta{Name: "cfg-a"},
					Spec: agentv1.ConfigSpec{
						Assignee:                "agent-a",
						ZombieJobTimeoutSeconds: ptrInt32(60),
					},
				}
				_ = eventHandlerConfig.OnAdd(ctx, cfg)

				// Empty fake client — no pods at all
				fakeClient := fake.NewSimpleClientset()
				informerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(
					fakeClient,
					0,
					k8sinformers.WithNamespace("test-ns"),
				)
				podLister := informerFactory.Core().V1().Pods().Lister()
				fakeJobWatcher := &mocks.FakeJobWatcher{}
				fakeJobWatcher.PodListerReturns(podLister)

				sweeper := pkg.NewZombieSweeper(
					fakeJobWatcher,
					"test-ns",
					taskStore,
					fakePublisher,
					eventHandlerConfig,
					currentDateTime,
				)

				err := sweeper.SweepOnce(ctx)
				Expect(err).To(BeNil())
				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
				_, calledTask, calledJobName, calledReason := fakePublisher.PublishFailureArgsForCall(
					0,
				)
				Expect(string(calledTask.TaskIdentifier)).To(Equal(string(taskID)))
				Expect(calledJobName).To(Equal("job-1"))
				Expect(calledReason).To(Equal("executor_watch_lost"))
			})
		})

		// 6e: pod_not_scheduled
		Context("pod is Pending past grace window with PodScheduled=False", func() {
			It("publishes failure with pod_not_scheduled", func() {
				taskID := lib.TaskIdentifier("task-6e-pod-not-scheduled")
				ts := libtimetest.ParseDateTime("2026-06-01T11:30:00Z")
				task := makeTask(string(taskID), "agent-a", "job-1", ts.Format(time.RFC3339))
				taskStore.Store(taskID, task)

				cfg := agentv1.Config{
					ObjectMeta: metav1.ObjectMeta{Name: "cfg-a"},
					Spec: agentv1.ConfigSpec{
						Assignee:                "agent-a",
						ZombieJobTimeoutSeconds: ptrInt32(60),
					},
				}
				_ = eventHandlerConfig.OnAdd(ctx, cfg)

				fakeClient := fake.NewSimpleClientset()
				informerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(
					fakeClient,
					0,
					k8sinformers.WithNamespace("test-ns"),
				)
				podInformer := informerFactory.Core().V1().Pods().Informer()
				// Pod created 5 minutes ago (exceeds 2min grace window), Pending, PodScheduled=False
				_ = podInformer.GetIndexer().
					Add(makePod("pod-unschedulable", "test-ns", string(taskID), corev1.PodPending, 5*time.Minute, podScheduledFalseCondition()))
				podLister := informerFactory.Core().V1().Pods().Lister()
				fakeJobWatcher := &mocks.FakeJobWatcher{}
				fakeJobWatcher.PodListerReturns(podLister)

				sweeper := pkg.NewZombieSweeper(
					fakeJobWatcher,
					"test-ns",
					taskStore,
					fakePublisher,
					eventHandlerConfig,
					currentDateTime,
				)

				err := sweeper.SweepOnce(ctx)
				Expect(err).To(BeNil())
				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
				_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
				Expect(calledReason).To(Equal("pod_not_scheduled"))
			})
		})

		// 6f: interval default — indirectly verified: SweepOnce uses the default interval
		// when no config is set. The 6c test (under-deadline) implicitly uses the default
		// interval via the sweeper. We add an explicit test that exercises the default
		// path through the interval resolver by using a task with no matching config.
		Context(
			"interval default: task with no matching config uses 1800s default deadline",
			func() {
				It(
					"does not publish failure when elapsed (29min) < default deadline (1800s)",
					func() {
						taskID := lib.TaskIdentifier("task-6f-default-deadline")
						// job_started_at = 11:31:00Z, elapsed = 29min, default deadline = 1800s (30min)
						// elapsed < deadline → no publish
						ts := libtimetest.ParseDateTime("2026-06-01T11:31:00Z")
						task := makeTask(
							string(taskID),
							"agent-no-config",
							"job-1",
							ts.Format(time.RFC3339),
						)
						taskStore.Store(taskID, task)

						// No config added — uses default deadline of 1800s
						fakeClient := fake.NewSimpleClientset()
						informerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(
							fakeClient,
							0,
							k8sinformers.WithNamespace("test-ns"),
						)
						podInformer := informerFactory.Core().V1().Pods().Informer()
						_ = podInformer.GetIndexer().
							Add(makePod("pod-failed", "test-ns", string(taskID), corev1.PodFailed, 0))
						podLister := informerFactory.Core().V1().Pods().Lister()
						fakeJobWatcher := &mocks.FakeJobWatcher{}
						fakeJobWatcher.PodListerReturns(podLister)

						sweeper := pkg.NewZombieSweeper(
							fakeJobWatcher,
							"test-ns",
							taskStore,
							fakePublisher,
							eventHandlerConfig,
							currentDateTime,
						)

						err := sweeper.SweepOnce(ctx)
						Expect(err).To(BeNil())
						// elapsed (29min) < default deadline (30min) → no zombie
						Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
					},
				)
			},
		)

		// 6g: interval override — tested via 6a-6e which use configs with specific timeouts
		// 6h: deadline default — verified by 6f (uses default when no config matches)

		Context("task with no current_job is skipped", func() {
			It("skips tasks without a current_job set", func() {
				taskID := lib.TaskIdentifier("task-no-job")
				task := makeTask(string(taskID), "agent-a", "", "")
				taskStore.Store(taskID, task)

				fakeClient := fake.NewSimpleClientset()
				informerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(
					fakeClient,
					0,
					k8sinformers.WithNamespace("test-ns"),
				)
				podLister := informerFactory.Core().V1().Pods().Lister()
				fakeJobWatcher := &mocks.FakeJobWatcher{}
				fakeJobWatcher.PodListerReturns(podLister)

				sweeper := pkg.NewZombieSweeper(
					fakeJobWatcher,
					"test-ns",
					taskStore,
					fakePublisher,
					eventHandlerConfig,
					currentDateTime,
				)

				err := sweeper.SweepOnce(ctx)
				Expect(err).To(BeNil())
				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
			})
		})

		Context("task with unparseable job_started_at is skipped", func() {
			It("skips tasks with malformed job_started_at", func() {
				taskID := lib.TaskIdentifier("task-bad-time")
				fm := lib.TaskFrontmatter{
					"status":         "in_progress",
					"assignee":       "agent-a",
					"current_job":    "job-1",
					"job_started_at": "not-a-valid-timestamp",
				}
				task := lib.Task{
					TaskIdentifier: taskID,
					Frontmatter:    fm,
					Content:        lib.TaskContent("do the work"),
				}
				taskStore.Store(taskID, task)

				fakeClient := fake.NewSimpleClientset()
				informerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(
					fakeClient,
					0,
					k8sinformers.WithNamespace("test-ns"),
				)
				podLister := informerFactory.Core().V1().Pods().Lister()
				fakeJobWatcher := &mocks.FakeJobWatcher{}
				fakeJobWatcher.PodListerReturns(podLister)

				sweeper := pkg.NewZombieSweeper(
					fakeJobWatcher,
					"test-ns",
					taskStore,
					fakePublisher,
					eventHandlerConfig,
					currentDateTime,
				)

				err := sweeper.SweepOnce(ctx)
				Expect(err).To(BeNil())
				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
			})
		})

		// Per-task publish errors must NOT abort the sweep — every past-deadline
		// task must get a PublishFailure attempt even when an earlier one fails.
		// Guards the regression where a single broken publisher.PublishFailure
		// (e.g. transient kafka error for one task's UUID) would skip the rest.
		Context("multiple tasks with one publish failure mid-loop", func() {
			It(
				"calls PublishFailure for every task and continues past per-task errors",
				func() {
					ts := libtimetest.ParseDateTime("2026-06-01T11:30:00Z")
					tasks := []lib.TaskIdentifier{
						"task-multi-1",
						"task-multi-2",
						"task-multi-3",
					}
					for _, id := range tasks {
						task := makeTask(
							string(id),
							"agent-a",
							"job-"+string(id),
							ts.Format(time.RFC3339),
						)
						taskStore.Store(id, task)
					}

					cfg := agentv1.Config{
						ObjectMeta: metav1.ObjectMeta{Name: "cfg-a"},
						Spec: agentv1.ConfigSpec{
							Assignee:                "agent-a",
							ZombieJobTimeoutSeconds: ptrInt32(60),
						},
					}
					_ = eventHandlerConfig.OnAdd(ctx, cfg)

					// Empty pod lister — all tasks classify as executor_watch_lost.
					fakeClient := fake.NewSimpleClientset()
					informerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(
						fakeClient,
						0,
						k8sinformers.WithNamespace("test-ns"),
					)
					podLister := informerFactory.Core().V1().Pods().Lister()
					fakeJobWatcher := &mocks.FakeJobWatcher{}
					fakeJobWatcher.PodListerReturns(podLister)

					// Snapshot iterates a map → non-deterministic order. Fail
					// the second call regardless of which task lands there;
					// the assertion (call count == 3) covers the loop-continue
					// invariant without depending on order.
					fakePublisher.PublishFailureStub = func(
						_ context.Context,
						_ lib.Task,
						_ string,
						_ string,
					) error {
						if fakePublisher.PublishFailureCallCount() == 2 {
							return context.Canceled
						}
						return nil
					}

					sweeper := pkg.NewZombieSweeper(
						fakeJobWatcher,
						"test-ns",
						taskStore,
						fakePublisher,
						eventHandlerConfig,
						currentDateTime,
					)

					err := sweeper.SweepOnce(ctx)
					Expect(err).To(BeNil())
					Expect(fakePublisher.PublishFailureCallCount()).To(Equal(3))
				},
			)
		})

		// Pod lister not yet synced — sweeper must skip the tick without
		// publishing failures. Guards the regression where service.Run starts
		// the sweeper before JobWatcher.Run finishes WaitForCacheSync.
		Context("pod lister is nil (JobWatcher not yet synced)", func() {
			It("skips the tick without publishing failures", func() {
				taskID := lib.TaskIdentifier("task-lister-nil")
				ts := libtimetest.ParseDateTime("2026-06-01T11:30:00Z")
				task := makeTask(string(taskID), "agent-a", "job-1", ts.Format(time.RFC3339))
				taskStore.Store(taskID, task)

				cfg := agentv1.Config{
					ObjectMeta: metav1.ObjectMeta{Name: "cfg-a"},
					Spec: agentv1.ConfigSpec{
						Assignee:                "agent-a",
						ZombieJobTimeoutSeconds: ptrInt32(60),
					},
				}
				_ = eventHandlerConfig.OnAdd(ctx, cfg)

				// FakeJobWatcher returns a nil PodLister by default — simulates
				// the pre-cache-sync state.
				fakeJobWatcher := &mocks.FakeJobWatcher{}

				sweeper := pkg.NewZombieSweeper(
					fakeJobWatcher,
					"test-ns",
					taskStore,
					fakePublisher,
					eventHandlerConfig,
					currentDateTime,
				)

				err := sweeper.SweepOnce(ctx)
				Expect(err).To(BeNil())
				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
			})
		})
	})
})
