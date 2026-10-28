// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	"time"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/bborbe/agent-task-executor/mocks"
	"github.com/bborbe/agent-task-executor/pkg"
)

var _ = Describe("JobWatcher", func() {
	var (
		ctx             context.Context
		fakePublisher   *mocks.FakeResultPublisher
		taskStore       *pkg.TaskStore
		fakeKubeClient  *fake.Clientset
		watcher         pkg.JobWatcher
		testTask        lib.Task
		testTaskID      lib.TaskIdentifier
		currentDateTime libtime.CurrentDateTime
		watcherStart    time.Time
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakePublisher = &mocks.FakeResultPublisher{}
		taskStore = pkg.NewTaskStore()
		fakeKubeClient = fake.NewClientset()
		watcherStart = time.Date(2026, 10, 28, 6, 0, 0, 0, time.UTC)
		currentDateTime = libtime.NewCurrentDateTime()
		currentDateTime.SetNow(libtime.DateTime(watcherStart))
		testTaskID = lib.TaskIdentifier("test-task-uuid-1234")
		testTask = lib.Task{
			TaskIdentifier: testTaskID,
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"assignee": "claude",
			},
			Content: lib.TaskContent("do the work"),
		}
		watcher = pkg.NewJobWatcher(
			fakeKubeClient,
			"test-ns",
			pkg.NewEventHandlerConfig(),
			taskStore,
			fakePublisher,
			nil,
			currentDateTime,
		)
	})

	makeJob := func(name string, taskID string, conditions ...batchv1.JobCondition) *batchv1.Job {
		labels := map[string]string{}
		if taskID != "" {
			labels["agent.benjamin-borbe.de/task-id"] = taskID
		}
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "test-ns",
				Labels:    labels,
			},
			Status: batchv1.JobStatus{
				Conditions: conditions,
			},
		}
		return job
	}

	failedCondition := func(msg string) batchv1.JobCondition {
		return batchv1.JobCondition{
			Type:    batchv1.JobFailed,
			Status:  corev1.ConditionTrue,
			Message: msg,
		}
	}

	succeededCondition := func() batchv1.JobCondition {
		return batchv1.JobCondition{
			Type:   batchv1.JobComplete,
			Status: corev1.ConditionTrue,
		}
	}

	Describe("HandleJob", func() {
		It("records the success time even when the task is not in the store", func() {
			completedAt := metav1.NewTime(time.Date(2026, 10, 21, 5, 55, 43, 0, time.UTC))
			job := makeJob("job-s", string(testTaskID), succeededCondition())
			job.Status.CompletionTime = &completedAt
			job.Annotations = map[string]string{pkg.TaskPhaseAnnotationKey: "execution"}

			watcher.HandleJob(ctx, job)

			succeeded, ok := taskStore.LastSucceeded(testTaskID)
			Expect(ok).To(BeTrue())
			Expect(succeeded.CompletedAt).To(Equal(completedAt.Time))
			Expect(succeeded.Phase).To(Equal("execution"))
		})

		It("records no success for a failed Job", func() {
			job := makeJob("job-f", string(testTaskID), failedCondition("OOMKilled"))
			taskStore.Store(testTaskID, testTask)

			watcher.HandleJob(ctx, job)

			_, ok := taskStore.LastSucceeded(testTaskID)
			Expect(ok).To(BeFalse())
		})

		It("publishes synthetic failure and deletes job on Failed state", func() {
			job := makeJob("job-1", string(testTaskID), failedCondition("OOMKilled"))
			_, err := fakeKubeClient.BatchV1().
				Jobs("test-ns").
				Create(ctx, job, metav1.CreateOptions{})
			Expect(err).To(BeNil())
			taskStore.Store(testTaskID, testTask)

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
			_, calledTask, calledJobName, calledReason := fakePublisher.PublishFailureArgsForCall(0)
			Expect(string(calledTask.TaskIdentifier)).To(Equal(string(testTaskID)))
			Expect(calledJobName).To(Equal("job-1"))
			Expect(calledReason).To(Equal("JobFailed"))

			_, err = fakeKubeClient.BatchV1().Jobs("test-ns").Get(ctx, "job-1", metav1.GetOptions{})
			Expect(err).To(BeNil())
		})

		It(
			"does NOT publish synthetic failure but DOES clear current_job for Succeeded job",
			func() {
				job := makeJob("job-2", string(testTaskID), succeededCondition())
				_, err := fakeKubeClient.BatchV1().
					Jobs("test-ns").
					Create(ctx, job, metav1.CreateOptions{})
				Expect(err).To(BeNil())
				taskStore.Store(testTaskID, testTask)

				watcher.HandleJob(ctx, job)

				// No synthetic failure — the agent already published its real result.
				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
				// But current_job must be cleared so the vault task does not carry a
				// stale job reference the sweeper can never see.
				Expect(fakePublisher.PublishClearCurrentJobCallCount()).To(Equal(1))
				_, calledTask, calledJobName := fakePublisher.PublishClearCurrentJobArgsForCall(0)
				Expect(string(calledTask.TaskIdentifier)).To(Equal(string(testTaskID)))
				Expect(calledJobName).To(Equal("job-2"))
				// task cleaned up from store
				_, ok := taskStore.Load(testTaskID)
				Expect(ok).To(BeFalse())

				_, err = fakeKubeClient.BatchV1().
					Jobs("test-ns").
					Get(ctx, "job-2", metav1.GetOptions{})
				Expect(err).To(BeNil())
			},
		)

		It(
			"skips synthetic failure and current_job clear for Succeeded job when task is not in store",
			func() {
				job := makeJob("job-3", string(testTaskID), succeededCondition())
				_, err := fakeKubeClient.BatchV1().
					Jobs("test-ns").
					Create(ctx, job, metav1.CreateOptions{})
				Expect(err).To(BeNil())
				// intentionally NOT storing task in taskStore

				watcher.HandleJob(ctx, job)

				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
				Expect(fakePublisher.PublishClearCurrentJobCallCount()).To(Equal(0))

				_, err = fakeKubeClient.BatchV1().
					Jobs("test-ns").
					Get(ctx, "job-3", metav1.GetOptions{})
				Expect(err).To(BeNil())
			},
		)

		It("does not evict a re-spawned task when a stale job's success is re-delivered", func() {
			// Regression for the prod observation 2026-09-04 on task 67120247:
			// job-1 succeeded + cleared + evicted, the task was re-dispatched and
			// respawned as job-2 (the store entry carries current_job=job-2), then
			// the informer's ~5 min re-delivery of job-1's success hit HandleJob.
			// The success path published nothing (deduped per job) but
			// unconditionally evicted the store entry tracking job-2 — so job-2's
			// own success later no-oped and current_job stayed set forever.
			taskWithCurrentJob := testTask
			taskWithCurrentJob.Frontmatter["current_job"] = "job-2"
			taskStore.Store(testTaskID, taskWithCurrentJob)
			// job-2 is the current, still-running job in the cluster — the store
			// entry must survive the stale job-1 re-delivery because the running
			// job's terminal path still needs it.
			runningJob2 := makeJob("job-2", string(testTaskID))
			_, err := fakeKubeClient.BatchV1().
				Jobs("test-ns").
				Create(ctx, runningJob2, metav1.CreateOptions{})
			Expect(err).To(BeNil())

			// 1. Stale re-delivery of job-1's success must NOT evict job-2's entry.
			watcher.HandleJob(ctx, makeJob("job-1", string(testTaskID), succeededCondition()))
			_, ok := taskStore.Load(testTaskID)
			Expect(ok).To(BeTrue(), "stale success re-delivery must not evict the re-spawned task")

			// 2. The current job's own success must still clear + evict.
			watcher.HandleJob(ctx, makeJob("job-2", string(testTaskID), succeededCondition()))
			Expect(fakePublisher.PublishClearCurrentJobCallCount()).To(Equal(2))
			_, ok = taskStore.Load(testTaskID)
			Expect(ok).To(BeFalse(), "current job's success evicts the store entry")
		})

		It(
			"evicts the stale store entry when the second success arrives in the late-clear ordering",
			func() {
				// Regression for the prod observation 2026-09-05 on task e79ada78
				// (v0.11.4): job-1 succeeded + cleared + evicted, the task respawned
				// as job-2, but the store entry was re-admitted with the STALE first-job
				// name (the second spawn's notification had not refreshed the
				// frontmatter). The informer re-delivered job-1's success ~60s late,
				// after job-2 spawned — it hit the publisher dedupe (event=clear_dedupe)
				// and tracksJob saw a match (store held job-1), evicting the entry.
				// When job-2's own success then arrived, tracksJob compared storedJob
				// (job-1) vs eventJob (job-2) and skipped — pinning the store entry to
				// the dead job-1 name so current_job never converged. The entry must
				// converge: the second success's clear fires AND the stale entry is
				// evicted (both jobs are terminal, no active job remains).
				taskWithStaleJob := testTask
				taskWithStaleJob.Frontmatter["current_job"] = "job-1"
				taskStore.Store(testTaskID, taskWithStaleJob)

				job1 := makeJob("job-1", string(testTaskID), succeededCondition())
				_, err := fakeKubeClient.BatchV1().
					Jobs("test-ns").
					Create(ctx, job1, metav1.CreateOptions{})
				Expect(err).To(BeNil())
				job2 := makeJob("job-2", string(testTaskID), succeededCondition())
				_, err = fakeKubeClient.BatchV1().
					Jobs("test-ns").
					Create(ctx, job2, metav1.CreateOptions{})
				Expect(err).To(BeNil())

				watcher.HandleJob(ctx, job2)

				// The second success's clear fires (published before the guard).
				Expect(fakePublisher.PublishClearCurrentJobCallCount()).To(Equal(1))
				_, calledTask, calledJobName := fakePublisher.PublishClearCurrentJobArgsForCall(0)
				Expect(string(calledTask.TaskIdentifier)).To(Equal(string(testTaskID)))
				Expect(calledJobName).To(Equal("job-2"))
				// And the stale entry must converge — evicted, not pinned to job-1.
				_, ok := taskStore.Load(testTaskID)
				Expect(
					ok,
				).To(BeFalse(), "stale store entry must be evicted so current_job converges")
			},
		)

		It("ignores jobs without task-id label", func() {
			job := makeJob("job-4", "", failedCondition("crash"))
			_, err := fakeKubeClient.BatchV1().
				Jobs("test-ns").
				Create(ctx, job, metav1.CreateOptions{})
			Expect(err).To(BeNil())

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
			// job should NOT be deleted
			_, err = fakeKubeClient.BatchV1().Jobs("test-ns").Get(ctx, "job-4", metav1.GetOptions{})
			Expect(err).To(BeNil())
		})

		It("keeps Failed job (TTL cleanup) when task is not in taskStore", func() {
			job := makeJob("job-5", string(testTaskID), failedCondition("evicted"))
			_, err := fakeKubeClient.BatchV1().
				Jobs("test-ns").
				Create(ctx, job, metav1.CreateOptions{})
			Expect(err).To(BeNil())
			// intentionally NOT storing task in taskStore

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))

			_, err = fakeKubeClient.BatchV1().Jobs("test-ns").Get(ctx, "job-5", metav1.GetOptions{})
			Expect(err).To(BeNil())
		})

		It("removes task from taskStore after handling terminal job", func() {
			job := makeJob("job-6", string(testTaskID), failedCondition("crash"))
			_, err := fakeKubeClient.BatchV1().
				Jobs("test-ns").
				Create(ctx, job, metav1.CreateOptions{})
			Expect(err).To(BeNil())
			taskStore.Store(testTaskID, testTask)

			watcher.HandleJob(ctx, job)

			_, ok := taskStore.Load(testTaskID)
			Expect(ok).To(BeFalse())
		})

		It("ignores jobs that are neither failed nor succeeded", func() {
			job := makeJob("job-8", string(testTaskID))
			_, err := fakeKubeClient.BatchV1().
				Jobs("test-ns").
				Create(ctx, job, metav1.CreateOptions{})
			Expect(err).To(BeNil())
			taskStore.Store(testTaskID, testTask)

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
			// job still exists (not terminal)
			_, err = fakeKubeClient.BatchV1().Jobs("test-ns").Get(ctx, "job-8", metav1.GetOptions{})
			Expect(err).To(BeNil())
		})

		It("uses the JobFailed literal when the condition has no reason", func() {
			job := makeJob("job-7", string(testTaskID), batchv1.JobCondition{
				Type:   batchv1.JobFailed,
				Status: corev1.ConditionTrue,
			})
			_, err := fakeKubeClient.BatchV1().
				Jobs("test-ns").
				Create(ctx, job, metav1.CreateOptions{})
			Expect(err).To(BeNil())
			taskStore.Store(testTaskID, testTask)

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
			_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
			Expect(calledReason).To(Equal("JobFailed"))
		})
	})

	Describe("HandleJob for a failed Job whose task is not in the store", func() {
		// Regression for the silent crashes of task ad12e9fb (2026-10-28, job
		// github-update-go-agent-ad12e9fb-20261028065801, pod_crash_no_stdout
		// ContainerStatusUnknown/137) and task 9371a3d2 (2026-10-27): the store
		// entry had already been evicted, so the failure was logged at V(3) and
		// dropped, leaving the record with a dead current_job and no ## Failure.
		failedAt := func(at time.Time) batchv1.JobCondition {
			return batchv1.JobCondition{
				Type:               batchv1.JobFailed,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(at),
			}
		}

		createJob := func(job *batchv1.Job) {
			_, err := fakeKubeClient.BatchV1().
				Jobs("test-ns").
				Create(ctx, job, metav1.CreateOptions{})
			Expect(err).To(BeNil())
		}

		It("publishes the failure when the Job failed after the watcher started", func() {
			job := makeJob("job-fresh", string(testTaskID), failedAt(watcherStart.Add(time.Minute)))
			job.Annotations = map[string]string{pkg.TaskPhaseAnnotationKey: "execution"}
			createJob(job)
			currentDateTime.SetNow(libtime.DateTime(watcherStart.Add(2 * time.Minute)))

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishStoreMissFailureCallCount()).To(Equal(1))
			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
			_, calledTask, calledJobName, calledReason := fakePublisher.PublishStoreMissFailureArgsForCall(
				0,
			)
			Expect(calledTask.TaskIdentifier).To(Equal(testTaskID))
			Expect(calledJobName).To(Equal("job-fresh"))
			Expect(calledReason).To(Equal("JobFailed"))
			// The phase keys the re-drive streak and reason refs exactly as the
			// store-hit path would.
			Expect(calledTask.Frontmatter["phase"]).To(Equal("execution"))
			// The real max_triggers is unknown here; carrying the key keeps the
			// publisher from writing its default over an operator-set cap.
			_, hasMaxTriggers := calledTask.Frontmatter["max_triggers"]
			Expect(hasMaxTriggers).To(BeTrue())
		})

		It("does not publish when the Job failed before the watcher started", func() {
			job := makeJob(
				"job-stale",
				string(testTaskID),
				failedAt(watcherStart.Add(-time.Minute)),
			)
			createJob(job)
			currentDateTime.SetNow(libtime.DateTime(watcherStart.Add(2 * time.Minute)))

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishStoreMissFailureCallCount()).To(Equal(0))
		})

		It("does not publish once the failure is older than the publish window", func() {
			job := makeJob("job-old", string(testTaskID), failedAt(watcherStart.Add(time.Minute)))
			createJob(job)
			currentDateTime.SetNow(libtime.DateTime(watcherStart.Add(2 * time.Hour)))

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishStoreMissFailureCallCount()).To(Equal(0))
		})

		It("does not publish while another Job for the task is still active", func() {
			job := makeJob("job-fresh", string(testTaskID), failedAt(watcherStart.Add(time.Minute)))
			createJob(job)
			createJob(makeJob("job-newer", string(testTaskID)))
			currentDateTime.SetNow(libtime.DateTime(watcherStart.Add(2 * time.Minute)))

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishStoreMissFailureCallCount()).To(Equal(0))
		})

		It("keeps the store-hit path unchanged for a task in the store", func() {
			job := makeJob("job-hit", string(testTaskID), failedAt(watcherStart.Add(time.Minute)))
			createJob(job)
			taskStore.Store(testTaskID, testTask)
			currentDateTime.SetNow(libtime.DateTime(watcherStart.Add(2 * time.Minute)))

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
			_, calledTask, calledJobName, calledReason := fakePublisher.PublishFailureArgsForCall(0)
			Expect(calledTask).To(Equal(testTask))
			Expect(calledJobName).To(Equal("job-hit"))
			Expect(calledReason).To(Equal("JobFailed"))
			_, ok := taskStore.Load(testTaskID)
			Expect(ok).To(BeFalse())
		})

		It("does not publish a failure for a succeeded Job with an empty store", func() {
			completedAt := metav1.NewTime(watcherStart.Add(time.Minute))
			job := makeJob("job-ok", string(testTaskID), succeededCondition())
			job.Status.CompletionTime = &completedAt
			createJob(job)
			currentDateTime.SetNow(libtime.DateTime(watcherStart.Add(2 * time.Minute)))

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
		})
	})

	Describe("HandleJob with DeadlineExceeded", func() {
		It("maps DeadlineExceeded job condition to deadline_exceeded", func() {
			job := makeJob("job-deadline", string(testTaskID), batchv1.JobCondition{
				Type:    batchv1.JobFailed,
				Status:  corev1.ConditionTrue,
				Reason:  "DeadlineExceeded",
				Message: "Job was active longer than specified deadline",
			})
			_, err := fakeKubeClient.BatchV1().
				Jobs("test-ns").
				Create(ctx, job, metav1.CreateOptions{})
			Expect(err).To(BeNil())
			taskStore.Store(testTaskID, testTask)

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
			_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
			Expect(calledReason).To(Equal("deadline_exceeded"))
		})

		It(
			"maps an unresolvable-pod BackoffLimitExceeded to its condition plus the unresolved marker",
			func() {
				job := makeJob("job-backoff", string(testTaskID), batchv1.JobCondition{
					Type:    batchv1.JobFailed,
					Status:  corev1.ConditionTrue,
					Reason:  "BackoffLimitExceeded",
					Message: "Job failed due to backoff limit",
				})
				_, err := fakeKubeClient.BatchV1().
					Jobs("test-ns").
					Create(ctx, job, metav1.CreateOptions{})
				Expect(err).To(BeNil())
				taskStore.Store(testTaskID, testTask)

				watcher.HandleJob(ctx, job)

				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
				_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
				Expect(calledReason).To(Equal("BackoffLimitExceeded (pod-unresolved)"))
			},
		)
	})

	Describe("jobFailureReason mapping", func() {
		It("returns deadline_exceeded for DeadlineExceeded", func() {
			job := makeJob("j", string(testTaskID), batchv1.JobCondition{
				Type:   batchv1.JobFailed,
				Status: corev1.ConditionTrue,
				Reason: "DeadlineExceeded",
			})
			Expect(pkg.JobFailureReason(job)).To(Equal(pkg.ZombieReasonDeadlineExceeded))
		})

		It("returns pod_crash_no_stdout for BackoffLimitExceeded", func() {
			job := makeJob("j", string(testTaskID), batchv1.JobCondition{
				Type:   batchv1.JobFailed,
				Status: corev1.ConditionTrue,
				Reason: "BackoffLimitExceeded",
			})
			Expect(pkg.JobFailureReason(job)).To(Equal(pkg.ZombieReasonPodCrashNoStdout))
		})

		It("returns pod_crash_no_stdout for other Failed condition reasons", func() {
			job := makeJob("j", string(testTaskID), batchv1.JobCondition{
				Type:   batchv1.JobFailed,
				Status: corev1.ConditionTrue,
				Reason: "",
			})
			Expect(pkg.JobFailureReason(job)).To(Equal(pkg.ZombieReasonPodCrashNoStdout))
		})
	})

	Describe("HandlePod", func() {
		makePod := func(name string, taskID string, phase corev1.PodPhase, containerStatuses []corev1.ContainerStatus, ownerRefs []metav1.OwnerReference, podStatusReason string) *corev1.Pod {
			labels := map[string]string{}
			if taskID != "" {
				labels["agent.benjamin-borbe.de/task-id"] = taskID
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:            name,
					Namespace:       "test-ns",
					Labels:          labels,
					OwnerReferences: ownerRefs,
				},
				Status: corev1.PodStatus{
					Phase: phase,
				},
			}
			if len(containerStatuses) > 0 {
				pod.Status.ContainerStatuses = containerStatuses
			}
			if podStatusReason != "" {
				pod.Status.Reason = podStatusReason
			}
			return pod
		}

		makeJobOwnerRef := func(name string) []metav1.OwnerReference {
			return []metav1.OwnerReference{
				{
					Kind: "Job",
					Name: name,
				},
			}
		}

		It("publishes failure for ImagePullBackOff container", func() {
			pod := makePod(
				"pod-imgpull",
				string(testTaskID),
				corev1.PodPending,
				[]corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "ImagePullBackOff",
							},
						},
					},
				},
				makeJobOwnerRef("my-job"),
				"",
			)
			taskStore.Store(testTaskID, testTask)

			watcher.HandlePod(ctx, pod)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
			_, _, calledJobName, calledReason := fakePublisher.PublishFailureArgsForCall(0)
			Expect(calledJobName).To(Equal("my-job"))
			Expect(calledReason).To(Equal("image_pull_backoff"))
		})

		It("publishes failure for ErrImagePull container", func() {
			pod := makePod(
				"pod-errimg",
				string(testTaskID),
				corev1.PodPending,
				[]corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "ErrImagePull",
							},
						},
					},
				},
				makeJobOwnerRef("my-job"),
				"",
			)
			taskStore.Store(testTaskID, testTask)

			watcher.HandlePod(ctx, pod)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
			_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
			Expect(calledReason).To(Equal("image_pull_backoff"))
		})

		It("publishes failure for CrashLoopBackOff container", func() {
			pod := makePod(
				"pod-crashloop",
				string(testTaskID),
				corev1.PodPending,
				[]corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "CrashLoopBackOff",
							},
						},
					},
				},
				makeJobOwnerRef("my-job"),
				"",
			)
			taskStore.Store(testTaskID, testTask)

			watcher.HandlePod(ctx, pod)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
			_, _, calledJobName, calledReason := fakePublisher.PublishFailureArgsForCall(0)
			Expect(calledJobName).To(Equal("my-job"))
			Expect(calledReason).To(Equal("pod_crash_no_stdout"))
		})

		It("publishes failure for Evicted pod", func() {
			pod := makePod(
				"pod-evicted",
				string(testTaskID),
				corev1.PodPending,
				nil,
				makeJobOwnerRef("my-job"),
				"Evicted",
			)
			taskStore.Store(testTaskID, testTask)

			watcher.HandlePod(ctx, pod)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
			_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
			Expect(calledReason).To(Equal("pod_evicted"))
		})

		It("publishes failure for PodFailed with non-zero exit code", func() {
			pod := makePod(
				"pod-crash",
				string(testTaskID),
				corev1.PodFailed,
				[]corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								ExitCode: 137,
							},
						},
					},
				},
				makeJobOwnerRef("my-job"),
				"",
			)
			taskStore.Store(testTaskID, testTask)

			watcher.HandlePod(ctx, pod)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
			_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
			Expect(calledReason).To(Equal("pod_crash_no_stdout"))
		})

		It("does NOT publish failure for healthy Running pod", func() {
			pod := makePod(
				"pod-running",
				string(testTaskID),
				corev1.PodRunning,
				nil,
				makeJobOwnerRef("my-job"),
				"",
			)
			taskStore.Store(testTaskID, testTask)

			watcher.HandlePod(ctx, pod)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
		})

		It("does NOT publish failure when task is not in store", func() {
			pod := makePod(
				"pod-imgpull",
				string(testTaskID),
				corev1.PodPending,
				[]corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "ImagePullBackOff",
							},
						},
					},
				},
				makeJobOwnerRef("my-job"),
				"",
			)

			watcher.HandlePod(ctx, pod)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
		})

		It("does NOT publish failure when pod has no Job ownerRef", func() {
			pod := makePod(
				"pod-noowner",
				string(testTaskID),
				corev1.PodPending,
				[]corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "ImagePullBackOff",
							},
						},
					},
				},
				nil,
				"",
			)
			taskStore.Store(testTaskID, testTask)

			watcher.HandlePod(ctx, pod)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
		})
	})
})
