// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"

	lib "github.com/bborbe/agent/lib"
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
		ctx            context.Context
		fakePublisher  *mocks.FakeResultPublisher
		taskStore      *pkg.TaskStore
		fakeKubeClient *fake.Clientset
		watcher        pkg.JobWatcher
		testTask       lib.Task
		testTaskID     lib.TaskIdentifier
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakePublisher = &mocks.FakeResultPublisher{}
		taskStore = pkg.NewTaskStore()
		fakeKubeClient = fake.NewClientset()
		testTaskID = lib.TaskIdentifier("test-task-uuid-1234")
		testTask = lib.Task{
			TaskIdentifier: testTaskID,
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"assignee": "claude",
			},
			Content: lib.TaskContent("do the work"),
		}
		watcher = pkg.NewJobWatcher(fakeKubeClient, "test-ns", taskStore, fakePublisher)
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
			Expect(calledReason).To(Equal("pod_crash_no_stdout"))

			_, err = fakeKubeClient.BatchV1().Jobs("test-ns").Get(ctx, "job-1", metav1.GetOptions{})
			Expect(err).To(BeNil())
		})

		It("does NOT publish synthetic failure for Succeeded job (trusts agent publish)", func() {
			job := makeJob("job-2", string(testTaskID), succeededCondition())
			_, err := fakeKubeClient.BatchV1().
				Jobs("test-ns").
				Create(ctx, job, metav1.CreateOptions{})
			Expect(err).To(BeNil())
			taskStore.Store(testTaskID, testTask)

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))
			// task cleaned up from store
			_, ok := taskStore.Load(testTaskID)
			Expect(ok).To(BeFalse())

			_, err = fakeKubeClient.BatchV1().Jobs("test-ns").Get(ctx, "job-2", metav1.GetOptions{})
			Expect(err).To(BeNil())
		})

		It("skips synthetic failure for Succeeded job when task is not in store", func() {
			job := makeJob("job-3", string(testTaskID), succeededCondition())
			_, err := fakeKubeClient.BatchV1().
				Jobs("test-ns").
				Create(ctx, job, metav1.CreateOptions{})
			Expect(err).To(BeNil())
			// intentionally NOT storing task in taskStore

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(0))

			_, err = fakeKubeClient.BatchV1().Jobs("test-ns").Get(ctx, "job-3", metav1.GetOptions{})
			Expect(err).To(BeNil())
		})

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

		It("uses pod_crash_no_stdout when condition has no message", func() {
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
			Expect(calledReason).To(Equal("pod_crash_no_stdout"))
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

		It("maps BackoffLimitExceeded job condition to deadline_exceeded", func() {
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
			Expect(calledReason).To(Equal("deadline_exceeded"))
		})
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

		It("returns deadline_exceeded for BackoffLimitExceeded", func() {
			job := makeJob("j", string(testTaskID), batchv1.JobCondition{
				Type:   batchv1.JobFailed,
				Status: corev1.ConditionTrue,
				Reason: "BackoffLimitExceeded",
			})
			Expect(pkg.JobFailureReason(job)).To(Equal(pkg.ZombieReasonDeadlineExceeded))
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
