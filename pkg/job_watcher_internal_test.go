// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	corev1listers "k8s.io/client-go/listers/core/v1"
)

var _ = Describe("jobWatcher backoff-limit pod detail", func() {
	var (
		ctx           context.Context
		fakePublisher *fakeResultPublisher
		taskStore     *TaskStore
		watcher       JobWatcher
		testTask      lib.Task
		testTaskID    lib.TaskIdentifier
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakePublisher = &fakeResultPublisher{}
		taskStore = NewTaskStore()
		testTaskID = lib.TaskIdentifier("test-task-uuid-1234")
		testTask = lib.Task{
			TaskIdentifier: testTaskID,
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"assignee": "claude",
			},
			Content: lib.TaskContent("do the work"),
		}
		watcher = NewJobWatcher(fake.NewSimpleClientset(), "test-ns", taskStore, fakePublisher)
	})

	makeJob := func(name string, taskID string, conditions ...batchv1.JobCondition) *batchv1.Job {
		labels := map[string]string{}
		if taskID != "" {
			labels["agent.benjamin-borbe.de/task-id"] = taskID
		}
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "test-ns",
				Labels:    labels,
			},
			Status: batchv1.JobStatus{
				Conditions: conditions,
			},
		}
	}

	backoffCondition := func() batchv1.JobCondition {
		return batchv1.JobCondition{
			Type:    batchv1.JobFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "BackoffLimitExceeded",
			Message: "Job has reached the specified backoff limit",
		}
	}

	makeOwnedTerminatedPod := func(name, jobName, taskID, terminatedReason string, exitCode int32, createdAt string) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "test-ns",
				Labels: map[string]string{
					"agent.benjamin-borbe.de/task-id": taskID,
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						Kind: "Job",
						Name: jobName,
					},
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodFailed,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								Reason:   terminatedReason,
								ExitCode: exitCode,
							},
						},
					},
				},
			},
		}
		if createdAt != "" {
			t, err := time.Parse(time.RFC3339, createdAt)
			if err != nil {
				panic(err)
			}
			pod.CreationTimestamp = metav1.NewTime(t)
		}
		return pod
	}

	Describe("HandleJob with resolvable pod lister", func() {
		It("publishes pod_oom_killed with OOMKilled/137 detail for an OOMKilled pod", func() {
			taskStore.Store(testTaskID, testTask)
			job := makeJob("job-oom", string(testTaskID), backoffCondition())
			setPodLister(
				watcher,
				makeOwnedTerminatedPod(
					"pod-oom",
					"job-oom",
					string(testTaskID),
					"OOMKilled",
					137,
					"2026-08-16T00:00:00Z",
				),
			)

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
			_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
			Expect(calledReason).To(Equal("pod_oom_killed (OOMKilled/137)"))
			Expect(calledReason).NotTo(ContainSubstring("deadline_exceeded"))
		})

		It(
			"publishes pod_crash_no_stdout with Error/1 detail for a non-OOM terminated pod",
			func() {
				taskStore.Store(testTaskID, testTask)
				job := makeJob("job-error", string(testTaskID), backoffCondition())
				setPodLister(
					watcher,
					makeOwnedTerminatedPod(
						"pod-error",
						"job-error",
						string(testTaskID),
						"Error",
						1,
						"2026-08-16T00:00:00Z",
					),
				)

				watcher.HandleJob(ctx, job)

				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
				_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
				Expect(calledReason).To(Equal("pod_crash_no_stdout (Error/1)"))
				Expect(calledReason).NotTo(ContainSubstring("deadline_exceeded"))
				Expect(calledReason).NotTo(ContainSubstring("pod_oom_killed"))
			},
		)

		It(
			"uses the terminal (most recently created) pod's state when a job owns multiple pods",
			func() {
				taskStore.Store(testTaskID, testTask)
				job := makeJob("job-multi", string(testTaskID), backoffCondition())
				setPodLister(
					watcher,
					makeOwnedTerminatedPod(
						"pod-1",
						"job-multi",
						string(testTaskID),
						"Error",
						1,
						"2026-08-16T00:00:00Z",
					),
					makeOwnedTerminatedPod(
						"pod-2",
						"job-multi",
						string(testTaskID),
						"OOMKilled",
						137,
						"2026-08-16T00:05:00Z",
					),
				)

				watcher.HandleJob(ctx, job)

				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
				_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
				Expect(calledReason).To(Equal("pod_oom_killed (OOMKilled/137)"))
			},
		)

		It("publishes the bare pod_crash_no_stdout when no pod is resolvable", func() {
			taskStore.Store(testTaskID, testTask)
			job := makeJob("job-empty", string(testTaskID), backoffCondition())
			setPodLister(watcher)

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
			_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
			Expect(calledReason).To(Equal("pod_crash_no_stdout"))
		})

		It("publishes the bare pod_crash_no_stdout when the pod lister lookup fails", func() {
			taskStore.Store(testTaskID, testTask)
			job := makeJob("job-listererr", string(testTaskID), backoffCondition())
			jw, ok := watcher.(*jobWatcher)
			Expect(ok).To(BeTrue())
			var lister corev1listers.PodLister = errorPodLister{}
			jw.podLister.Store(&lister)

			watcher.HandleJob(ctx, job)

			Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
			_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
			Expect(calledReason).To(Equal("pod_crash_no_stdout"))
		})

		It(
			"publishes the bare pod_crash_no_stdout when the owned pod has no non-zero terminated container",
			func() {
				taskStore.Store(testTaskID, testTask)
				job := makeJob("job-running", string(testTaskID), backoffCondition())
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod-running",
						Namespace: "test-ns",
						Labels: map[string]string{
							"agent.benjamin-borbe.de/task-id": string(testTaskID),
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								Kind: "Job",
								Name: "job-running",
							},
						},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
					},
				}
				setPodLister(watcher, pod)

				watcher.HandleJob(ctx, job)

				Expect(fakePublisher.PublishFailureCallCount()).To(Equal(1))
				_, _, _, calledReason := fakePublisher.PublishFailureArgsForCall(0)
				Expect(calledReason).To(Equal("pod_crash_no_stdout"))
			},
		)
	})
})

// newTestPodLister builds a corev1listers.PodLister backed by a fake informer
// indexer seeded with the given pods, mirroring the pattern in
// zombie_sweeper_test.go.
func newTestPodLister(pods ...*corev1.Pod) corev1listers.PodLister {
	fakeClient := fake.NewSimpleClientset()
	informerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(
		fakeClient,
		0,
		k8sinformers.WithNamespace("test-ns"),
	)
	for _, pod := range pods {
		_ = informerFactory.Core().V1().Pods().Informer().GetIndexer().Add(pod)
	}
	return informerFactory.Core().V1().Pods().Lister()
}

// setPodLister stores the given pods into the watcher's pod lister without
// running the informer (Run is never called in tests, so PodLister would
// otherwise return nil).
func setPodLister(w JobWatcher, pods ...*corev1.Pod) {
	lister := newTestPodLister(pods...)
	jw, ok := w.(*jobWatcher)
	if !ok {
		panic("setPodLister: watcher is not a *jobWatcher")
	}
	jw.podLister.Store(&lister)
}

// errorPodLister is a corev1listers.PodLister stub whose namespace List fails,
// exercising the lister-lookup error branch of terminalPodDetail.
type errorPodLister struct{}

func (errorPodLister) List(_ labels.Selector) ([]*corev1.Pod, error) {
	return nil, nil
}

func (errorPodLister) Pods(_ string) corev1listers.PodNamespaceLister {
	return errorPodNamespaceLister{}
}

type errorPodNamespaceLister struct{}

func (errorPodNamespaceLister) List(_ labels.Selector) ([]*corev1.Pod, error) {
	return nil, context.Canceled
}

func (errorPodNamespaceLister) Get(_ string) (*corev1.Pod, error) {
	return nil, nil
}

// fakeResultPublisher is a test-local recording stand-in for the counterfeiter
// FakeResultPublisher, which cannot be imported here: the mocks package imports
// pkg, and an internal test file (package pkg) importing it would create an
// import cycle. It exposes the same PublishFailure call-recording API the
// counterfeiter fake does, so the spec assertions are unchanged.
type fakeResultPublisher struct {
	publishFailureCalls  []resultPublisherCall
	clearCurrentJobCalls []clearCurrentJobCall
}

type clearCurrentJobCall struct {
	ctx     context.Context
	task    lib.Task
	jobName string
}

type resultPublisherCall struct {
	ctx     context.Context
	task    lib.Task
	jobName string
	reason  string
}

func (f *fakeResultPublisher) PublishSpawnNotification(
	ctx context.Context,
	task lib.Task,
	jobName string,
) error {
	return nil
}

func (f *fakeResultPublisher) PublishFailure(
	ctx context.Context,
	task lib.Task,
	jobName string,
	reason string,
) error {
	f.publishFailureCalls = append(f.publishFailureCalls, resultPublisherCall{
		ctx:     ctx,
		task:    task,
		jobName: jobName,
		reason:  reason,
	})
	return nil
}

func (f *fakeResultPublisher) PublishClearCurrentJob(
	ctx context.Context,
	task lib.Task,
	jobName string,
) error {
	f.clearCurrentJobCalls = append(f.clearCurrentJobCalls, clearCurrentJobCall{
		ctx:     ctx,
		task:    task,
		jobName: jobName,
	})
	return nil
}

func (f *fakeResultPublisher) PublishIncrementTriggerCount(
	ctx context.Context,
	task lib.Task,
) error {
	return nil
}

func (f *fakeResultPublisher) PublishSetTriggerScope(
	ctx context.Context,
	task lib.Task,
	scope string,
	count int,
) error {
	return nil
}

func (f *fakeResultPublisher) PublishTypeMismatchFailure(
	ctx context.Context,
	task lib.Task,
	reason string,
) error {
	return nil
}

func (f *fakeResultPublisher) PublishRaw(
	ctx context.Context,
	operation base.CommandOperation,
	payload interface{},
) error {
	return nil
}

func (f *fakeResultPublisher) PublishFailureCallCount() int {
	return len(f.publishFailureCalls)
}

func (f *fakeResultPublisher) PublishFailureArgsForCall(
	i int,
) (context.Context, lib.Task, string, string) {
	c := f.publishFailureCalls[i]
	return c.ctx, c.task, c.jobName, c.reason
}

func (f *fakeResultPublisher) PublishClearCurrentJobCallCount() int {
	return len(f.clearCurrentJobCalls)
}

func (f *fakeResultPublisher) PublishClearCurrentJobArgsForCall(
	i int,
) (context.Context, lib.Task, string) {
	c := f.clearCurrentJobCalls[i]
	return c.ctx, c.task, c.jobName
}
