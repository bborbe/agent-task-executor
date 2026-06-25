// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build envtest

package envtest_test

import (
	"context"
	"os"
	"testing"
	"time"

	libk8s "github.com/bborbe/k8s"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	lib "github.com/bborbe/agent/lib"
	mocks "github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// controller-runtime v0.21.x is compatible with k8s.io/client-go v0.36.x
// (client-go 0.36 ships with Kubernetes 1.36; controller-runtime 0.21.x
// supports Kubernetes 1.21–1.31; envtest binaries are pinned to 1.31.0).

func TestEnvtest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "executor envtest suite")
}

var _ = BeforeSuite(func() {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if os.Getenv("ENVTEST_REQUIRED") == "1" {
			Fail(
				"KUBEBUILDER_ASSETS not set but ENVTEST_REQUIRED=1; envtest binaries must be available under precommit",
			)
		}
		Skip("KUBEBUILDER_ASSETS not set; run via `make test-envtest` or `make precommit`")
	}
})

var _ = Describe("JobWatcher (envtest)", func() {
	var (
		testEnv    *envtest.Environment
		cfg        *rest.Config
		kubeClient kubernetes.Interface
		ctx        context.Context
		cancel     context.CancelFunc
	)

	BeforeEach(func() {
		testEnv = &envtest.Environment{}
		var err error
		cfg, err = testEnv.Start()
		Expect(err).NotTo(HaveOccurred())
		kubeClient, err = kubernetes.NewForConfig(cfg)
		Expect(err).NotTo(HaveOccurred())
		ctx, cancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		cancel()
		Expect(testEnv.Stop()).To(Succeed())
	})

	It("classifies ImagePullBackOff and publishes one failure within the bound", func() {
		ns := "default"
		taskID := lib.TaskIdentifier("envtest-task-1")
		jobName := "envtest-job-1"
		publisher := &mocks.FakeResultPublisher{}
		store := pkg.NewTaskStore()
		store.Store(taskID, lib.Task{
			TaskIdentifier: taskID,
			Frontmatter: lib.TaskFrontmatter{
				"current_job": jobName,
				"assignee":    "envtest-agent",
			},
		})
		watcher := pkg.NewJobWatcher(kubeClient, libk8s.Namespace(ns), store, publisher)

		runErrCh := make(chan error, 1)
		go func() { runErrCh <- watcher.Run(ctx) }()

		// Create a Pod with the task-id label and a bogus image. envtest does
		// not run a kubelet, so we inject the ImagePullBackOff status ourselves
		// via the Status subresource; the informer sees the update the same way
		// it would in a real cluster.
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "envtest-pod-1",
				Namespace: ns,
				Labels: map[string]string{
					"agent.benjamin-borbe.de/task-id": string(taskID),
				},
				OwnerReferences: []metav1.OwnerReference{
					{APIVersion: "batch/v1", Kind: "Job", Name: jobName, UID: "fake-job-uid"},
				},
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{
					{Name: "agent", Image: "docker.example.com/does-not-exist:envtest"},
				},
			},
		}
		_, err := kubeClient.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
		Expect(
			err,
		).NotTo(HaveOccurred(), "if Create returns 422, add required defaults; do not silently catch")

		// Status subresource update flow — 4 steps to avoid ResourceVersion races
		// and default-mutator overwrites:
		//   1. Get the canonical Pod (fresh ResourceVersion).
		//   2. Mutate Status on the fetched object.
		//   3. UpdateStatus with the fetched object.
		//   4. Get again to confirm the status survived.
		// Step 1: Get
		fetched, err := kubeClient.CoreV1().Pods(ns).Get(ctx, "envtest-pod-1", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		// Step 2: mutate Status on the freshly-fetched object
		fetched.Status.Phase = corev1.PodPending
		fetched.Status.ContainerStatuses = []corev1.ContainerStatus{
			{
				Name: "agent",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason:  "ImagePullBackOff",
						Message: "Back-off pulling image",
					},
				},
			},
		}
		// Step 3: UpdateStatus
		_, err = kubeClient.CoreV1().Pods(ns).UpdateStatus(ctx, fetched, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())
		// Step 4: Get to confirm the status survived
		confirmed, err := kubeClient.CoreV1().
			Pods(ns).
			Get(ctx, "envtest-pod-1", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(confirmed.Status.ContainerStatuses).To(HaveLen(1))
		Expect(confirmed.Status.ContainerStatuses[0].State.Waiting).NotTo(BeNil())
		Expect(
			confirmed.Status.ContainerStatuses[0].State.Waiting.Reason,
		).To(Equal("ImagePullBackOff"))

		// Acceptance bound: 2 * zombieSweeperIntervalSeconds = 2 * 60s = 120s.
		// In practice the informer reacts in well under a second once the
		// status update lands; we use a generous wait with polling to stay
		// well inside the bound while keeping the test fast.
		Eventually(publisher.PublishFailureCallCount, 30*time.Second, 100*time.Millisecond).
			Should(Equal(1), "expected one PublishFailure call within bound")

		// Confirm "exactly one" — Eventually passes at the FIRST observation of 1;
		// Consistently verifies no second call lands over a short follow-up window.
		Consistently(publisher.PublishFailureCallCount, 2*time.Second, 200*time.Millisecond).
			Should(Equal(1), "expected exactly one PublishFailure call (no duplicates)")

		_, _, gotJobName, gotReason := publisher.PublishFailureArgsForCall(0)
		Expect(gotJobName).To(Equal(jobName))
		Expect(gotReason).To(Equal(string(pkg.ZombieReasonImagePullBackOff)))
	})
})
