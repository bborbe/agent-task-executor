// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"sync"
	"time"

	lib "github.com/bborbe/agent"
	libk8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// This is the SC1 reproduction for the restart-bound success-record hole: a Job
// that completed before this pod started arrives in its namespace informer's
// initial LIST, whose handler calls run while HasSynced() is still false for
// every object but the last.
//
// On the pre-fix build the per-namespace sync barrier dropped that delivery, so
// neither sink held a record and the spawn funnel read "no success" and
// re-spawned (measured on nuke dev 2026-10-28: informer start 19:50:47.510,
// first handling 19:56:15.335, re-spawn 19:52:51). The fix handles a succeeded
// Job ahead of the barrier, so both sinks hold the record.
var _ = Describe("jobWatcher succeeded Job before the namespace caches sync", func() {
	var (
		ctx         context.Context
		watcher     *jobWatcher
		releaser    *recordingClaimReleaser
		completedAt time.Time
	)

	BeforeEach(func() {
		ctx = context.Background()
		releaser = &recordingClaimReleaser{}
		provider := &namespaceTestProvider{}
		provider.setItems(
			configNamed("a", "ns-a"),
			configNamed("b", "ns-b"),
		)
		watched, ok := NewJobWatcher(
			fake.NewClientset(),
			"dev",
			provider,
			NewTaskStore(),
			&fakeResultPublisher{},
			releaser,
			libtime.NewCurrentDateTime(),
		).(*jobWatcher)
		Expect(ok).To(BeTrue())
		watcher = watched
		// ns-a stays unsynced — the state client-go puts every object of an
		// initial LIST in except the last. ns-b is the synced control.
		watcher.newNamespaceInformer = func(
			_ kubernetes.Interface,
			namespace libk8s.Namespace,
		) (jobWatcherNamespaceInformer, error) {
			return &fakeNamespaceInformer{synced: namespace == "ns-b"}, nil
		}
		Expect(watcher.reconcileNamespaceSet(ctx)).To(Succeed())
		completedAt = time.Date(2026, 10, 28, 19, 50, 5, 0, time.UTC)
	})

	// expectRecorded asserts both sinks hold the record for taskID, which is
	// what the fix makes true for an unsynced delivery and what the pre-fix
	// build fails.
	expectRecorded := func(taskID lib.TaskIdentifier) {
		_, found := watcher.taskStore.LastSucceeded(taskID)
		Expect(found).To(BeTrue(), "in-memory success record missing for task %s", taskID)

		records := releaser.successRecords()
		Expect(records).To(HaveLen(1))
		Expect(records[0].taskID).To(Equal(taskID))
		Expect(records[0].phase).To(Equal("execution"))
		Expect(records[0].completedAt).To(Equal(completedAt))
	}

	It("records the success when the Job is delivered before its namespace synced", func() {
		taskID := lib.TaskIdentifier("unsynced-task")
		watcher.HandleJob(ctx, succeededJob("job-unsynced", string(taskID), "ns-a", completedAt))

		expectRecorded(taskID)
	})

	It("records the success for the synced control too", func() {
		taskID := lib.TaskIdentifier("synced-task")
		watcher.HandleJob(ctx, succeededJob("job-synced", string(taskID), "ns-b", completedAt))

		expectRecorded(taskID)
	})
})

// recordingClaimReleaser records the shared success writes the watcher delegates
// to the ClaimReleaser. The deployed implementation records the success and holds
// the claim; this test only reads the record.
//
// Hand-rolled rather than counterfeiter's FakeClaimReleaser: this is an internal
// test (package pkg) and it drives unexported watcher state, while mocks/ carries
// `var _ pkg.ClaimReleaser` and so imports this package — the two together are an
// import cycle. Every internal test in pkg/ follows the same rule; the generated
// fakes are for the pkg_test external suite.
type recordingClaimReleaser struct {
	mu       sync.Mutex
	records  []recordedSuccess
	releases int
}

type recordedSuccess struct {
	taskID      lib.TaskIdentifier
	phase       string
	completedAt time.Time
}

func (r *recordingClaimReleaser) ReleaseTaskClaim(context.Context, lib.TaskIdentifier) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.releases++
}

func (r *recordingClaimReleaser) ReleaseTaskClaimAfterSuccess(
	_ context.Context,
	taskID lib.TaskIdentifier,
	phase string,
	completedAt time.Time,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, recordedSuccess{
		taskID:      taskID,
		phase:       phase,
		completedAt: completedAt,
	})
}

func (r *recordingClaimReleaser) successRecords() []recordedSuccess {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedSuccess, len(r.records))
	copy(out, r.records)
	return out
}

// succeededJob builds a terminal succeeded Job as the Job informer delivers it.
func succeededJob(name, taskID, namespace string, completedAt time.Time) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				jobWatcherTaskIDLabel: taskID,
			},
			Annotations: map[string]string{
				TaskPhaseAnnotationKey: "execution",
			},
		},
		Status: batchv1.JobStatus{
			CompletionTime: &metav1.Time{Time: completedAt},
			Conditions: []batchv1.JobCondition{
				{
					Type:   batchv1.JobComplete,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
}
