// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	libk8s "github.com/bborbe/k8s"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// testJob returns a Job carrying taskID's label with the given conditions.
func testJob(name, taskID string, conditions ...batchv1.JobCondition) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "test-ns",
			Labels:    map[string]string{claimTaskIDLabel: taskID},
		},
		Status: batchv1.JobStatus{Conditions: conditions},
	}
}

func TestKubeJobControlIsJobRunning(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		jobs []*batchv1.Job
		want bool
	}{
		{name: "no job", jobs: nil, want: false},
		{
			name: "a running job",
			jobs: []*batchv1.Job{testJob("job-1", "task-1")},
			want: true,
		},
		{
			name: "a succeeded job",
			jobs: []*batchv1.Job{testJob("job-1", "task-1", batchv1.JobCondition{
				Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
			})},
			want: false,
		},
		{
			name: "a failed job",
			jobs: []*batchv1.Job{testJob("job-1", "task-1", batchv1.JobCondition{
				Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
			})},
			want: false,
		},
		{
			name: "another task's running job does not count",
			jobs: []*batchv1.Job{testJob("job-1", "task-2")},
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := fake.NewClientset()
			for _, job := range test.jobs {
				if _, err := client.BatchV1().Jobs("test-ns").Create(
					ctx, job, metav1.CreateOptions{},
				); err != nil {
					t.Fatalf("seed job: %v", err)
				}
			}

			got, err := NewKubeJobControl(client, "test-ns", nil, nil).IsJobRunning(ctx, "task-1")
			if err != nil {
				t.Fatalf("IsJobRunning: %v", err)
			}
			if got != test.want {
				t.Fatalf("IsJobRunning = %v, want %v", got, test.want)
			}
		})
	}
}

func TestKubeJobControlDeleteJob(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset(
		testJob("job-1", "task-1"),
		testJob("job-2", "task-1"),
		testJob("job-3", "task-2"),
	)
	control := NewKubeJobControl(client, "test-ns", nil, nil)

	if err := control.DeleteJob(ctx, "task-1"); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}

	remaining, err := client.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(remaining.Items) != 1 || remaining.Items[0].Name != "job-3" {
		t.Fatalf("expected only job-3 to survive, got %v", remaining.Items)
	}

	// Deleting an absent task's Jobs is not an error: the fence must tolerate a
	// Job that is already gone.
	if err := control.DeleteJob(ctx, "task-absent"); err != nil {
		t.Fatalf("DeleteJob for an absent task: %v", err)
	}
}

// staticResolver is a ConfigResolver that answers every lookup with one Config.
type staticResolver struct{ config pkg.AgentConfiguration }

func (r staticResolver) Resolve(context.Context, string) (pkg.AgentConfiguration, error) {
	return r.config, nil
}

func (r staticResolver) ResolveByTaskType(
	context.Context,
	string,
	string,
) (pkg.AgentConfiguration, error) {
	return r.config, nil
}

// resolverFor returns a resolver whose every Config carries jobNamespace.
func resolverFor(jobNamespace string) pkg.ConfigResolver {
	return staticResolver{config: pkg.AgentConfiguration{JobNamespace: jobNamespace}}
}

// failingResolver is a ConfigResolver that fails every lookup.
type failingResolver struct{}

func (failingResolver) Resolve(context.Context, string) (pkg.AgentConfiguration, error) {
	return pkg.AgentConfiguration{}, stderrors.New("resolver unavailable")
}

func (failingResolver) ResolveByTaskType(
	context.Context,
	string,
	string,
) (pkg.AgentConfiguration, error) {
	return pkg.AgentConfiguration{}, stderrors.New("resolver unavailable")
}

// fakeTaskView is a TaskView holding a fixed task set. It stands in for the
// compacted-topic view, which is what the fence reads: the view answers for
// tasks this process never saw spawn, so it survives the restart that empties
// the spawn-time TaskStore.
type fakeTaskView struct {
	tasks map[lib.TaskIdentifier]lib.Task
}

func (v *fakeTaskView) CaptureHighWaterMarks(context.Context) error { return nil }

func (v *fakeTaskView) ConsumeMessage(context.Context, *sarama.ConsumerMessage) error {
	return nil
}

func (v *fakeTaskView) Ready() bool { return true }

func (v *fakeTaskView) Snapshot() []lib.Task {
	out := make([]lib.Task, 0, len(v.tasks))
	for _, task := range v.tasks {
		out = append(out, task)
	}
	return out
}

func (v *fakeTaskView) Get(id lib.TaskIdentifier) (lib.Task, bool) {
	task, ok := v.tasks[id]
	return task, ok
}

// viewWithTask returns a view holding one task assigned to claude.
func viewWithTask(id lib.TaskIdentifier) *fakeTaskView {
	return &fakeTaskView{tasks: map[lib.TaskIdentifier]lib.Task{
		id: {TaskIdentifier: id, Frontmatter: lib.TaskFrontmatter{"assignee": "claude"}},
	}}
}

// TestKubeJobControlFollowsJobNamespace pins the fence to the namespace the
// Config put the Job in. Without it IsJobRunning reads false and DeleteJob
// deletes nothing for a Job in the sandbox, so a claim that cannot renew leaves
// its Job running unclaimed — the duplicate the claims exist to prevent.
//
// The task exists only in the view, never in the spawn-time TaskStore: that is
// the restart shape, where recovery asks about a Job this pod was running before
// it restarted. Reading the store there released a live sandbox Job's claim.
func TestKubeJobControlFollowsJobNamespace(t *testing.T) {
	ctx := context.Background()
	const sandbox = "dev-agents-sandbox"

	sandboxJob := testJob("job-1", "task-1")
	sandboxJob.Namespace = sandbox

	client := fake.NewClientset(sandboxJob)
	control := NewKubeJobControl(
		client,
		"test-ns",
		staticResolver{config: pkg.AgentConfiguration{Assignee: "claude", JobNamespace: sandbox}},
		viewWithTask("task-1"),
	)

	running, err := control.IsJobRunning(ctx, "task-1")
	if err != nil {
		t.Fatalf("IsJobRunning: %v", err)
	}
	if !running {
		t.Fatal("IsJobRunning = false for a Job in the sandbox namespace")
	}

	if err := control.DeleteJob(ctx, "task-1"); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	remaining, err := client.BatchV1().Jobs(sandbox).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(remaining.Items) != 0 {
		t.Fatalf("expected the sandbox Job to be deleted, got %v", remaining.Items)
	}
}

// TestKubeJobControlNamespaceFallbacks pins every path that cannot name a
// namespace to the executor's own. The fallback is fail-safe for releasing a
// claim and fail-open for duplicate prevention, so widening it silently is a
// correctness regression, not a refactor.
func TestKubeJobControlNamespaceFallbacks(t *testing.T) {
	const sandbox = "dev-agents-sandbox"

	tests := []struct {
		name     string
		resolver pkg.ConfigResolver
		view     TaskView
		want     libk8s.Namespace
	}{
		{"no resolver", nil, viewWithTask("task-1"), "test-ns"},
		{"no view", resolverFor(sandbox), nil, "test-ns"},
		{"unknown task", resolverFor(sandbox), viewWithTask("task-other"), "test-ns"},
		{"resolver error", failingResolver{}, viewWithTask("task-1"), "test-ns"},
		{"empty jobNamespace", resolverFor(""), viewWithTask("task-1"), "test-ns"},
		{
			"equal to the executor namespace",
			resolverFor("test-ns"),
			viewWithTask("task-1"),
			"test-ns",
		},
		{"configured jobNamespace", resolverFor(sandbox), viewWithTask("task-1"), sandbox},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			control := &kubeJobControl{
				kubeClient: fake.NewClientset(),
				namespace:  "test-ns",
				resolver:   test.resolver,
				view:       test.view,
			}

			got := control.namespaceFor(context.Background(), "task-1")
			if got != test.want {
				t.Fatalf("namespaceFor = %q, want %q", got, test.want)
			}
		})
	}
}
