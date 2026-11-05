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

func (v *fakeTaskView) Snapshot() []lib.Task { return nil }

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

// clearedAssigneeResolver mirrors the production resolver for the case this
// test exists for: an empty assignee resolves to no Config at all, while the
// task's own task_type still names exactly one.
type clearedAssigneeResolver struct {
	config pkg.AgentConfiguration
	// taskTypeErr is what ResolveByTaskType answers, so the ambiguity and
	// not-found arms are reachable. nil means "return config".
	taskTypeErr error
	// byTaskTypeArgs records every (taskType, preferAssignee) pair the resolver
	// was asked for, so the test fails if namespaceFor passes the wrong key.
	byTaskTypeArgs *[][2]string
}

func (r clearedAssigneeResolver) Resolve(
	_ context.Context,
	assignee string,
) (pkg.AgentConfiguration, error) {
	if assignee == "" {
		return pkg.AgentConfiguration{}, pkg.ErrConfigNotFound
	}
	return r.config, nil
}

func (r clearedAssigneeResolver) ResolveByTaskType(
	_ context.Context,
	taskType string,
	preferAssignee string,
) (pkg.AgentConfiguration, error) {
	if r.byTaskTypeArgs != nil {
		*r.byTaskTypeArgs = append(*r.byTaskTypeArgs, [2]string{taskType, preferAssignee})
	}
	if r.taskTypeErr != nil {
		return pkg.AgentConfiguration{}, r.taskTypeErr
	}
	return r.config, nil
}

// viewWithClearedAssignee returns a view holding one task whose assignee has
// been cleared and whose task_type is intact — the shape a dark-factory agent
// writes back between phases, and the shape the executor's own restore loop
// keeps re-publishing.
func viewWithClearedAssignee(id lib.TaskIdentifier, taskType string) *fakeTaskView {
	return &fakeTaskView{tasks: map[lib.TaskIdentifier]lib.Task{
		id: {
			TaskIdentifier: id,
			Frontmatter:    lib.TaskFrontmatter{"assignee": "", "task_type": taskType},
		},
	}}
}

// TestKubeJobControlResolvesNamespaceForTaskWithClearedAssignee is the
// regression this test file exists for.
//
// A task whose assignee was cleared resolves to no Config by assignee, so
// namespaceFor fell back to the executor's own namespace, IsJobRunning read
// false for a Job that was running in the Config's sandbox, and after
// JoblessGrace the lease manager released a claim whose Job was still running.
// The peer cluster then claimed the free ref and started the same task id —
// the cross-cluster duplicate. Measured 2026-11-04 on two task ids
// (8e0eb923, bb0d817e), both `github-dark-factory-agent` with
// jobNamespace: development-sandbox.
//
// The task_type is the routing key that survives a cleared assignee — the same
// lookup startup recovery already uses for exactly this shape.
func TestKubeJobControlResolvesNamespaceForTaskWithClearedAssignee(t *testing.T) {
	ctx := context.Background()
	const sandbox = "development-sandbox"

	sandboxJob := testJob("job-1", "task-1")
	sandboxJob.Namespace = sandbox

	client := fake.NewClientset(sandboxJob)
	var args [][2]string
	control := NewKubeJobControl(
		client,
		"agent",
		clearedAssigneeResolver{
			config: pkg.AgentConfiguration{
				Assignee:     "github-dark-factory-agent",
				JobNamespace: sandbox,
			},
			byTaskTypeArgs: &args,
		},
		viewWithClearedAssignee("task-1", "dark-factory-implement"),
	)

	running, err := control.IsJobRunning(ctx, "task-1")
	if err != nil {
		t.Fatalf("IsJobRunning: %v", err)
	}
	if !running {
		t.Fatal(
			"IsJobRunning = false for a Job in the sandbox namespace of a task with a cleared assignee",
		)
	}

	// The fallback must be keyed on the task's own task_type, and must not
	// smuggle the cleared assignee back in as a tie-breaker.
	if len(args) == 0 {
		t.Fatal("ResolveByTaskType was never called")
	}
	for _, call := range args {
		if call[0] != "dark-factory-implement" {
			t.Fatalf("ResolveByTaskType taskType = %q, want dark-factory-implement", call[0])
		}
		if call[1] != "" {
			t.Fatalf("ResolveByTaskType preferAssignee = %q, want empty", call[1])
		}
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

// TestKubeJobControlFallsBackWhenTaskTypeCannotRoute pins the three ways the
// task_type fallback can refuse to name a namespace: several Configs declare the
// type, none does, or the resolver itself is unusable. All three must land on
// the executor's namespace — the fence must never be the reason a claim cannot
// be released, and a fail-closed branch here would hold claims forever.
//
// The Job is therefore invisible, which is the pre-existing fail-open behaviour
// namespaceFor documents; this test pins the fallback, it does not endorse it.
func TestKubeJobControlFallsBackWhenTaskTypeCannotRoute(t *testing.T) {
	tests := []struct {
		name        string
		taskTypeErr error
	}{
		{name: "task_type matches several Configs", taskTypeErr: pkg.ErrConfigAmbiguous},
		{name: "task_type matches no Config", taskTypeErr: pkg.ErrConfigNotFound},
		{name: "resolver unavailable", taskTypeErr: stderrors.New("resolver unavailable")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			control := &kubeJobControl{
				kubeClient: fake.NewClientset(),
				namespace:  "agent",
				resolver: clearedAssigneeResolver{
					config:      pkg.AgentConfiguration{JobNamespace: "development-sandbox"},
					taskTypeErr: test.taskTypeErr,
				},
				view: viewWithClearedAssignee("task-1", "dark-factory-implement"),
			}

			got := control.namespaceFor(context.Background(), "task-1")
			if got != "agent" {
				t.Fatalf("namespaceFor = %q, want the executor namespace %q", got, "agent")
			}
		})
	}
}

// TestNamespaceFallbackReason pins the operator-facing label: an ambiguous
// task_type and an absent Config are different fleet problems and must not both
// read as "no_config".
func TestNamespaceFallbackReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "ambiguous", err: pkg.ErrConfigAmbiguous, want: "ambiguous_config"},
		{name: "not found", err: pkg.ErrConfigNotFound, want: "no_config"},
		{name: "other", err: stderrors.New("resolver unavailable"), want: "resolve_error"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := namespaceFallbackReason(test.err); got != test.want {
				t.Fatalf("namespaceFallbackReason = %q, want %q", got, test.want)
			}
		})
	}
}
