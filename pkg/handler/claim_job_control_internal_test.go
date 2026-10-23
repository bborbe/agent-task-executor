// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"testing"

	lib "github.com/bborbe/agent"
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

// TestKubeJobControlFollowsJobNamespace pins the fence to the namespace the
// Config put the Job in. Without it IsJobRunning reads false and DeleteJob
// deletes nothing for a Job in the sandbox, so a claim that cannot renew leaves
// its Job running unclaimed — the duplicate the claims exist to prevent.
func TestKubeJobControlFollowsJobNamespace(t *testing.T) {
	ctx := context.Background()
	const sandbox = "dev-agents-sandbox"

	sandboxJob := testJob("job-1", "task-1")
	sandboxJob.Namespace = sandbox

	client := fake.NewClientset(sandboxJob)
	store := pkg.NewTaskStore()
	store.Store("task-1", lib.Task{
		TaskIdentifier: "task-1",
		Frontmatter:    lib.TaskFrontmatter{"assignee": "claude"},
	})
	control := NewKubeJobControl(
		client,
		"test-ns",
		staticResolver{config: pkg.AgentConfiguration{Assignee: "claude", JobNamespace: sandbox}},
		store,
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
