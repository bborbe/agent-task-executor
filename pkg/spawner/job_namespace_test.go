// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner_test

import (
	"context"
	"strings"
	"testing"

	lib "github.com/bborbe/agent"
	libk8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/spawner"
)

func newSpawnerForNamespace(
	t *testing.T,
	executorNamespace string,
) (*fake.Clientset, spawner.JobSpawner) {
	t.Helper()
	client := fake.NewClientset()
	return client, spawner.NewJobSpawner(
		client,
		libk8s.Namespace(executorNamespace),
		"kafka:9092",
		"dev",
		"",
		libtime.NewCurrentDateTime(),
		1800,
		"",
		"",
		0,
	)
}

func countJobCreateActions(client *fake.Clientset) int {
	n := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" && action.GetResource().Resource == "jobs" {
			n++
		}
	}
	return n
}

func jobCreateNamespace(client *fake.Clientset) string {
	var createdNamespace string
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" && action.GetResource().Resource == "jobs" {
			createdNamespace = action.GetNamespace()
		}
	}
	return createdNamespace
}

func namespaceTestTask() lib.Task {
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier("abc12345"),
		Frontmatter:    lib.TaskFrontmatter{"assignee": "claude"},
	}
}

func TestSpawnRejectsExecutorNamespace(t *testing.T) {
	ctx := context.Background()
	client, jobSpawner := newSpawnerForNamespace(t, "dev")

	task := namespaceTestTask()
	config := pkg.AgentConfiguration{
		Assignee:     "claude",
		Image:        "my-image:latest",
		JobNamespace: "dev",
	}

	_, err := jobSpawner.SpawnJob(ctx, task, config)
	if err == nil {
		t.Fatal("expected error when JobNamespace equals executor namespace")
	}
	if !strings.Contains(err.Error(), "may not equal the executor namespace") {
		t.Fatalf("unexpected error message: %v", err)
	}
	if n := countJobCreateActions(client); n != 0 {
		t.Fatalf("expected no job create action, got %d", n)
	}
}

func TestSpawnTargetNamespace(t *testing.T) {
	ctx := context.Background()

	t.Run("explicit namespace", func(t *testing.T) {
		client, jobSpawner := newSpawnerForNamespace(t, "dev")
		config := pkg.AgentConfiguration{
			Assignee:     "claude",
			Image:        "my-image:latest",
			JobNamespace: "dev-agents-sandbox",
		}
		if _, err := jobSpawner.SpawnJob(ctx, namespaceTestTask(), config); err != nil {
			t.Fatalf("SpawnJob failed: %v", err)
		}
		if got := jobCreateNamespace(client); got != "dev-agents-sandbox" {
			t.Fatalf("expected job in dev-agents-sandbox, got %q", got)
		}
	})

	t.Run("empty namespace falls back to executor", func(t *testing.T) {
		client, jobSpawner := newSpawnerForNamespace(t, "dev")
		config := pkg.AgentConfiguration{
			Assignee: "claude",
			Image:    "my-image:latest",
		}
		if _, err := jobSpawner.SpawnJob(ctx, namespaceTestTask(), config); err != nil {
			t.Fatalf("SpawnJob failed: %v", err)
		}
		if got := jobCreateNamespace(client); got != "dev" {
			t.Fatalf("expected job in dev, got %q", got)
		}
	})
}

func TestJobPodLabels(t *testing.T) {
	ctx := context.Background()
	client, jobSpawner := newSpawnerForNamespace(t, "dev")
	config := pkg.AgentConfiguration{
		Assignee: "claude",
		Image:    "my-image:latest",
	}

	if _, err := jobSpawner.SpawnJob(ctx, namespaceTestTask(), config); err != nil {
		t.Fatalf("SpawnJob failed: %v", err)
	}

	jobs, err := client.BatchV1().Jobs("dev").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list jobs failed: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs.Items))
	}
	if got := jobs.Items[0].Spec.Template.Labels["app"]; got != "agent" {
		t.Fatalf("expected pod label app=agent, got %q", got)
	}
}

func TestJobLookupsUseResolvedNamespace(t *testing.T) {
	ctx := context.Background()
	client, s := newSpawnerForNamespace(t, "dev")
	namespace := libk8s.Namespace("dev-agents-sandbox")

	_, _ = s.IsJobActive(ctx, namespace, lib.TaskIdentifier("abc12345"))
	_, _ = s.JobExists(ctx, namespace, "some-job")
	_, _ = s.CountActiveJobs(ctx, namespace, "claude")
	_, _ = s.ListActiveJobs(ctx, namespace, "claude")

	lookups := 0
	for _, action := range client.Actions() {
		if action.GetResource().Resource != "jobs" {
			continue
		}
		verb := action.GetVerb()
		if verb != "list" && verb != "get" {
			continue
		}
		lookups++
		if got := action.GetNamespace(); got != "dev-agents-sandbox" {
			t.Fatalf("expected lookup in dev-agents-sandbox, got %q for %s", got, verb)
		}
	}
	if lookups < 4 {
		t.Fatalf("expected at least 4 job lookup actions, got %d", lookups)
	}
}
