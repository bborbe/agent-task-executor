// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner_test

import (
	"context"
	stderrors "errors"
	"strings"
	"testing"

	libk8s "github.com/bborbe/k8s"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/spawner"
)

func newServiceNamespaceLoop(
	t *testing.T,
	executorNamespace string,
	configs []agentv1.Config,
) (*fake.Clientset, spawner.ServiceReconcileLoop, *fakeProviderImpl) {
	t.Helper()
	client := fake.NewClientset()
	reconciler := spawner.NewServiceReconciler(
		libk8s.NewStatefulSetDeployer(client),
		client,
		libk8s.Namespace(executorNamespace),
		"standard",
	)
	provider := &fakeProviderImpl{configs: configs}
	resolver := &mocks.FakeConfigResolver{}
	resolver.ResolveReturns(pkg.AgentConfiguration{
		Assignee: "identity",
		Type:     agentv1.AgentTypeService,
		Image:    "docker.io/bborbe/agent-pi:v0.1.7",
	}, nil)
	loop := spawner.NewServiceReconcileLoop(provider, resolver, reconciler, 0)
	return client, loop, provider
}

func serviceNamespaceConfig(
	name string,
	executorNamespace string,
	serviceNamespace string,
) agentv1.Config {
	return agentv1.Config{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: executorNamespace,
			UID:       "11111111-2222-3333-4444-555555555555",
		},
		Spec: agentv1.ConfigSpec{
			Assignee:         name,
			Image:            "docker.io/bborbe/agent-pi:v0.1.7",
			Type:             agentv1.AgentTypeService,
			ServiceNamespace: serviceNamespace,
		},
	}
}

func countStatefulSetCreateActions(client *fake.Clientset) int {
	n := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" && action.GetResource().Resource == "statefulsets" {
			n++
		}
	}
	return n
}

func statefulSetCreateNamespace(client *fake.Clientset) string {
	var createdNamespace string
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" && action.GetResource().Resource == "statefulsets" {
			createdNamespace = action.GetNamespace()
		}
	}
	return createdNamespace
}

func statefulSetDeleteNamespaces(client *fake.Clientset) []string {
	var namespaces []string
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "statefulsets" {
			namespaces = append(namespaces, action.GetNamespace())
		}
	}
	return namespaces
}

// seedOwnedStatefulSet creates a StatefulSet in namespace carrying the same
// owner reference buildStatefulSet emits, so the reconciler treats it as its own.
func seedOwnedStatefulSet(
	t *testing.T,
	client *fake.Clientset,
	config agentv1.Config,
	namespace string,
) {
	t.Helper()
	seed := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      config.Name,
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(
					&config,
					agentv1.SchemeGroupVersion.WithKind("Config"),
				),
			},
		},
	}
	if _, err := client.AppsV1().
		StatefulSets(namespace).
		Create(context.Background(), seed, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed StatefulSet failed: %v", err)
	}
}

func TestServiceReconcileRejectsExecutorNamespace(t *testing.T) {
	ctx := context.Background()
	config := serviceNamespaceConfig("identity", "dev", "dev")
	client, loop, _ := newServiceNamespaceLoop(t, "dev", []agentv1.Config{config})

	err := loop.ReconcileOnce(ctx)
	if err == nil {
		t.Fatal("expected error when serviceNamespace equals executor namespace")
	}
	if !strings.Contains(err.Error(), "may not equal the executor namespace") {
		t.Fatalf("unexpected error message: %v", err)
	}
	if !strings.Contains(err.Error(), "identity") {
		t.Fatalf("expected error to name the Config, got: %v", err)
	}
	if n := countStatefulSetCreateActions(client); n != 0 {
		t.Fatalf("expected no statefulset create action, got %d", n)
	}
}

func TestServiceReconcileTargetNamespace(t *testing.T) {
	ctx := context.Background()

	t.Run("explicit namespace", func(t *testing.T) {
		config := serviceNamespaceConfig("identity", "dev", "agent-sandbox")
		client, loop, _ := newServiceNamespaceLoop(t, "dev", []agentv1.Config{config})

		if err := loop.ReconcileOnce(ctx); err != nil {
			t.Fatalf("ReconcileOnce failed: %v", err)
		}
		if got := statefulSetCreateNamespace(client); got != "agent-sandbox" {
			t.Fatalf("expected statefulset in agent-sandbox, got %q", got)
		}
	})

	t.Run("unset namespace falls back to executor", func(t *testing.T) {
		config := serviceNamespaceConfig("identity", "dev", "")
		client, loop, _ := newServiceNamespaceLoop(t, "dev", []agentv1.Config{config})

		if err := loop.ReconcileOnce(ctx); err != nil {
			t.Fatalf("ReconcileOnce failed: %v", err)
		}
		if got := statefulSetCreateNamespace(client); got != "dev" {
			t.Fatalf("expected statefulset in dev, got %q", got)
		}
	})
}

func TestServiceReconcileRemovesPreviousNamespace(t *testing.T) {
	ctx := context.Background()

	config := serviceNamespaceConfig("identity", "dev", "")
	client, loop, provider := newServiceNamespaceLoop(t, "dev", []agentv1.Config{config})

	// First pass deploys into the executor namespace. Building the object through
	// the reconciler makes its owner reference genuinely owned, so the removal
	// below cannot pass with a subtly wrong ownerRef.
	if err := loop.ReconcileOnce(ctx); err != nil {
		t.Fatalf("first ReconcileOnce failed: %v", err)
	}

	provider.configs = []agentv1.Config{serviceNamespaceConfig("identity", "dev", "agent-sandbox")}
	if err := loop.ReconcileOnce(ctx); err != nil {
		t.Fatalf("second ReconcileOnce failed: %v", err)
	}

	if got := statefulSetCreateNamespace(client); got != "agent-sandbox" {
		t.Fatalf("expected statefulset created in agent-sandbox, got %q", got)
	}
	deleted := statefulSetDeleteNamespaces(client)
	if len(deleted) != 1 || deleted[0] != "dev" {
		t.Fatalf("expected one delete against dev, got %v", deleted)
	}
}

func TestServiceReconcileUnsetNamespace(t *testing.T) {
	ctx := context.Background()
	config := serviceNamespaceConfig("identity", "dev", "")
	client, loop, _ := newServiceNamespaceLoop(t, "dev", []agentv1.Config{config})

	if err := loop.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce failed: %v", err)
	}
	if got := statefulSetCreateNamespace(client); got != "dev" {
		t.Fatalf("expected statefulset in dev, got %q", got)
	}
	if deleted := statefulSetDeleteNamespaces(client); len(deleted) != 0 {
		t.Fatalf("expected no delete for an unchanged namespace, got %v", deleted)
	}
}

func TestServiceReconcileSurvivesListDenial(t *testing.T) {
	ctx := context.Background()
	config := serviceNamespaceConfig("identity", "dev", "")
	client, loop, _ := newServiceNamespaceLoop(t, "dev", []agentv1.Config{config})

	// A refused cluster-wide list (the chart may not grant it yet) must not fail a
	// reconcile that has already created its StatefulSet.
	client.PrependReactor("list", "statefulsets", func(
		action k8stesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetNamespace() != "" {
			return false, nil, nil
		}
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "apps", Resource: "statefulsets"},
			"",
			stderrors.New("forbidden"),
		)
	})

	if err := loop.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce must succeed when the sweep list is denied: %v", err)
	}
	if n := countStatefulSetCreateActions(client); n != 1 {
		t.Fatalf("expected one statefulset create action, got %d", n)
	}
	if deleted := statefulSetDeleteNamespaces(client); len(deleted) != 0 {
		t.Fatalf("expected no delete when the list is denied, got %v", deleted)
	}
}

func TestServiceUndeployTargetNamespace(t *testing.T) {
	ctx := context.Background()

	config := serviceNamespaceConfig("identity", "dev", "agent-sandbox")
	config.Spec.Type = agentv1.AgentTypeJob
	client, loop, _ := newServiceNamespaceLoop(t, "dev", []agentv1.Config{config})

	seedOwnedStatefulSet(t, client, config, "agent-sandbox")

	if err := loop.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce failed: %v", err)
	}

	deleted := statefulSetDeleteNamespaces(client)
	if len(deleted) != 1 || deleted[0] != "agent-sandbox" {
		t.Fatalf("expected one delete against agent-sandbox, got %v", deleted)
	}
}

func TestServiceUndeployRemovesPreviousNamespace(t *testing.T) {
	ctx := context.Background()

	// A Config that changed from service to job and cleared serviceNamespace
	// resolves to the executor namespace, leaving the sandbox copy unreachable by
	// the resolved-namespace lookup. The cluster-wide sweep must still remove it.
	config := serviceNamespaceConfig("identity", "dev", "")
	config.Spec.Type = agentv1.AgentTypeJob
	client, loop, _ := newServiceNamespaceLoop(t, "dev", []agentv1.Config{config})

	seedOwnedStatefulSet(t, client, config, "agent-sandbox")

	if err := loop.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce failed: %v", err)
	}

	deleted := statefulSetDeleteNamespaces(client)
	if len(deleted) != 1 || deleted[0] != "agent-sandbox" {
		t.Fatalf("expected one delete against agent-sandbox, got %v", deleted)
	}
}

// TestServiceReconcileCrossNamespaceOwnerRef pins the rule that a cross-namespace
// StatefulSet carries no ownerRef. A namespaced owner that does not exist in the
// dependent's namespace is treated as absent, and Kubernetes then garbage-collects
// the dependent — so stamping the Config as owner of a workload in another
// namespace would make the moved agent delete itself. The label replaces it, and
// the sweep uses the label to recognise what it may remove.
func TestServiceReconcileCrossNamespaceOwnerRef(t *testing.T) {
	ctx := context.Background()

	t.Run("cross-namespace carries no ownerRef but is labelled", func(t *testing.T) {
		config := serviceNamespaceConfig("identity", "dev", "agent-sandbox")
		client, loop, _ := newServiceNamespaceLoop(t, "dev", []agentv1.Config{config})

		if err := loop.ReconcileOnce(ctx); err != nil {
			t.Fatalf("ReconcileOnce failed: %v", err)
		}
		created, err := client.AppsV1().StatefulSets("agent-sandbox").
			Get(ctx, "identity", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get created statefulset: %v", err)
		}
		if len(created.OwnerReferences) != 0 {
			t.Fatalf(
				"cross-namespace statefulset must carry no ownerRef, got %v",
				created.OwnerReferences,
			)
		}
		if got := created.Labels["agent.benjamin-borbe.de/config-name"]; got != "identity" {
			t.Fatalf("expected config-name label identity, got %q", got)
		}
	})

	t.Run("same namespace keeps the Config ownerRef", func(t *testing.T) {
		config := serviceNamespaceConfig("identity", "dev", "")
		client, loop, _ := newServiceNamespaceLoop(t, "dev", []agentv1.Config{config})

		if err := loop.ReconcileOnce(ctx); err != nil {
			t.Fatalf("ReconcileOnce failed: %v", err)
		}
		created, err := client.AppsV1().StatefulSets("dev").
			Get(ctx, "identity", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get created statefulset: %v", err)
		}
		if len(created.OwnerReferences) != 1 {
			t.Fatalf(
				"same-namespace statefulset must keep its ownerRef, got %v",
				created.OwnerReferences,
			)
		}
		if created.OwnerReferences[0].Kind != "Config" {
			t.Fatalf("expected ownerRef kind Config, got %q", created.OwnerReferences[0].Kind)
		}
	})
}
