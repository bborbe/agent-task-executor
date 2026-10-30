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

// seedLabelledCopy creates a StatefulSet carrying the label trio buildStatefulSet
// stamps. withOwnerRef=false is what a copy in another namespace looks like.
func seedLabelledCopy(
	t *testing.T,
	client *fake.Clientset,
	config agentv1.Config,
	namespace string,
	uid string,
	executorNS string,
	withOwnerRef bool,
) {
	t.Helper()
	seed := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      config.Name,
			Namespace: namespace,
			Labels: map[string]string{
				"agent.benjamin-borbe.de/config-name":        config.Name,
				"agent.benjamin-borbe.de/config-uid":         uid,
				"agent.benjamin-borbe.de/executor-namespace": executorNS,
			},
		},
	}
	if withOwnerRef {
		seed.OwnerReferences = []metav1.OwnerReference{
			*metav1.NewControllerRef(&config, agentv1.SchemeGroupVersion.WithKind("Config")),
		}
	}
	if _, err := client.AppsV1().
		StatefulSets(namespace).
		Create(context.Background(), seed, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed labelled copy failed: %v", err)
	}
}

// A StatefulSet labelled for a DIFFERENT Config object that happens to share this
// one's name must survive the sweep. Two executors in one cluster each running a
// `claude-interactive` is the dev/prod pairing this feature exists for, and a
// name-only match made each delete the other's workload — and its PVC — every tick.
func TestServiceSweepSparesAnotherConfigWithTheSameName(t *testing.T) {
	ctx := context.Background()
	config := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
	client, loop, _ := newServiceNamespaceLoop(t, "dev", []agentv1.Config{config})

	seedLabelledCopy(
		t,
		client,
		config,
		"prod-sandbox",
		"99999999-8888-7777-6666-555555555555",
		"prod",
		false,
	)

	if err := loop.ReconcileOnce(ctx); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if _, err := client.AppsV1().StatefulSets("prod-sandbox").
		Get(ctx, config.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("sweep deleted another Config's workload with the same name: %v", err)
	}
}

// The sweep must still remove this Config's OWN previous copy, or a namespace
// change leaves the old workload running with the Config's Secret.
func TestServiceSweepRemovesOwnPreviousCopy(t *testing.T) {
	ctx := context.Background()
	config := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
	client, loop, _ := newServiceNamespaceLoop(t, "dev", []agentv1.Config{config})

	seedLabelledCopy(t, client, config, "old-sandbox", string(config.UID), "dev", false)

	if err := loop.ReconcileOnce(ctx); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if _, err := client.AppsV1().StatefulSets("old-sandbox").
		Get(ctx, config.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected own previous copy to be removed, got err=%v", err)
	}
}

// Deleting the Config CR removes it from the provider list, so no per-Config path
// runs for it any more. Without the reaper its cross-namespace StatefulSet keeps
// running with the Config's Secret env and its PVC, with nothing to collect it.
//
// ⚠️ A SECOND Config is kept alive on purpose. An empty list is not "everything was
// deleted", it is an unsynced store, and the reaper refuses it — so modelling the
// deletion as an empty list would test the wrong thing entirely.
func TestServiceReapRemovesCopyWhoseConfigIsGone(t *testing.T) {
	ctx := context.Background()
	gone := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
	survivor := serviceNamespaceConfig("pi-interactive", "dev", "dev-sandbox")
	survivor.UID = "aaaaaaaa-1111-2222-3333-444444444444"
	client, loop, provider := newServiceNamespaceLoop(t, "dev", []agentv1.Config{gone, survivor})
	seedLabelledCopy(t, client, gone, "dev-sandbox", string(gone.UID), "dev", false)

	// The CR is deleted: the provider no longer lists it, but still lists the other.
	provider.configs = []agentv1.Config{survivor}

	// TWO passes, because one is never enough: the first sighting is only recorded.
	// See TestServiceReapNeedsTwoPassesBeforeDeleting.
	for i := 0; i < 2; i++ {
		if err := loop.ReconcileOnce(ctx); err != nil {
			t.Fatalf("reconcile %d failed: %v", i, err)
		}
	}

	if _, err := client.AppsV1().StatefulSets("dev-sandbox").
		Get(ctx, gone.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected orphaned cross-namespace copy to be reaped, got err=%v", err)
	}
}

// ⚠️ One pass must never delete, and this is the guard that makes a partially
// synced Config store harmless. The first reconcile pass after a restart runs
// concurrently with the informer, and client-go delivers the initial LIST one
// object at a time (`processDeltas` → `OnAdd` per object), so a pass can see a
// non-empty SUBSET of the Configs. Every Config missing from that subset looks
// deleted. Requiring a second sighting means the worst a partial store can do is
// delay a reap by one interval.
func TestServiceReapNeedsTwoPassesBeforeDeleting(t *testing.T) {
	ctx := context.Background()
	gone := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
	survivor := serviceNamespaceConfig("pi-interactive", "dev", "dev-sandbox")
	survivor.UID = "aaaaaaaa-1111-2222-3333-444444444444"
	client, loop, provider := newServiceNamespaceLoop(t, "dev", []agentv1.Config{gone, survivor})
	seedLabelledCopy(t, client, gone, "dev-sandbox", string(gone.UID), "dev", false)

	// A store that has delivered only SOME Configs: `gone` is absent, but this is a
	// partial view, not a deletion.
	provider.configs = []agentv1.Config{survivor}

	if err := loop.ReconcileOnce(ctx); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}

	if _, err := client.AppsV1().StatefulSets("dev-sandbox").
		Get(ctx, gone.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("a single pass deleted a copy on a possibly-partial Config view: %v", err)
	}
}

// The reaper must never touch a workload the garbage collector owns, must never
// touch one whose Config still exists, and must never touch another executor's.
// A stale or momentarily empty Config list would otherwise take every service
// agent down — including, in the last case, a peer executor's.
func TestServiceReapSparesLiveOwnedAndForeignWorkloads(t *testing.T) {
	ctx := context.Background()
	live := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
	client := fake.NewClientset()
	reconciler := spawner.NewServiceReconciler(
		libk8s.NewStatefulSetDeployer(client),
		client,
		libk8s.Namespace("dev"),
		"standard",
	)

	// Live Config, cross-namespace copy: keep.
	seedLabelledCopy(t, client, live, "dev-sandbox", string(live.UID), "dev", false)
	// Same-namespace copy: carries an ownerRef, so the GC is authoritative.
	seedLabelledCopy(t, client, live, "dev", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "dev", true)
	// Another executor's copy, same Config name and a UID absent from this list.
	// Its UID is not live here by definition, so only the executor label spares it.
	seedLabelledCopy(
		t,
		client,
		live,
		"prod-sandbox",
		"99999999-8888-7777-6666-555555555555",
		"prod",
		false,
	)

	for i := 0; i < 2; i++ {
		reconciler.ReapOrphanedCopies(ctx, []agentv1.Config{live})
	}

	for _, ns := range []string{"dev-sandbox", "dev", "prod-sandbox"} {
		if _, err := client.AppsV1().StatefulSets(ns).
			Get(ctx, live.Name, metav1.GetOptions{}); err != nil {
			t.Fatalf("reaper removed a workload it must spare in %s: %v", ns, err)
		}
	}
}

// ⚠️ The first reconcile pass after every restart runs before the Config store has
// listed anything, and ReapOrphanedCopies receives an empty slice. Treating that as
// "every Config is gone" deletes every cross-namespace service workload this executor
// stamped — and their PVCs, because WhenDeleted is Delete. This is the difference
// between a restart and data loss.
func TestServiceReapDoesNothingOnAnEmptyConfigList(t *testing.T) {
	ctx := context.Background()
	live := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
	client := fake.NewClientset()
	reconciler := spawner.NewServiceReconciler(
		libk8s.NewStatefulSetDeployer(client),
		client,
		libk8s.Namespace("dev"),
		"standard",
	)

	seedLabelledCopy(t, client, live, "dev-sandbox", string(live.UID), "dev", false)

	// Exactly what the store returns before the informer has synced.
	for i := 0; i < 2; i++ {
		reconciler.ReapOrphanedCopies(ctx, nil)
	}

	if _, err := client.AppsV1().StatefulSets("dev-sandbox").
		Get(ctx, live.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("an unsynced Config store destroyed a live cross-namespace workload: %v", err)
	}
}
