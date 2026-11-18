// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner_test

import (
	"context"
	stderrors "errors"

	libk8s "github.com/bborbe/k8s"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/spawner"
)

func newServiceNamespaceLoop(
	executorNamespace string,
	configs []agentv1.Config,
) (*fake.Clientset, spawner.ServiceReconcileLoop, *fakeProviderImpl) {
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
	client *fake.Clientset,
	config agentv1.Config,
	namespace string,
) {
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
	_, err := client.AppsV1().
		StatefulSets(namespace).
		Create(context.Background(), seed, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred(), "seed StatefulSet failed")
}

// seedLabelledCopy creates a StatefulSet carrying the label trio buildStatefulSet
// stamps. withOwnerRef=false is what a copy in another namespace looks like.
func seedLabelledCopy(
	client *fake.Clientset,
	config agentv1.Config,
	namespace string,
	uid string,
	executorNS string,
	withOwnerRef bool,
) {
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
	_, err := client.AppsV1().
		StatefulSets(namespace).
		Create(context.Background(), seed, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred(), "seed labelled copy failed")
}

// seedCopyWithObjectUID is seedLabelledCopy with the StatefulSet's own UID pinned,
// so a spec can model the same name being replaced by a genuinely different object.
func seedCopyWithObjectUID(
	client *fake.Clientset,
	config agentv1.Config,
	namespace string,
	objectUID string,
) {
	seed := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      config.Name,
			Namespace: namespace,
			UID:       types.UID(objectUID),
			Labels: map[string]string{
				"agent.benjamin-borbe.de/config-name":        config.Name,
				"agent.benjamin-borbe.de/config-uid":         string(config.UID),
				"agent.benjamin-borbe.de/executor-namespace": "dev",
			},
		},
	}
	_, err := client.AppsV1().
		StatefulSets(namespace).
		Create(context.Background(), seed, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred(), "seed copy with object UID failed")
}

// newServiceReconciler builds a reconciler over a fresh fake clientset, for the
// specs that drive ReapOrphanedCopies directly rather than through a pass.
func newServiceReconciler() (*fake.Clientset, spawner.ServiceReconciler) {
	client := fake.NewClientset()
	return client, spawner.NewServiceReconciler(
		libk8s.NewStatefulSetDeployer(client),
		client,
		libk8s.Namespace("dev"),
		"standard",
	)
}

var _ = Describe("service namespace", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("refuses a serviceNamespace equal to the executor namespace", func() {
		config := serviceNamespaceConfig("identity", "dev", "dev")
		client, loop, _ := newServiceNamespaceLoop("dev", []agentv1.Config{config})

		err := loop.ReconcileOnce(ctx)
		Expect(
			err,
		).To(HaveOccurred(), "expected error when serviceNamespace equals executor namespace")
		Expect(err.Error()).To(ContainSubstring("may not equal the executor namespace"))
		Expect(err.Error()).To(ContainSubstring("identity"), "error must name the Config")
		Expect(countStatefulSetCreateActions(client)).To(Equal(0),
			"a rejected namespace must not create a StatefulSet")
	})

	It("creates the StatefulSet in the named namespace", func() {
		config := serviceNamespaceConfig("identity", "dev", "agent-sandbox")
		client, loop, _ := newServiceNamespaceLoop("dev", []agentv1.Config{config})

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		Expect(statefulSetCreateNamespace(client)).To(Equal("agent-sandbox"))
	})

	It("falls back to the executor namespace when the field is unset", func() {
		config := serviceNamespaceConfig("identity", "dev", "")
		client, loop, _ := newServiceNamespaceLoop("dev", []agentv1.Config{config})

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		Expect(statefulSetCreateNamespace(client)).To(Equal("dev"))
	})

	It("removes the previous copy when the namespace changes", func() {
		config := serviceNamespaceConfig("identity", "dev", "")
		client, loop, provider := newServiceNamespaceLoop("dev", []agentv1.Config{config})

		// First pass deploys into the executor namespace. Building the object through
		// the reconciler makes its owner reference genuinely owned, so the removal
		// below cannot pass with a subtly wrong ownerRef.
		Expect(loop.ReconcileOnce(ctx)).To(Succeed(), "first pass")

		provider.configs = []agentv1.Config{
			serviceNamespaceConfig("identity", "dev", "agent-sandbox"),
		}
		Expect(loop.ReconcileOnce(ctx)).To(Succeed(), "second pass")

		Expect(statefulSetCreateNamespace(client)).To(Equal("agent-sandbox"))
		Expect(statefulSetDeleteNamespaces(client)).To(Equal([]string{"dev"}))
	})

	It("deletes nothing for an unchanged unset Config", func() {
		config := serviceNamespaceConfig("identity", "dev", "")
		client, loop, _ := newServiceNamespaceLoop("dev", []agentv1.Config{config})

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		Expect(statefulSetCreateNamespace(client)).To(Equal("dev"))
		Expect(statefulSetDeleteNamespaces(client)).To(BeEmpty())
	})

	It("survives a denied cluster-wide list", func() {
		config := serviceNamespaceConfig("identity", "dev", "")
		client, loop, _ := newServiceNamespaceLoop("dev", []agentv1.Config{config})

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

		Expect(loop.ReconcileOnce(ctx)).To(Succeed(),
			"a denied sweep list must not fail the reconcile")
		Expect(countStatefulSetCreateActions(client)).To(Equal(1))
		Expect(statefulSetDeleteNamespaces(client)).To(BeEmpty())
	})

	It("undeploys from the resolved namespace", func() {
		config := serviceNamespaceConfig("identity", "dev", "agent-sandbox")
		config.Spec.Type = agentv1.AgentTypeJob
		client, loop, _ := newServiceNamespaceLoop("dev", []agentv1.Config{config})

		seedOwnedStatefulSet(client, config, "agent-sandbox")

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		Expect(statefulSetDeleteNamespaces(client)).To(Equal([]string{"agent-sandbox"}))
	})

	It("removes the sandbox copy when a service becomes a job", func() {
		// A Config that changed from service to job and cleared serviceNamespace
		// resolves to the executor namespace, leaving the sandbox copy unreachable by
		// the resolved-namespace lookup. The cluster-wide sweep must still remove it.
		config := serviceNamespaceConfig("identity", "dev", "")
		config.Spec.Type = agentv1.AgentTypeJob
		client, loop, _ := newServiceNamespaceLoop("dev", []agentv1.Config{config})

		seedOwnedStatefulSet(client, config, "agent-sandbox")

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		Expect(statefulSetDeleteNamespaces(client)).To(Equal([]string{"agent-sandbox"}))
	})

	// A cross-namespace StatefulSet carries no ownerRef. A namespaced owner that does
	// not exist in the dependent's namespace is treated as absent, and Kubernetes then
	// garbage-collects the dependent — so stamping the Config as owner of a workload
	// in another namespace would make the moved agent delete itself. The label
	// replaces it, and the sweep uses the label to recognise what it may remove.
	It("gives a cross-namespace copy no ownerRef but the label trio", func() {
		config := serviceNamespaceConfig("identity", "dev", "agent-sandbox")
		client, loop, _ := newServiceNamespaceLoop("dev", []agentv1.Config{config})

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		created, err := client.AppsV1().StatefulSets("agent-sandbox").
			Get(ctx, "identity", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred(), "get created statefulset")
		Expect(created.OwnerReferences).To(BeEmpty(),
			"cross-namespace statefulset must carry no ownerRef")
		Expect(created.Labels["agent.benjamin-borbe.de/config-name"]).To(Equal("identity"))
	})

	It("keeps the Config ownerRef on a same-namespace copy", func() {
		config := serviceNamespaceConfig("identity", "dev", "")
		client, loop, _ := newServiceNamespaceLoop("dev", []agentv1.Config{config})

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		created, err := client.AppsV1().StatefulSets("dev").
			Get(ctx, "identity", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred(), "get created statefulset")
		Expect(created.OwnerReferences).To(HaveLen(1),
			"same-namespace statefulset must keep its ownerRef")
		Expect(created.OwnerReferences[0].Kind).To(Equal("Config"))
	})

	// A StatefulSet labelled for a DIFFERENT Config object that happens to share this
	// one's name must survive the sweep. Two executors in one cluster each running a
	// `claude-interactive` is the dev/prod pairing this feature exists for, and a
	// name-only match made each delete the other's workload — and its PVC — every tick.
	It("spares another Config's workload that shares this Config's name", func() {
		config := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
		client, loop, _ := newServiceNamespaceLoop("dev", []agentv1.Config{config})

		seedLabelledCopy(
			client,
			config,
			"prod-sandbox",
			"99999999-8888-7777-6666-555555555555",
			"prod",
			false,
		)

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		_, err := client.AppsV1().StatefulSets("prod-sandbox").
			Get(ctx, config.Name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred(),
			"sweep deleted another Config's workload with the same name")
	})

	// The sweep must still remove this Config's OWN previous copy, or a namespace
	// change leaves the old workload running with the Config's Secret.
	It("removes this Config's own previous copy", func() {
		config := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
		client, loop, _ := newServiceNamespaceLoop("dev", []agentv1.Config{config})

		seedLabelledCopy(client, config, "old-sandbox", string(config.UID), "dev", false)

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		_, err := client.AppsV1().StatefulSets("old-sandbox").
			Get(ctx, config.Name, metav1.GetOptions{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected own previous copy to be removed")
	})

	// Deleting the Config CR removes it from the provider list, so no per-Config path
	// runs for it any more. Without the reaper its cross-namespace StatefulSet keeps
	// running with the Config's Secret env and its PVC, with nothing to collect it.
	//
	// ⚠️ A SECOND Config is kept alive on purpose. An empty list is not "everything was
	// deleted", it is an unsynced store, and the reaper refuses it — so modelling the
	// deletion as an empty list would test the wrong thing entirely.
	It("reaps a copy whose Config is gone", func() {
		gone := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
		survivor := serviceNamespaceConfig("pi-interactive", "dev", "dev-sandbox")
		survivor.UID = "aaaaaaaa-1111-2222-3333-444444444444"
		client, loop, provider := newServiceNamespaceLoop(
			"dev",
			[]agentv1.Config{gone, survivor},
		)
		seedLabelledCopy(client, gone, "dev-sandbox", string(gone.UID), "dev", false)

		// The CR is deleted: the provider no longer lists it, but still lists the other.
		provider.configs = []agentv1.Config{survivor}

		// TWO passes, because one is never enough: the first sighting is only recorded.
		for i := 0; i < 2; i++ {
			Expect(loop.ReconcileOnce(ctx)).To(Succeed(), "reconcile pass")
		}

		_, err := client.AppsV1().StatefulSets("dev-sandbox").
			Get(ctx, gone.Name, metav1.GetOptions{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(),
			"expected orphaned cross-namespace copy to be reaped")
	})

	// ⚠️ One pass must never delete, and this is the guard that makes a partially
	// synced Config store harmless. The first reconcile pass after a restart runs
	// concurrently with the informer, and client-go delivers the initial LIST one
	// object at a time (`processDeltas` → `OnAdd` per object), so a pass can see a
	// non-empty SUBSET of the Configs. Every Config missing from that subset looks
	// deleted. Requiring a second sighting means the worst a partial store can do is
	// delay a reap by one interval.
	It("needs two consecutive passes before deleting", func() {
		gone := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
		survivor := serviceNamespaceConfig("pi-interactive", "dev", "dev-sandbox")
		survivor.UID = "aaaaaaaa-1111-2222-3333-444444444444"
		client, loop, provider := newServiceNamespaceLoop(
			"dev",
			[]agentv1.Config{gone, survivor},
		)
		seedLabelledCopy(client, gone, "dev-sandbox", string(gone.UID), "dev", false)

		// A store that has delivered only SOME Configs: `gone` is absent, but this is a
		// partial view, not a deletion.
		provider.configs = []agentv1.Config{survivor}

		Expect(loop.ReconcileOnce(ctx)).To(Succeed(), "first pass")

		_, err := client.AppsV1().StatefulSets("dev-sandbox").
			Get(ctx, gone.Name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred(),
			"a single pass deleted a copy on a possibly-partial Config view")
	})

	// ⚠️ A candidate key that is only namespace/name identifies a NAME, not a copy.
	// A StatefulSet deleted and recreated between passes would inherit the previous
	// pass's sighting, and in the partial-store window this gate exists for it would
	// then be deleted on its first real sighting — taking its PVC, because
	// WhenDeleted is Delete. The key therefore carries the copy's own UID.
	It("does not let a recreated copy inherit the previous sighting", func() {
		gone := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
		survivor := serviceNamespaceConfig("pi-interactive", "dev", "dev-sandbox")
		survivor.UID = "aaaaaaaa-1111-2222-3333-444444444444"
		client, loop, provider := newServiceNamespaceLoop(
			"dev",
			[]agentv1.Config{gone, survivor},
		)

		seedCopyWithObjectUID(client, gone, "dev-sandbox", "sts-uid-1")

		// Pass 1: `gone` is absent from a partial store, so its copy is recorded as a
		// candidate and nothing is deleted.
		provider.configs = []agentv1.Config{survivor}
		Expect(loop.ReconcileOnce(ctx)).To(Succeed(), "first pass")

		// Between passes the copy is replaced by a fresh object with the same name and
		// labels but a new UID.
		Expect(client.AppsV1().StatefulSets("dev-sandbox").Delete(
			ctx, gone.Name, metav1.DeleteOptions{},
		)).To(Succeed())
		seedCopyWithObjectUID(client, gone, "dev-sandbox", "sts-uid-2")

		// Pass 2: still a partial store, and the new object's name is already in the
		// previous candidate set — but its identity is not, so this is its FIRST
		// sighting and it must survive.
		Expect(loop.ReconcileOnce(ctx)).To(Succeed(), "second pass")

		_, err := client.AppsV1().StatefulSets("dev-sandbox").
			Get(ctx, gone.Name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred(),
			"a recreated copy was deleted on its first sighting, inheriting the old one's")
	})

	// The reaper must never touch a workload the garbage collector owns, must never
	// touch one whose Config still exists, and must never touch another executor's.
	// A stale or momentarily empty Config list would otherwise take every service
	// agent down — including, in the last case, a peer executor's.
	It("spares live, owned and foreign workloads", func() {
		live := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
		client, reconciler := newServiceReconciler()

		// Live Config, cross-namespace copy: keep.
		seedLabelledCopy(client, live, "dev-sandbox", string(live.UID), "dev", false)
		// Same-namespace copy: carries an ownerRef, so the GC is authoritative.
		seedLabelledCopy(
			client, live, "dev", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "dev", true,
		)
		// Another executor's copy, same Config name and a UID absent from this list.
		// Its UID is not live here by definition, so only the executor label spares it.
		seedLabelledCopy(
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
			_, err := client.AppsV1().StatefulSets(ns).Get(ctx, live.Name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "reaper removed a workload it must spare in "+ns)
		}
	})

	// ⚠️ The first reconcile pass after every restart runs before the Config store has
	// listed anything, and ReapOrphanedCopies receives an empty slice. Treating that as
	// "every Config is gone" deletes every cross-namespace service workload this executor
	// stamped — and their PVCs, because WhenDeleted is Delete. This is the difference
	// between a restart and data loss.
	It("reaps nothing on an empty Config list", func() {
		live := serviceNamespaceConfig("claude-interactive", "dev", "dev-sandbox")
		client, reconciler := newServiceReconciler()

		seedLabelledCopy(client, live, "dev-sandbox", string(live.UID), "dev", false)

		// Exactly what the store returns before the informer has synced.
		for i := 0; i < 2; i++ {
			reconciler.ReapOrphanedCopies(ctx, nil)
		}

		_, err := client.AppsV1().StatefulSets("dev-sandbox").
			Get(ctx, live.Name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred(),
			"an unsynced Config store destroyed a live cross-namespace workload")
	})
})
