// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	stderrors "errors"
	"reflect"
	"sync"
	"testing"
	"time"

	lib "github.com/bborbe/agent"
	libk8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

// namespaceTestProvider is a controllable libk8s.Provider[agentv1.Config].
type namespaceTestProvider struct {
	mu    sync.Mutex
	items []agentv1.Config
	err   error
}

func (p *namespaceTestProvider) Get(ctx context.Context) ([]agentv1.Config, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return nil, p.err
	}
	items := make([]agentv1.Config, len(p.items))
	copy(items, p.items)
	return items, nil
}

func (p *namespaceTestProvider) setItems(items ...agentv1.Config) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.items = items
}

func (p *namespaceTestProvider) setErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// fakeNamespaceInformer records starts and lets a test control cache sync and
// handler-registration failures.
type fakeNamespaceInformer struct {
	mu        sync.Mutex
	synced    bool
	started   int
	podLister corev1listers.PodLister
	addJobErr error
	addPodErr error
}

func (f *fakeNamespaceInformer) Start(ctx context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started++
}

func (f *fakeNamespaceInformer) HasSynced() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.synced
}

func (f *fakeNamespaceInformer) AddJobEventHandler(handler cache.ResourceEventHandler) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.addJobErr
}

func (f *fakeNamespaceInformer) AddPodEventHandler(handler cache.ResourceEventHandler) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.addPodErr
}

func (f *fakeNamespaceInformer) PodLister() corev1listers.PodLister {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.podLister
}

func (f *fakeNamespaceInformer) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started
}

// failedJob builds a terminal-failed Job named name, in namespace, carrying
// taskID under the agent.benjamin-borbe.de/task-id label and a true
// batchv1.JobFailed condition.
func failedJob(name, taskID, namespace string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				jobWatcherTaskIDLabel: taskID,
			},
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{
					Type:   batchv1.JobFailed,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
}

func configNamed(name, jobNamespace string) agentv1.Config {
	return agentv1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       agentv1.ConfigSpec{JobNamespace: jobNamespace},
	}
}

func namespaceStrings(namespaces []libk8s.Namespace) []string {
	result := make([]string, len(namespaces))
	for i, namespace := range namespaces {
		result[i] = namespace.String()
	}
	return result
}

func TestJobWatcherNamespaces(t *testing.T) {
	ctx := context.Background()

	withEmpty := []agentv1.Config{
		configNamed("empty", ""),
		configNamed("sandbox-1", "dev-agents-sandbox"),
		configNamed("sandbox-2", "dev-agents-sandbox"),
		configNamed("other", "dev-other"),
	}
	got := namespaceStrings(resolveJobWatcherNamespaces(ctx, "dev", withEmpty))
	want := []string{"dev", "dev-agents-sandbox", "dev-other"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("with empty jobNamespace: got %v, want %v", got, want)
	}

	withoutEmpty := []agentv1.Config{
		configNamed("sandbox-1", "dev-agents-sandbox"),
		configNamed("sandbox-2", "dev-agents-sandbox"),
		configNamed("other", "dev-other"),
	}
	got = namespaceStrings(resolveJobWatcherNamespaces(ctx, "dev", withoutEmpty))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("without empty jobNamespace: got %v, want %v", got, want)
	}

	got = namespaceStrings(resolveJobWatcherNamespaces(ctx, "dev", nil))
	if !reflect.DeepEqual(got, []string{"dev"}) {
		t.Fatalf("no configs: got %v, want [dev]", got)
	}

	rejected := []agentv1.Config{configNamed("rejected", "dev")}
	got = namespaceStrings(resolveJobWatcherNamespaces(ctx, "dev", rejected))
	if !reflect.DeepEqual(got, []string{"dev"}) {
		t.Fatalf("rejected config: got %v, want [dev]", got)
	}
}

func newNamespaceTestWatcher(
	t *testing.T,
	provider *namespaceTestProvider,
	publisher *fakeResultPublisher,
) *jobWatcher {
	t.Helper()
	w, ok := NewJobWatcher(
		fake.NewClientset(),
		"dev",
		provider,
		NewTaskStore(),
		publisher,
		nil,
		libtime.NewCurrentDateTime(),
	).(*jobWatcher)
	if !ok {
		t.Fatalf("watcher is not a *jobWatcher")
	}
	return w
}

func syncedInformerFactory() func(
	kubernetes.Interface,
	libk8s.Namespace,
) (jobWatcherNamespaceInformer, error) {
	return func(kubernetes.Interface, libk8s.Namespace) (jobWatcherNamespaceInformer, error) {
		return &fakeNamespaceInformer{synced: true}, nil
	}
}

func TestJobWatcherTerminalFailureAcrossNamespaces(t *testing.T) {
	ctx := context.Background()
	provider := &namespaceTestProvider{}
	provider.setItems(
		configNamed("empty", ""),
		configNamed("sandbox", "dev-agents-sandbox"),
	)
	publisher := &fakeResultPublisher{}
	w := newNamespaceTestWatcher(t, provider, publisher)
	created := map[libk8s.Namespace]*fakeNamespaceInformer{}
	w.newNamespaceInformer = func(
		_ kubernetes.Interface,
		ns libk8s.Namespace,
	) (jobWatcherNamespaceInformer, error) {
		informer := &fakeNamespaceInformer{synced: true}
		created[ns] = informer
		return informer, nil
	}
	if err := w.reconcileNamespaceSet(ctx); err != nil {
		t.Fatalf("reconcile namespace set: %v", err)
	}
	if len(created) != 2 {
		t.Fatalf("created %d informer pairs, want 2", len(created))
	}
	for ns, informer := range created {
		if informer.startCount() != 1 {
			t.Fatalf("informer for %s started %d times, want 1", ns, informer.startCount())
		}
	}

	taskID := lib.TaskIdentifier("sandbox-task")
	w.taskStore.Store(taskID, lib.Task{TaskIdentifier: taskID})
	w.HandleJob(ctx, failedJob("job-sandbox", string(taskID), "dev-agents-sandbox"))

	if publisher.PublishFailureCallCount() != 1 {
		t.Fatalf("publish failure calls = %d, want 1", publisher.PublishFailureCallCount())
	}
	_, _, jobName, _ := publisher.PublishFailureArgsForCall(0)
	if jobName != "job-sandbox" {
		t.Fatalf("published job name = %q, want job-sandbox", jobName)
	}
	if _, ok := w.taskStore.Load(taskID); ok {
		t.Fatalf("task %s still in store, want evicted", taskID)
	}
}

func TestJobWatcherThreeNamespaces(t *testing.T) {
	ctx := context.Background()
	provider := &namespaceTestProvider{}
	provider.setItems(
		configNamed("a", "ns-a"),
		configNamed("b", "ns-b"),
		configNamed("c", "ns-c"),
	)
	publisher := &fakeResultPublisher{}
	w := newNamespaceTestWatcher(t, provider, publisher)
	w.newNamespaceInformer = syncedInformerFactory()
	if err := w.reconcileNamespaceSet(ctx); err != nil {
		t.Fatalf("reconcile namespace set: %v", err)
	}

	namespaces := []string{"ns-a", "ns-b", "ns-c"}
	for _, ns := range namespaces {
		taskID := lib.TaskIdentifier("task-" + ns)
		w.taskStore.Store(taskID, lib.Task{TaskIdentifier: taskID})
		w.HandleJob(ctx, failedJob("job-"+ns, string(taskID), ns))
	}

	if publisher.PublishFailureCallCount() != 3 {
		t.Fatalf("publish failure calls = %d, want 3", publisher.PublishFailureCallCount())
	}
	for _, ns := range namespaces {
		if _, ok := w.taskStore.Load(lib.TaskIdentifier("task-" + ns)); ok {
			t.Fatalf("task for %s still in store, want evicted", ns)
		}
	}
}

func TestJobWatcherIgnoresUnconfiguredNamespace(t *testing.T) {
	ctx := context.Background()
	provider := &namespaceTestProvider{}
	provider.setItems(configNamed("a", "ns-a"))
	publisher := &fakeResultPublisher{}
	w := newNamespaceTestWatcher(t, provider, publisher)
	w.newNamespaceInformer = syncedInformerFactory()
	if err := w.reconcileNamespaceSet(ctx); err != nil {
		t.Fatalf("reconcile namespace set: %v", err)
	}

	taskID := lib.TaskIdentifier("unconfigured-task")
	w.taskStore.Store(taskID, lib.Task{TaskIdentifier: taskID})
	w.HandleJob(ctx, failedJob("job-other", string(taskID), "dev-other"))

	if publisher.PublishFailureCallCount() != 0 {
		t.Fatalf("publish failure calls = %d, want 0", publisher.PublishFailureCallCount())
	}
	if _, ok := w.taskStore.Load(taskID); !ok {
		t.Fatalf("task %s evicted from store, want present", taskID)
	}
}

func TestJobWatcherFailsLoudly(t *testing.T) {
	t.Run("config provider error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		provider := &namespaceTestProvider{}
		provider.setErr(stderrors.New("config provider down"))
		w := newNamespaceTestWatcher(t, provider, &fakeResultPublisher{})
		if err := w.Run(ctx); err == nil {
			t.Fatalf("Run returned nil, want provider error")
		}
	})

	t.Run("informer construction error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		provider := &namespaceTestProvider{}
		provider.setItems(configNamed("a", "ns-a"))
		w := newNamespaceTestWatcher(t, provider, &fakeResultPublisher{})
		w.newNamespaceInformer = func(
			kubernetes.Interface,
			libk8s.Namespace,
		) (jobWatcherNamespaceInformer, error) {
			return nil, stderrors.New("informer construction failed")
		}
		if err := w.Run(ctx); err == nil {
			t.Fatalf("Run returned nil, want informer construction error")
		}
	})

	t.Run("job handler registration error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		provider := &namespaceTestProvider{}
		provider.setItems(configNamed("a", "ns-a"))
		w := newNamespaceTestWatcher(t, provider, &fakeResultPublisher{})
		w.newNamespaceInformer = func(
			kubernetes.Interface,
			libk8s.Namespace,
		) (jobWatcherNamespaceInformer, error) {
			return &fakeNamespaceInformer{
				synced:    true,
				addJobErr: stderrors.New("job handler registration failed"),
			}, nil
		}
		if err := w.Run(ctx); err == nil {
			t.Fatalf("Run returned nil, want job handler registration error")
		}
	})

	t.Run("pod handler registration error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		provider := &namespaceTestProvider{}
		provider.setItems(configNamed("a", "ns-a"))
		w := newNamespaceTestWatcher(t, provider, &fakeResultPublisher{})
		w.newNamespaceInformer = func(
			kubernetes.Interface,
			libk8s.Namespace,
		) (jobWatcherNamespaceInformer, error) {
			return &fakeNamespaceInformer{
				synced:    true,
				addPodErr: stderrors.New("pod handler registration failed"),
			}, nil
		}
		if err := w.Run(ctx); err == nil {
			t.Fatalf("Run returned nil, want pod handler registration error")
		}
	})
}

// namespaceInformerRegistry records how many times an informer pair was
// constructed per namespace, so a test can prove that a namespace already
// being watched is not restarted by the re-resolution tick.
type namespaceInformerRegistry struct {
	mu            sync.Mutex
	informers     map[libk8s.Namespace]*fakeNamespaceInformer
	constructions map[libk8s.Namespace]int
}

func newNamespaceInformerRegistry() *namespaceInformerRegistry {
	return &namespaceInformerRegistry{
		informers:     map[libk8s.Namespace]*fakeNamespaceInformer{},
		constructions: map[libk8s.Namespace]int{},
	}
}

func (r *namespaceInformerRegistry) new(
	_ kubernetes.Interface,
	namespace libk8s.Namespace,
) (jobWatcherNamespaceInformer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.constructions[namespace]++
	informer := &fakeNamespaceInformer{synced: true}
	r.informers[namespace] = informer
	return informer, nil
}

func (r *namespaceInformerRegistry) count(namespace libk8s.Namespace) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.constructions[namespace]
}

func (r *namespaceInformerRegistry) constructed(namespaces ...libk8s.Namespace) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, namespace := range namespaces {
		if r.constructions[namespace] == 0 {
			return false
		}
	}
	return true
}

func waitForConstructedNamespaces(
	t *testing.T,
	registry *namespaceInformerRegistry,
	namespaces ...libk8s.Namespace,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !registry.constructed(namespaces...) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for namespaces %v", namespaceStrings(namespaces))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestJobWatcherReresolvesNamespaceSet(t *testing.T) {
	provider := &namespaceTestProvider{}
	provider.setItems(configNamed("one", "ns-one"))
	registry := newNamespaceInformerRegistry()

	w, ok := NewJobWatcher(
		fake.NewClientset(),
		"dev",
		provider,
		NewTaskStore(),
		&fakeResultPublisher{},
		nil,
		libtime.NewCurrentDateTime(),
	).(*jobWatcher)
	if !ok {
		t.Fatalf("NewJobWatcher did not return *jobWatcher")
	}
	w.newNamespaceInformer = registry.new

	old := jobWatcherNamespaceReresolveInterval
	jobWatcherNamespaceReresolveInterval = 10 * time.Millisecond
	t.Cleanup(func() { jobWatcherNamespaceReresolveInterval = old })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(ctx)
	}()

	waitForConstructedNamespaces(t, registry, "dev", "ns-one")

	provider.setErr(stderrors.New("config provider down"))
	time.Sleep(35 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("Run returned on a tick provider error: %v", err)
	default:
	}
	provider.setErr(nil)

	provider.setItems(configNamed("one", "ns-one"), configNamed("two", "ns-two"))
	waitForConstructedNamespaces(t, registry, "ns-two")

	if got := registry.count("ns-two"); got != 1 {
		t.Fatalf("ns-two constructed %d times, want 1", got)
	}
	if got := registry.count("ns-one"); got != 1 {
		t.Fatalf("ns-one constructed %d times, want 1", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error after cancellation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return after context cancellation")
	}
}

func TestJobWatcherAwaitsCacheSync(t *testing.T) {
	ctx := context.Background()
	provider := &namespaceTestProvider{}
	provider.setItems(
		configNamed("a", "ns-a"),
		configNamed("b", "ns-b"),
	)
	publisher := &fakeResultPublisher{}
	w := newNamespaceTestWatcher(t, provider, publisher)
	w.newNamespaceInformer = func(
		_ kubernetes.Interface,
		ns libk8s.Namespace,
	) (jobWatcherNamespaceInformer, error) {
		return &fakeNamespaceInformer{synced: ns != "ns-a"}, nil
	}
	if err := w.reconcileNamespaceSet(ctx); err != nil {
		t.Fatalf("reconcile namespace set: %v", err)
	}

	unsyncedTask := lib.TaskIdentifier("unsynced-task")
	w.taskStore.Store(unsyncedTask, lib.Task{TaskIdentifier: unsyncedTask})
	w.HandleJob(ctx, failedJob("job-unsynced", string(unsyncedTask), "ns-a"))
	if publisher.PublishFailureCallCount() != 0 {
		t.Fatalf("publish failure calls = %d for unsynced namespace, want 0",
			publisher.PublishFailureCallCount())
	}
	if _, ok := w.taskStore.Load(unsyncedTask); !ok {
		t.Fatalf("task %s evicted from unsynced namespace, want present", unsyncedTask)
	}

	syncedTask := lib.TaskIdentifier("synced-task")
	w.taskStore.Store(syncedTask, lib.Task{TaskIdentifier: syncedTask})
	w.HandleJob(ctx, failedJob("job-synced", string(syncedTask), "ns-b"))
	if publisher.PublishFailureCallCount() != 1 {
		t.Fatalf("publish failure calls = %d for synced namespace, want 1",
			publisher.PublishFailureCallCount())
	}
}

// namespacePodLister builds a corev1listers.PodLister backed by a fake informer
// indexer seeded with the given pods in namespace.
func namespacePodLister(namespace string, pods ...*corev1.Pod) corev1listers.PodLister {
	fakeClient := fake.NewSimpleClientset()
	informerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(
		fakeClient,
		0,
		k8sinformers.WithNamespace(namespace),
	)
	for _, pod := range pods {
		_ = informerFactory.Core().V1().Pods().Informer().GetIndexer().Add(pod)
	}
	return informerFactory.Core().V1().Pods().Lister()
}

// configFor builds a Config with an assignee and jobNamespace.
func configFor(name, assignee, jobNamespace string) agentv1.Config {
	return agentv1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: agentv1.ConfigSpec{
			Assignee:     assignee,
			JobNamespace: jobNamespace,
		},
	}
}

func ptrInt32(v int32) *int32 {
	return &v
}

func runningPod(name, namespace string, taskID lib.TaskIdentifier) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{jobWatcherTaskIDLabel: string(taskID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func oomPodOwnedBy(name, namespace, jobName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "Job", Name: jobName},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   "OOMKilled",
							ExitCode: 137,
						},
					},
				},
			},
		},
	}
}

func hasPodNamed(pods []*corev1.Pod, name string) bool {
	for _, pod := range pods {
		if pod.Name == name {
			return true
		}
	}
	return false
}

type namespacePodListerFixture struct {
	watcher         *jobWatcher
	publisher       *fakeResultPublisher
	taskStore       *TaskStore
	taskID          lib.TaskIdentifier
	currentDateTime libtime.CurrentDateTime
	eventHandlerCfg EventHandlerConfig
}

// newNamespacePodListerInformer returns an informer factory that seeds ns-a
// with the pods the per-namespace tests need and leaves ns-b unsynced.
func newNamespacePodListerInformer(
	taskID lib.TaskIdentifier,
) func(kubernetes.Interface, libk8s.Namespace) (jobWatcherNamespaceInformer, error) {
	return func(
		_ kubernetes.Interface,
		ns libk8s.Namespace,
	) (jobWatcherNamespaceInformer, error) {
		switch ns.String() {
		case "ns-a":
			return &fakeNamespaceInformer{
				synced: true,
				podLister: namespacePodLister(
					"ns-a",
					runningPod("pod-ns-a", "ns-a", "other-task"),
					oomPodOwnedBy("pod-oom-a", "ns-a", "job-a"),
					runningPod("pod-sweep", "ns-a", taskID),
				),
			}, nil
		case "dev":
			return &fakeNamespaceInformer{
				synced:    true,
				podLister: namespacePodLister("dev", runningPod("pod-dev", "dev", "other-task")),
			}, nil
		default:
			return &fakeNamespaceInformer{synced: false}, nil
		}
	}
}

func newNamespacePodListerFixture(
	t *testing.T,
	ctx context.Context,
) *namespacePodListerFixture {
	t.Helper()
	taskID := lib.TaskIdentifier("sweep-task")
	provider := &namespaceTestProvider{}
	provider.setItems(
		configFor("a", "agent-a", "ns-a"),
		configFor("b", "agent-b", "ns-b"),
		configFor("bad", "agent-bad", "dev"),
		configFor("dup1", "agent-dup", "ns-a"),
		configFor("dup2", "agent-dup", "ns-b"),
	)
	publisher := &fakeResultPublisher{}
	w := newNamespaceTestWatcher(t, provider, publisher)
	w.newNamespaceInformer = newNamespacePodListerInformer(taskID)
	if err := w.reconcileNamespaceSet(ctx); err != nil {
		t.Fatalf("reconcile namespace set: %v", err)
	}

	startedAt, err := libtime.ParseDateTime(ctx, "2026-06-01T11:55:00Z")
	if err != nil {
		t.Fatalf("parse job_started_at: %v", err)
	}
	w.taskStore.Store(taskID, lib.Task{
		TaskIdentifier: taskID,
		Frontmatter: lib.TaskFrontmatter{
			"assignee":       "agent-a",
			"current_job":    "job-a",
			"job_started_at": startedAt.Format(time.RFC3339),
		},
	})

	currentDateTime := libtime.NewCurrentDateTime()
	now, err := libtime.ParseDateTime(ctx, "2026-06-01T12:00:00Z")
	if err != nil {
		t.Fatalf("parse now: %v", err)
	}
	currentDateTime.SetNow(*now)
	eventHandlerCfg := NewEventHandlerConfig()
	_ = eventHandlerCfg.OnAdd(ctx, agentv1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: "sweep-cfg"},
		Spec: agentv1.ConfigSpec{
			Assignee:                "agent-a",
			JobNamespace:            "ns-a",
			ZombieJobTimeoutSeconds: ptrInt32(60),
		},
	})

	return &namespacePodListerFixture{
		watcher:         w,
		publisher:       publisher,
		taskStore:       w.taskStore,
		taskID:          taskID,
		currentDateTime: currentDateTime,
		eventHandlerCfg: eventHandlerCfg,
	}
}

func TestPodListerPerNamespace(t *testing.T) {
	ctx := context.Background()
	f := newNamespacePodListerFixture(t, ctx)

	lister := f.watcher.PodLister("ns-a")
	if lister == nil {
		t.Fatalf("PodLister(ns-a) = nil, want the ns-a lister")
	}
	pods, err := lister.Pods("ns-a").List(labels.Everything())
	if err != nil {
		t.Fatalf("list ns-a pods: %v", err)
	}
	if !hasPodNamed(pods, "pod-ns-a") {
		t.Fatalf("ns-a lister missing pod-ns-a")
	}
	if hasPodNamed(pods, "pod-dev") {
		t.Fatalf("ns-a lister leaked the dev lister's pod-dev")
	}
	if lister := f.watcher.PodLister("ns-b"); lister != nil {
		t.Fatalf("PodLister(ns-b) = non-nil, want nil for an unsynced namespace")
	}

	assertTerminalPodDetailFollowsJobNamespace(t, f.watcher)
	assertResolveTaskNamespace(t, ctx)
	assertSweeperUsesPerTaskLister(t, ctx, f)
}

func assertTerminalPodDetailFollowsJobNamespace(t *testing.T, w *jobWatcher) {
	t.Helper()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "job-a", Namespace: "ns-a"},
	}
	reason, exitCode, ok := w.terminalPodDetail(job)
	if !ok {
		t.Fatalf("terminalPodDetail(job-a) ok = false, want true from the ns-a lister")
	}
	if reason != "OOMKilled" || exitCode != 137 {
		t.Fatalf("terminalPodDetail(job-a) = %s/%d, want OOMKilled/137", reason, exitCode)
	}
}

func assertResolveTaskNamespace(t *testing.T, ctx context.Context) {
	t.Helper()
	cfgs := []agentv1.Config{
		configFor("a", "agent-a", "ns-a"),
		configFor("b", "agent-b", "ns-b"),
		configFor("bad", "agent-bad", "dev"),
		configFor("dup1", "agent-dup", "ns-a"),
		configFor("dup2", "agent-dup", "ns-b"),
	}
	taskWithAssignee := func(assignee string) lib.Task {
		return lib.Task{Frontmatter: lib.TaskFrontmatter{"assignee": assignee}}
	}
	if got := resolveTaskNamespace(ctx, "dev", taskWithAssignee("agent-a"), cfgs); got != "ns-a" {
		t.Fatalf("agent-a resolves to %q, want ns-a", got)
	}
	if got := resolveTaskNamespace(ctx, "dev", taskWithAssignee("nobody"), cfgs); got != "dev" {
		t.Fatalf("unknown assignee resolves to %q, want dev", got)
	}
	if got := resolveTaskNamespace(ctx, "dev", taskWithAssignee("agent-bad"), cfgs); got != "dev" {
		t.Fatalf("resolution-error fallback resolves to %q, want dev", got)
	}
	if got := resolveTaskNamespace(ctx, "dev", taskWithAssignee("agent-dup"), cfgs); got != "ns-a" {
		t.Fatalf("duplicate assignee resolves to %q, want the first match ns-a", got)
	}
}

func assertSweeperUsesPerTaskLister(
	t *testing.T,
	ctx context.Context,
	f *namespacePodListerFixture,
) {
	t.Helper()
	sweeper := NewZombieSweeper(
		f.watcher,
		"dev",
		f.taskStore,
		f.publisher,
		f.eventHandlerCfg,
		f.currentDateTime,
	)
	if err := sweeper.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if f.publisher.PublishFailureCallCount() != 0 {
		t.Fatalf(
			"published %d failures, want 0: the sweeper must read the ns-a lister, not dev's",
			f.publisher.PublishFailureCallCount(),
		)
	}
}
