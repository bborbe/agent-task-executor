// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

//counterfeiter:generate -o ../mocks/job_watcher.go --fake-name FakeJobWatcher . JobWatcher

// JobWatcher watches batch/v1 Jobs and their Pods in the executor's namespace and
// in every namespace a Config names, and publishes synthetic failure results for
// terminal-state objects that belong to spawned tasks.
type JobWatcher interface {
	// Run starts the Job and Pod informers and blocks until ctx is cancelled.
	Run(ctx context.Context) error
	// HandleJob processes a single Job (invoked by the informer event handlers
	// and by unit tests directly, avoiding the need for a fake informer).
	HandleJob(ctx context.Context, job *batchv1.Job)
	// HandlePod processes a single Pod (invoked by the Pod informer event handler
	// and by unit tests directly).
	HandlePod(ctx context.Context, pod *corev1.Pod)
	// PodLister returns the Pod lister backed by namespace's informer cache, or
	// nil when that namespace is not watched or its cache has not synced. The
	// returned lister is safe for concurrent read access.
	PodLister(namespace libk8s.Namespace) corev1listers.PodLister
}

// NewJobWatcher creates a JobWatcher. The executor namespace is watched
// unconditionally; the provider supplies the Configs whose resolved
// jobNamespace values extend that set.
func NewJobWatcher(
	kubeClient kubernetes.Interface,
	namespace libk8s.Namespace,
	configProvider libk8s.Provider[agentv1.Config],
	taskStore *TaskStore,
	publisher ResultPublisher,
	claimReleaser ClaimReleaser,
	currentDateTimeGetter libtime.CurrentDateTimeGetter,
) JobWatcher {
	w := &jobWatcher{
		kubeClient:            kubeClient,
		namespace:             namespace,
		configProvider:        configProvider,
		taskStore:             taskStore,
		publisher:             publisher,
		claimReleaser:         claimReleaser,
		currentDateTimeGetter: currentDateTimeGetter,
		startedAt:             currentDateTimeGetter.Now(),
		newNamespaceInformer:  newJobWatcherNamespaceInformer,
	}
	w.watched = map[libk8s.Namespace]bool{namespace: true}
	w.synced = map[libk8s.Namespace]bool{namespace: true}
	w.informers = map[libk8s.Namespace]jobWatcherNamespaceInformer{}
	return w
}

type jobWatcher struct {
	kubeClient     kubernetes.Interface
	namespace      libk8s.Namespace
	configProvider libk8s.Provider[agentv1.Config]
	taskStore      *TaskStore
	publisher      ResultPublisher
	claimReleaser  ClaimReleaser
	// currentDateTimeGetter and startedAt bound which failed Jobs may be
	// published without a store entry: see publishStoreMissFailure.
	currentDateTimeGetter libtime.CurrentDateTimeGetter
	startedAt             libtime.DateTime
	mu                    sync.Mutex
	watched               map[libk8s.Namespace]bool
	synced                map[libk8s.Namespace]bool
	informers             map[libk8s.Namespace]jobWatcherNamespaceInformer
	newNamespaceInformer  func(kubernetes.Interface, libk8s.Namespace) (jobWatcherNamespaceInformer, error)
}

const (
	jobWatcherResyncPeriod = 5 * time.Minute
	jobWatcherTaskIDLabel  = "agent.benjamin-borbe.de/task-id"
)

// jobWatcherNamespaceReresolveInterval is how often the watched namespace set
// is re-derived from the Configs. It is a package-level variable, not
// configuration and not an injected clock, so tests can shorten it directly.
var jobWatcherNamespaceReresolveInterval = 60 * time.Second

// jobWatcherNamespaceInformer is one namespace's Job and Pod informers. It is
// an interface so the namespace-set tests can count starts and control the
// cache-sync barrier without an API server.
type jobWatcherNamespaceInformer interface {
	Start(ctx context.Context)
	HasSynced() bool
	AddJobEventHandler(handler cache.ResourceEventHandler) error
	AddPodEventHandler(handler cache.ResourceEventHandler) error
	PodLister() corev1listers.PodLister
}

type sharedInformerNamespaceInformer struct {
	factory k8sinformers.SharedInformerFactory
	job     cache.SharedIndexInformer
	pod     cache.SharedIndexInformer
}

func (s *sharedInformerNamespaceInformer) Start(ctx context.Context) {
	s.factory.Start(ctx.Done())
}

func (s *sharedInformerNamespaceInformer) HasSynced() bool {
	return s.job.HasSynced() && s.pod.HasSynced()
}

func (s *sharedInformerNamespaceInformer) AddJobEventHandler(
	handler cache.ResourceEventHandler,
) error {
	_, err := s.job.AddEventHandler(handler)
	return err
}

func (s *sharedInformerNamespaceInformer) AddPodEventHandler(
	handler cache.ResourceEventHandler,
) error {
	_, err := s.pod.AddEventHandler(handler)
	return err
}

func (s *sharedInformerNamespaceInformer) PodLister() corev1listers.PodLister {
	return s.factory.Core().V1().Pods().Lister()
}

var newJobWatcherNamespaceInformer = func(
	kubeClient kubernetes.Interface,
	namespace libk8s.Namespace,
) (jobWatcherNamespaceInformer, error) {
	factory := k8sinformers.NewSharedInformerFactoryWithOptions(
		kubeClient,
		jobWatcherResyncPeriod,
		k8sinformers.WithNamespace(namespace.String()),
		k8sinformers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = jobWatcherTaskIDLabel
		}),
	)
	return &sharedInformerNamespaceInformer{
		factory: factory,
		job:     factory.Batch().V1().Jobs().Informer(),
		pod:     factory.Core().V1().Pods().Informer(),
	}, nil
}

// resolveJobWatcherNamespaces returns the namespaces the watcher must observe:
// the executor's own namespace unconditionally, plus the resolved jobNamespace
// of every Config. Duplicates collapse. A Config whose jobNamespace fails
// resolution is skipped with an ERROR naming the Config and the rejected value;
// it spawns no Jobs in that state, and one bad Config must not stop the watcher
// for every other namespace.
func resolveJobWatcherNamespaces(
	ctx context.Context,
	executorNamespace libk8s.Namespace,
	cfgs []agentv1.Config,
) []libk8s.Namespace {
	seen := map[libk8s.Namespace]struct{}{executorNamespace: {}}
	for _, cfg := range cfgs {
		namespace, err := ResolveJobNamespace(ctx, executorNamespace, cfg.Spec.JobNamespace)
		if err != nil {
			glog.Errorf("job watcher: skip config %s: %v", cfg.Name, err)
			continue
		}
		seen[namespace] = struct{}{}
	}
	namespaces := make([]libk8s.Namespace, 0, len(seen))
	for namespace := range seen {
		namespaces = append(namespaces, namespace)
	}
	sort.Slice(namespaces, func(i, j int) bool {
		return namespaces[i].String() < namespaces[j].String()
	})
	return namespaces
}

func (w *jobWatcher) Run(ctx context.Context) error {
	if err := w.reconcileNamespaceSet(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(jobWatcherNamespaceReresolveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			cfgs, err := w.configProvider.Get(ctx)
			if err != nil {
				// The Config provider is populated asynchronously and can fail
				// transiently; the already-running informers keep watching and
				// the next tick retries. Killing the executor over this is the
				// worse outcome.
				glog.Errorf("job watcher: re-resolve namespace set: %v", err)
				continue
			}
			if err := w.startInformersForNamespaces(
				ctx,
				resolveJobWatcherNamespaces(ctx, w.namespace, cfgs),
			); err != nil {
				// A wiring failure while starting a new namespace's informers
				// is deterministic and must fail the run, exactly as it does at
				// startup.
				return errors.Wrapf(ctx, err, "start informers for re-resolved namespaces")
			}
		}
	}
}

func (w *jobWatcher) reconcileNamespaceSet(ctx context.Context) error {
	cfgs, err := w.configProvider.Get(ctx)
	if err != nil {
		return errors.Wrapf(ctx, err, "list configs for job watcher namespaces")
	}
	return w.startInformersForNamespaces(ctx, resolveJobWatcherNamespaces(ctx, w.namespace, cfgs))
}

func (w *jobWatcher) startInformersForNamespaces(
	ctx context.Context,
	namespaces []libk8s.Namespace,
) error {
	for _, namespace := range namespaces {
		if w.informerStarted(namespace) {
			continue
		}
		if err := w.startNamespaceInformer(ctx, namespace); err != nil {
			return err
		}
	}
	return nil
}

func (w *jobWatcher) informerStarted(namespace libk8s.Namespace) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.informers[namespace]
	return ok
}

func (w *jobWatcher) startNamespaceInformer(
	ctx context.Context,
	namespace libk8s.Namespace,
) error {
	informer, err := w.newNamespaceInformer(w.kubeClient, namespace)
	if err != nil {
		return errors.Wrapf(ctx, err, "create informers for namespace %s", namespace)
	}
	if err := informer.AddJobEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			job, ok := obj.(*batchv1.Job)
			if !ok {
				return
			}
			w.HandleJob(ctx, job)
		},
		UpdateFunc: func(_, newObj interface{}) {
			job, ok := newObj.(*batchv1.Job)
			if !ok {
				return
			}
			w.HandleJob(ctx, job)
		},
	}); err != nil {
		return errors.Wrapf(ctx, err, "add job event handler for namespace %s", namespace)
	}
	if err := informer.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return
			}
			w.HandlePod(ctx, pod)
		},
		UpdateFunc: func(_, newObj interface{}) {
			pod, ok := newObj.(*corev1.Pod)
			if !ok {
				return
			}
			w.HandlePod(ctx, pod)
		},
	}); err != nil {
		return errors.Wrapf(ctx, err, "add pod event handler for namespace %s", namespace)
	}
	w.mu.Lock()
	w.watched[namespace] = true
	w.synced[namespace] = false
	w.informers[namespace] = informer
	w.mu.Unlock()
	informer.Start(ctx)
	glog.V(2).Infof("job and pod informers started in namespace %s", namespace)
	return nil
}

// namespaceReady reports whether the watcher may handle events for namespace.
// A namespace no Config names is never ready. Before an informer is started
// for a watched namespace, the pre-Run seed decides; once it is started, the
// namespace's own cache-sync state decides.
func (w *jobWatcher) namespaceReady(namespace libk8s.Namespace) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.watched[namespace] {
		return false
	}
	informer, ok := w.informers[namespace]
	if !ok {
		return w.synced[namespace]
	}
	if w.synced[namespace] {
		return true
	}
	if informer.HasSynced() {
		w.synced[namespace] = true
		return true
	}
	return false
}

func (w *jobWatcher) PodLister(namespace libk8s.Namespace) corev1listers.PodLister {
	return w.podListerForNamespace(namespace)
}

func (w *jobWatcher) podListerForNamespace(
	namespace libk8s.Namespace,
) corev1listers.PodLister {
	w.mu.Lock()
	defer w.mu.Unlock()
	informer, ok := w.informers[namespace]
	if !ok {
		return nil
	}
	if !w.synced[namespace] {
		if !informer.HasSynced() {
			return nil
		}
		w.synced[namespace] = true
	}
	return informer.PodLister()
}

// releaseClaim releases this executor's held claim for taskID. A nil releaser
// (claim mode off) is a no-op. ReleaseTaskClaim logs its own failures; the
// lease loop's retryPendingReleases retries a failed delete on its next tick.
//
// A store entry that tracks a NEWER job means this is a stale re-delivery of a
// superseded Job's terminal event (the informer re-delivers terminal Jobs every
// ~5 min). Releasing there would delete the claim taken for the job that is
// still running, leaving it unclaimed for a peer to take over. An absent entry
// is not stale — after a restart the claim must still be released.
func (w *jobWatcher) releaseClaim(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	jobName string,
) {
	if !w.claimReleaseAllowed(taskID, jobName) {
		return
	}
	w.claimReleaser.ReleaseTaskClaim(ctx, taskID)
}

// releaseClaimAfterSuccess holds the claim for the success grace window rather
// than deleting it, so a peer cluster cannot claim the task while this cluster's
// result is still on its way to the vault. See ReleaseTaskClaimAfterSuccess.
func (w *jobWatcher) releaseClaimAfterSuccess(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	jobName string,
	phase string,
	completedAt time.Time,
) {
	if !w.claimReleaseAllowed(taskID, jobName) {
		return
	}
	w.claimReleaser.ReleaseTaskClaimAfterSuccess(ctx, taskID, phase, completedAt)
}

// releaseSucceededJobClaim releases the claim of a succeeded Job, holding it for
// the success grace window instead of deleting it.
//
// A succeeded Job is not yet a finished task: the agent published its result, but
// the vault learns about it on the controller's own cycle, and until it does a
// peer cluster still reads the task as eligible with no record that it just ran.
// Holding the claim across that gap makes the peer's Claim fail rather than
// running the task a second time. CompletionTime is what the hold is measured
// from; a Complete Job always has one, and without it the safe fallback is the
// immediate release rather than an invented timestamp.
func (w *jobWatcher) releaseSucceededJobClaim(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	job *batchv1.Job,
) {
	if job.Status.CompletionTime == nil {
		w.releaseClaim(ctx, taskID, job.Name)
		return
	}
	w.releaseClaimAfterSuccess(
		ctx,
		taskID,
		job.Name,
		job.Annotations[TaskPhaseAnnotationKey],
		job.Status.CompletionTime.Time,
	)
}

// claimReleaseAllowed reports whether this Job's terminal event may release the
// task's claim at all, and logs why when it may not.
func (w *jobWatcher) claimReleaseAllowed(
	taskID lib.TaskIdentifier,
	jobName string,
) bool {
	if w.claimReleaser == nil {
		return false
	}
	if task, ok := w.taskStore.Load(taskID); ok && !w.tracksJob(task, jobName) {
		glog.V(2).Infof(
			"skip claim release for task %s: job %s superseded by %s",
			taskID, jobName, task.Frontmatter.CurrentJob(),
		)
		return false
	}
	return true
}

func (w *jobWatcher) HandleJob(ctx context.Context, job *batchv1.Job) {
	if !w.namespaceReady(libk8s.Namespace(job.Namespace)) {
		return
	}
	taskIDStr, ok := job.Labels["agent.benjamin-borbe.de/task-id"]
	if !ok || taskIDStr == "" {
		return
	}
	taskID := lib.TaskIdentifier(taskIDStr)

	if IsJobFailed(job) {
		reason := w.failureReason(job)
		// The raw JobFailed condition is logged alongside the published reason:
		// the published value is what operators read, but the condition is what
		// tells them which Kubernetes failure they are looking at, and it is not
		// otherwise recoverable — failed Jobs are not retained, and the record
		// store carries only the collapsed reason.
		condition, _ := jobFailedCondition(job)
		glog.V(2).Infof(
			"job %s/%s failed (task %s): reason=%s condition=%s",
			job.Namespace, job.Name, taskID, reason, condition,
		)
		w.releaseClaim(ctx, taskID, job.Name)
		w.handleTerminal(ctx, taskID, job, reason, true)
		return
	}
	if IsJobSucceeded(job) {
		// Succeeded job (exit 0) means the agent's main.Run returned nil, which
		// implies its result was published to Kafka successfully. Do NOT publish
		// a synthetic failure — doing so races the real result and triggers an
		// infinite respawn loop via the vault poll on the controller side.
		// But the vault task still carries current_job/job_started_at from the
		// spawn notification, and nobody clears them on the success path (the
		// controller's result_writer writes phase/status/notes only). Without a
		// clear here the task would sit terminal with a stale current_job that
		// the zombie sweeper can never classify — evicting the task from the
		// store on success makes it invisible to Snapshot(). Publish the clear
		// (deduped per job) before eviction.
		glog.V(2).
			Infof("job %s/%s succeeded (task %s): trusting agent publish, no synthetic result",
				job.Namespace, job.Name, taskID)
		// Remembered before the store check: the task entry may already be
		// evicted (or, after an executor restart, never stored), but the spawn
		// path still needs to know a result is on its way to the vault.
		// CompletionTime, not now, so a re-delivered old Job keeps its age.
		if job.Status.CompletionTime != nil {
			w.taskStore.MarkSucceeded(
				taskID,
				job.Annotations[TaskPhaseAnnotationKey],
				job.Status.CompletionTime.Time,
			)
		}
		w.releaseSucceededJobClaim(ctx, taskID, job)
		if task, ok := w.taskStore.Load(taskID); ok {
			w.handleSucceeded(ctx, taskID, task, job)
		}
	}
}

// handleSucceeded publishes the current_job clear for a succeeded job and evicts
// the store entry — but only when the entry tracks THIS job, or the tracked job
// is no longer active. The informer re-delivers a terminal Job every ~5 min;
// when the task has since been re-dispatched and re-spawned under a NEWER job,
// the stale success re-delivery must not evict the entry the current job's
// success path needs — the respawned-second-success race via the job informer
// (observed prod 2026-09-04 on task 67120247: job-1's re-delivered success
// evicted job-2's store entry). The mirror case (observed prod 2026-09-05 on
// task e79ada78) is a stale STORE: a late first-job clear left the entry pinned
// to an already-terminal job while the second job's success arrives — tracksJob
// then compares storedJob (first job) vs eventJob (second job) and skips,
// pinning current_job to the dead job name. When the tracked job is no longer
// active, the entry is stale and must converge (evict), so the task's terminal
// state is not left pointing at a dead job.
func (w *jobWatcher) handleSucceeded(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	task lib.Task,
	job *batchv1.Job,
) {
	if err := w.publisher.PublishClearCurrentJob(ctx, task, job.Name); err != nil {
		// Keep the task in the store so the informer's terminal
		// re-delivery (~5 min) retries the clear. Evicting on failure
		// would drop the only cleanup path for current_job.
		glog.Errorf(
			"publish current_job clear for task %s (job %s): %v",
			taskID,
			job.Name,
			err,
		)
		return
	}
	if !w.tracksJob(task, job.Name) {
		// Mismatch between the store's current_job and the succeeded job. Two
		// orderings reach here: (a) the event is stale — a terminal re-delivery
		// for a previous job while a NEWER job still runs (PR #45, task
		// 67120247) — the entry must be kept for the running job's terminal
		// path; or (b) the store is stale — a late first-job clear left the
		// entry pinned to an already-terminal job while the second job's
		// success arrives (task e79ada78) — the entry must converge. Distinguish
		// by liveness: if any job for the task is still active, keep the entry;
		// otherwise the tracked current_job is terminal/gone and the stale
		// entry is evicted.
		active, err := w.taskHasActiveJob(ctx, job.Namespace, taskID)
		if err != nil {
			glog.Errorf(
				"check active job for task %s after tracksJob mismatch: %v",
				taskID,
				err,
			)
			return
		}
		if active {
			return
		}
	}
	w.taskStore.Delete(taskID)
}

// taskHasActiveJob reports whether any non-terminal Job exists for the task.
// Mirrors jobSpawner.IsJobActive: a Job is terminal (not active) when it carries
// a Failed/Succeeded condition, has Succeeded>0, or has Failed>0 with no active
// pods. Used by handleSucceeded/publishSyntheticFailure to tell a stale terminal
// event (task still running, entry must be kept) from a stale store entry (task
// terminal, entry pinned to a dead job name, entry must converge).
func (w *jobWatcher) taskHasActiveJob(
	ctx context.Context,
	namespace string,
	taskID lib.TaskIdentifier,
) (bool, error) {
	jobs, err := w.kubeClient.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "agent.benjamin-borbe.de/task-id=" + string(taskID),
	})
	if err != nil {
		return false, errors.Wrapf(ctx, err, "list jobs for task %s", taskID)
	}
	for _, job := range jobs.Items {
		select {
		case <-ctx.Done():
			return false, errors.Wrapf(ctx, ctx.Err(), "list jobs for task %s cancelled", taskID)
		default:
		}
		if IsJobFailed(&job) || IsJobSucceeded(&job) {
			continue
		}
		if job.Status.Succeeded > 0 {
			continue
		}
		if job.Status.Failed > 0 && job.Status.Active == 0 {
			continue
		}
		return true, nil
	}
	return false, nil
}

// tracksJob reports whether the store entry's recorded current_job matches the
// job being processed (or records no job at all, defensively). A mismatch means
// the task was re-spawned under a newer job since this job's terminal event was
// emitted, so the event is stale and must not evict the entry the newer job's
// terminal path relies on.
func (w *jobWatcher) tracksJob(task lib.Task, jobName string) bool {
	currentJob := task.Frontmatter.CurrentJob()
	return currentJob == "" || currentJob == jobName
}

// handleTerminal publishes a synthetic failure when appropriate. Job cleanup is
// handled by K8s via TTLSecondsAfterFinished (set on the spawned Job), so the
// pod/log stays available until TTL expires for debugging.
// alwaysPublish is true for Failed jobs; for Succeeded jobs it only publishes if
// the task is still in the TaskStore (agent has not yet published a result).
func (w *jobWatcher) handleTerminal(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	job *batchv1.Job,
	reason string,
	alwaysPublish bool,
) {
	task, ok := w.taskStore.Load(taskID)
	if ok {
		w.publishSyntheticFailure(ctx, taskID, task, job, reason)
		return
	}
	if alwaysPublish && w.publishStoreMissFailure(ctx, taskID, job, reason) {
		return
	}
	w.logMissingTask(taskID, job, alwaysPublish)
}

// storeMissPublishWindow bounds how old a failure may be and still be published
// without a store entry. It must stay below dedupeTTL: within the window every
// informer re-delivery of an already-published Job lands inside PublishFailure's
// per-job dedupe, and past the window the store-miss path stops publishing
// before the dedupe entry can expire. It must exceed jobWatcherResyncPeriod so a
// publish that failed is retried by the next re-delivery.
const storeMissPublishWindow = 30 * time.Minute

// publishStoreMissFailure publishes the failure of a Job whose task has already
// left the TaskStore, and reports whether it handled the Job.
//
// The store entry is evicted by any terminal-status event for the task, by a
// type mismatch, and by an executor restart, so a failed Job routinely outlives
// it. Dropping the failure there made crashes silent: task ad12e9fb (2026-10-28,
// pod_crash_no_stdout ContainerStatusUnknown/137) and task 9371a3d2 (2026-10-27)
// kept a dead current_job and never got a ## Failure section.
//
// A store miss is also the normal state for a failure that was ALREADY
// published — the store-hit path evicts after publishing, and the informer
// re-delivers every terminal Job each resync and re-lists all of them after a
// restart. Only a failure newer than this watcher's start and inside
// storeMissPublishWindow is published: an older one was either handled by this
// process (and is deduped) or belongs to a previous process, which keeps the
// old log-and-drop behaviour rather than risk a second ## Failure and a second
// trigger_count bump.
//
// A task with another Job still active is left alone: the failure clears
// current_job, and clearing it under a running Job would make the task look idle.
func (w *jobWatcher) publishStoreMissFailure(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	job *batchv1.Job,
	reason string,
) bool {
	failedAt, ok := jobFailedAt(job)
	if !ok || !w.freshStoreMissFailure(failedAt) {
		return false
	}
	active, err := w.taskHasActiveJob(ctx, job.Namespace, taskID)
	if err != nil {
		glog.Errorf("check active job for task %s before store-miss failure: %v", taskID, err)
		return false
	}
	if active {
		glog.V(2).Infof(
			"skip store-miss failure for task %s (job %s): another job is still active",
			taskID, job.Name,
		)
		return true
	}
	if err := w.publisher.PublishStoreMissFailure(ctx, taskFromFailedJob(taskID, job), job.Name, reason); err != nil {
		glog.Errorf(
			"publish synthetic failure for task %s (job %s) — task not in store: %v",
			taskID, job.Name, err,
		)
		return true
	}
	glog.V(2).Infof(
		"published synthetic failure for task %s (job %s) — task not in store",
		taskID, job.Name,
	)
	return true
}

// freshStoreMissFailure reports whether failedAt is after the watcher started
// and inside storeMissPublishWindow.
func (w *jobWatcher) freshStoreMissFailure(failedAt time.Time) bool {
	if !failedAt.After(w.startedAt.Time()) {
		return false
	}
	return w.currentDateTimeGetter.Now().Time().Sub(failedAt) < storeMissPublishWindow
}

// jobFailedAt returns when the Job failed: its JobFailed condition's
// LastTransitionTime, else its CompletionTime. ok is false when neither is set;
// an unknown failure time is treated as stale.
func jobFailedAt(job *batchv1.Job) (time.Time, bool) {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue &&
			!c.LastTransitionTime.IsZero() {
			return c.LastTransitionTime.Time, true
		}
	}
	if job.Status.CompletionTime != nil && !job.Status.CompletionTime.IsZero() {
		return job.Status.CompletionTime.Time, true
	}
	return time.Time{}, false
}

// taskFromFailedJob builds the minimal task PublishFailure needs when the
// original task content is gone.
//
//   - TaskIdentifier comes from the Job's task-id label.
//   - phase comes from the Job's phase annotation, so the re-drive streak and
//     reason refs are keyed exactly as on the store-hit path. A Job spawned before
//     the annotation existed carries none and records under the "none" phase.
//   - target_vault is left unset. The Job does not carry it, and the executor
//     serves every vault and has none of its own; an empty TargetVault is
//     processed by every controller, and those without the task file skip it.
//   - max_triggers is present with a nil value. crashFailureUpdates arms the
//     cap only when the key is absent, and the controller applies Updates
//     blindly, so an absent key here would overwrite an operator-set cap of 5 or
//     6 with the default 3. The real value is unknown, so the cap is not armed on
//     this path; the next failure seen with the task in the store arms it.
func taskFromFailedJob(taskID lib.TaskIdentifier, job *batchv1.Job) lib.Task {
	frontmatter := lib.TaskFrontmatter{"max_triggers": nil}
	if phase := job.Annotations[TaskPhaseAnnotationKey]; phase != "" {
		frontmatter["phase"] = phase
	}
	return lib.Task{
		TaskIdentifier: taskID,
		Frontmatter:    frontmatter,
	}
}

func (w *jobWatcher) publishSyntheticFailure(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	task lib.Task,
	job *batchv1.Job,
	reason string,
) {
	if err := w.publisher.PublishFailure(ctx, task, job.Name, reason); err != nil {
		glog.Errorf("publish synthetic failure for task %s (job %s): %v", taskID, job.Name, err)
	} else {
		glog.V(2).Infof("published synthetic failure for task %s (job %s)", taskID, job.Name)
	}
	// Same guard as the success path: a stale failure re-delivery for a previous
	// job must not evict the entry tracking the current (re-spawned) job — unless
	// the tracked job is itself no longer active, in which case the store entry
	// is stale and must converge (see handleSucceeded).
	if !w.tracksJob(task, job.Name) {
		active, err := w.taskHasActiveJob(ctx, job.Namespace, taskID)
		if err != nil {
			glog.Errorf(
				"check active job for task %s after tracksJob mismatch: %v",
				taskID,
				err,
			)
			return
		}
		if active {
			return
		}
	}
	w.taskStore.Delete(taskID)
}

func (w *jobWatcher) logMissingTask(
	taskID lib.TaskIdentifier,
	job *batchv1.Job,
	alwaysPublish bool,
) {
	if alwaysPublish {
		// V(3), not Warning: the Job informer re-delivers an already-handled terminal
		// Job every ~5 minutes, so a failed Job whose task was already deleted from the
		// store re-enters this branch repeatedly and the old Warning read as a failure.
		// Only failures publishStoreMissFailure judged stale reach here: they predate
		// this process or its publish window.
		glog.V(3).Infof(
			"task %s not in task store; job %s/%s failed but cannot publish synthetic failure (no original task content)",
			taskID,
			job.Namespace,
			job.Name,
		)
		return
	}
	glog.V(3).Infof(
		"task %s not in task store; job %s/%s succeeded — agent likely published result already",
		taskID, job.Namespace, job.Name,
	)
}

// IsJobFailed reports whether the Job carries a true JobFailed condition.
//
// Conditions, not the pod counters, are the source of truth: an
// activeDeadlineSeconds kill sets this condition while leaving
// .status.failed and .status.active at zero. Any caller deciding whether a
// Job is still running must use this (see spawner.IsJobActive) — reading the
// counters alone reports a deadline-killed Job as active forever.
func IsJobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// IsJobSucceeded reports whether the Job carries a true JobComplete condition.
func IsJobSucceeded(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// isBackoffLimitExceeded reports whether the Job's terminal Failed condition
// has Reason "BackoffLimitExceeded" (the Job exhausted its retry backoff
// limit).
func isBackoffLimitExceeded(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue &&
			c.Reason == "BackoffLimitExceeded" {
			return true
		}
	}
	return false
}

const (
	// jobFailedReasonless is published for a JobFailed condition whose Reason is
	// empty, so the record names something instead of collapsing to a blank.
	jobFailedReasonless = "JobFailed"
	// backoffLimitExceededReason is the JobFailed condition that reaches
	// backoffFailureReason.
	backoffLimitExceededReason = "BackoffLimitExceeded"
	// podUnresolvedMarker names the branch where the Job's pod is gone before
	// the watcher can read its termination state.
	podUnresolvedMarker = "pod-unresolved"
)

// JobFailureReason maps a failed Job's conditions to a ZombieReason. Returns
// ZombieReasonDeadlineExceeded when a Failed condition has Reason
// "DeadlineExceeded" (the kubelet killed the pod for running past
// activeDeadlineSeconds). Returns the generic pod-failure reason
// ZombieReasonPodCrashNoStdout for "BackoffLimitExceeded" (the Job exhausted
// its retry backoff limit — a distinct reason from a deadline kill; HandleJob
// resolves the pod's terminated reason + exit code and surfaces them in the
// published reason) and for any other Failed condition (the pod terminated
// non-zero and no AgentResult was observed; the Job-condition informer only
// fires AFTER terminal state, so absence of an AgentResult is implicit at this
// point).
func JobFailureReason(job *batchv1.Job) ZombieReason {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			switch c.Reason {
			case "DeadlineExceeded":
				return ZombieReasonDeadlineExceeded
			case "BackoffLimitExceeded":
				return ZombieReasonPodCrashNoStdout
			}
		}
	}
	return ZombieReasonPodCrashNoStdout
}

// failureReason returns the reason string to publish for a failed Job.
//
// A DeadlineExceeded failure publishes the vocabulary value unchanged. A
// BackoffLimitExceeded failure surfaces the pod's terminated reason + exit code
// when the pod is still resolvable, and names the condition plus an
// unresolved-pod marker when it is not (see backoffFailureReason). Every other
// failure publishes the Job's own JobFailed condition, with the pod's
// terminated reason + exit code appended when the pod is still resolvable (see
// fallthroughFailureReason) — the condition is what these records were
// previously missing.
func (w *jobWatcher) failureReason(job *batchv1.Job) string {
	if JobFailureReason(job) == ZombieReasonDeadlineExceeded {
		return ZombieReasonDeadlineExceeded.String()
	}
	if isBackoffLimitExceeded(job) {
		return w.backoffFailureReason(job)
	}
	return w.fallthroughFailureReason(job)
}

// backoffFailureReason classifies a BackoffLimitExceeded Job using its resolved
// pod's terminal state: the dedicated pod_oom_killed for an OOMKilled pod, else
// the generic pod-failure reason, always appending the terminated reason + exit
// code when a pod is resolvable so operators see the cause without inspecting
// the pod. When the pod is gone before the watcher reads it, the published
// value names the condition and says so — a bare pod_crash_no_stdout diagnoses
// nothing.
func (w *jobWatcher) backoffFailureReason(job *batchv1.Job) string {
	terminatedReason, exitCode, ok := w.terminalPodDetail(job)
	if !ok {
		return fmt.Sprintf("%s (%s)", backoffLimitExceededReason, podUnresolvedMarker)
	}
	if terminatedReason == "OOMKilled" {
		return fmt.Sprintf("%s (%s/%d)", ZombieReasonPodOOMKilled, terminatedReason, exitCode)
	}
	return fmt.Sprintf("%s (%s/%d)", ZombieReasonPodCrashNoStdout, terminatedReason, exitCode)
}

// fallthroughFailureReason names the Job's actual JobFailed condition,
// appending the pod's terminated reason and exit code when the pod is still
// resolvable. A condition whose Reason is empty publishes jobFailedReasonless
// rather than a blank, so the record always names something.
func (w *jobWatcher) fallthroughFailureReason(job *batchv1.Job) string {
	condition, ok := jobFailedCondition(job)
	if !ok {
		return ZombieReasonPodCrashNoStdout.String()
	}
	terminatedReason, exitCode, podOK := w.terminalPodDetail(job)
	if !podOK {
		return condition
	}
	return fmt.Sprintf("%s (%s/%d)", condition, terminatedReason, exitCode)
}

// jobFailedCondition returns the reason of the Job's true JobFailed condition.
// ok is false when the Job carries none. An empty Reason is reported as
// jobFailedReasonless so the published value is never blank.
func jobFailedCondition(job *batchv1.Job) (string, bool) {
	for _, c := range job.Status.Conditions {
		if c.Type != batchv1.JobFailed || c.Status != corev1.ConditionTrue {
			continue
		}
		if c.Reason == "" {
			return jobFailedReasonless, true
		}
		return c.Reason, true
	}
	return "", false
}

// HandlePod processes a Pod that has transitioned to a terminal failure state.
// It publishes a single zombie failure event and returns without deleting the
// task from the TaskStore — the Job-condition path or the deadline sweeper
// performs the final delete when terminal state is observed.
func (w *jobWatcher) HandlePod(ctx context.Context, pod *corev1.Pod) {
	if !w.namespaceReady(libk8s.Namespace(pod.Namespace)) {
		return
	}
	taskIDStr, ok := pod.Labels["agent.benjamin-borbe.de/task-id"]
	if !ok || taskIDStr == "" {
		return
	}
	taskID := lib.TaskIdentifier(taskIDStr)

	reason := classifyPodFailure(pod)
	if reason == "" {
		return
	}

	task, ok := w.taskStore.Load(taskID)
	if !ok {
		glog.V(3).Infof(
			"pod %s/%s (task %s) in %s state but task not in store; sweeper will handle if still in flight",
			pod.Namespace, pod.Name, taskID, reason,
		)
		return
	}

	jobName := ownerJobName(pod)
	if jobName == "" {
		glog.V(2).Infof(
			"pod %s/%s (task %s) in %s state but has no Job ownerRef; ignoring",
			pod.Namespace, pod.Name, taskID, reason,
		)
		return
	}

	if err := w.publisher.PublishFailure(ctx, task, jobName, reason.String()); err != nil {
		glog.Errorf(
			"publish pod-state failure for task %s (pod %s reason %s): %v",
			taskID, pod.Name, reason, err,
		)
		return
	}
	glog.V(2).Infof(
		"published pod-state failure for task %s (pod %s reason %s)",
		taskID, pod.Name, reason,
	)
	// Do NOT call w.taskStore.Delete here. The pod may transition again (e.g. evicted then
	// rescheduled). The Job-condition path or the deadline sweeper performs the final delete
	// when terminal state is observed. Dedupe in PublishFailure (prompt 1) prevents
	// double-publish for the same job name.
}

// classifyPodFailure returns a non-empty ZombieReason when the Pod is in a
// terminal failure state we recognize. Returns "" for healthy, pending-without-
// excessive-delay, and any state we should not act on from the informer path.
// pod_not_scheduled is deliberately NOT returned here — it requires a grace
// window the informer cannot evaluate (a freshly created Pod is always briefly
// Pending before scheduling). The deadline sweeper (separate prompt) owns that
// classification.
func classifyPodFailure(pod *corev1.Pod) ZombieReason {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			switch cs.State.Waiting.Reason {
			case "ImagePullBackOff", "ErrImagePull":
				return ZombieReasonImagePullBackOff
			case "CrashLoopBackOff":
				// With BackoffLimit=0 in the spawner, crash-looping pods never
				// reach PodFailed phase, so this branch is the only signal that
				// classifies them before activeDeadlineSeconds fires.
				return ZombieReasonPodCrashNoStdout
			}
		}
	}
	if pod.Status.Reason == "Evicted" {
		return ZombieReasonPodEvicted
	}
	if pod.Status.Phase == corev1.PodFailed {
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
				return ZombieReasonPodCrashNoStdout
			}
		}
	}
	return ""
}

// ownerJobName returns the name of the Job that owns the Pod, or "" when no
// Job ownerRef is present.
func ownerJobName(pod *corev1.Pod) string {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "Job" {
			return ref.Name
		}
	}
	return ""
}

// hasNonZeroTerminatedContainer reports whether any container terminated with
// a non-zero exit code.
func hasNonZeroTerminatedContainer(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			return true
		}
	}
	return false
}

// terminalPodDetail returns the terminated reason and exit code of the Job's
// most recently created pod that has a non-zero terminated container, matched
// by Job owner reference. ok is false when the pod lister is not yet synced,
// the lister lookup fails, or no owned pod carries a non-zero terminated
// container.
func (w *jobWatcher) terminalPodDetail(
	job *batchv1.Job,
) (terminatedReason string, exitCode int32, ok bool) {
	lister := w.PodLister(libk8s.Namespace(job.Namespace))
	if lister == nil {
		return "", 0, false
	}
	pods, err := lister.Pods(job.Namespace).List(labels.Everything())
	if err != nil {
		glog.Errorf("job watcher: list pods for job %s/%s: %v", job.Namespace, job.Name, err)
		return "", 0, false
	}
	var best *corev1.Pod
	for _, pod := range pods {
		if ownerJobName(pod) != job.Name {
			continue
		}
		if !hasNonZeroTerminatedContainer(pod) {
			continue
		}
		if best == nil || pod.CreationTimestamp.After(best.CreationTimestamp.Time) {
			best = pod
		}
	}
	if best == nil {
		return "", 0, false
	}
	for _, cs := range best.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			return cs.State.Terminated.Reason, cs.State.Terminated.ExitCode, true
		}
	}
	return "", 0, false
}
