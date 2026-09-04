// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
	"github.com/golang/glog"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

//counterfeiter:generate -o ../mocks/job_watcher.go --fake-name FakeJobWatcher . JobWatcher

// JobWatcher watches batch/v1 Jobs and their Pods in the executor's namespace and
// publishes synthetic failure results for terminal-state objects that belong to
// spawned tasks.
type JobWatcher interface {
	// Run starts the Job and Pod informers and blocks until ctx is cancelled.
	Run(ctx context.Context) error
	// HandleJob processes a single Job (invoked by the informer event handlers
	// and by unit tests directly, avoiding the need for a fake informer).
	HandleJob(ctx context.Context, job *batchv1.Job)
	// HandlePod processes a single Pod (invoked by the Pod informer event handler
	// and by unit tests directly).
	HandlePod(ctx context.Context, pod *corev1.Pod)
	// PodLister returns the Pod lister backed by the shared informer cache, for
	// use by the deadline sweeper. The returned lister is safe for concurrent
	// read access.
	PodLister() corev1listers.PodLister
}

// NewJobWatcher creates a JobWatcher.
func NewJobWatcher(
	kubeClient kubernetes.Interface,
	namespace libk8s.Namespace,
	taskStore *TaskStore,
	publisher ResultPublisher,
) JobWatcher {
	return &jobWatcher{
		kubeClient: kubeClient,
		namespace:  namespace,
		taskStore:  taskStore,
		publisher:  publisher,
	}
}

type jobWatcher struct {
	kubeClient kubernetes.Interface
	namespace  libk8s.Namespace
	taskStore  *TaskStore
	publisher  ResultPublisher
	podLister  atomic.Pointer[corev1listers.PodLister]
}

func (w *jobWatcher) Run(ctx context.Context) error {
	factory := k8sinformers.NewSharedInformerFactoryWithOptions(
		w.kubeClient,
		5*time.Minute,
		k8sinformers.WithNamespace(string(w.namespace)),
		k8sinformers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = "agent.benjamin-borbe.de/task-id"
		}),
	)
	informer := factory.Batch().V1().Jobs().Informer()

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
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
	})
	if err != nil {
		return errors.Wrapf(ctx, err, "add job informer event handler")
	}

	podInformer := factory.Core().V1().Pods().Informer()
	_, err = podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
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
	})
	if err != nil {
		return errors.Wrapf(ctx, err, "add pod informer event handler")
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced, podInformer.HasSynced) {
		return errors.Errorf(ctx, "timed out waiting for job/pod informer cache sync")
	}
	lister := factory.Core().V1().Pods().Lister()
	w.podLister.Store(&lister)
	glog.V(2).Infof("job and pod informer started in namespace %s", w.namespace)
	<-ctx.Done()
	return nil
}

func (w *jobWatcher) PodLister() corev1listers.PodLister {
	lister := w.podLister.Load()
	if lister == nil {
		return nil
	}
	return *lister
}

func (w *jobWatcher) HandleJob(ctx context.Context, job *batchv1.Job) {
	taskIDStr, ok := job.Labels["agent.benjamin-borbe.de/task-id"]
	if !ok || taskIDStr == "" {
		return
	}
	taskID := lib.TaskIdentifier(taskIDStr)

	if IsJobFailed(job) {
		reason := w.failureReason(job)
		glog.V(2).Infof("job %s/%s failed (task %s): %s", job.Namespace, job.Name, taskID, reason)
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
		if task, ok := w.taskStore.Load(taskID); ok {
			w.handleSucceeded(ctx, taskID, task, job)
		}
	}
}

// handleSucceeded publishes the current_job clear for a succeeded job and evicts
// the store entry — but only when the entry tracks THIS job. The informer
// re-delivers a terminal Job every ~5 min; when the task has since been
// re-dispatched and re-spawned under a NEWER job, the stale success re-delivery
// must not evict the entry the current job's success path needs — the
// respawned-second-success race via the job informer (observed prod 2026-09-04
// on task 67120247: job-1's re-delivered success evicted job-2's store entry).
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
		return
	}
	w.taskStore.Delete(taskID)
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
	w.logMissingTask(taskID, job, alwaysPublish)
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
	// job must not evict the entry tracking the current (re-spawned) job.
	if w.tracksJob(task, job.Name) {
		w.taskStore.Delete(taskID)
	}
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
		// The synthetic failure was already published on the first (real) observation.
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

// failureReason returns the reason string to publish for a failed Job. A
// BackoffLimitExceeded failure surfaces the pod's terminated reason + exit
// code when the pod is still resolvable (see backoffFailureReason); every
// other failure publishes the bare ZombieReason value.
func (w *jobWatcher) failureReason(job *batchv1.Job) string {
	reason := JobFailureReason(job)
	if reason == ZombieReasonDeadlineExceeded || !isBackoffLimitExceeded(job) {
		return reason.String()
	}
	return w.backoffFailureReason(job, reason)
}

// backoffFailureReason classifies a BackoffLimitExceeded Job using its
// resolved pod's terminal state: the dedicated pod_oom_killed for an OOMKilled
// pod, else the generic pod-failure reason, always appending the terminated
// reason + exit code when a pod is resolvable so operators see the cause
// without inspecting the pod.
func (w *jobWatcher) backoffFailureReason(job *batchv1.Job, base ZombieReason) string {
	terminatedReason, exitCode, ok := w.terminalPodDetail(job)
	if !ok {
		return base.String()
	}
	if terminatedReason == "OOMKilled" {
		return fmt.Sprintf("%s (%s/%d)", ZombieReasonPodOOMKilled, terminatedReason, exitCode)
	}
	return fmt.Sprintf("%s (%s/%d)", base.String(), terminatedReason, exitCode)
}

// HandlePod processes a Pod that has transitioned to a terminal failure state.
// It publishes a single zombie failure event and returns without deleting the
// task from the TaskStore — the Job-condition path or the deadline sweeper
// performs the final delete when terminal state is observed.
func (w *jobWatcher) HandlePod(ctx context.Context, pod *corev1.Pod) {
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
	lister := w.PodLister()
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
