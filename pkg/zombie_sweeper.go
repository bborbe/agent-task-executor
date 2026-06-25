// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1listers "k8s.io/client-go/listers/core/v1"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

//counterfeiter:generate -o ../mocks/zombie_sweeper.go --fake-name FakeZombieSweeper . ZombieSweeper

// ZombieSweeper is a background goroutine that periodically classifies stuck
// tasks as zombies and emits failure events. It is the safety net for the
// informer-driven paths in JobWatcher (which handle the cases k8s notifies us
// about). The sweeper handles: pods unschedulable beyond a grace window,
// executor restart losing watch on a Job, and any deadline path the informer
// misses (Job-condition deferred indefinitely, informer cache drift).
type ZombieSweeper interface {
	// Run blocks until ctx is cancelled. Each tick (interval sourced from the
	// first non-nil ConfigSpec.ZombieSweeperIntervalSeconds across the resolver's
	// configs, else DefaultZombieSweeperIntervalSeconds) it calls SweepOnce.
	Run(ctx context.Context) error
	// SweepOnce performs a single sweep pass. Exposed for unit tests so they
	// do not have to manage tickers. Returns an error only on context
	// cancellation paths; per-task classification errors are logged.
	SweepOnce(ctx context.Context) error
}

// NewZombieSweeper creates a ZombieSweeper. The JobWatcher is held by reference
// (not its lister) because the lister is only populated after JobWatcher.Run
// completes its cache sync; service.Run starts all components concurrently, so
// extracting the lister at wiring time would capture nil. SweepOnce resolves
// the lister lazily on every tick and skips the tick if it is not yet ready.
func NewZombieSweeper(
	jobWatcher JobWatcher,
	namespace libk8s.Namespace,
	taskStore *TaskStore,
	publisher ResultPublisher,
	configProvider EventHandlerConfig,
	currentDateTime libtime.CurrentDateTimeGetter,
) ZombieSweeper {
	return &zombieSweeper{
		jobWatcher:      jobWatcher,
		namespace:       namespace,
		taskStore:       taskStore,
		publisher:       publisher,
		configProvider:  configProvider,
		currentDateTime: currentDateTime,
	}
}

type zombieSweeper struct {
	jobWatcher      JobWatcher
	namespace       libk8s.Namespace
	taskStore       *TaskStore
	publisher       ResultPublisher
	configProvider  EventHandlerConfig
	currentDateTime libtime.CurrentDateTimeGetter
}

const (
	// podNotScheduledGraceWindow is the age threshold past which a Pending Pod
	// with PodScheduled=False is classified pod_not_scheduled. Must exceed
	// typical scheduler latency comfortably; 2 minutes is empirically generous.
	podNotScheduledGraceWindow = 2 * time.Minute
)

// NOTE on "no recent heartbeat" from spec DB #9 / AC #6:
// The spec predicate is `elapsed > deadline AND pod not Running AND no recent
// heartbeat`. This codebase has NO separate heartbeat channel today — the only
// liveness signal for a running job is "is a Pod currently Running?". Therefore
// "no recent heartbeat" is implemented as "no Pod in PodRunning phase observed
// for this task". If a per-job heartbeat is added later (a follow-up spec),
// this predicate gets a real check; for now `classify` treats `pod not Running`
// as covering both halves of the conjunction.

func (s *zombieSweeper) Run(ctx context.Context) error {
	// Interval is resolved once at startup. CRD changes to ZombieSweeperIntervalSeconds
	// take effect only after pod restart. Acceptable because executor pods are short-lived
	// relative to CRD reconciliation cycles.
	interval, err := s.resolveSweeperInterval(ctx)
	if err != nil {
		return errors.Wrapf(ctx, err, "resolve sweeper interval")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	glog.V(2).Infof("zombie sweeper started interval=%s", interval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.SweepOnce(ctx); err != nil {
				// Per-tick failures (transient lister errors, ctx-scoped
				// failures from publisher) must NOT kill the sweeper goroutine
				// — that would tear down the executor via service.Run. Log and
				// continue.
				glog.Errorf("zombie sweeper tick: %v", err)
			}
		}
	}
}

func (s *zombieSweeper) SweepOnce(ctx context.Context) error {
	// Resolve the Pod lister lazily — JobWatcher.Run populates it only after
	// the informer cache has synced, and service.Run starts all components
	// concurrently. If the lister is not yet available, skip this tick rather
	// than publishing spurious failures.
	lister := s.jobWatcher.PodLister()
	if lister == nil {
		glog.V(2).Infof("sweep skipped: pod lister not yet synced")
		return nil
	}
	snapshot := s.taskStore.Snapshot()
	now := s.currentDateTime.Now().Time()
	// Fetch configs ONCE per tick — used by taskDeadline() for every task in
	// the snapshot. Avoids N calls into the provider per sweep.
	cfgs, err := s.configProvider.Get(ctx)
	if err != nil {
		return errors.Wrapf(ctx, err, "list configs")
	}
	for taskID, task := range snapshot {
		jobName := task.Frontmatter.CurrentJob()
		if jobName == "" {
			// No active job recorded; nothing to sweep.
			continue
		}
		jobStartedAt, err := task.Frontmatter.JobStartedAt()
		if err != nil || jobStartedAt.IsZero() {
			glog.V(3).Infof(
				"zombie sweeper: task %s job_started_at unparseable or zero; skipping",
				taskID,
			)
			continue
		}
		deadline := s.taskDeadline(task, cfgs)
		elapsed := now.Sub(jobStartedAt)
		if elapsed < deadline {
			continue
		}
		reason := s.classify(lister, taskID, now)
		if reason == "" {
			continue
		}
		if err := s.publisher.PublishFailure(ctx, task, jobName, reason.String()); err != nil {
			glog.Errorf(
				"zombie sweeper: publish failure for task %s (job %s reason %s): %v",
				taskID, jobName, reason, err,
			)
			continue
		}
		glog.V(2).Infof(
			"zombie sweeper: published failure for task %s (job %s reason %s elapsed=%s)",
			taskID, jobName, reason, elapsed,
		)
	}
	return nil
}

func (s *zombieSweeper) taskDeadline(task lib.Task, cfgs []agentv1.Config) time.Duration {
	assignee := task.Frontmatter.Assignee().String()
	for _, cfg := range cfgs {
		if cfg.Spec.Assignee == assignee && cfg.Spec.ZombieJobTimeoutSeconds != nil {
			return time.Duration(*cfg.Spec.ZombieJobTimeoutSeconds) * time.Second
		}
	}
	return time.Duration(agentv1.DefaultZombieJobTimeoutSeconds) * time.Second
}

func (s *zombieSweeper) resolveSweeperInterval(ctx context.Context) (time.Duration, error) {
	cfgs, err := s.configProvider.Get(ctx)
	if err != nil {
		return 0, errors.Wrapf(ctx, err, "list configs")
	}
	for _, cfg := range cfgs {
		if cfg.Spec.ZombieSweeperIntervalSeconds != nil {
			return time.Duration(*cfg.Spec.ZombieSweeperIntervalSeconds) * time.Second, nil
		}
	}
	return time.Duration(agentv1.DefaultZombieSweeperIntervalSeconds) * time.Second, nil
}

// classify determines whether a past-deadline task is a zombie and which
// reason applies. Returns "" when the task is NOT a zombie (Pod still Running
// — implicit heartbeat). Inspects Pod state via the shared Pod informer's
// lister (introduced by prompt 2). Spec Failure-Mode row "k8s API rate-limit
// (429)" mandates: "Sweeper relies on informer cache (no per-cycle list)" —
// we MUST NOT issue API LIST calls here.
func (s *zombieSweeper) classify(
	lister corev1listers.PodLister,
	taskID lib.TaskIdentifier,
	now time.Time,
) ZombieReason {
	selector := labels.SelectorFromSet(labels.Set{
		"agent.benjamin-borbe.de/task-id": string(taskID),
	})
	pods, err := lister.Pods(s.namespace.String()).List(selector)
	if err != nil {
		glog.Errorf("zombie sweeper: lister pods for task %s: %v", taskID, err)
		return ""
	}
	// Zero pods AND past-deadline AND a Job was supposed to be running →
	// executor lost the watch (Job exists in k8s but Pod GC happened, or the
	// Job never created a Pod and was restarted across executor lifetimes).
	// "No recent heartbeat" reduces to "no Pod observed" since this codebase
	// has no separate heartbeat channel.
	if len(pods) == 0 {
		return ZombieReasonExecutorWatchLost
	}
	for _, pod := range pods {
		// Healthy Running — NOT a zombie. A Running pod is the implicit
		// heartbeat in the current system (no separate heartbeat channel).
		if pod.Status.Phase == corev1.PodRunning {
			return ""
		}
		// Pending past the unschedulable grace window with PodScheduled=False.
		if pod.Status.Phase == corev1.PodPending {
			age := now.Sub(pod.CreationTimestamp.Time)
			if age > podNotScheduledGraceWindow && hasPodScheduledFalse(pod) {
				return ZombieReasonPodNotScheduled
			}
		}
	}
	// Past deadline, no Running pod, no specific Pod-state reason — fall
	// back to deadline_exceeded.
	return ZombieReasonDeadlineExceeded
}

// hasPodScheduledFalse returns true when the Pod has a PodScheduled=False
// condition (kube-scheduler could not place the pod).
func hasPodScheduledFalse(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse {
			return true
		}
	}
	return false
}
