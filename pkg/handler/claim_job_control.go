// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// claimTaskIDLabel is the Job label SpawnJob stamps with the task identifier.
// It is the only key the claim JobControl looks jobs up by.
const claimTaskIDLabel = "agent.benjamin-borbe.de/task-id"

// kubeJobControl is the production claim.JobControl: the fence deletes the Job
// of a claim whose renewals keep failing, and restart recovery asks whether a
// held claim's Job still runs. Both resolve jobs by the task-id label, so a
// claim can only ever act on its own task's Job.
type kubeJobControl struct {
	kubeClient kubernetes.Interface
	namespace  libk8s.Namespace
	resolver   pkg.ConfigResolver
	view       TaskView
}

// NewKubeJobControl returns the JobControl the lease manager needs before a
// claim-taking mode may run its fence. Without it a claim that cannot renew
// keeps its Job running unclaimed, which is the duplicate the claims exist to
// prevent.
//
// resolver and view let the fence find a Job a Config has placed outside the
// executor's namespace; either may be nil, in which case every lookup targets
// the executor's namespace — today's behaviour, and the only behaviour possible
// before a Config sets jobNamespace.
//
// The task comes from the TaskView, never from the spawn-time TaskStore. The
// store is empty after a restart (it is filled at spawn, pkg/task_store.go), and
// startup recovery asks about Jobs this pod was running BEFORE it restarted —
// exactly the tasks a store-only lookup cannot answer. Reading the store here
// released the claims of sandboxed Jobs that were still running, so a peer could
// claim and run the same task twice. The view replays the compacted task event
// topic, so it answers for tasks this process never saw spawn.
func NewKubeJobControl(
	kubeClient kubernetes.Interface,
	namespace libk8s.Namespace,
	resolver pkg.ConfigResolver,
	view TaskView,
) claim.JobControl {
	return &kubeJobControl{
		kubeClient: kubeClient,
		namespace:  namespace,
		resolver:   resolver,
		view:       view,
	}
}

// namespaceFor resolves the namespace taskID's Job lives in. Every failure path
// falls back to the executor's namespace: the fence must never be the reason a
// claim cannot be released, and before a Config sets jobNamespace that fallback
// is also the correct answer.
func (c *kubeJobControl) namespaceFor(ctx context.Context, taskID string) libk8s.Namespace {
	if c.resolver == nil || c.view == nil {
		return c.namespace
	}
	task, ok := c.view.Get(lib.TaskIdentifier(taskID))
	if !ok {
		return c.namespace
	}
	config, err := c.resolver.Resolve(ctx, task.Frontmatter.Assignee().String())
	if err != nil {
		return c.namespace
	}
	namespace, err := pkg.ResolveJobNamespace(ctx, c.namespace, config.JobNamespace)
	if err != nil {
		return c.namespace
	}
	return namespace
}

// IsJobRunning reports whether a non-terminal Job exists for taskID.
func (c *kubeJobControl) IsJobRunning(ctx context.Context, taskID string) (bool, error) {
	jobs, err := c.jobsForTask(ctx, c.namespaceFor(ctx, taskID), taskID)
	if err != nil {
		return false, err
	}
	for _, job := range jobs {
		if !pkg.IsJobFailed(&job) && !pkg.IsJobSucceeded(&job) {
			return true, nil
		}
	}
	return false, nil
}

// DeleteJob deletes every Job for taskID. Deleting an absent Job is not an
// error: the fence is the last resort and must not report a Job already gone.
func (c *kubeJobControl) DeleteJob(ctx context.Context, taskID string) error {
	namespace := c.namespaceFor(ctx, taskID)
	jobs, err := c.jobsForTask(ctx, namespace, taskID)
	if err != nil {
		return err
	}
	propagation := metav1.DeletePropagationBackground
	for _, job := range jobs {
		if delErr := c.kubeClient.BatchV1().Jobs(string(namespace)).Delete(
			ctx,
			job.Name,
			metav1.DeleteOptions{PropagationPolicy: &propagation},
		); delErr != nil && !k8serrors.IsNotFound(delErr) {
			return errors.Wrapf(ctx, delErr, "delete job %s for task %s", job.Name, taskID)
		}
	}
	return nil
}

// jobsForTask lists the Jobs carrying taskID's label.
func (c *kubeJobControl) jobsForTask(
	ctx context.Context,
	namespace libk8s.Namespace,
	taskID string,
) ([]batchv1.Job, error) {
	jobs, err := c.kubeClient.BatchV1().Jobs(string(namespace)).List(
		ctx,
		metav1.ListOptions{LabelSelector: claimTaskIDLabel + "=" + taskID},
	)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "list jobs for task %s", taskID)
	}
	return jobs.Items, nil
}
