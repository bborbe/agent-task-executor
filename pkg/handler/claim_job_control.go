// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

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
}

// NewKubeJobControl returns the JobControl the lease manager needs before a
// claim-taking mode may run its fence. Without it a claim that cannot renew
// keeps its Job running unclaimed, which is the duplicate the claims exist to
// prevent.
func NewKubeJobControl(
	kubeClient kubernetes.Interface,
	namespace libk8s.Namespace,
) claim.JobControl {
	return &kubeJobControl{kubeClient: kubeClient, namespace: namespace}
}

// IsJobRunning reports whether a non-terminal Job exists for taskID.
func (c *kubeJobControl) IsJobRunning(ctx context.Context, taskID string) (bool, error) {
	jobs, err := c.jobsForTask(ctx, taskID)
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
	jobs, err := c.jobsForTask(ctx, taskID)
	if err != nil {
		return err
	}
	propagation := metav1.DeletePropagationBackground
	for _, job := range jobs {
		if delErr := c.kubeClient.BatchV1().Jobs(string(c.namespace)).Delete(
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
	taskID string,
) ([]batchv1.Job, error) {
	jobs, err := c.kubeClient.BatchV1().Jobs(string(c.namespace)).List(
		ctx,
		metav1.ListOptions{LabelSelector: claimTaskIDLabel + "=" + taskID},
	)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "list jobs for task %s", taskID)
	}
	return jobs.Items, nil
}
