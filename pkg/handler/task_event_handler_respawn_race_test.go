// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/handler"
)

// respawnedSecondSuccessTaskID is the task identifier used by the
// respawned-second-success regression spec.
const respawnedSecondSuccessTaskID = lib.TaskIdentifier("race-task-1")

var _ = Describe("respawned-second-success race", func() {
	var (
		ctx                 context.Context
		fakeSpawner         *mocks.FakeJobSpawner
		fakeResolver        *mocks.FakeConfigResolver
		fakeResultPublisher *mocks.FakeResultPublisher
		fakeGitRestClient   *mocks.FakeGitRestClient
		taskStore           *pkg.TaskStore
		h                   handler.TaskEventHandler
		watcher             pkg.JobWatcher
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeSpawner = new(mocks.FakeJobSpawner)
		fakeResolver = &mocks.FakeConfigResolver{}
		fakeResolver.ResolveReturns(
			pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"},
			nil,
		)
		fakeResultPublisher = &mocks.FakeResultPublisher{}
		fakeGitRestClient = &mocks.FakeGitRestClient{}
		fakeGitRestClient.IsReadyReturns(true, nil)
		taskStore = pkg.NewTaskStore()
		h = handler.NewTaskEventHandler(
			fakeSpawner,
			base.Branch("prod"),
			fakeResolver,
			fakeResultPublisher,
			taskStore,
			libtime.NewCurrentDateTime(),
			fakeGitRestClient,
			"24 Tasks/*.md",
		)
		watcher = pkg.NewJobWatcher(fake.NewClientset(), "test-ns", taskStore, fakeResultPublisher)
	})

	buildMsg := func(task lib.Task) *sarama.ConsumerMessage {
		value, err := json.Marshal(task)
		Expect(err).To(BeNil())
		return &sarama.ConsumerMessage{Value: value}
	}

	It(
		"clears current_job on a respawned second success even when a stale terminal event evicts the store",
		func() {
			// Regression for the 2026-09-03 prod observation on task 893c33b9: the
			// first job's success cleared current_job and evicted the store entry,
			// the task was re-dispatched and respawned, then a stale `completed`
			// TaskUpdated event from the FIRST job's result chain landed and hit
			// cleanupTerminalTask, deleting the just-re-spawned entry. The second
			// job's success then no-oped on the store miss and current_job stayed
			// set forever.
			fakeSpawner.IsJobActiveReturns(false, nil)

			succeeded := func() batchv1.JobCondition {
				return batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}
			}
			makeJob := func(name string, condition batchv1.JobCondition) *batchv1.Job {
				return &batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name:      name,
						Namespace: "test-ns",
						Labels: map[string]string{
							"agent.benjamin-borbe.de/task-id": string(respawnedSecondSuccessTaskID),
						},
					},
					Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{condition}},
				}
			}
			dispatch := func(status, phase, currentJob, jobStartedAt string) lib.Task {
				fm := lib.TaskFrontmatter{
					"status":   status,
					"phase":    phase,
					"assignee": "claude",
				}
				if currentJob != "" {
					fm["current_job"] = currentJob
				}
				if jobStartedAt != "" {
					fm["job_started_at"] = jobStartedAt
				}
				return lib.Task{TaskIdentifier: respawnedSecondSuccessTaskID, Frontmatter: fm}
			}

			// 1. First dispatch spawns job-1 and puts the task in the store.
			fakeSpawner.SpawnJobReturns("race-job-1", nil)
			Expect(
				h.ConsumeMessage(ctx, buildMsg(dispatch("in_progress", "execution", "", ""))),
			).To(BeNil())
			_, ok := taskStore.Load(respawnedSecondSuccessTaskID)
			Expect(ok).To(BeTrue(), "task should be in the store after first spawn")

			// 2. First job succeeds: clear published, store evicted.
			watcher.HandleJob(ctx, makeJob("race-job-1", succeeded()))
			Expect(fakeResultPublisher.PublishClearCurrentJobCallCount()).To(Equal(1))
			_, ok = taskStore.Load(respawnedSecondSuccessTaskID)
			Expect(ok).To(BeFalse(), "task should be evicted after first success")

			// 3. Task re-dispatched and respawned as job-2 (stale job_started_at
			// from job-1, current_job already cleared by step 2).
			fakeSpawner.SpawnJobReturns("race-job-2", nil)
			Expect(
				h.ConsumeMessage(
					ctx,
					buildMsg(dispatch("in_progress", "execution", "", "2026-09-03T20:59:04Z")),
				),
			).To(BeNil())
			_, ok = taskStore.Load(respawnedSecondSuccessTaskID)
			Expect(ok).To(BeTrue(), "task should be back in the store after respawn")

			// 4. Stale completed terminal event from the FIRST job's chain lands
			// late. It must NOT evict the re-spawned task.
			Expect(
				h.ConsumeMessage(
					ctx,
					buildMsg(dispatch("completed", "done", "", "2026-09-03T20:59:04Z")),
				),
			).To(BeNil())
			_, ok = taskStore.Load(respawnedSecondSuccessTaskID)
			Expect(ok).To(BeTrue(), "stale terminal event must not evict the re-spawned task")

			// 5. Second job succeeds: the clear must still fire (this is the
			// success-path clear for job-2, second call overall).
			watcher.HandleJob(ctx, makeJob("race-job-2", succeeded()))
			Expect(fakeResultPublisher.PublishClearCurrentJobCallCount()).To(Equal(2),
				"second job's success must clear current_job despite the stale terminal event")
		},
	)
})
