// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"strings"
	"time"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// claimHarness wires a real TaskEventHandler for claim-mode specs. It exposes
// the fake claim client and the number of times the ref-client factory ran, so
// a spec can assert that a mode never constructs a claim client.
type claimHarness struct {
	eventHandler handler.ClaimLifecycleEventHandler
	spawner      *mocks.FakeJobSpawner
	resolver     *mocks.FakeConfigResolver
	client       *mocks.FakeClaimRefClient
	taskStore    *pkg.TaskStore
	publisher    *mocks.FakeResultPublisher
	factoryCalls int
}

// noopClaimJobControl satisfies claim.JobControl where a spec exercises the
// dispatch gate but not the fence or restart recovery.
type noopClaimJobControl struct{}

func (noopClaimJobControl) IsJobRunning(context.Context, string) (bool, error) {
	return false, nil
}

func (noopClaimJobControl) DeleteJob(context.Context, string) error { return nil }

// claimHarnessVault is the vault slug the handler owns. The event path skips any
// task stamped with another vault, so every fixture carries this one.
const claimHarnessVault = "private-personal"

// claimBoolPtr returns a pointer to b, for the optional rerunOnLostClaim field.
func claimBoolPtr(b bool) *bool { return &b }

// newClaimHarness builds a handler in the given claim mode with defaults that
// let a qualifying task spawn exactly once.
func newClaimHarness(mode pkg.ClaimMode) *claimHarness {
	spawner := new(mocks.FakeJobSpawner)
	spawner.SpawnJobReturns("job-1", nil)
	spawner.IsJobActiveReturns(false, nil)
	spawner.ListActiveJobsReturns(map[lib.TaskIdentifier]bool{}, nil)
	spawner.CountActiveJobsReturns(0, nil)

	resolver := new(mocks.FakeConfigResolver)
	// Explicit opt-in: the harness models an agent that may run a second time
	// after a lost claim. An unset field now parks, so leaving it out here would
	// turn every takeover spec into a park spec.
	resolver.ResolveReturns(pkg.AgentConfiguration{
		Assignee:         "claude",
		Image:            "img",
		RerunOnLostClaim: claimBoolPtr(true),
	}, nil)

	client := new(mocks.FakeClaimRefClient)
	publisher := new(mocks.FakeResultPublisher)
	taskStore := pkg.NewTaskStore()
	// The event path refuses to spawn until the task view has replayed (a cold
	// pod's first events are a stale backlog), so these specs model a view that
	// is ready. Without this every one of them sees zero spawns.
	view := new(mocks.FakeTaskView)
	view.ReadyReturns(true)
	h := &claimHarness{
		spawner:   spawner,
		resolver:  resolver,
		client:    client,
		taskStore: taskStore,
		publisher: publisher,
	}
	h.eventHandler = handler.NewTaskEventHandler(
		spawner,
		"dev",
		base.Branch("prod"),
		"self",
		true,
		true,
		resolver,
		publisher,
		taskStore,
		libtime.NewCurrentDateTime(),
		view,
		claimHarnessVault,
		handler.DefaultReconcilePassTimeout,
		handler.DefaultReconcileInterval,
		mode,
		func() claim.ClaimRefClient {
			h.factoryCalls++
			return client
		},
		noopClaimJobControl{},
	)
	return h
}

// consume publishes one task through the Kafka consumer path.
func (h *claimHarness) consume(task lib.Task) error {
	value, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return h.eventHandler.ConsumeMessage(
		context.Background(),
		&sarama.ConsumerMessage{Value: value},
	)
}

// holdClaim takes a claim for taskID directly through the handler's lease
// manager, without going through the spawn path, and returns the held view.
func (h *claimHarness) holdClaim(taskID string) claim.HeldClaim {
	ctx := context.Background()
	manager, err := h.eventHandler.ClaimLeaseManager(ctx)
	Expect(err).NotTo(HaveOccurred())
	held, err := manager.Claim(ctx, "claims/task/"+taskID, taskID)
	Expect(err).NotTo(HaveOccurred())
	return *held
}

// watcher returns a JobWatcher that releases through this harness's handler.
func (h *claimHarness) watcher() pkg.JobWatcher {
	return pkg.NewJobWatcher(
		fake.NewClientset(),
		"test-ns",
		h.taskStore,
		h.publisher,
		h.eventHandler,
	)
}

// terminalJob returns a Job carrying taskID and the given terminal condition.
func terminalJob(name, taskID string, condition batchv1.JobCondition) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "test-ns",
			Labels:    map[string]string{"agent.benjamin-borbe.de/task-id": taskID},
		},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{condition}},
	}
	if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
		now := metav1.Now()
		job.Status.CompletionTime = &now
	}
	return job
}

// succeededCondition is the JobComplete condition a successful Job carries.
func succeededCondition() batchv1.JobCondition {
	return batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}
}

// failedCondition is the JobFailed condition a failed Job carries.
func failedCondition(message string) batchv1.JobCondition {
	return batchv1.JobCondition{
		Type:    batchv1.JobFailed,
		Status:  corev1.ConditionTrue,
		Message: message,
	}
}

// claimResultLabels is the closed result set of executor_claim_total.
var claimResultLabels = []string{
	"claimed", "conflict", "renewed", "recovered", "renew_failed",
	"claim_lost", "fenced", "taken_over", "takeover_too_early",
	"released", "already_released", "adopted", "error",
}

// snapshotClaims reads every series of executor_claim_total.
func snapshotClaims() map[string]float64 {
	snapshot := make(map[string]float64, len(claimResultLabels))
	for _, name := range claimResultLabels {
		snapshot[name] = testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues(name))
	}
	return snapshot
}

// expectClaimDelta asserts the delta for every result series; unlisted series
// must not change.
func expectClaimDelta(before, after map[string]float64, expected map[string]float64) {
	for _, name := range claimResultLabels {
		Expect(after[name]-before[name]).To(
			Equal(expected[name]),
			"executor_claim_total{result=%q} delta",
			name,
		)
	}
}

// qualifyingClaimTask is a task every dispatch filter admits: in_progress,
// execution phase, claude assignee, prod stage, and no cluster, ref or
// max_triggers field.
func qualifyingClaimTask() lib.Task {
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier("tid-claim-off-1"),
		Vault:          claimHarnessVault,
		Frontmatter: lib.TaskFrontmatter{
			"status":   "in_progress",
			"phase":    string(domain.TaskPhaseExecution),
			"assignee": "claude",
			"stage":    "prod",
		},
	}
}

// conflictOnCreate is a Put stub that conflicts when the create is attempted
// and returns a sha for any takeover write.
func conflictOnCreate() func(
	context.Context, string, string, string, string, libtime.DateTime,
) (string, error) {
	return func(
		_ context.Context,
		_ string,
		expectedSha string,
		_ string,
		_ string,
		_ libtime.DateTime,
	) (string, error) {
		if expectedSha == "" {
			return "", claim.ErrLeaseConflict
		}
		return "new-sha", nil
	}
}

// listedClaimRef is the single claim ref the fake lists for key.
func listedClaimRef(key, sha string, leaseUntil time.Time) claim.ClaimRef {
	return claim.ClaimRef{
		Ref:        "refs/" + key,
		Sha:        sha,
		Cluster:    "other",
		TaskID:     "previous-task",
		LeaseUntil: leaseUntil.UTC().Format(time.RFC3339),
	}
}

// claimKeyFor returns the claim key DeriveClaimKey produces for task.
func claimKeyFor(task lib.Task) string {
	return "claims/task/" + string(task.TaskIdentifier)
}

// dispatchPutCount counts PUT calls whose key is a dispatch claim (task- or
// repo-keyed), ignoring the redrive streak refs the handler also writes.
func dispatchPutCount(harness *claimHarness) int {
	count := 0
	for i := 0; i < harness.client.PutCallCount(); i++ {
		_, key, _, _, _, _ := harness.client.PutArgsForCall(i)
		if strings.HasPrefix(key, "claims/task/") || strings.HasPrefix(key, "claims/repo/") {
			count++
		}
	}
	return count
}

var _ = Describe("claim mode off", func() {
	It("spawns one Job and constructs no claim client", func() {
		harness := newClaimHarness(pkg.ClaimModeOff)
		before := snapshotClaims()

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())

		after := snapshotClaims()
		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(1))
		Expect(harness.factoryCalls).To(Equal(0))
		Expect(harness.client.PutCallCount()).To(Equal(0))
		Expect(harness.client.DeleteCallCount()).To(Equal(0))
		Expect(harness.client.ListCallCount()).To(Equal(0))
		expectClaimDelta(before, after, map[string]float64{})
	})
})

var _ = Describe("claim gate", func() {
	It("AC5: on blocks the Job when the claim request fails", func() {
		harness := newClaimHarness(pkg.ClaimModeOn)
		harness.client.PutReturns("", stderrors.New("git-rest down"))
		harness.client.ListReturns(nil, nil)
		before := snapshotClaims()

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())

		after := snapshotClaims()
		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(0))
		expectClaimDelta(before, after, map[string]float64{"error": 1})
	})

	It("AC5: shadow still spawns when the claim request fails", func() {
		harness := newClaimHarness(pkg.ClaimModeShadow)
		harness.client.PutReturns("", stderrors.New("git-rest down"))
		harness.client.ListReturns(nil, nil)
		before := snapshotClaims()

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())

		after := snapshotClaims()
		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(1))
		expectClaimDelta(before, after, map[string]float64{"error": 1})
	})

	It("AC4: on spawns after a successful claim", func() {
		harness := newClaimHarness(pkg.ClaimModeOn)
		harness.client.PutReturns("sha-1", nil)
		before := snapshotClaims()

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())

		after := snapshotClaims()
		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(1))
		expectClaimDelta(before, after, map[string]float64{"claimed": 1})
	})

	It("AC8: a repo-keyed task claims under one key per repository", func() {
		harness := newClaimHarness(pkg.ClaimModeOn)
		task := qualifyingClaimTask()
		task.Frontmatter["task_type"] = "update-go"
		task.Frontmatter["repo"] = "Bborbe/Agent"
		harness.client.PutReturns("sha-1", nil)
		before := snapshotClaims()

		Expect(harness.consume(task)).To(Succeed())

		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(1))
		_, key, expectedSha, _, taskID, _ := harness.client.PutArgsForCall(0)
		Expect(key).To(Equal("claims/repo/bborbe-agent"))
		Expect(expectedSha).To(Equal(""))
		Expect(taskID).To(Equal(string(task.TaskIdentifier)))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{"claimed": 1})
	})

	It("AC3: shadow spawns when the lease is still live", func() {
		harness := newClaimHarness(pkg.ClaimModeShadow)
		harness.client.PutStub = conflictOnCreate()
		harness.client.ListReturns([]claim.ClaimRef{
			listedClaimRef(
				claimKeyFor(qualifyingClaimTask()),
				"live-sha",
				time.Now().UTC().Add(5*time.Minute),
			),
		}, nil)
		before := snapshotClaims()

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())

		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(1))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"conflict":           1,
			"takeover_too_early": 1,
		})
	})

	It("AC4: on blocks the Job when the lease is still live", func() {
		harness := newClaimHarness(pkg.ClaimModeOn)
		harness.client.PutStub = conflictOnCreate()
		harness.client.ListReturns([]claim.ClaimRef{
			listedClaimRef(
				claimKeyFor(qualifyingClaimTask()),
				"live-sha",
				time.Now().UTC().Add(5*time.Minute),
			),
		}, nil)
		before := snapshotClaims()

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())

		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(0))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"conflict":           1,
			"takeover_too_early": 1,
		})
	})

	It("AC6: on spawns after taking over an expired lease", func() {
		harness := newClaimHarness(pkg.ClaimModeOn)
		task := qualifyingClaimTask()
		key := claimKeyFor(task)
		harness.client.PutStub = conflictOnCreate()
		harness.client.ListReturns([]claim.ClaimRef{
			listedClaimRef(key, "expired-sha", time.Now().UTC().Add(-31*time.Second)),
		}, nil)
		before := snapshotClaims()

		Expect(harness.consume(task)).To(Succeed())

		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(1))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"conflict":   1,
			"taken_over": 1,
		})
		Expect(dispatchPutCount(harness)).To(Equal(2))
		_, putKey, expectedSha, cluster, _, _ := harness.client.PutArgsForCall(1)
		Expect(putKey).To(Equal(key))
		Expect(expectedSha).To(Equal("expired-sha"))
		Expect(cluster).To(Equal("self"))
	})

	It("AC6: parks instead of spawning when rerunOnLostClaim is false", func() {
		harness := newClaimHarness(pkg.ClaimModeOn)
		harness.resolver.ResolveReturns(pkg.AgentConfiguration{
			Assignee:         "claude",
			Image:            "img",
			RerunOnLostClaim: claimBoolPtr(false),
		}, nil)
		task := qualifyingClaimTask()
		key := claimKeyFor(task)
		harness.client.PutStub = conflictOnCreate()
		harness.client.ListReturns([]claim.ClaimRef{
			listedClaimRef(key, "expired-sha", time.Now().UTC().Add(-31*time.Second)),
		}, nil)
		before := snapshotClaims()

		Expect(harness.consume(task)).To(Succeed())

		// The takeover succeeded, but the Config forbids a second run: no Job is
		// created, and the task is parked at human_review instead.
		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(0))
		Expect(harness.publisher.PublishParkLostClaimCallCount()).To(Equal(1))
		_, parkedTask := harness.publisher.PublishParkLostClaimArgsForCall(0)
		Expect(parkedTask.TaskIdentifier).To(Equal(task.TaskIdentifier))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"conflict":   1,
			"taken_over": 1,
		})
	})

	It("AC6: on blocks a lease inside the takeover grace", func() {
		harness := newClaimHarness(pkg.ClaimModeOn)
		harness.client.PutStub = conflictOnCreate()
		harness.client.ListReturns([]claim.ClaimRef{
			listedClaimRef(
				claimKeyFor(qualifyingClaimTask()),
				"grace-sha",
				time.Now().UTC().Add(-5*time.Second),
			),
		}, nil)
		before := snapshotClaims()

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())

		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(0))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"conflict":           1,
			"takeover_too_early": 1,
		})
	})

	It("AC6: on blocks a lease that is still valid", func() {
		harness := newClaimHarness(pkg.ClaimModeOn)
		harness.client.PutStub = conflictOnCreate()
		harness.client.ListReturns([]claim.ClaimRef{
			listedClaimRef(
				claimKeyFor(qualifyingClaimTask()),
				"live-sha",
				time.Now().UTC().Add(5*time.Minute),
			),
		}, nil)
		before := snapshotClaims()

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())

		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(0))
		expectClaimDelta(before, snapshotClaims(), map[string]float64{
			"conflict":           1,
			"takeover_too_early": 1,
		})
	})

	It("AC7(a): a foreign-cluster task makes no claim request", func() {
		harness := newClaimHarness(pkg.ClaimModeShadow)
		task := qualifyingClaimTask()
		task.Frontmatter["cluster"] = "other"
		before := testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_cluster"),
		)

		Expect(harness.consume(task)).To(Succeed())

		after := testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_cluster"),
		)
		Expect(harness.client.PutCallCount()).To(Equal(0))
		Expect(after - before).To(Equal(float64(1)))
	})

	It("AC7(b): a task deferred at the concurrency cap makes no claim request", func() {
		harness := newClaimHarness(pkg.ClaimModeShadow)
		harness.resolver.ResolveReturns(
			pkg.AgentConfiguration{
				Assignee:          "claude",
				Image:             "img",
				MaxConcurrentJobs: 1,
			},
			nil,
		)
		harness.spawner.ListActiveJobsReturns(activeJobs(1), nil)
		before := testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("deferred_concurrency_cap"),
		)

		Expect(harness.consume(qualifyingClaimTask())).To(Succeed())

		after := testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("deferred_concurrency_cap"),
		)
		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(0))
		Expect(harness.client.PutCallCount()).To(Equal(0))
		Expect(after - before).To(Equal(float64(1)))
	})

	It("AC7(c): a redrive-capped task makes no additional claim request", func() {
		harness := newClaimHarness(pkg.ClaimModeShadow)
		task := qualifyingClaimTask()
		task.TaskIdentifier = lib.TaskIdentifier("tid-claim-redrive")

		for i := 0; i < 3; i++ {
			Expect(harness.consume(task)).To(Succeed())
		}
		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(3))
		putCallsBefore := harness.client.PutCallCount()
		cappedBefore := testutil.ToFloat64(metrics.RedriveCappedTotal)

		Expect(harness.consume(task)).To(Succeed())

		Expect(harness.spawner.SpawnJobCallCount()).To(Equal(3))
		Expect(harness.client.PutCallCount()).To(Equal(putCallsBefore))
		Expect(testutil.ToFloat64(metrics.RedriveCappedTotal) - cappedBefore).
			To(Equal(float64(1)))
	})
})
