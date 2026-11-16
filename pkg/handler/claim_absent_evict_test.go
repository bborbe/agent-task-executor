// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

// Plain Go test, not Ginkgo: it drives the reconcile pass with a real task view
// so the eviction is observed where it matters — in the next pass's snapshot —
// and a table-free linear sequence reads better than a spec tree here.

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	stdtime "time"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/handler"
)

// stepClock is a CurrentDateTimeGetter the test advances by hand.
type stepClock struct {
	mu  sync.Mutex
	now stdtime.Time
}

func newStepClock(now stdtime.Time) *stepClock { return &stepClock{now: now} }

func (c *stepClock) Now() libtime.DateTime {
	c.mu.Lock()
	defer c.mu.Unlock()
	return libtime.DateTime(c.now)
}

func (c *stepClock) Advance(d stdtime.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// notFoundClaimClient answers every acquire with the vault's 404 for a task the
// index does not know. Read answers ErrClaimFree, which is what the lease
// manager sees before it tries to create the claim.
type notFoundClaimClient struct {
	mu       sync.Mutex
	acquires int
}

func (c *notFoundClaimClient) Acquire(
	context.Context, claim.ClaimTarget, string, string, int,
) (*claim.Claim, error) {
	c.mu.Lock()
	c.acquires++
	c.mu.Unlock()
	return nil, &claim.StatusError{Action: "acquire", Target: "task tid-absent", Status: 404}
}

func (c *notFoundClaimClient) Renew(
	context.Context, claim.ClaimTarget, string, string, string, int,
) (*claim.Claim, error) {
	return nil, &claim.StatusError{Action: "renew", Target: "task tid-absent", Status: 404}
}

func (c *notFoundClaimClient) List(context.Context, string) ([]claim.Claim, error) {
	return nil, nil
}

func (c *notFoundClaimClient) Release(context.Context, claim.ClaimTarget, string) error {
	return nil
}

func (c *notFoundClaimClient) Read(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
	return nil, claim.ErrClaimFree
}

func (c *notFoundClaimClient) acquireCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acquires
}

// absentJobControl satisfies claim.JobControl where the fence and restart
// recovery are not exercised.
type absentJobControl struct{}

func (absentJobControl) IsJobRunning(context.Context, string) (bool, error) { return false, nil }

func (absentJobControl) DeleteJob(context.Context, string) error { return nil }

// absentIndexClient reports a vault index that does not list the task. Its age
// decides whether that silence corroborates the 404.
type absentIndexClient struct{ ageSeconds int }

func (c absentIndexClient) Read(context.Context) (*claim.TaskIndex, error) {
	return &claim.TaskIndex{IndexAgeSeconds: c.ageSeconds}, nil
}

// absentClaimHarness is the wiring a reconcile pass needs to reach the claim
// gate for one record the vault does not contain.
type absentClaimHarness struct {
	view        handler.TaskView
	spawner     *mocks.FakeJobSpawner
	claimClient *notFoundClaimClient
	handler     handler.ClaimLifecycleEventHandler
	task        lib.Task
}

func newAbsentClaimHarness(
	t *testing.T,
	clock *stepClock,
	index claim.VaultAPIIndexClient,
) *absentClaimHarness {
	t.Helper()
	ctx := context.Background()
	// Real view: the assertion is that the NEXT pass no longer sees the record,
	// which a fake view's Snapshot could not demonstrate.
	view := handler.NewTaskView(
		func(context.Context, libkafka.Topic) (libkafka.PartitionOffsets, error) {
			return libkafka.PartitionOffsets{0: 1}, nil
		},
		libkafka.Topic("test-topic"),
		clock,
	)
	if err := view.CaptureHighWaterMarks(ctx); err != nil {
		t.Fatalf("capture high-water marks: %v", err)
	}
	task := lib.Task{
		TaskIdentifier: lib.TaskIdentifier("tid-absent"),
		Vault:          testTaskVault,
		Frontmatter: lib.TaskFrontmatter{
			"status":   "in_progress",
			"phase":    string(domain.TaskPhaseExecution),
			"assignee": "claude",
			"stage":    "prod",
		},
	}
	value, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	if err := view.ConsumeMessage(ctx, &sarama.ConsumerMessage{
		Partition: 0, Offset: 0, Key: []byte(task.TaskIdentifier), Value: value,
	}); err != nil {
		t.Fatalf("consume task event: %v", err)
	}
	if !view.Ready() {
		t.Fatal("view did not become ready")
	}

	spawner := new(mocks.FakeJobSpawner)
	spawner.SpawnJobReturns("job-1", nil)
	spawner.IsJobActiveReturns(false, nil)
	spawner.ListActiveJobsReturns(map[lib.TaskIdentifier]bool{}, nil)
	spawner.CountActiveJobsReturns(0, nil)
	resolver := &mocks.FakeConfigResolver{}
	resolver.ResolveReturns(pkg.AgentConfiguration{Assignee: "claude", Image: "img"}, nil)
	claimClient := &notFoundClaimClient{}

	return &absentClaimHarness{
		view:        view,
		spawner:     spawner,
		claimClient: claimClient,
		task:        task,
		handler: handler.NewTaskEventHandler(
			spawner,
			"dev",
			base.Branch("prod"),
			"self",
			true,
			true,
			resolver,
			new(mocks.FakeResultPublisher),
			pkg.NewTaskStore(),
			clock,
			view,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeOn,
			func() claim.ClaimRefClient { return new(mocks.FakeClaimRefClient) },
			handler.WithClaimJobControl(absentJobControl{}),
			handler.WithClaimClientFactory(func() claim.ClaimClient { return claimClient }),
			handler.WithVaultAPIIndexClient(index),
		),
	}
}

// TestReconcileEvictsRecordWhoseClaimAnswers404 drives two reconcile passes
// over a record the vault does not contain and asserts it is evicted from the
// view once the index-lag grace has expired — and that the first pass, inside
// the grace, leaves it alone.
func TestReconcileEvictsRecordWhoseClaimAnswers404(t *testing.T) {
	ctx := context.Background()
	clock := newStepClock(stdtime.Date(2026, 11, 16, 10, 0, 0, 0, stdtime.UTC))
	harness := newAbsentClaimHarness(t, clock, absentIndexClient{ageSeconds: 20})

	// Pass 1: inside the grace, a single 404 must not remove the record — the
	// index lags a fresh write, so the task may well exist.
	if err := harness.handler.ReconcileOnce(ctx); err != nil {
		t.Fatalf("first reconcile pass: %v", err)
	}
	if harness.claimClient.acquireCount() == 0 {
		t.Fatal("first pass never reached the claim gate")
	}
	if _, ok := harness.view.Get(harness.task.TaskIdentifier); !ok {
		t.Fatal("record evicted on the first 404, inside the index-lag grace")
	}

	// Pass 2: past the grace, the 404 is no longer ambiguous.
	clock.Advance(6 * stdtime.Minute)
	if err := harness.handler.ReconcileOnce(ctx); err != nil {
		t.Fatalf("second reconcile pass: %v", err)
	}
	if _, ok := harness.view.Get(harness.task.TaskIdentifier); ok {
		t.Fatal("record still in the view after the 404 grace expired")
	}
	if harness.spawner.SpawnJobCallCount() != 0 {
		t.Fatalf("expected no spawn, got %d", harness.spawner.SpawnJobCallCount())
	}
}

// TestReconcileKeepsRecordWhenTheIndexIsStale pins the second half of the
// trigger: past the grace, a 404 alone is still not enough. A stale index omits
// every task, so trusting its silence would evict live records during one
// vault-api outage.
func TestReconcileKeepsRecordWhenTheIndexIsStale(t *testing.T) {
	ctx := context.Background()
	clock := newStepClock(stdtime.Date(2026, 11, 16, 10, 0, 0, 0, stdtime.UTC))
	harness := newAbsentClaimHarness(t, clock, absentIndexClient{ageSeconds: 3600})

	if err := harness.handler.ReconcileOnce(ctx); err != nil {
		t.Fatalf("first reconcile pass: %v", err)
	}
	clock.Advance(6 * stdtime.Minute)
	if err := harness.handler.ReconcileOnce(ctx); err != nil {
		t.Fatalf("second reconcile pass: %v", err)
	}
	if _, ok := harness.view.Get(harness.task.TaskIdentifier); !ok {
		t.Fatal("record evicted on a stale index's silence")
	}
}
