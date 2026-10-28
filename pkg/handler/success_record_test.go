// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	stderrors "errors"
	"strconv"
	"strings"
	"sync"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/handler"
)

// memoryRefClient is a compare-and-swap ref store that outlives the handlers in
// one spec, which is the point: the shared record only means something if a
// second handler — the restarted pod — reads what the first one wrote.
type memoryRefClient struct {
	mu   sync.Mutex
	refs map[string]memoryRef
	seq  int
}

type memoryRef struct {
	sha        string
	leaseUntil string
}

func newMemoryRefClient() *memoryRefClient {
	return &memoryRefClient{refs: map[string]memoryRef{}}
}

func (c *memoryRefClient) List(_ context.Context, prefix string) ([]claim.ClaimRef, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]claim.ClaimRef, 0, len(c.refs))
	for key, ref := range c.refs {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		out = append(out, claim.ClaimRef{
			Ref:        "refs/" + key,
			Sha:        ref.sha,
			Cluster:    claim.SuccessRecordCluster,
			LeaseUntil: ref.leaseUntil,
		})
	}
	return out, nil
}

func (c *memoryRefClient) Put(
	_ context.Context,
	key string,
	expectedSha string,
	_ string,
	_ string,
	leaseUntil libtime.DateTime,
) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, exists := c.refs[key]
	if expectedSha == "" && exists {
		return "", claim.ErrLeaseConflict
	}
	if expectedSha != "" && (!exists || current.sha != expectedSha) {
		return "", claim.ErrLeaseConflict
	}
	c.seq++
	sha := "sha-" + strconv.Itoa(c.seq)
	c.refs[key] = memoryRef{
		sha:        sha,
		leaseUntil: leaseUntil.Time().UTC().Format(time.RFC3339),
	}
	return sha, nil
}

func (c *memoryRefClient) Delete(_ context.Context, key string, expectedSha string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, exists := c.refs[key]
	if !exists || current.sha != expectedSha {
		return claim.ErrLeaseConflict
	}
	delete(c.refs, key)
	return nil
}

var _ = Describe("the shared success record", func() {
	var (
		ctx                 context.Context
		fakeSpawner         *mocks.FakeJobSpawner
		fakeResolver        *mocks.FakeConfigResolver
		fakeResultPublisher *mocks.FakeResultPublisher
		fakeView            *mocks.FakeTaskView
		currentDateTime     libtime.CurrentDateTime
		now                 time.Time
		refs                *memoryRefClient
	)

	// newHandler builds a handler over its OWN in-memory store, so two handlers
	// over one refs client model a pod restart exactly: the memory is gone, the
	// shared claim store is not.
	newHandler := func(store *pkg.TaskStore) handler.ClaimLifecycleEventHandler {
		return handler.NewTaskEventHandler(
			fakeSpawner,
			"dev",
			base.Branch("prod"),
			"",
			true,
			true,
			fakeResolver,
			fakeResultPublisher,
			store,
			currentDateTime,
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			// Shadow, not off: the success record is built only in the modes that
			// take a claim, and shadow takes one without ever blocking a dispatch,
			// so the spawn path still runs when the claim machinery is absent.
			pkg.ClaimModeShadow,
			func() claim.ClaimRefClient { return refs },
		)
	}

	// newHandlerWithStore builds a handler whose shared record is the supplied
	// fake, so a spec can drive the store's error and hit paths directly.
	newHandlerWithStore := func(store claim.SuccessStore) handler.ClaimLifecycleEventHandler {
		return handler.NewTaskEventHandler(
			fakeSpawner,
			"dev",
			base.Branch("prod"),
			"",
			true,
			true,
			fakeResolver,
			fakeResultPublisher,
			pkg.NewTaskStore(),
			currentDateTime,
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeShadow,
			func() claim.ClaimRefClient { return refs },
			handler.WithSuccessStore(store),
		)
	}

	// eligibleTask mirrors the shape every other reconcile spec uses: status
	// in_progress, a triggerable phase, an assignee, the executor's branch stage,
	// no current_job, stamped with this executor's vault.
	eligibleTask := func(phase domain.TaskPhase) lib.Task {
		return lib.Task{
			TaskIdentifier: "tid-a",
			Vault:          testTaskVault,
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(phase),
				"assignee": "claude",
				"stage":    "prod",
			},
		}
	}

	BeforeEach(func() {
		ctx = context.Background()
		now = time.Date(2026, 10, 28, 6, 0, 0, 0, time.UTC)
		fakeSpawner = new(mocks.FakeJobSpawner)
		fakeResolver = &mocks.FakeConfigResolver{}
		fakeResolver.ResolveReturns(
			pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"},
			nil,
		)
		fakeResultPublisher = &mocks.FakeResultPublisher{}
		fakeView = &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		currentDateTime = libtime.NewCurrentDateTime()
		currentDateTime.SetNow(libtime.DateTime(now))
		refs = newMemoryRefClient()
	})

	It("suppresses a re-spawn from a fresh store after a restart", func() {
		// SC2's restart half. A per-pod map returns "never succeeded" on a new
		// instance, so an in-memory-only suppression re-opens the duplicate on
		// every deploy. The record is written by the pod that ran the Job and read
		// by the pod that replaced it.
		first := newHandler(pkg.NewTaskStore())
		first.ReleaseTaskClaimAfterSuccess(
			ctx, "tid-a", "execution", now.Add(-2*time.Hour),
		)

		restarted := newHandler(pkg.NewTaskStore())
		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})
		fakeSpawner.IsJobActiveReturns(false, nil)

		Expect(restarted.ReconcileOnce(ctx)).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("suppresses after the pod rewrites trigger_scope and job_started_at", func() {
		// SC2's rewrite half. The agent pods rewrite the frontmatter from a
		// snapshot taken at the start of their run, so trigger_scope and
		// job_started_at come back at their pre-spawn values. A suppression that
		// lived in either field would read as "not yet run" and spawn again.
		first := newHandler(pkg.NewTaskStore())
		first.ReleaseTaskClaimAfterSuccess(
			ctx, "tid-a", "execution", now.Add(-2*time.Hour),
		)

		rewritten := eligibleTask(domain.TaskPhaseExecution)
		rewritten.Frontmatter["trigger_scope"] = "planning:"
		rewritten.Frontmatter["job_started_at"] = "2026-10-21T05:00:00Z"

		restarted := newHandler(pkg.NewTaskStore())
		fakeView.SnapshotReturns([]lib.Task{rewritten})
		fakeSpawner.IsJobActiveReturns(false, nil)

		Expect(restarted.ReconcileOnce(ctx)).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("does not suppress a restart's first pass when no record was written", func() {
		// The control for both specs above: the same fresh-store handler spawns
		// when the shared store holds nothing, so a pass cannot be green because
		// the guard refuses everything.
		restarted := newHandler(pkg.NewTaskStore())
		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})
		fakeSpawner.IsJobActiveReturns(false, nil)

		Expect(restarted.ReconcileOnce(ctx)).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("releases the suppression when the phase changed since the success", func() {
		// SC3's negative control through the shared record: a success at planning
		// says nothing about execution, so the restarted pod must spawn.
		first := newHandler(pkg.NewTaskStore())
		first.ReleaseTaskClaimAfterSuccess(
			ctx, "tid-a", "planning", now.Add(-2*time.Hour),
		)

		restarted := newHandler(pkg.NewTaskStore())
		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})
		fakeSpawner.IsJobActiveReturns(false, nil)

		Expect(restarted.ReconcileOnce(ctx)).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("suppresses from a shared record when the pod's memory is empty", func() {
		// The read path itself, with no ref client in play: a store that answers
		// with a completion at the task's phase suppresses the spawn.
		store := new(mocks.FakeSuccessStore)
		// Inside a reconcile pass the read is the pass snapshot, not Lookup.
		store.SnapshotReturns(
			map[string]map[string]libtime.DateTime{
				"tid-a": {"execution": libtime.DateTime(now.Add(-2 * time.Hour))},
			},
			nil,
		)

		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})
		fakeSpawner.IsJobActiveReturns(false, nil)

		Expect(newHandlerWithStore(store).ReconcileOnce(ctx)).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("falls back to memory when the shared read fails", func() {
		// A store that cannot be read is not evidence either way. Suppressing on a
		// failed read would strand a task behind an unreachable store, so the guard
		// behaves exactly as it did before the record existed.
		store := new(mocks.FakeSuccessStore)
		store.SnapshotReturns(nil, stderrors.New("git-rest down"))

		fakeView.SnapshotReturns([]lib.Task{eligibleTask(domain.TaskPhaseExecution)})
		fakeSpawner.IsJobActiveReturns(false, nil)

		Expect(newHandlerWithStore(store).ReconcileOnce(ctx)).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})

	It("releases a recurring task once its cadence elapsed, across a restart", func() {
		// The cadence release must survive the restart too, or a recurring task
		// whose record is shared is suppressed on every new pod for good.
		first := newHandler(pkg.NewTaskStore())
		first.ReleaseTaskClaimAfterSuccess(
			ctx, "tid-a", "execution", now.Add(-8*24*time.Hour),
		)

		task := eligibleTask(domain.TaskPhaseExecution)
		task.Frontmatter["recurring"] = "weekly"

		restarted := newHandler(pkg.NewTaskStore())
		fakeView.SnapshotReturns([]lib.Task{task})
		fakeSpawner.IsJobActiveReturns(false, nil)

		Expect(restarted.ReconcileOnce(ctx)).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	})
})
