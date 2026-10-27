// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	lib "github.com/bborbe/agent"
	libk8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
	"github.com/prometheus/client_golang/prometheus/testutil"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// readOnlyClaimClient is a claim.ClaimClient whose Read is the test's, and whose
// writes panic: the probe must never write, so a write reaching this fake is a
// defect rather than an unexpected call. It is defined here rather than taken
// from the mocks package, which imports pkg/handler and so cannot be imported by
// an internal test of it.
type readOnlyClaimClient struct {
	read func(context.Context, claim.ClaimTarget) (*claim.Claim, error)
}

func (c readOnlyClaimClient) Read(
	ctx context.Context,
	target claim.ClaimTarget,
) (*claim.Claim, error) {
	return c.read(ctx, target)
}

func (readOnlyClaimClient) Acquire(
	context.Context, claim.ClaimTarget, string, string, int,
) (*claim.Claim, error) {
	panic("the claim probe must not acquire")
}

func (readOnlyClaimClient) Renew(
	context.Context, claim.ClaimTarget, string, string, string, int,
) (*claim.Claim, error) {
	panic("the claim probe must not renew")
}

func (readOnlyClaimClient) List(context.Context, string) ([]claim.Claim, error) {
	panic("the claim probe must not list")
}

func (readOnlyClaimClient) Release(context.Context, claim.ClaimTarget, string) error {
	panic("the claim probe must not release")
}

// inactiveSpawner reports no active Job for any task, which is the state this
// guard exists for: a Job that runs somewhere this cluster cannot see.
type inactiveSpawner struct{}

func (inactiveSpawner) SpawnJob(
	context.Context, lib.Task, pkg.AgentConfiguration,
) (string, error) {
	return "", nil
}

func (inactiveSpawner) IsJobActive(
	context.Context, libk8s.Namespace, lib.TaskIdentifier,
) (bool, error) {
	return false, nil
}

func (inactiveSpawner) CountActiveJobs(context.Context, libk8s.Namespace, string) (int, error) {
	return 0, nil
}

func (inactiveSpawner) ListActiveJobs(
	context.Context, libk8s.Namespace, string,
) (map[lib.TaskIdentifier]bool, error) {
	return map[lib.TaskIdentifier]bool{}, nil
}

// JobExists reports the stored Job as still present — the foreign case: the Job
// runs somewhere this cluster cannot see, so the guard must keep suppressing.
func (inactiveSpawner) JobExists(context.Context, libk8s.Namespace, string) (bool, error) {
	return true, nil
}

// currentJobTask is a task whose frontmatter names jobName, started startedAgo
// before now.
func currentJobTask(jobName string, startedAgo time.Duration) lib.Task {
	startedAt := time.Now().Add(-startedAgo).UTC().Format(time.RFC3339)
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier("tid-current-job-1"),
		Frontmatter: lib.TaskFrontmatter{
			"current_job":    jobName,
			"job_started_at": startedAt,
		},
	}
}

// jobConfig is the config the guard is asked to decide under.
func jobConfig() *pkg.AgentConfiguration {
	return &pkg.AgentConfiguration{Assignee: "pr-reviewer-agent"}
}

// newCurrentJobHandler builds a handler whose store and spawner are empty.
func newCurrentJobHandler() *taskEventHandler {
	return &taskEventHandler{
		jobSpawner:       inactiveSpawner{},
		taskStore:        pkg.NewTaskStore(),
		currentDateTime:  libtime.NewCurrentDateTime(),
		deferredRespawns: map[lib.TaskIdentifier]deferredEntry{},
	}
}

// TestJobProvablyFinishedRejectsAnotherClustersJob covers the incident this
// guard exists for: the store still names the Job this executor created, while
// the frontmatter names a different one — so that Job belongs to another
// cluster, and its absence here says nothing about whether it is running.
func TestJobProvablyFinishedRejectsAnotherClustersJob(t *testing.T) {
	h := newCurrentJobHandler()
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)
	h.taskStore.Store(task.TaskIdentifier, lib.Task{
		TaskIdentifier: task.TaskIdentifier,
		Frontmatter: lib.TaskFrontmatter{
			"current_job": "pr-reviewer-agent-94c1801c-20261022202004",
		},
	})

	finished, err := h.jobProvablyFinished(
		context.Background(),
		task,
		"pr-reviewer-agent-94c1801c-20261022205503",
		libk8s.Namespace("dev"),
	)
	if err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil", err)
	}
	if finished {
		t.Fatal("provably finished = true for a Job this executor did not spawn; want false")
	}
}

// TestJobProvablyFinishedAcceptsItsOwnJob covers the ordinary respawn: the store
// names the same Job and it is no longer active, so it is over.
func TestJobProvablyFinishedAcceptsItsOwnJob(t *testing.T) {
	h := newCurrentJobHandler()
	jobName := "pr-reviewer-agent-94c1801c-20261022202004"
	task := currentJobTask(jobName, 7*time.Minute)
	h.taskStore.Store(task.TaskIdentifier, task)

	finished, err := h.jobProvablyFinished(
		context.Background(),
		task,
		jobName,
		libk8s.Namespace("dev"),
	)
	if err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil", err)
	}
	if !finished {
		t.Fatal("provably finished = false for a Job this executor spawned and saw end; want true")
	}
}

// claimedCurrentJobHandler is newCurrentJobHandler with claim mode on and the
// given claim read installed: the shared record the no-store-entry case is
// decided on.
func claimedCurrentJobHandler(
	read func(context.Context, claim.ClaimTarget) (*claim.Claim, error),
) *taskEventHandler {
	h := newCurrentJobHandler()
	h.claimMode = pkg.ClaimModeOn
	h.claimClientFactory = func() claim.ClaimClient {
		return readOnlyClaimClient{read: read}
	}
	return h
}

// liveClaim is a claim another cluster holds right now.
func liveClaim() *claim.Claim {
	return &claim.Claim{
		Holder:     "sun",
		LeaseUntil: libtime.DateTime(time.Now().Add(2 * time.Minute)),
	}
}

// TestJobProvablyFinishedRejectsAClaimedForeignJobWithNoStoreEntry is SC1's
// case: the store is empty (a cold pod, or a spawn this executor never made),
// so nothing local can tell another cluster's running Job from this executor's
// finished one. The live claim is the shared fact that can, and it says the
// task is running — so the respawn must not proceed.
func TestJobProvablyFinishedRejectsAClaimedForeignJobWithNoStoreEntry(t *testing.T) {
	h := claimedCurrentJobHandler(
		func(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
			return liveClaim(), nil
		},
	)
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)

	finished, err := h.jobProvablyFinished(
		context.Background(),
		task,
		"pr-reviewer-agent-94c1801c-20261022205503",
		libk8s.Namespace("dev"),
	)
	if err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil", err)
	}
	if finished {
		t.Fatal("provably finished = true while a live claim holds the task; want false")
	}
}

// TestJobProvablyFinishedAcceptsAnUnclaimedJobWithNoStoreEntry is SC2's
// negative control: a genuine restart, where the previous Job really has
// finished and its claim is gone. Suppressing here would strand the task
// forever, so the respawn must proceed.
func TestJobProvablyFinishedAcceptsAnUnclaimedJobWithNoStoreEntry(t *testing.T) {
	h := claimedCurrentJobHandler(
		func(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
			return nil, claim.ErrClaimFree
		},
	)
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)

	finished, err := h.jobProvablyFinished(
		context.Background(),
		task,
		"pr-reviewer-agent-94c1801c-20261022205503",
		libk8s.Namespace("dev"),
	)
	if err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil", err)
	}
	if !finished {
		t.Fatal("provably finished = false with no claim held; want true so a restart respawns")
	}
}

// TestJobProvablyFinishedAsksForTheTasksOwnClaim asserts the probe reads the
// claim the task's spawn would take, not some other key: a probe that reads the
// wrong target returns "free" on every input and would pass both tests above
// while distinguishing nothing.
func TestJobProvablyFinishedAsksForTheTasksOwnClaim(t *testing.T) {
	var asked claim.ClaimTarget
	h := claimedCurrentJobHandler(
		func(_ context.Context, target claim.ClaimTarget) (*claim.Claim, error) {
			asked = target
			return nil, claim.ErrClaimFree
		},
	)
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)

	if _, err := h.jobProvablyFinished(
		context.Background(),
		task,
		"pr-reviewer-agent-94c1801c-20261022205503",
		libk8s.Namespace("dev"),
	); err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil", err)
	}
	if asked.TaskID != string(task.TaskIdentifier) {
		t.Errorf(
			"claim read for task id %q; want %q",
			asked.TaskID,
			task.TaskIdentifier,
		)
	}
}

// TestJobProvablyFinishedTreatsAnExpiredClaimAsFinished covers the takeover
// rule: a claim past its lease is one nobody renewed, so it is not evidence that
// anything is still running.
func TestJobProvablyFinishedTreatsAnExpiredClaimAsFinished(t *testing.T) {
	h := claimedCurrentJobHandler(
		func(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
			return &claim.Claim{
				Holder:     "sun",
				LeaseUntil: libtime.DateTime(time.Now().Add(-time.Minute)),
			}, nil
		},
	)
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)

	finished, err := h.jobProvablyFinished(
		context.Background(),
		task,
		"pr-reviewer-agent-94c1801c-20261022205503",
		libk8s.Namespace("dev"),
	)
	if err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil", err)
	}
	if !finished {
		t.Fatal("provably finished = false for an expired claim; want true")
	}
}

// TestJobProvablyFinishedTreatsAnUnreadableClaimAsUnfinished pins the failure
// direction: a claim store that cannot be read is not evidence that no one holds
// the task. Suppressing is safe because the next pass retries, and the counter
// is what makes a fleet held by a broken claim read visible.
func TestJobProvablyFinishedTreatsAnUnreadableClaimAsUnfinished(t *testing.T) {
	h := claimedCurrentJobHandler(
		func(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
			return nil, stderrors.New("vault-api unreachable")
		},
	)
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)
	before := testutil.ToFloat64(
		metrics.TaskEventsTotal.WithLabelValues("claim_probe_failed"),
	)

	finished, err := h.jobProvablyFinished(
		context.Background(),
		task,
		"pr-reviewer-agent-94c1801c-20261022205503",
		libk8s.Namespace("dev"),
	)
	if err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil (the read is retried, not fatal)", err)
	}
	if finished {
		t.Fatal("provably finished = true after a failed claim read; want false")
	}
	after := testutil.ToFloat64(
		metrics.TaskEventsTotal.WithLabelValues("claim_probe_failed"),
	)
	if delta := after - before; delta != 1 {
		t.Errorf("claim_probe_failed delta = %v; want 1", delta)
	}
}

// TestJobProvablyFinishedWithoutAClaimStoreKeepsPriorBehaviour states the
// residual, rather than leaving it implicit: claim mode off keeps no shared
// record, so the ambiguity cannot be resolved and the pre-existing behaviour
// stands. Bounding the case by the Job's deadline instead was tried and
// reverted — it withholds a legitimate respawn for up to 30 minutes, which the
// deferred-respawn contract does not allow.
func TestJobProvablyFinishedWithoutAClaimStoreKeepsPriorBehaviour(t *testing.T) {
	h := newCurrentJobHandler()
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)

	finished, err := h.jobProvablyFinished(
		context.Background(),
		task,
		"pr-reviewer-agent-94c1801c-20261022205503",
		libk8s.Namespace("dev"),
	)
	if err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil", err)
	}
	if !finished {
		t.Fatal("provably finished = false with no claim store; want true (prior behaviour)")
	}
}

// goneSpawner reports every named Job as absent from this cluster: the Job
// finished and was garbage-collected before the informer observed a terminal
// update, which is the leak the store can never clear by itself.
type goneSpawner struct{ inactiveSpawner }

func (goneSpawner) JobExists(context.Context, libk8s.Namespace, string) (bool, error) {
	return false, nil
}

// TestJobProvablyFinishedClearsALeakedStoreEntry is the convergence guard. The
// store entry is only cleared by the job informer processing a terminal event,
// and the informer registers no DeleteFunc, so a Job GC'd before that event was
// seen leaves an entry nothing removes. Without the self-heal the task is
// suppressed on every pass, forever — silent, and indistinguishable from an idle
// agent. With it, the cluster's own answer (the Job is gone) drops the entry and
// the task respawns.
func TestJobProvablyFinishedClearsALeakedStoreEntry(t *testing.T) {
	h := newCurrentJobHandler()
	h.jobSpawner = goneSpawner{}
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)
	h.taskStore.Store(task.TaskIdentifier, lib.Task{
		TaskIdentifier: task.TaskIdentifier,
		Frontmatter: lib.TaskFrontmatter{
			"current_job": "pr-reviewer-agent-94c1801c-20261022202004",
		},
	})

	finished, err := h.jobProvablyFinished(
		context.Background(),
		task,
		"pr-reviewer-agent-94c1801c-20261022205503",
		libk8s.Namespace("dev"),
	)
	if err != nil {
		t.Fatalf("jobProvablyFinished returned %v; want nil", err)
	}
	if !finished {
		t.Fatal(
			"provably finished = false for a leaked store entry; want true so the task respawns",
		)
	}
	if _, ok := h.taskStore.Load(task.TaskIdentifier); ok {
		t.Fatal("store entry still present after the stale-Job clear; want it dropped")
	}
}

// TestCheckActiveCurrentJobSuppressesAForeignJob is the end-to-end assertion: a
// dispatch pass whose current_job names a Job this executor never created spawns
// nothing and re-defers instead of dropping the task.
func TestCheckActiveCurrentJobSuppressesAForeignJob(t *testing.T) {
	ctx := context.Background()
	h := newCurrentJobHandler()
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)
	h.taskStore.Store(task.TaskIdentifier, lib.Task{
		TaskIdentifier: task.TaskIdentifier,
		Frontmatter: lib.TaskFrontmatter{
			"current_job": "pr-reviewer-agent-94c1801c-20261022202004",
		},
	})
	before := testutil.ToFloat64(
		metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_job"),
	)

	suppress, err := h.checkActiveCurrentJob(
		ctx,
		task,
		"pr-reviewer-agent-94c1801c-20261022205503",
		jobConfig(),
		nil,
	)

	if err != nil {
		t.Fatalf("checkActiveCurrentJob: %v", err)
	}
	if !suppress {
		t.Fatal("suppress = false for a foreign Job; want true — this is the duplicate")
	}
	after := testutil.ToFloat64(
		metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_job"),
	)
	if delta := after - before; delta != 1 {
		t.Errorf("skipped_foreign_job delta = %v; want 1", delta)
	}
	entry, ok := h.deferredRespawns[task.TaskIdentifier]
	if !ok {
		t.Fatal("no deferred entry stored; the task would not be re-checked")
	}
	if entry.retryAfter.IsZero() {
		t.Error("deferred entry retryAfter = zero; want a future re-check")
	}
}

// TestCheckActiveCurrentJobSuppressesAClaimedForeignJobWithNoStoreEntry is the
// end-to-end half of SC1: a dispatch pass on a cold pod — empty store, no local
// Job — must not spawn while the task is claimed elsewhere, and must re-defer it
// so the decision is revisited rather than dropped.
//
// It is also SC1(b): the deferral is only worth anything because the check it
// defers to returns the distinguishing result, which the sibling test above
// (claimed vs free) pins directly. This one asserts the deferral fires.
func TestCheckActiveCurrentJobSuppressesAClaimedForeignJobWithNoStoreEntry(t *testing.T) {
	ctx := context.Background()
	h := claimedCurrentJobHandler(
		func(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
			return liveClaim(), nil
		},
	)
	task := currentJobTask("pr-reviewer-agent-94c1801c-20261022205503", 7*time.Minute)
	before := testutil.ToFloat64(
		metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_job"),
	)

	suppress, err := h.checkActiveCurrentJob(
		ctx,
		task,
		"pr-reviewer-agent-94c1801c-20261022205503",
		jobConfig(),
		nil,
	)

	if err != nil {
		t.Fatalf("checkActiveCurrentJob: %v", err)
	}
	if !suppress {
		t.Fatal("suppress = false for a task claimed by another cluster; want true")
	}
	after := testutil.ToFloat64(
		metrics.TaskEventsTotal.WithLabelValues("skipped_foreign_job"),
	)
	if delta := after - before; delta != 1 {
		t.Errorf("skipped_foreign_job delta = %v; want 1", delta)
	}
	entry, ok := h.deferredRespawns[task.TaskIdentifier]
	if !ok {
		t.Fatal("no deferred entry stored; the task would never be re-checked")
	}
	if entry.retryAfter.IsZero() {
		t.Error("deferred entry retryAfter = zero; want a future re-check")
	}
}
