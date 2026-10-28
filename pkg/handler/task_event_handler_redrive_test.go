// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	stderrors "errors"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/claim/claimtest"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// sharedClaimRefTransport is an in-memory git-rest stand-in that several
// harnesses can share. Its CAS semantics mirror the real ref client closely
// enough for the redrive-streak specs: a create conflicts when the key exists,
// an update conflicts unless the expected sha matches, and a delete conflicts
// unless the key exists with the expected sha.
type sharedClaimRefTransport struct {
	mu      sync.Mutex
	refs    map[string]claim.ClaimRef // key WITHOUT the "refs/" prefix
	nextSha int
	listErr error
	putErr  error
}

func newSharedClaimRefTransport() *sharedClaimRefTransport {
	return &sharedClaimRefTransport{refs: map[string]claim.ClaimRef{}}
}

// seed stores a ref directly, bypassing CAS, so a spec can model state left by
// an earlier process.
func (t *sharedClaimRefTransport) seed(
	key, sha, cluster, taskID, leaseUntil string,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refs[key] = claim.ClaimRef{
		Ref:        "refs/" + key,
		Sha:        sha,
		Cluster:    cluster,
		TaskID:     taskID,
		LeaseUntil: leaseUntil,
	}
}

// attach installs the transport as client's List, Put and Delete.
func (t *sharedClaimRefTransport) attach(client *mocks.FakeClaimRefClient) {
	client.ListCalls(t.list)
	client.PutCalls(t.put)
	client.DeleteCalls(t.delete)
}

func (t *sharedClaimRefTransport) list(
	_ context.Context,
	prefix string,
) ([]claim.ClaimRef, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.listErr != nil {
		return nil, t.listErr
	}
	result := make([]claim.ClaimRef, 0, len(t.refs))
	for key, ref := range t.refs {
		if keyMatchesPrefix(key, prefix) {
			result = append(result, ref)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Ref < result[j].Ref })
	return result, nil
}

func (t *sharedClaimRefTransport) put(
	_ context.Context,
	key string,
	expectedSha string,
	cluster string,
	taskID string,
	leaseUntil libtime.DateTime,
) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.putErr != nil {
		return "", t.putErr
	}
	existing, exists := t.refs[key]
	switch {
	case expectedSha == "" && exists:
		return "", claim.ErrLeaseConflict
	case expectedSha != "" && (!exists || existing.Sha != expectedSha):
		return "", claim.ErrLeaseConflict
	}
	t.nextSha++
	sha := "sha-" + strconv.Itoa(t.nextSha)
	t.refs[key] = claim.ClaimRef{
		Ref:        "refs/" + key,
		Sha:        sha,
		Cluster:    cluster,
		TaskID:     taskID,
		LeaseUntil: leaseUntil.UTC().Format(time.RFC3339),
	}
	return sha, nil
}

func (t *sharedClaimRefTransport) delete(
	_ context.Context,
	key string,
	expectedSha string,
) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.putErr != nil {
		return t.putErr
	}
	existing, exists := t.refs[key]
	if !exists || existing.Sha != expectedSha {
		return claim.ErrLeaseConflict
	}
	delete(t.refs, key)
	return nil
}

// keyMatchesPrefix applies git-rest's prefix listing rule.
func keyMatchesPrefix(key, prefix string) bool {
	if key == prefix {
		return true
	}
	if strings.HasSuffix(prefix, "/") {
		return strings.HasPrefix(key, prefix)
	}
	return strings.HasPrefix(key, prefix+"/")
}

// newRedriveHarness mirrors newClaimHarness but takes the ref client, so two
// harnesses can share one transport and one persisted streak.
func newRedriveHarness(mode pkg.ClaimMode, client *mocks.FakeClaimRefClient) *claimHarness {
	spawner := new(mocks.FakeJobSpawner)
	spawner.SpawnJobReturns("job-1", nil)
	spawner.IsJobActiveReturns(false, nil)
	spawner.ListActiveJobsReturns(map[lib.TaskIdentifier]bool{}, nil)
	spawner.CountActiveJobsReturns(0, nil)

	resolver := new(mocks.FakeConfigResolver)
	resolver.ResolveReturns(pkg.AgentConfiguration{Assignee: "claude", Image: "img"}, nil)

	publisher := new(mocks.FakeResultPublisher)
	taskStore := pkg.NewTaskStore()
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
		handler.WithClaimJobControl(noopClaimJobControl{}),
		handler.WithClaimClientFactory(func() claim.ClaimClient {
			return claimtest.NewClient(client, libtime.NewCurrentDateTime().Now)
		}),
	)
	return h
}

// countRefs returns how many stored refs sit under prefix. Assertions about the
// streak must use this rather than client.PutCallCount(): the claim lifecycle
// writes its own refs through the same client, so a bare Put count says nothing
// about the streak.
func (t *sharedClaimRefTransport) countRefs(prefix string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for key := range t.refs {
		if strings.HasPrefix(key, prefix) {
			n++
		}
	}
	return n
}

// seedRedriveStreak pre-loads n streak refs for id at phase. The spawn path no
// longer writes them — the streak counts published failures and is written by
// resultPublisher.recordRedriveFailure — so a spec that needs a capped record
// seeds the streak instead of spawning n times.
func seedRedriveStreak(
	transport *sharedClaimRefTransport,
	id, phase string,
	n int,
) {
	for i := 1; i <= n; i++ {
		transport.seed(
			fmt.Sprintf("claims/redrive/%s/%s/%d", id, phase, i),
			fmt.Sprintf("seed-%s-%d", id, i),
			claim.RedriveStreakCluster,
			id,
			"",
		)
	}
}

// redriveTask is a qualifying task under the given id.
func redriveTask(id string) lib.Task {
	task := qualifyingClaimTask()
	task.TaskIdentifier = lib.TaskIdentifier(id)
	return task
}

var _ = Describe("redrive streak persistence", func() {
	It("AC7: caps across two clusters that share one streak store", func() {
		transport := newSharedClaimRefTransport()
		client := new(mocks.FakeClaimRefClient)
		transport.attach(client)
		a := newRedriveHarness(pkg.ClaimModeShadow, client)
		b := newRedriveHarness(pkg.ClaimModeShadow, client)
		task := redriveTask("tid-redrive-shared")
		// The streak counts PUBLISHED FAILURES and is written by the result
		// publisher, not by the spawn. Seed three, as three failures would.
		seedRedriveStreak(transport, "tid-redrive-shared", "execution", 3)

		Expect(a.consume(task)).To(Succeed())
		Expect(b.consume(task)).To(Succeed())
		Expect(a.spawner.SpawnJobCallCount() + b.spawner.SpawnJobCallCount()).To(Equal(0))

		Expect(flag.Set("v", "1")).To(Succeed())
		out := captureStderr(func() {
			Expect(b.consume(task)).To(Succeed())
		})
		Expect(flag.Set("v", "0")).To(Succeed())
		Expect(out).To(ContainSubstring(fmt.Sprintf(
			"event=redrive_capped task=%s task_type=%s streak=3 cap=3 phase=%q",
			task.TaskIdentifier, "", "execution",
		)))

		Expect(a.consume(task)).To(Succeed())
		Expect(a.spawner.SpawnJobCallCount() + b.spawner.SpawnJobCallCount()).To(Equal(0))
	})

	It("AC7(a): four successful spawns write no streak ref and never reach the cap", func() {
		// The defect this pins: the streak used to advance on every successful
		// SpawnJob, so three exit-0 spawns reached the cap with no reason to
		// classify and the record was refused forever. A spawn publishes no
		// failure, so it must write nothing — the streak counts published
		// failures only.
		//
		// The FOURTH consume is the discriminating one, and it is not padding.
		// The store is readable and reports 0, while the in-memory map has
		// counted all three attempts. Under `max(memory, stored)` — the shape
		// this change replaced — the fourth consume reads 3, sees the cap, and
		// refuses; only `count = stored` lets it spawn. Three consumes pass
		// either way, because the third is checked while memory is still 2, so
		// a regression back to max() would go undetected.
		transport := newSharedClaimRefTransport()
		client := new(mocks.FakeClaimRefClient)
		transport.attach(client)
		a := newRedriveHarness(pkg.ClaimModeShadow, client)
		task := redriveTask("tid-redrive-spawn-only")

		for i := 0; i < 4; i++ {
			Expect(a.consume(task)).To(Succeed())
		}

		Expect(a.spawner.SpawnJobCallCount()).To(Equal(4))
		Expect(transport.countRefs("claims/redrive/tid-redrive-spawn-only/")).To(Equal(0))
	})

	It("AC7: a restarted pod reads the streak left by an earlier one", func() {
		transport := newSharedClaimRefTransport()
		client := new(mocks.FakeClaimRefClient)
		transport.attach(client)
		id := "tid-redrive-restart"
		for n := 1; n <= 3; n++ {
			transport.seed(
				fmt.Sprintf("claims/redrive/%s/execution/%d", id, n),
				fmt.Sprintf("seed-%d", n),
				claim.RedriveStreakCluster,
				id,
				"",
			)
		}
		c := newRedriveHarness(pkg.ClaimModeShadow, client)

		Expect(c.consume(redriveTask(id))).To(Succeed())

		Expect(c.spawner.SpawnJobCallCount()).To(Equal(0))
	})

	It(
		"AC8(a): a failed store read falls back to memory and the capped event re-reads both stores",
		func() {
			transport := newSharedClaimRefTransport()
			transport.listErr = stderrors.New("git-rest down")
			client := new(mocks.FakeClaimRefClient)
			transport.attach(client)
			a := newRedriveHarness(pkg.ClaimModeShadow, client)
			task := redriveTask("tid-redrive-read-fail")

			for i := 0; i < 3; i++ {
				Expect(a.consume(task)).To(Succeed())
			}
			Expect(a.spawner.SpawnJobCallCount()).To(Equal(3))
			listCalls := client.ListCallCount()

			Expect(a.consume(task)).To(Succeed())

			// The spawn is still refused from the in-memory floor, but reaching the
			// cap no longer short-circuits: the capped event reads the streak and then
			// the reasons, so the classifier can run on the ordinary path. A third
			// read is the shared success record, which the guard consults whenever the
			// pod's own memory has no record for the task — one list per event on that
			// path, and one per PASS on the reconcile path, which reads a snapshot.
			Expect(a.spawner.SpawnJobCallCount()).To(Equal(3))
			Expect(client.ListCallCount()).To(Equal(listCalls + 3))
		},
	)

	It("AC8(b): a failed store read warns and never clears the streak", func() {
		transport := newSharedClaimRefTransport()
		transport.listErr = stderrors.New("git-rest down")
		client := new(mocks.FakeClaimRefClient)
		transport.attach(client)
		a := newRedriveHarness(pkg.ClaimModeShadow, client)
		task := redriveTask("tid-redrive-read-warn")

		out := captureStderr(func() {
			Expect(a.consume(task)).To(Succeed())
		})

		Expect(a.spawner.SpawnJobCallCount()).To(Equal(1))
		Expect(out).To(ContainSubstring(
			"event=redrive_streak_read_failed task=" + string(task.TaskIdentifier),
		))
		Expect(client.DeleteCallCount()).To(Equal(0))
	})

	It("AC8(c): with the store unreadable the memory floor still caps", func() {
		transport := newSharedClaimRefTransport()
		transport.listErr = stderrors.New("git-rest down")
		client := new(mocks.FakeClaimRefClient)
		transport.attach(client)
		a := newRedriveHarness(pkg.ClaimModeShadow, client)
		task := redriveTask("tid-redrive-write-fail")

		// The store cannot be read, so the in-memory attempt floor is the only
		// bound left. It still caps at maxConsecutiveRedrives — which is the
		// whole point of keeping it: the failure count is unavailable, so the
		// record must not run unbounded.
		for i := 0; i < 4; i++ {
			Expect(a.consume(task)).To(Succeed())
		}
		Expect(a.spawner.SpawnJobCallCount()).To(Equal(3))
	})

	It("AC8(d): an unparseable task id writes nothing and still caps in memory", func() {
		transport := newSharedClaimRefTransport()
		client := new(mocks.FakeClaimRefClient)
		transport.attach(client)
		a := newRedriveHarness(pkg.ClaimModeShadow, client)
		task := redriveTask("bad/id")

		// The id is unparseable, so every store read fails and the in-memory
		// floor bounds the record. Nothing is written to the store on the spawn
		// path any more, so the old write-failure line is gone with it.
		for i := 0; i < 3; i++ {
			Expect(a.consume(task)).To(Succeed())
		}

		Expect(a.spawner.SpawnJobCallCount()).To(Equal(3))
		Expect(client.PutCallCount()).To(Equal(0))
		Expect(client.ListCallCount()).To(Equal(0))
		Expect(client.DeleteCallCount()).To(Equal(0))
	})

	It("AC9: claim mode off constructs no store and sends no request", func() {
		client := new(mocks.FakeClaimRefClient)
		a := newRedriveHarness(pkg.ClaimModeOff, client)
		task := redriveTask("tid-redrive-off")

		for i := 0; i < 4; i++ {
			Expect(a.consume(task)).To(Succeed())
		}

		Expect(a.spawner.SpawnJobCallCount()).To(Equal(3))
		Expect(a.factoryCalls).To(Equal(0))
		Expect(client.PutCallCount()).To(Equal(0))
		Expect(client.ListCallCount()).To(Equal(0))
		Expect(client.DeleteCallCount()).To(Equal(0))
	})

	It("labels a capped spawn by its task type and phase", func() {
		transport := newSharedClaimRefTransport()
		client := new(mocks.FakeClaimRefClient)
		transport.attach(client)
		a := newRedriveHarness(pkg.ClaimModeShadow, client)
		task := redriveTask("tid-redrive-labels")
		seedRedriveStreak(transport, "tid-redrive-labels", "execution", 3)

		before := testutil.ToFloat64(
			metrics.RedriveCappedTotal.WithLabelValues("", "execution"),
		)
		Expect(a.consume(task)).To(Succeed())
		Expect(testutil.ToFloat64(
			metrics.RedriveCappedTotal.WithLabelValues("", "execution"),
		)).To(Equal(before + 1))
	})

	It("counts a capped spawn at another phase in a separate series", func() {
		transport := newSharedClaimRefTransport()
		client := new(mocks.FakeClaimRefClient)
		transport.attach(client)
		a := newRedriveHarness(pkg.ClaimModeShadow, client)

		executionTask := redriveTask("tid-redrive-phase-execution")
		seedRedriveStreak(transport, "tid-redrive-phase-execution", "execution", 3)
		Expect(a.consume(executionTask)).To(Succeed())

		planningTask := redriveTask("tid-redrive-phase-planning")
		planningTask.Frontmatter["phase"] = "planning"
		seedRedriveStreak(transport, "tid-redrive-phase-planning", "planning", 3)
		Expect(a.consume(planningTask)).To(Succeed())

		executionBefore := testutil.ToFloat64(
			metrics.RedriveCappedTotal.WithLabelValues("", "execution"),
		)
		planningBefore := testutil.ToFloat64(
			metrics.RedriveCappedTotal.WithLabelValues("", "planning"),
		)

		Expect(a.consume(planningTask)).To(Succeed())

		Expect(testutil.ToFloat64(
			metrics.RedriveCappedTotal.WithLabelValues("", "planning"),
		)).To(Equal(planningBefore + 1))
		Expect(testutil.ToFloat64(
			metrics.RedriveCappedTotal.WithLabelValues("", "execution"),
		)).To(Equal(executionBefore))
	})

	It("uses an empty task_type value when the task declares none", func() {
		transport := newSharedClaimRefTransport()
		client := new(mocks.FakeClaimRefClient)
		transport.attach(client)
		a := newRedriveHarness(pkg.ClaimModeShadow, client)
		task := redriveTask("tid-redrive-no-type")
		seedRedriveStreak(transport, "tid-redrive-no-type", "execution", 3)

		before := testutil.ToFloat64(
			metrics.RedriveCappedTotal.WithLabelValues("", "execution"),
		)

		// A two-value call is the contract: a one-value call would panic here.
		Expect(a.consume(task)).To(Succeed())

		Expect(testutil.ToFloat64(
			metrics.RedriveCappedTotal.WithLabelValues("", "execution"),
		)).To(Equal(before + 1))
	})

	It("uses the declared task_type as the label value", func() {
		transport := newSharedClaimRefTransport()
		client := new(mocks.FakeClaimRefClient)
		transport.attach(client)
		a := newRedriveHarness(pkg.ClaimModeShadow, client)
		task := redriveTask("tid-redrive-typed")
		task.Frontmatter["task_type"] = "update-go"
		seedRedriveStreak(transport, "tid-redrive-typed", "execution", 3)

		emptyBefore := testutil.ToFloat64(
			metrics.RedriveCappedTotal.WithLabelValues("", "execution"),
		)
		typedBefore := testutil.ToFloat64(
			metrics.RedriveCappedTotal.WithLabelValues("update-go", "execution"),
		)

		Expect(a.consume(task)).To(Succeed())

		Expect(testutil.ToFloat64(
			metrics.RedriveCappedTotal.WithLabelValues("update-go", "execution"),
		)).To(Equal(typedBefore + 1))
		Expect(testutil.ToFloat64(
			metrics.RedriveCappedTotal.WithLabelValues("", "execution"),
		)).To(Equal(emptyBefore))
	})
})
