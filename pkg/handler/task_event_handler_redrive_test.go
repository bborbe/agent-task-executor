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

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/handler"
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
	)
	return h
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

		Expect(a.consume(task)).To(Succeed())
		Expect(b.consume(task)).To(Succeed())
		Expect(a.consume(task)).To(Succeed())
		Expect(a.spawner.SpawnJobCallCount() + b.spawner.SpawnJobCallCount()).To(Equal(3))

		Expect(flag.Set("v", "1")).To(Succeed())
		out := captureStderr(func() {
			Expect(b.consume(task)).To(Succeed())
		})
		Expect(flag.Set("v", "0")).To(Succeed())
		Expect(out).To(ContainSubstring(fmt.Sprintf(
			"event=redrive_capped task=%s streak=3 cap=3 phase=%q",
			task.TaskIdentifier, "execution",
		)))

		Expect(a.consume(task)).To(Succeed())
		Expect(a.spawner.SpawnJobCallCount() + b.spawner.SpawnJobCallCount()).To(Equal(3))
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

	It("AC8(a): a failed store read falls back to memory and is not re-read once capped", func() {
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

		Expect(a.spawner.SpawnJobCallCount()).To(Equal(3))
		Expect(client.ListCallCount()).To(Equal(listCalls))
	})

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

	It("AC8(c): a failed store write still counts in memory", func() {
		transport := newSharedClaimRefTransport()
		transport.putErr = stderrors.New("git-rest down")
		client := new(mocks.FakeClaimRefClient)
		transport.attach(client)
		a := newRedriveHarness(pkg.ClaimModeShadow, client)
		task := redriveTask("tid-redrive-write-fail")

		out := captureStderr(func() {
			Expect(a.consume(task)).To(Succeed())
		})
		Expect(a.spawner.SpawnJobCallCount()).To(Equal(1))
		Expect(out).To(ContainSubstring(
			"event=redrive_streak_write_failed task=" + string(task.TaskIdentifier),
		))

		for i := 0; i < 3; i++ {
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

		out := captureStderr(func() {
			Expect(a.consume(task)).To(Succeed())
		})
		Expect(out).To(ContainSubstring("event=redrive_streak_write_failed task=bad/id"))

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
})
