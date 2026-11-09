// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"flag"
	"io"
	"os"
	"sync"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// ghostInternalVault is the vault the internal fixture tasks belong to. The
// external suite's testTaskVault lives in package handler_test.
const ghostInternalVault = "openclaw"

// ghostView is a ready TaskView holding the fixture snapshot.
type ghostView struct {
	TaskView
	tasks []lib.Task
}

func (v ghostView) Ready() bool { return true }

func (v ghostView) Snapshot() []lib.Task { return v.tasks }

func (v ghostView) Get(lib.TaskIdentifier) (lib.Task, bool) { return lib.Task{}, false }

// ghostRecordingPublisher records every PublishClearCurrentJob call, including
// whether the per-task claim lock was held at publish time.
type ghostRecordingPublisher struct {
	stubResultPublisher
	h      *taskEventHandler
	mu     sync.Mutex
	clears []string
	held   []bool
}

func (p *ghostRecordingPublisher) PublishClearCurrentJob(
	_ context.Context,
	task lib.Task,
	jobName string,
) error {
	held := p.lockHeld(task.TaskIdentifier)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clears = append(p.clears, jobName)
	p.held = append(p.held, held)
	return nil
}

// lockHeld reports whether the per-task claim lock is held right now, without
// blocking the publisher. TryLock returning false means the lock is held by
// some goroutine, not necessarily this publisher. The flag is only meaningful in
// an uncontended spec (row q2); never assert it in row q, where the test itself
// holds the lock.
func (p *ghostRecordingPublisher) lockHeld(taskID lib.TaskIdentifier) bool {
	p.h.claimLocksMu.Lock()
	lock, ok := p.h.claimLocks[taskID]
	p.h.claimLocksMu.Unlock()
	if !ok {
		return false
	}
	if lock.TryLock() {
		lock.Unlock()
		return false
	}
	return true
}

// clearCalls returns a copy of the recorded job names. Row q reads it while the
// pass runs in another goroutine, so the slice must be read under the lock.
func (p *ghostRecordingPublisher) clearCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.clears...)
}

// lockStates returns a copy of the recorded lock states, one per publish.
func (p *ghostRecordingPublisher) lockStates() []bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]bool(nil), p.held...)
}

// ghostCaptureInternalLog runs fn with glog writing to a pipe at V(2) and
// returns everything written. It mirrors deferredGateCaptureStderr but uses
// Gomega, because the internal suite's captureRefusalLog takes a *testing.T.
func ghostCaptureInternalLog(fn func()) string {
	Expect(flag.Set("logtostderr", "true")).To(Succeed())
	Expect(flag.Set("v", "2")).To(Succeed())
	defer func() {
		Expect(flag.Set("v", "0")).To(Succeed())
		Expect(flag.Set("logtostderr", "false")).To(Succeed())
	}()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	Expect(err).ToNot(HaveOccurred())
	os.Stderr = writer

	fn()
	glog.Flush()

	os.Stderr = original
	Expect(writer.Close()).To(Succeed())
	out, err := io.ReadAll(reader)
	Expect(err).ToNot(HaveOccurred())
	Expect(reader.Close()).To(Succeed())
	return string(out)
}

// ghostInternalTask is the base record every internal spec starts from.
func ghostInternalTask() lib.Task {
	return lib.Task{
		TaskIdentifier: "ghost-task-1",
		Vault:          ghostInternalVault,
		Frontmatter: lib.TaskFrontmatter{
			"status":      "in_progress",
			"phase":       string(domain.TaskPhaseHumanReview),
			"assignee":    "claude",
			"stage":       "prod",
			"current_job": "ghost-job-1",
		},
	}
}

// newGhostInternalHandler builds the real handler with internal fakes.
func newGhostInternalHandler(
	view TaskView,
	publisher *ghostRecordingPublisher,
	read func(context.Context, claim.ClaimTarget) (*claim.Claim, error),
) *taskEventHandler {
	raw := NewTaskEventHandler(
		goneSpawner{},
		"dev",
		base.Branch("prod"),
		"",
		true,
		true,
		staticResolver{config: pkg.AgentConfiguration{Assignee: "claude", Image: "img"}},
		publisher,
		pkg.NewTaskStore(),
		libtime.NewCurrentDateTime(),
		view,
		ghostInternalVault,
		DefaultReconcilePassTimeout,
		DefaultReconcileInterval,
		pkg.ClaimModeOn,
		func() claim.ClaimRefClient { return noopClaimRefClient{} },
		WithClaimJobControl(noopJobControl{}),
		WithClaimClientFactory(func() claim.ClaimClient {
			return readOnlyClaimClient{read: read}
		}),
	)
	h, ok := raw.(*taskEventHandler)
	Expect(ok).To(BeTrue())
	publisher.h = h
	return h
}

var _ = Describe("ghost current_job locked re-check", func() {
	var (
		ctx       context.Context
		publisher *ghostRecordingPublisher
		h         *taskEventHandler
	)

	BeforeEach(func() {
		ctx = context.Background()
		publisher = &ghostRecordingPublisher{}
		h = nil
	})

	It("keeps the field when a spawn is marked pending during the claim read", func() {
		h = newGhostInternalHandler(
			ghostView{tasks: []lib.Task{ghostInternalTask()}},
			publisher,
			func(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
				h.markSpawnPending("ghost-task-1")
				return nil, claim.ErrClaimFree
			},
		)

		out := ghostCaptureInternalLog(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(publisher.clears).To(BeEmpty())
		Expect(out).To(ContainSubstring("guard=store_entry"))
	})

	It("keeps the field without remote reads when a spawn is already pending", func() {
		probed := false
		h = newGhostInternalHandler(
			ghostView{tasks: []lib.Task{ghostInternalTask()}},
			publisher,
			func(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
				probed = true
				return nil, claim.ErrClaimFree
			},
		)
		h.markSpawnPending("ghost-task-1")

		out := ghostCaptureInternalLog(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(publisher.clears).To(BeEmpty())
		Expect(out).To(ContainSubstring("guard=store_entry"))
		Expect(probed).To(BeFalse())
	})

	It("publishes only under the per-task claim lock (row q)", func() {
		reachedProbe := make(chan struct{})
		var probeOnce sync.Once
		h = newGhostInternalHandler(
			ghostView{tasks: []lib.Task{ghostInternalTask()}},
			publisher,
			func(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
				probeOnce.Do(func() { close(reachedProbe) })
				return nil, claim.ErrClaimFree
			},
		)

		unlock := h.lockClaimRelease("ghost-task-1")
		released := false
		defer func() {
			if !released {
				unlock()
			}
		}()

		done := make(chan error, 1)
		out := ghostCaptureInternalLog(func() {
			go func() {
				done <- h.ReconcileOnce(ctx)
			}()
			// Wait until the pass has passed the claim probe and is about to
			// take the lock, so the Consistently window proves the lock blocks
			// the publish rather than the pass not having reached it yet.
			Eventually(reachedProbe, 5*time.Second).Should(BeClosed())
			// While the pass waits for the lock, nothing may be published.
			Consistently(func() []string {
				return publisher.clearCalls()
			}, 200*time.Millisecond).Should(BeEmpty())

			h.markSpawnPending("ghost-task-1")
			unlock()
			released = true

			var passErr error
			Eventually(done).Should(Receive(&passErr))
			Expect(passErr).ToNot(HaveOccurred())
		})

		Expect(publisher.clearCalls()).To(BeEmpty())
		Expect(out).To(ContainSubstring("guard=store_entry"))
	})

	It("publishes under the per-task claim lock (row q2)", func() {
		h = newGhostInternalHandler(
			ghostView{tasks: []lib.Task{ghostInternalTask()}},
			publisher,
			func(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
				return nil, claim.ErrClaimFree
			},
		)

		ghostCaptureInternalLog(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(publisher.clearCalls()).To(Equal([]string{"ghost-job-1"}))
		Expect(publisher.lockStates()).To(Equal([]bool{true}))
	})

	It("clears when nothing is pending", func() {
		h = newGhostInternalHandler(
			ghostView{tasks: []lib.Task{ghostInternalTask()}},
			publisher,
			func(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
				return nil, claim.ErrClaimFree
			},
		)

		ghostCaptureInternalLog(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(publisher.clears).To(Equal([]string{"ghost-job-1"}))
	})
})
