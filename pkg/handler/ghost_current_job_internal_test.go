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

// ghostRecordingPublisher records every PublishClearCurrentJob call.
type ghostRecordingPublisher struct {
	stubResultPublisher
	mu     sync.Mutex
	clears []string
}

func (p *ghostRecordingPublisher) PublishClearCurrentJob(
	_ context.Context,
	_ lib.Task,
	jobName string,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clears = append(p.clears, jobName)
	return nil
}

// clearCalls returns a copy of the recorded job names. Row q reads it while the
// pass runs in another goroutine, so the slice must be read under the lock.
func (p *ghostRecordingPublisher) clearCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.clears...)
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
		h = newGhostInternalHandler(
			ghostView{tasks: []lib.Task{ghostInternalTask()}},
			publisher,
			func(context.Context, claim.ClaimTarget) (*claim.Claim, error) {
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
