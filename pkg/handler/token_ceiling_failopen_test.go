// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"
	stderrors "errors"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// failingTokenSpendReader is a hand-written claim.TokenSpendReader whose reads
// always fail. There is no counterfeiter mock for the interface on purpose (its
// doc comment explains why), so the fail-open regression spec doubles it here.
type failingTokenSpendReader struct{ err error }

func (r failingTokenSpendReader) ReadForAssignee(
	context.Context,
	string,
) ([]claim.TaskIndexItem, error) {
	return nil, r.err
}

func (r failingTokenSpendReader) ReadDocument(
	context.Context,
	string,
) (*claim.TaskDocumentItem, error) {
	return nil, r.err
}

var _ = Describe("ConsumeMessage token ceiling read failure", func() {
	It("admits the spawn and returns nil when ReadForAssignee fails", func() {
		ctx := context.Background()

		fakeSpawner := new(mocks.FakeJobSpawner)
		fakeResolver := &mocks.FakeConfigResolver{}
		fakeResultPublisher := &mocks.FakeResultPublisher{}
		fakeView := &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)

		clock := libtime.NewCurrentDateTime()
		clock.SetNow(libtimetest.ParseDateTime("2026-11-18T12:00:00Z"))

		ceiling := int64(1)
		fakeResolver.ResolveReturns(pkg.AgentConfiguration{
			Assignee:     "claude",
			Image:        "my-image:latest",
			TokenCeiling: &ceiling,
			RunWindow:    &agentv1.RunWindow{WeekdayStart: "00:00", WeekdayEnd: "23:59"},
		}, nil)
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeSpawner.SpawnJobReturns("job-1", nil)

		h := handler.NewTaskEventHandler(
			fakeSpawner,
			"dev",
			base.Branch("prod"),
			"",
			true,
			true,
			fakeResolver,
			fakeResultPublisher,
			pkg.NewTaskStore(),
			clock,
			fakeView,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeOff,
			nil,
			handler.WithTokenSpendReader(
				failingTokenSpendReader{err: stderrors.New("dial tcp: connection refused")},
			),
		)

		task := lib.Task{
			TaskIdentifier: lib.TaskIdentifier("tid-token-ceiling-read-fail"),
			Vault:          testTaskVault,
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    string(domain.TaskPhaseExecution),
				"assignee": "claude",
				"stage":    "prod",
			},
		}
		value, err := json.Marshal(task)
		Expect(err).To(BeNil())

		before := testutil.ToFloat64(metrics.TokenCeilingReadFailedTotal)
		err = h.ConsumeMessage(ctx, &sarama.ConsumerMessage{Value: value})
		Expect(err).To(BeNil())
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(
			testutil.ToFloat64(metrics.TokenCeilingReadFailedTotal) - before,
		).To(Equal(float64(1)))
	})
})
