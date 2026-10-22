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
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

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
	eventHandler handler.TaskEventHandler
	spawner      *mocks.FakeJobSpawner
	client       *mocks.FakeClaimRefClient
	factoryCalls int
}

// newClaimHarness builds a handler in the given claim mode with defaults that
// let a qualifying task spawn exactly once.
func newClaimHarness(mode pkg.ClaimMode) *claimHarness {
	spawner := new(mocks.FakeJobSpawner)
	spawner.SpawnJobReturns("job-1", nil)
	spawner.IsJobActiveReturns(false, nil)
	spawner.ListActiveJobsReturns(map[lib.TaskIdentifier]bool{}, nil)
	spawner.CountActiveJobsReturns(0, nil)

	resolver := new(mocks.FakeConfigResolver)
	resolver.ResolveReturns(pkg.AgentConfiguration{Assignee: "claude", Image: "img"}, nil)

	client := new(mocks.FakeClaimRefClient)
	h := &claimHarness{spawner: spawner, client: client}
	h.eventHandler = handler.NewTaskEventHandler(
		spawner,
		base.Branch("prod"),
		"self",
		true,
		true,
		resolver,
		new(mocks.FakeResultPublisher),
		pkg.NewTaskStore(),
		libtime.NewCurrentDateTime(),
		new(mocks.FakeGitRestClient),
		"24 Tasks/*.md",
		handler.DefaultReconcilePassTimeout,
		handler.DefaultReconcileInterval,
		mode,
		func() claim.ClaimRefClient {
			h.factoryCalls++
			return client
		},
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
		Frontmatter: lib.TaskFrontmatter{
			"status":   "in_progress",
			"phase":    string(domain.TaskPhaseExecution),
			"assignee": "claude",
			"stage":    "prod",
		},
	}
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
