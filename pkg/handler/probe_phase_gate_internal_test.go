// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"testing"

	lib "github.com/bborbe/agent"
	taskcmd "github.com/bborbe/agent/command/task"
	"github.com/bborbe/vault-cli/pkg/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/probe"
	"github.com/bborbe/agent-task-executor/pkg/probe/mocks"
)

// TestProbePhasePassesExecutorGate is the boundary test between the probe's
// write side and the executor's read side. The original defect was the string
// "in_progress" passing the write side and being normalised to "execution" by
// Phase() on the read side, so a membership check against the list the probe
// picked from would not catch it: the phase must be re-parsed exactly as the
// executor does and then checked against the effective trigger set.
func TestProbePhasePassesExecutorGate(t *testing.T) {
	tests := []struct {
		name    string
		trigger *agentv1.Trigger
	}{
		{
			name:    "[planning]",
			trigger: &agentv1.Trigger{Phases: domain.TaskPhases{domain.TaskPhasePlanning}},
		},
		{
			name:    "[execution]",
			trigger: &agentv1.Trigger{Phases: domain.TaskPhases{domain.TaskPhaseExecution}},
		},
		{
			name: "[execution, planning] prefers planning",
			trigger: &agentv1.Trigger{
				Phases: domain.TaskPhases{domain.TaskPhaseExecution, domain.TaskPhasePlanning},
			},
		},
		{
			name: "[todo, planning, execution, ai_review] prefers planning",
			trigger: &agentv1.Trigger{
				Phases: domain.TaskPhases{
					domain.TaskPhaseTodo,
					domain.TaskPhasePlanning,
					domain.TaskPhaseExecution,
					domain.TaskPhaseAIReview,
				},
			},
		},
		{
			name: "[todo, execution] skips todo",
			trigger: &agentv1.Trigger{
				Phases: domain.TaskPhases{domain.TaskPhaseTodo, domain.TaskPhaseExecution},
			},
		},
		{
			name:    "[ai_review]",
			trigger: &agentv1.Trigger{Phases: domain.TaskPhases{domain.TaskPhaseAIReview}},
		},
		{
			name: "[human_review, execution] skips the terminal phase",
			trigger: &agentv1.Trigger{
				Phases: domain.TaskPhases{domain.TaskPhaseHumanReview, domain.TaskPhaseExecution},
			},
		},
		{
			name:    "nil trigger falls back to planning",
			trigger: nil,
		},
		{
			name:    "empty phases fall back to planning",
			trigger: &agentv1.Trigger{Phases: domain.TaskPhases{}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			createFrontmatter, updateFrontmatter := runProbe(t, test.trigger)
			assertPhasePassesGate(t, "create", createFrontmatter, test.trigger)
			assertPhasePassesGate(t, "update", updateFrontmatter, test.trigger)
		})
	}
}

// runProbe drives the probe once for a Config carrying trigger and returns the
// frontmatter of both commands it published.
func runProbe(t *testing.T, trigger *agentv1.Trigger) (lib.TaskFrontmatter, lib.TaskFrontmatter) {
	t.Helper()
	cfg := agentv1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-a"},
		Spec:       agentv1.ConfigSpec{Assignee: "agent-a", Trigger: trigger},
	}
	configProvider := new(mocks.FakeConfigProvider)
	configProvider.GetReturns([]agentv1.Config{cfg}, nil)
	publisher := new(mocks.FakeCommandPublisher)
	runner := probe.NewHealthcheckRunner(configProvider, publisher, "dev")

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if publisher.PublishCallCount() != 2 {
		t.Fatalf("PublishCallCount() = %d, want 2", publisher.PublishCallCount())
	}

	_, _, createPayload := publisher.PublishArgsForCall(0)
	createCmd, ok := createPayload.(taskcmd.CreateCommand)
	if !ok {
		t.Fatalf("payload at call 0 is %T, want taskcmd.CreateCommand", createPayload)
	}
	_, _, updatePayload := publisher.PublishArgsForCall(1)
	updateCmd, ok := updatePayload.(taskcmd.UpdateFrontmatterCommand)
	if !ok {
		t.Fatalf("payload at call 1 is %T, want taskcmd.UpdateFrontmatterCommand", updatePayload)
	}
	return createCmd.Frontmatter, updateCmd.Updates
}

// assertPhasePassesGate re-parses the written phase exactly as the executor does
// and checks the effective trigger accepts it.
func assertPhasePassesGate(
	t *testing.T,
	name string,
	fm lib.TaskFrontmatter,
	trigger *agentv1.Trigger,
) {
	t.Helper()
	config := &pkg.AgentConfiguration{Trigger: trigger}
	phase := fm.Phase()
	if phase == nil {
		t.Fatalf("%s frontmatter phase is nil, want a spawnable phase", name)
	}
	if !effectiveTriggerPhases(config).Contains(*phase) {
		t.Fatalf(
			"%s frontmatter phase %q is not accepted by the effective trigger %v",
			name,
			*phase,
			effectiveTriggerPhases(config),
		)
	}
	if IsTerminal(*phase) {
		t.Fatalf("%s frontmatter phase %q is terminal", name, *phase)
	}
}
