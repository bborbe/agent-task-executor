// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"flag"
	"io"
	"os"
	"strings"
	"testing"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus/testutil"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// refusalResolver returns one Config, or one error, for every lookup. Both
// methods are overridden because the task_type-fallback gate calls
// ResolveByTaskType, and the embedded nil interface would panic there.
type refusalResolver struct {
	pkg.ConfigResolver
	config *pkg.AgentConfiguration
	err    error
}

func (r refusalResolver) Resolve(context.Context, string) (pkg.AgentConfiguration, error) {
	if r.err != nil {
		return pkg.AgentConfiguration{}, r.err
	}
	if r.config == nil {
		return pkg.AgentConfiguration{}, nil
	}
	return *r.config, nil
}

func (r refusalResolver) ResolveByTaskType(
	context.Context, string, string,
) (pkg.AgentConfiguration, error) {
	return r.Resolve(context.Background(), "")
}

// reconcileRefusalGates is the closed gate vocabulary, in filter order.
func reconcileRefusalGates() []string {
	return []string{
		metrics.ReconcileRefusedGateForeignCluster,
		metrics.ReconcileRefusedGateUnresolvedConfig,
		metrics.ReconcileRefusedGateTerminalStatus,
		metrics.ReconcileRefusedGateStatusNotTriggered,
		metrics.ReconcileRefusedGatePhaseNotTriggered,
		metrics.ReconcileRefusedGateTerminalPhase,
		metrics.ReconcileRefusedGateStageMismatch,
		metrics.ReconcileRefusedGateNoRouting,
	}
}

// refusalGateCounts reads the child series rather than the bare collector: the
// vec is pre-initialised with eight series, which panics ToFloat64.
func refusalGateCounts() map[string]float64 {
	counts := make(map[string]float64, len(reconcileRefusalGates()))
	for _, gate := range reconcileRefusalGates() {
		counts[gate] = testutil.ToFloat64(
			metrics.ReconcileRefusedTotal.WithLabelValues(gate),
		)
	}
	return counts
}

// newRefusalTestHandler builds the real handler, so deferredEpochs, resolver and
// every other map a refusal touches are initialised. A bare struct literal
// leaves them nil and panics on the first gate.
func newRefusalTestHandler(t *testing.T, resolver pkg.ConfigResolver) *taskEventHandler {
	t.Helper()
	handler := NewTaskEventHandler(
		&stubJobSpawner{},
		"dev",
		"prod",
		"self",
		true,
		true,
		resolver,
		stubResultPublisher{},
		pkg.NewTaskStore(),
		libtime.NewCurrentDateTime(),
		notReadyView{},
		"v",
		DefaultReconcilePassTimeout,
		DefaultReconcileInterval,
		pkg.ClaimModeOff,
		nil,
	)
	h, ok := handler.(*taskEventHandler)
	if !ok {
		t.Fatalf("expected *taskEventHandler, got %T", handler)
	}
	return h
}

// refusalTask builds a task whose frontmatter reaches exactly one gate.
func refusalTask(id string, frontmatter lib.TaskFrontmatter) lib.Task {
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier(id),
		Frontmatter:    frontmatter,
	}
}

type reconcileRefusalCase struct {
	name       string
	task       lib.Task
	config     *pkg.AgentConfiguration
	resolveErr error
	wantGate   string
}

// reconcileRefusalCases is held outside the test function so its length stays
// inside the funlen budget.
var reconcileRefusalCases = []reconcileRefusalCase{
	{
		name: "foreign cluster",
		task: refusalTask("refuse-foreign", lib.TaskFrontmatter{
			"status": "in_progress", "phase": "execution", "stage": "prod", "cluster": "other",
		}),
		wantGate: metrics.ReconcileRefusedGateForeignCluster,
	},
	{
		name: "unresolved config",
		task: refusalTask("refuse-unresolved", lib.TaskFrontmatter{
			"status": "in_progress", "phase": "execution", "assignee": "nobody", "stage": "prod",
		}),
		resolveErr: pkg.ErrConfigNotFound,
		wantGate:   metrics.ReconcileRefusedGateUnresolvedConfig,
	},
	{
		name: "terminal status",
		task: refusalTask("refuse-terminal-status", lib.TaskFrontmatter{
			"status": "completed", "phase": "execution", "assignee": "claude", "stage": "prod",
		}),
		config:   &pkg.AgentConfiguration{Assignee: "claude"},
		wantGate: metrics.ReconcileRefusedGateTerminalStatus,
	},
	{
		name: "status not triggered",
		task: refusalTask("refuse-status", lib.TaskFrontmatter{
			"status": "hold", "phase": "execution", "assignee": "claude", "stage": "prod",
		}),
		config:   &pkg.AgentConfiguration{Assignee: "claude"},
		wantGate: metrics.ReconcileRefusedGateStatusNotTriggered,
	},
	{
		name: "phase not triggered",
		task: refusalTask("refuse-phase", lib.TaskFrontmatter{
			"status": "in_progress", "phase": "todo", "assignee": "claude", "stage": "prod",
		}),
		config:   &pkg.AgentConfiguration{Assignee: "claude"},
		wantGate: metrics.ReconcileRefusedGatePhaseNotTriggered,
	},
	{
		name: "terminal phase",
		task: refusalTask("refuse-terminal-phase", lib.TaskFrontmatter{
			"status": "in_progress", "phase": "human_review", "assignee": "claude", "stage": "prod",
		}),
		config: &pkg.AgentConfiguration{
			Assignee: "claude",
			Trigger: &agentv1.Trigger{
				Phases:   domain.TaskPhases{domain.TaskPhaseHumanReview},
				Statuses: domain.TaskStatuses{domain.TaskStatusInProgress},
			},
		},
		wantGate: metrics.ReconcileRefusedGateTerminalPhase,
	},
	{
		name: "stage mismatch",
		task: refusalTask("refuse-stage", lib.TaskFrontmatter{
			"status": "in_progress", "phase": "execution", "assignee": "claude", "stage": "dev",
		}),
		config:   &pkg.AgentConfiguration{Assignee: "claude"},
		wantGate: metrics.ReconcileRefusedGateStageMismatch,
	},
	{
		name: "no routing",
		task: refusalTask("refuse-no-routing", lib.TaskFrontmatter{
			"status": "in_progress", "phase": "execution", "stage": "prod",
		}),
		wantGate: metrics.ReconcileRefusedGateNoRouting,
	},
}

// TestReconcileTaskRefusalGates pins one case per gate: that gate's series moves
// by one and no other gate's series moves.
func TestReconcileTaskRefusalGates(t *testing.T) {
	ctx := context.Background()
	for _, tc := range reconcileRefusalCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newRefusalTestHandler(t, refusalResolver{
				config: tc.config,
				err:    tc.resolveErr,
			})
			before := refusalGateCounts()

			_, config, ok, err := h.reconcileTask(ctx, tc.task)
			if err != nil {
				t.Fatalf("reconcileTask returned %v; want nil", err)
			}
			if ok {
				t.Fatal("reconcileTask admitted the task; want a refusal")
			}
			if config != nil {
				t.Fatalf("refusal returned config %+v; want nil", config)
			}

			after := refusalGateCounts()
			for _, gate := range reconcileRefusalGates() {
				want := 0.0
				if gate == tc.wantGate {
					want = 1
				}
				assertDelta(t, gate, after[gate]-before[gate], want)
			}
		})
	}
}

// TestReconcileRefusalLogsOncePerGateChange pins the transition rule: the
// counter moves on every refusal, the log line only when the gate changes.
func TestReconcileRefusalLogsOncePerGateChange(t *testing.T) {
	ctx := context.Background()
	h := newRefusalTestHandler(t, refusalResolver{
		config: &pkg.AgentConfiguration{Assignee: "claude"},
	})
	terminal := refusalTask("refuse-transition", lib.TaskFrontmatter{
		"status": "completed", "phase": "execution", "assignee": "claude", "stage": "prod",
	})
	statusHold := refusalTask("refuse-transition", lib.TaskFrontmatter{
		"status": "hold", "phase": "execution", "assignee": "claude", "stage": "prod",
	})

	before := refusalGateCounts()
	out := captureRefusalLog(t, func() {
		h.reconcileTask(ctx, terminal)
		h.reconcileTask(ctx, terminal)
	})
	if got := strings.Count(out, "event=reconcile_refused"); got != 1 {
		t.Fatalf("two refusals at one gate logged %d lines; want 1\n%s", got, out)
	}
	after := refusalGateCounts()
	assertDelta(
		t,
		metrics.ReconcileRefusedGateTerminalStatus,
		after[metrics.ReconcileRefusedGateTerminalStatus]-
			before[metrics.ReconcileRefusedGateTerminalStatus],
		2,
	)

	out = captureRefusalLog(t, func() {
		h.reconcileTask(ctx, statusHold)
	})
	if got := strings.Count(out, "event=reconcile_refused"); got != 1 {
		t.Fatalf("a refusal at a different gate logged %d lines; want 1\n%s", got, out)
	}

	h.clearReconcileRefusal(terminal.TaskIdentifier)
	out = captureRefusalLog(t, func() {
		h.reconcileTask(ctx, terminal)
	})
	if got := strings.Count(out, "event=reconcile_refused"); got != 1 {
		t.Fatalf("a refusal after a clear logged %d lines; want 1\n%s", got, out)
	}
}

// captureRefusalLog mirrors capturePinReportLog: glog writes to os.Stderr at
// V(2), so the pipe must replace it for the duration of fn.
func captureRefusalLog(t *testing.T, fn func()) string {
	t.Helper()
	if err := flag.Set("logtostderr", "true"); err != nil {
		t.Fatalf("set logtostderr: %v", err)
	}
	if err := flag.Set("v", "2"); err != nil {
		t.Fatalf("set v: %v", err)
	}
	defer func() {
		_ = flag.Set("v", "0")
		_ = flag.Set("logtostderr", "false")
	}()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create pipe: %v", err)
	}
	os.Stderr = writer
	defer func() { os.Stderr = original }()

	fn()
	glog.Flush()

	os.Stderr = original
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	return string(out)
}
