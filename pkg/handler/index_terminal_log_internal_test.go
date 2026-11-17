// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/prometheus/client_golang/prometheus/testutil"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// indexTerminalLogVault is the vault the fixture tasks belong to. testTaskVault
// lives in package handler_test and is not visible here.
const indexTerminalLogVault = "openclaw"

// indexTerminalLogView is a ready TaskView holding the fixture snapshot.
// Embedding the interface satisfies the rest of it without implementing them.
type indexTerminalLogView struct {
	TaskView
	tasks []lib.Task
}

func (v indexTerminalLogView) Ready() bool { return true }

func (v indexTerminalLogView) Snapshot() []lib.Task { return v.tasks }

// indexTerminalLogIndexClient reports every fixture id as terminal, which makes
// the gather suppress the whole snapshot.
type indexTerminalLogIndexClient struct {
	index *claim.TaskIndex
}

func (c indexTerminalLogIndexClient) Read(context.Context) (*claim.TaskIndex, error) {
	return c.index, nil
}

// indexTerminalLogTask builds one in-progress execution task that passes the
// eligibility filter and is then suppressed by the index gate.
func indexTerminalLogTask(id string) lib.Task {
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier(id),
		Vault:          indexTerminalLogVault,
		Frontmatter: lib.TaskFrontmatter{
			"status":   "in_progress",
			"phase":    "execution",
			"assignee": "claude",
		},
	}
}

// indexTerminalLogFixture builds 25 tasks whose lexicographic order equals their
// numeric order, and an index reporting every one of them terminal.
func indexTerminalLogFixture() ([]lib.Task, *claim.TaskIndex) {
	tasks := make([]lib.Task, 0, 25)
	items := make([]claim.TaskIndexItem, 0, 25)
	for i := 0; i < 25; i++ {
		id := fmt.Sprintf("gate-id-%02d", i)
		tasks = append(tasks, indexTerminalLogTask(id))
		items = append(items, claim.TaskIndexItem{
			ID:     id,
			Status: "completed",
			Phase:  "human_review",
		})
	}
	return tasks, &claim.TaskIndex{Items: items, Total: len(items), IndexAgeSeconds: 1}
}

// TestIndexTerminalSuppressionLogAndCounter pins the two halves of the
// observability contract: the per-pass log names at most maxDivergenceLogLines
// ids (the lexicographically lowest), while the counter counts every suppressed
// id on every pass.
func TestIndexTerminalSuppressionLogAndCounter(t *testing.T) {
	tasks, index := indexTerminalLogFixture()
	handler := NewTaskEventHandler(
		&stubJobSpawner{},
		"dev",
		"prod",
		"self",
		false,
		true,
		stubResolver{},
		stubResultPublisher{},
		pkg.NewTaskStore(),
		libtime.NewCurrentDateTime(),
		indexTerminalLogView{tasks: tasks},
		indexTerminalLogVault,
		DefaultReconcilePassTimeout,
		DefaultReconcileInterval,
		pkg.ClaimModeOff,
		nil,
		WithVaultAPIIndexClient(indexTerminalLogIndexClient{index: index}),
	)
	h, ok := handler.(*taskEventHandler)
	if !ok {
		t.Fatalf("expected *taskEventHandler, got %T", handler)
	}

	before := testutil.ToFloat64(
		metrics.ReconcileRefusedTotal.WithLabelValues(
			metrics.ReconcileRefusedGateIndexTerminal,
		),
	)

	out := captureRefusalLog(t, func() {
		if err := h.ReconcileOnce(context.Background()); err != nil {
			t.Fatalf("ReconcileOnce returned %v; want nil", err)
		}
	})
	if got := strings.Count(out, "event=index_terminal_suppressed task="); got != 20 {
		t.Fatalf("first pass logged %d suppression lines; want 20\n%s", got, out)
	}
	for i := 20; i < 25; i++ {
		id := fmt.Sprintf("gate-id-%02d", i)
		if strings.Contains(out, id) {
			t.Fatalf("first pass named %s, outside the 20-id cap\n%s", id, out)
		}
	}

	after := testutil.ToFloat64(
		metrics.ReconcileRefusedTotal.WithLabelValues(
			metrics.ReconcileRefusedGateIndexTerminal,
		),
	)
	if delta := after - before; delta != 25 {
		t.Fatalf("counter delta = %v; want 25 (uncapped)", delta)
	}

	out = captureRefusalLog(t, func() {
		if err := h.ReconcileOnce(context.Background()); err != nil {
			t.Fatalf("second ReconcileOnce returned %v; want nil", err)
		}
	})
	if got := strings.Count(out, "event=index_terminal_suppressed task="); got != 20 {
		t.Fatalf("second pass logged %d suppression lines; want 20\n%s", got, out)
	}
}
