// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite. Every test
// function is prefixed TestCheckTriggerBudget so the verification's
// -run 'TestCheckTriggerBudget' selects exactly this file.

import (
	"context"
	stderrors "errors"
	"testing"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
)

// refusalCall records one PublishTriggerBudgetRefusal invocation.
type refusalCall struct {
	taskID      lib.TaskIdentifier
	scope       string
	count       int
	maxTriggers int
}

// recordingRefusalPublisher embeds the package's stub and records the refusal
// publishes, returning a configured error.
type recordingRefusalPublisher struct {
	stubResultPublisher
	calls []refusalCall
	err   error
}

func (p *recordingRefusalPublisher) PublishTriggerBudgetRefusal(
	_ context.Context,
	task lib.Task,
	scope string,
	count int,
	maxTriggers int,
) error {
	p.calls = append(p.calls, refusalCall{
		taskID:      task.TaskIdentifier,
		scope:       scope,
		count:       count,
		maxTriggers: maxTriggers,
	})
	return p.err
}

// refusalFixture is the reproduction record from the spec: at cap in the
// planning scope. phase is load-bearing — triggerScope composes
// phase + ":" + ref[:8], so a fixture without it would not match the stored
// trigger_scope and the record would not be refused.
func refusalFixture() lib.TaskFrontmatter {
	return lib.TaskFrontmatter{
		"phase":         "planning",
		"trigger_count": 4,
		"max_triggers":  3,
		"trigger_scope": "planning:4963ffa6",
		"ref":           "4963ffa6aaaaaaaaaaaaaaaa",
	}
}

func triggerRefusalTestTask(fm lib.TaskFrontmatter) lib.Task {
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier("test-task-refusal"),
		Frontmatter:    fm,
	}
}

func TestCheckTriggerBudgetPublishesRefusal(t *testing.T) {
	pub := &recordingRefusalPublisher{}
	h := &taskEventHandler{resultPublisher: pub}

	refused, err := h.checkTriggerBudget(
		context.Background(),
		triggerRefusalTestTask(refusalFixture()),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !refused {
		t.Fatal("expected the spawn to be refused")
	}
	if len(pub.calls) != 1 {
		t.Fatalf("expected exactly one publish, got %d", len(pub.calls))
	}
	got := pub.calls[0]
	if got.scope != "planning:4963ffa6" || got.count != 4 || got.maxTriggers != 3 {
		t.Fatalf("unexpected refusal triple: %+v", got)
	}
}

func TestCheckTriggerBudgetSkipsRecordedTriple(t *testing.T) {
	fm := refusalFixture()
	fm["trigger_refusal_count"] = 4
	fm["trigger_refusal_max"] = 3
	fm["trigger_refusal_scope"] = "planning:4963ffa6"
	pub := &recordingRefusalPublisher{}
	h := &taskEventHandler{resultPublisher: pub}

	refused, err := h.checkTriggerBudget(context.Background(), triggerRefusalTestTask(fm))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !refused {
		t.Fatal("expected the spawn to be refused")
	}
	if len(pub.calls) != 0 {
		t.Fatalf("expected no publish, got %d", len(pub.calls))
	}
}

func TestCheckTriggerBudgetSkipsRecordedTripleAsFloat64(t *testing.T) {
	fm := refusalFixture()
	fm["trigger_refusal_count"] = float64(4)
	fm["trigger_refusal_max"] = float64(3)
	fm["trigger_refusal_scope"] = "planning:4963ffa6"
	pub := &recordingRefusalPublisher{}
	h := &taskEventHandler{resultPublisher: pub}

	refused, err := h.checkTriggerBudget(context.Background(), triggerRefusalTestTask(fm))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !refused {
		t.Fatal("expected the spawn to be refused")
	}
	if len(pub.calls) != 0 {
		t.Fatalf("expected no publish, got %d", len(pub.calls))
	}
}

func TestCheckTriggerBudgetReplacesOlderTriple(t *testing.T) {
	fm := refusalFixture()
	fm["trigger_refusal_count"] = 4
	fm["trigger_refusal_max"] = 3
	fm["trigger_refusal_scope"] = "planning:deadbeef"
	pub := &recordingRefusalPublisher{}
	h := &taskEventHandler{resultPublisher: pub}

	refused, err := h.checkTriggerBudget(context.Background(), triggerRefusalTestTask(fm))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !refused {
		t.Fatal("expected the spawn to be refused")
	}
	if len(pub.calls) != 1 {
		t.Fatalf("expected exactly one publish, got %d", len(pub.calls))
	}
	if pub.calls[0].scope != "planning:4963ffa6" {
		t.Fatalf("expected the current scope, got %q", pub.calls[0].scope)
	}
}

func TestCheckTriggerBudgetBelowCapDoesNotPublish(t *testing.T) {
	fm := refusalFixture()
	fm["trigger_count"] = 2
	pub := &recordingRefusalPublisher{}
	h := &taskEventHandler{resultPublisher: pub}

	refused, err := h.checkTriggerBudget(context.Background(), triggerRefusalTestTask(fm))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if refused {
		t.Fatal("expected the spawn to proceed")
	}
	if len(pub.calls) != 0 {
		t.Fatalf("expected no publish, got %d", len(pub.calls))
	}
}

func TestCheckTriggerBudgetKeepsStaleTripleWhenCounterReset(t *testing.T) {
	fm := refusalFixture()
	fm["trigger_count"] = 2
	fm["trigger_refusal_count"] = 4
	fm["trigger_refusal_max"] = 3
	fm["trigger_refusal_scope"] = "planning:4963ffa6"
	pub := &recordingRefusalPublisher{}
	h := &taskEventHandler{resultPublisher: pub}

	refused, err := h.checkTriggerBudget(context.Background(), triggerRefusalTestTask(fm))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if refused {
		t.Fatal("expected the spawn to proceed")
	}
	if len(pub.calls) != 0 {
		t.Fatalf("expected no publish, got %d", len(pub.calls))
	}

	fm["trigger_count"] = 3
	refused, err = h.checkTriggerBudget(context.Background(), triggerRefusalTestTask(fm))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !refused {
		t.Fatal("expected the spawn to be refused")
	}
	if len(pub.calls) != 1 {
		t.Fatalf("expected exactly one publish, got %d", len(pub.calls))
	}
	got := pub.calls[0]
	if got.scope != "planning:4963ffa6" || got.count != 3 || got.maxTriggers != 3 {
		t.Fatalf("unexpected refusal triple: %+v", got)
	}
}

func TestCheckTriggerBudgetPublishFailureStillRefuses(t *testing.T) {
	pub := &recordingRefusalPublisher{err: errors.New(context.Background(), "kafka unavailable")}
	h := &taskEventHandler{resultPublisher: pub}

	refused, err := h.checkTriggerBudget(
		context.Background(),
		triggerRefusalTestTask(refusalFixture()),
	)
	if err == nil {
		t.Fatal("expected a publish error")
	}
	if !refused {
		t.Fatal("expected the spawn to stay refused despite the failed write")
	}
	if len(pub.calls) != 1 {
		t.Fatalf("expected exactly one attempted publish, got %d", len(pub.calls))
	}
}

// TestTriggerBudgetRefusesSpawnWrapsWriteFailure pins the error type the
// deferred-respawn loop keys on. That loop treats every error it cannot
// classify as fatal, so a refusal-write failure that lost this type would exit
// the process on a transient publish error instead of waiting for the next
// reconcile pass.
func TestCheckTriggerBudgetRefusesSpawnWrapsWriteFailure(t *testing.T) {
	pub := &recordingRefusalPublisher{err: errors.New(context.Background(), "kafka unavailable")}
	h := &taskEventHandler{resultPublisher: pub}

	refused, err := h.triggerBudgetRefusesSpawn(
		context.Background(),
		triggerRefusalTestTask(refusalFixture()),
	)
	if !refused {
		t.Fatal("expected the spawn to stay refused despite the failed write")
	}
	var writeErr *triggerRefusalWriteError
	if !stderrors.As(err, &writeErr) {
		t.Fatalf("expected *triggerRefusalWriteError, got %T: %v", err, err)
	}
}

// TestTriggerBudgetRefusesSpawnNoErrorBelowCap proves the wrapper adds no error
// on the pass-through path, so the classification above cannot mask a genuine
// non-refusal.
func TestCheckTriggerBudgetRefusesSpawnNoErrorBelowCap(t *testing.T) {
	pub := &recordingRefusalPublisher{}
	h := &taskEventHandler{resultPublisher: pub}

	fm := refusalFixture()
	fm["trigger_count"] = 2
	refused, err := h.triggerBudgetRefusesSpawn(
		context.Background(),
		triggerRefusalTestTask(fm),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if refused {
		t.Fatal("expected the spawn not to be refused below the cap")
	}
}
