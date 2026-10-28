// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// checkTriggerBudget evaluates the scoped trigger cap for one spawn decision and
// returns (refused, error): refused reports whether the spawn must be suppressed,
// and a non-nil error reports that the refusal could not be written onto the
// record. It publishes no counter write — that is recordTriggerSpawn's job,
// published only after a Job was actually created — and a refusal now writes its
// own reason onto the record, beside and never over any older failure the record
// already carries.
//
// The cap default-engages for repo-backed tasks: a task whose frontmatter
// carries a ref (written by the agents that clone a repo, alongside clone_url
// and base_ref) scopes its budget to phase+ref via triggerScope, and that scope
// moves with the repo, so accrual against a constant scope cannot occur — the
// safe default-on form. An explicit max_triggers engages the cap for any task,
// with or without a ref. A task with neither ref nor max_triggers is a
// recurring task: it is not repo-backed, sits at a stable phase, and its scope
// is a constant, so a default cap would accrue across re-dispatches and fire at
// 3, re-creating the 2026-08-27 Daily Sentry Triage incident (the v0.7.1
// regression). That task stays uncapped by default.
//
// The cap value is always MaxTriggers(): the explicit max_triggers field, or
// the lib default of 3 when it is absent — a repo-backed task with no
// max_triggers is therefore capped at 3.
//
// Scoping also fixes the opt-in cap's correctness: a task that opts in no
// longer burns its budget across unrelated attempts, so an operator can set a
// tight max_triggers without it leaking into the next phase or the next commit.
//
// The counter write is deliberately split out into recordTriggerSpawn and
// published only AFTER a Job was actually created. Until 2026-10-19 the
// increment was published here, before SpawnJob, so every infrastructure
// failure — the `spawn_error ... context deadline exceeded` this change also
// fixes — consumed one of the task's attempts without the agent ever running.
// That is how github-update-go tasks reached trigger_count 5-7 while producing
// no work, and why a trigger_count reset alone could not hold.
func (h *taskEventHandler) checkTriggerBudget(
	ctx context.Context,
	task lib.Task,
) (bool, error) {
	currentScope := triggerScope(task.Frontmatter)
	storedScope, hasScope := task.Frontmatter.String("trigger_scope")

	// An ABSENT scope is not a CHANGED scope. Every task in flight today predates
	// this field, so treating absent as changed would hand each of them a free
	// budget reset on the first event after deploy — including one already looping
	// at cap, which would resume for another N. Absent means "adopt the current
	// scope, keep the count".
	scopeChanged := hasScope && storedScope != currentScope

	_, optedIn := task.Frontmatter["max_triggers"]
	_, hasRef := task.Frontmatter.String("ref")
	if (optedIn || hasRef) && !scopeChanged &&
		task.Frontmatter.TriggerCount() >= task.Frontmatter.MaxTriggers() {
		glog.V(2).Infof("skip task %s: trigger_count %d >= max_triggers %d in scope %q",
			task.TaskIdentifier,
			task.Frontmatter.TriggerCount(),
			task.Frontmatter.MaxTriggers(),
			currentScope,
		)
		metrics.TaskEventsTotal.WithLabelValues("skipped_trigger_cap").Inc()
		if err := h.recordTriggerRefusal(ctx, task, currentScope); err != nil {
			return true, err
		}
		return true, nil
	}
	return false, nil
}

// triggerBudgetRefusesSpawn reports whether the scoped trigger budget gate
// suppresses this spawn, counting a failed refusal write as an error. Both
// outcomes suppress, but the caller must still see the error, and spelling them
// as two branches pushed spawnIfNeededClaimed over gocognit's limit; folding
// them here keeps the funnel's branch count flat.
func (h *taskEventHandler) triggerBudgetRefusesSpawn(
	ctx context.Context,
	task lib.Task,
) (bool, error) {
	refused, err := h.checkTriggerBudget(ctx, task)
	if err != nil {
		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
		return true, err
	}
	return refused, nil
}

// recordTriggerRefusal publishes the refusal onto the record unless the record
// already carries the same refusal triple.
func (h *taskEventHandler) recordTriggerRefusal(
	ctx context.Context,
	task lib.Task,
	scope string,
) error {
	count := task.Frontmatter.TriggerCount()
	maxTriggers := task.Frontmatter.MaxTriggers()
	if triggerRefusalRecorded(task.Frontmatter, scope, count, maxTriggers) {
		return nil
	}
	if err := h.resultPublisher.PublishTriggerBudgetRefusal(
		ctx, task, scope, count, maxTriggers,
	); err != nil {
		return errors.Wrapf(
			ctx, err, "publish trigger budget refusal for task %s", task.TaskIdentifier,
		)
	}
	return nil
}

// triggerRefusalRecorded reports whether the record already carries this exact
// refusal triple. trigger_refusal_at is deliberately not compared: it changes on
// every write and would defeat the guard.
func triggerRefusalRecorded(
	fm lib.TaskFrontmatter,
	scope string,
	count int,
	maxTriggers int,
) bool {
	recordedCount, hasCount := fm.Int("trigger_refusal_count")
	recordedMax, hasMax := fm.Int("trigger_refusal_max")
	recordedScope, hasScope := fm.String("trigger_refusal_scope")
	if !hasCount || !hasMax || !hasScope {
		return false
	}
	return recordedCount == count && recordedMax == maxTriggers && recordedScope == scope
}
