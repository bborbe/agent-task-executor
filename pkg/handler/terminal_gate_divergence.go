// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// vaultItemFor returns the index item recorded for id and whether the index
// carries the id at all.
//
// Items are visited in order so a duplicate id keeps the last item, matching
// indexTerminalIDs and compareViewToIndex: a later item wins.
func vaultItemFor(index *claim.TaskIndex, id string) (claim.TaskIndexItem, bool) {
	if index == nil {
		return claim.TaskIndexItem{}, false
	}
	var item claim.TaskIndexItem
	found := false
	for _, candidate := range index.Items {
		if candidate.ID == id {
			item = candidate
			found = true
		}
	}
	return item, found
}

// vaultNonTerminalState returns the status and phase the vault index records for
// id when that state is non-terminal.
//
// "Terminal" is the same rule indexTerminalIDs applies: the status OR the phase
// is in the fixed terminal sets. Sharing the rule is what keeps this override
// from releasing a task the candidate gate suppressed for the same reason.
func vaultNonTerminalState(
	index *claim.TaskIndex,
	id string,
) (domain.TaskStatus, domain.TaskPhase, bool) {
	item, ok := vaultItemFor(index, id)
	if !ok {
		return "", "", false
	}
	status := domain.TaskStatus(item.Status)
	phase := domain.TaskPhase(item.Phase)
	if IsTerminalStatus(status) || IsTerminal(phase) {
		return "", "", false
	}
	return status, phase, true
}

// applyTerminalStatusGate decides whether the view's terminal status refuses the
// task and, when the vault disagrees, returns the task carrying the vault's
// status and phase.
//
// The view is a projection and the vault is the authority. A task whose view
// still reports a terminal status while the vault reports it non-terminal is
// re-armed work that has not replayed yet: it must be evaluated on the vault's
// pair, not refused. Returning the rewritten task matters as much as the
// decision — every gate below this one (trigger status, trigger phase, terminal
// phase) and the Job's phase annotation read the task's frontmatter, so an
// override that only cleared the gate would be refused one line later on the
// same stale pair.
//
// The override is deliberately narrow. A nil client, a failed read, an id the
// index does not carry, or an index state that is itself terminal all keep
// today's view-only refusal. The read reuses the pass cache, so it adds no
// request to the pass.
func (h *taskEventHandler) applyTerminalStatusGate(
	ctx context.Context,
	task lib.Task,
) (lib.Task, bool) {
	if !IsTerminalStatus(task.Frontmatter.Status()) {
		return task, false
	}
	if h.vaultIndexClient == nil {
		return task, true
	}
	index, _, err := h.vaultIndexFor(ctx)
	if err != nil || index == nil {
		h.logTerminalGateIndexUnavailable(ctx)
		return task, true
	}
	status, phase, ok := vaultNonTerminalState(index, string(task.TaskIdentifier))
	if !ok {
		return task, true
	}
	glog.V(2).Infof(
		"event=terminal_gate_divergence task=%s view_status=%s vault_status=%s decision_source=vault",
		task.TaskIdentifier,
		task.Frontmatter.Status(),
		status,
	)
	return taskWithStatusPhase(task, status, phase), false
}

// logTerminalGateIndexUnavailable emits the index-unavailable line at most once
// per pass. An unreadable index fails the override for every task at once;
// naming each one once a minute is a worse fault than the fallback it reports.
func (h *taskEventHandler) logTerminalGateIndexUnavailable(ctx context.Context) {
	if cache := vaultIndexCacheFrom(ctx); cache != nil {
		cache.logUnavailableOnce()
		return
	}
	glog.V(2).Infof("event=terminal_gate_index_unavailable gate=terminal_status")
}

// taskWithStatusPhase returns a copy of task whose frontmatter carries status
// and phase. The frontmatter map is copied, never written in place: snapshot
// tasks share it with the view.
func taskWithStatusPhase(
	task lib.Task,
	status domain.TaskStatus,
	phase domain.TaskPhase,
) lib.Task {
	frontmatter := make(lib.TaskFrontmatter, len(task.Frontmatter)+2)
	for key, value := range task.Frontmatter {
		frontmatter[key] = value
	}
	frontmatter["status"] = string(status)
	frontmatter["phase"] = string(phase)
	task.Frontmatter = frontmatter
	return task
}
