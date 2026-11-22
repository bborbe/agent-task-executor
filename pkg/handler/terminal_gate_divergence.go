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

// vaultStatusFor returns the status the vault index records for id and whether
// the index carries the id at all.
//
// Items are visited in order so a duplicate id keeps the last item, matching
// indexTerminalIDs and compareViewToIndex: a later item wins.
func vaultStatusFor(index *claim.TaskIndex, id string) (string, bool) {
	if index == nil {
		return "", false
	}
	var status string
	found := false
	for _, item := range index.Items {
		if item.ID == id {
			status = item.Status
			found = true
		}
	}
	return status, found
}

// terminalGateAllowsEvaluation reports whether the terminal-status gate should
// let the task through because the vault index disagrees with the view.
//
// The view is a projection and the vault is the authority, so a task whose view
// still reports a terminal status while the vault reports it non-terminal is
// re-armed work that has not replayed yet: it must be evaluated, not refused.
// The override is deliberately narrow. A nil client, a failed read, an id the
// index does not carry, or an index status that is itself terminal all keep
// today's view-only refusal. The read reuses the pass cache, so it adds no
// request to the pass.
func (h *taskEventHandler) terminalGateAllowsEvaluation(
	ctx context.Context,
	task lib.Task,
) bool {
	if h.vaultIndexClient == nil {
		return false
	}
	index, _, err := h.vaultIndexFor(ctx)
	if err != nil || index == nil {
		glog.V(2).Infof(
			"event=terminal_gate_index_unavailable gate=terminal_status task=%s",
			task.TaskIdentifier,
		)
		return false
	}
	vaultStatus, ok := vaultStatusFor(index, string(task.TaskIdentifier))
	if !ok {
		return false
	}
	if IsTerminalStatus(domain.TaskStatus(vaultStatus)) {
		return false
	}
	glog.V(2).Infof(
		"event=terminal_gate_divergence task=%s view_status=%s vault_status=%s decision_source=vault",
		task.TaskIdentifier,
		task.Frontmatter.Status(),
		vaultStatus,
	)
	return true
}

// terminalStatusRefuses reports whether the terminal-status gate refuses the
// task. It keeps the gate's view-only rule for every non-terminal view and
// delegates the override to terminalGateAllowsEvaluation, so reconcileTask's
// call site gains no branch.
func (h *taskEventHandler) terminalStatusRefuses(ctx context.Context, task lib.Task) bool {
	if !IsTerminalStatus(task.Frontmatter.Status()) {
		return false
	}
	return !h.terminalGateAllowsEvaluation(ctx, task)
}
