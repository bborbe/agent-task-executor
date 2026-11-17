// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"sort"

	lib "github.com/bborbe/agent"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// indexTerminalSuppression collects the ids one gather suppressed because the
// vault index reports their state as terminal. The counter is incremented per
// id as it is added; the log lines are capped and emitted once at the end of
// the gather.
type indexTerminalSuppression struct {
	ids []string
}

// add records one suppressed id and counts it. The counter is deliberately
// uncapped: it is the signal, so it must stay exact however many ids a pass
// suppresses.
func (s *indexTerminalSuppression) add(id string) {
	s.ids = append(s.ids, id)
	metrics.ReconcileRefusedTotal.WithLabelValues(
		metrics.ReconcileRefusedGateIndexTerminal,
	).Inc()
}

// report emits at most maxDivergenceLogLines lines, sorted ascending, one line
// per id. A stale index can classify many tasks as terminal; naming every one
// once a minute is a worse fault than the suppression it reports, and the
// counter carries the full count.
func (s *indexTerminalSuppression) report() {
	sort.Strings(s.ids)
	for i, id := range s.ids {
		if i >= maxDivergenceLogLines {
			return
		}
		glog.V(2).Infof("event=index_terminal_suppressed task=%s", id)
	}
}

// indexGateSuppresses reports whether the vault index says this task is
// terminal. It fails open: a missing client, a failed read or an id absent from
// the index suppresses nothing, so the pass drives the task exactly as before.
func (h *taskEventHandler) indexGateSuppresses(
	ctx context.Context,
	task lib.Task,
	suppressions *indexTerminalSuppression,
) bool {
	_, terminal, err := h.vaultIndexFor(ctx)
	if err != nil {
		return false
	}
	id := string(task.TaskIdentifier)
	if _, ok := terminal[id]; !ok {
		return false
	}
	suppressions.add(id)
	return true
}
