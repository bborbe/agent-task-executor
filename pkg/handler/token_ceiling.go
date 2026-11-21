// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"
	"strconv"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// vaultReadError marks a failed vault-api read in the token-ceiling gate. The
// gate is best-effort: a read failure fails open (the spawn proceeds) rather
// than failing the run, and the deferred-respawn loop treats a surfaced
// vaultReadError like spawnJobError instead of exiting the process.
type vaultReadError struct {
	err error
}

func (e *vaultReadError) Error() string { return e.err.Error() }

func (e *vaultReadError) Unwrap() error { return e.err }

// deferIfOverTokenCeiling reports whether the agent's token spend in the current run
// window already meets its configured ceiling, and if so defers the respawn. It runs
// after the window check (so it only ever sees an open window) and before the
// concurrency cap (so an over-budget agent does not hold a slot it could not use).
//
// The sum is a durable read: the assignee's tasks are listed and each task's
// metrics_runs history is read, summing the runs that completed inside the window. No
// in-memory accumulator, so a restart does not reset the budget.
func (h *taskEventHandler) deferIfOverTokenCeiling(
	ctx context.Context,
	task lib.Task,
	config *pkg.AgentConfiguration,
) (bool, error) {
	if config == nil || config.TokenCeiling == nil || *config.TokenCeiling <= 0 ||
		config.RunWindow == nil || h.tokenSpendReader == nil {
		return false, nil
	}
	now := h.currentDateTime.Now().Time()
	since, ok := pkg.CurrentRunWindowStart(now, config.RunWindow)
	if !ok {
		return false, nil
	}
	spent, err := h.tokenSpendSince(ctx, config.Assignee, since)
	if err != nil {
		// Only a vault-api read failure is best-effort. Anything else stays fatal.
		var vaultErr *vaultReadError
		if !stderrors.As(err, &vaultErr) {
			return false, err
		}
		// Fail open: a transient vault-api read failure must not defer the spawn
		// nor fail the run. Returning the error here reaches the deferred-respawn
		// loop as a fatal run.Func error and terminates the process (Sentry
		// NUKE-DEV-H2). Treat an unreadable spend as "not over budget" so the
		// spawn proceeds, exactly as a failed index read does.
		glog.Warningf(
			"event=token_ceiling_read_error task=%s assignee=%s err=%v",
			task.TaskIdentifier, config.Assignee, err,
		)
		metrics.TaskEventsTotal.WithLabelValues("token_ceiling_read_error").Inc()
		return false, nil
	}
	if spent < *config.TokenCeiling {
		return false, nil
	}
	glog.V(1).Infof(
		"event=token_ceiling task=%s assignee=%s spent=%d ceiling=%d since=%s",
		task.TaskIdentifier,
		config.Assignee,
		spent,
		*config.TokenCeiling,
		since.Format(time.RFC3339),
	)
	metrics.TaskEventsTotal.WithLabelValues("deferred_token_ceiling").Inc()
	// Retry at the concurrency delay, not the window's next open: the window is open
	// now, so the spend only drops when it rolls over — a re-check is cheaper than
	// computing that instant and re-parking on every event.
	h.deferredMu.Lock()
	h.deferredRespawns[task.TaskIdentifier] = deferredEntry{
		task:       task,
		config:     *config,
		retryAfter: now.Add(concurrencyCapRetryDelay),
	}
	h.deferredMu.Unlock()
	return true, nil
}

// tokenSpendSince sums the window token spend of every task assigned to assignee.
// A failed vault-api read is returned as a *vaultReadError so the caller can fail
// open and the deferred-respawn loop can treat it as non-fatal.
func (h *taskEventHandler) tokenSpendSince(
	ctx context.Context,
	assignee string,
	since time.Time,
) (int64, error) {
	items, err := h.tokenSpendReader.ReadForAssignee(ctx, assignee)
	if err != nil {
		return 0, &vaultReadError{
			err: errors.Wrapf(ctx, err, "read token spend for assignee %s", assignee),
		}
	}
	var total int64
	for _, item := range items {
		doc, err := h.tokenSpendReader.ReadDocument(ctx, item.ID)
		if err != nil {
			return 0, &vaultReadError{
				err: errors.Wrapf(ctx, err, "read token spend document %s", item.ID),
			}
		}
		total += sumRunTokensSince(doc.Frontmatter, since)
	}
	return total, nil
}

// sumRunTokensSince sums the four token counts of every metrics_runs entry whose
// completed_at is at or after since. A missing or malformed list is 0; an entry with
// an unparseable timestamp is skipped rather than failing the whole sum.
func sumRunTokensSince(frontmatter map[string]any, since time.Time) int64 {
	runs, _ := frontmatter["metrics_runs"].([]any)
	var total int64
	for _, raw := range runs {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		at, ok := runCompletedAt(entry["completed_at"])
		if !ok || at.Before(since) {
			continue
		}
		total += runTokenTotal(entry)
	}
	return total
}

func runCompletedAt(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}
	at, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return at, true
}

func runTokenTotal(entry map[string]any) int64 {
	var total int64
	for _, key := range []string{
		"input_tokens",
		"output_tokens",
		"cache_read_tokens",
		"cache_creation_tokens",
	} {
		total += intFromAny(entry[key])
	}
	return total
}

// intFromAny reads an integer from the decoded frontmatter, tolerating the shapes a
// YAML/JSON round-trip produces (int, int64, float64, and the quoted string
// vault-cli's `task set` writes).
func intFromAny(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case string:
		parsed, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}
