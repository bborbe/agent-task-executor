// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	lib "github.com/bborbe/agent"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

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
		return false, err
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
//
// At V(2) it also emits the diagnostic that tells a genuine zero apart from a
// fault: the population vault-api returned (count and ids), how many entries
// cleared the >= since filter across every document, and the first document's
// metrics_runs shape — its Go type, its first entry's completed_at as the parser
// produced it, the token keys runTokenTotal looks up, and their raw values. A
// bare spent=0 cannot separate an empty assignee list from a document without
// in-window runs from a present-but-unparseable token value, and each of those
// needs a different fix.
func (h *taskEventHandler) tokenSpendSince(
	ctx context.Context,
	assignee string,
	since time.Time,
) (int64, error) {
	items, err := h.tokenSpendReader.ReadForAssignee(ctx, assignee)
	if err != nil {
		return 0, err
	}
	ids := make([]string, 0, len(items))
	var total int64
	var counted int
	diag := absentRunDiagnostics()
	for i, item := range items {
		ids = append(ids, item.ID)
		doc, err := h.tokenSpendReader.ReadDocument(ctx, item.ID)
		if err != nil {
			return 0, err
		}
		sum, runs := sumRunTokensSinceWithCount(doc.Frontmatter, since)
		total += sum
		counted += runs
		if i == 0 {
			diag = describeFirstRun(doc.Frontmatter)
		}
	}
	glog.V(2).Infof(
		"event=token_spend_diagnostic assignee=%s spent=%d items=%d ids=%v "+
			"runs_after_since=%d first_metrics_runs_type=%s first_completed_at=%s "+
			"first_token_keys=%s first_token_values=%s since=%s",
		assignee,
		total,
		len(items),
		ids,
		counted,
		diag.listType,
		diag.completedAt,
		diag.tokenKeys,
		diag.tokenValues,
		since.Format(time.RFC3339),
	)
	return total, nil
}

// sumRunTokensSince sums the four token counts of every metrics_runs entry whose
// completed_at is at or after since. A missing or malformed list is 0; an entry with
// an unparseable timestamp is skipped rather than failing the whole sum.
func sumRunTokensSince(frontmatter map[string]any, since time.Time) int64 {
	total, _ := sumRunTokensSinceWithCount(frontmatter, since)
	return total
}

// sumRunTokensSinceWithCount is sumRunTokensSince plus the number of entries that
// cleared the >= since filter. The count is what separates a wrongly computed
// window boundary from a genuinely empty one, so it is returned rather than
// inferred from the total: a boundary that excludes every entry and a document
// that carries none both sum to 0, but only the first has entries below it.
func sumRunTokensSinceWithCount(frontmatter map[string]any, since time.Time) (int64, int) {
	runs, _ := frontmatter["metrics_runs"].([]any)
	var total int64
	var counted int
	for _, raw := range runs {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		at, ok := runCompletedAt(entry["completed_at"])
		if !ok || at.Before(since) {
			continue
		}
		counted++
		total += runTokenTotal(entry)
	}
	return total, counted
}

// runDiagnostics is the shape report for a task document's first metrics_runs
// entry. Each field names the state that would otherwise be indistinguishable
// from a healthy zero.
type runDiagnostics struct {
	listType    string
	completedAt string
	tokenKeys   string
	tokenValues string
}

// runTokenKeys is the exact key list runTokenTotal reads, in its order. The
// diagnostic reports presence and type per key so a renamed key is visible.
var runTokenKeys = []string{
	"input_tokens",
	"output_tokens",
	"cache_read_tokens",
	"cache_creation_tokens",
}

// absentRunDiagnostics is the report for a document carrying no metrics_runs. It is
// also the starting value when ReadForAssignee returned no documents at all, so an
// empty population reads `<absent>` rather than a blank that looks like a missing
// field in the log line.
func absentRunDiagnostics() runDiagnostics {
	return runDiagnostics{
		listType:    "<absent>",
		completedAt: "<absent>",
		tokenKeys:   "<absent>",
		tokenValues: "<absent>",
	}
}

// describeFirstRun reports the Go type of metrics_runs, the first entry's
// completed_at as runCompletedAt parsed it, and the token keys and raw values
// runTokenTotal would convert. An absent list, an empty list, a non-map entry
// and an unparseable timestamp each read distinctly, so no fault can present as
// a well-formed zero.
func describeFirstRun(frontmatter map[string]any) runDiagnostics {
	raw, ok := frontmatter["metrics_runs"]
	if !ok {
		return absentRunDiagnostics()
	}
	diag := runDiagnostics{listType: fmt.Sprintf("%T", raw)}
	runs, ok := raw.([]any)
	if !ok || len(runs) == 0 {
		diag.completedAt = "<no-entries>"
		diag.tokenKeys = "<no-entries>"
		diag.tokenValues = "<no-entries>"
		return diag
	}
	entry, ok := runs[0].(map[string]any)
	if !ok {
		diag.completedAt = fmt.Sprintf("<not-a-map:%T>", runs[0])
		diag.tokenKeys = "<not-a-map>"
		diag.tokenValues = "<not-a-map>"
		return diag
	}
	diag.completedAt = describeCompletedAt(entry["completed_at"])
	diag.tokenKeys, diag.tokenValues = describeRunTokens(entry)
	return diag
}

// describeCompletedAt prints the first entry's timestamp the way runCompletedAt
// parsed it. A value that fails RFC3339 parsing is marked as such rather than
// echoed, so an entry outside the window cannot read as merely out of range.
func describeCompletedAt(v any) string {
	if at, ok := runCompletedAt(v); ok {
		return at.Format(time.RFC3339)
	}
	return fmt.Sprintf("<unparsed:%T:%v>", v, v)
}

// describeRunTokens reports, for each key runTokenTotal reads, its Go type and its
// raw value. Both are needed: intFromAny returns 0 for a missing key and for a
// value it cannot parse, so a type or a value alone cannot separate the two.
func describeRunTokens(entry map[string]any) (keys, values string) {
	keyParts := make([]string, 0, len(runTokenKeys))
	valueParts := make([]string, 0, len(runTokenKeys))
	for _, key := range runTokenKeys {
		v, ok := entry[key]
		if !ok {
			keyParts = append(keyParts, key+"=<absent>")
			valueParts = append(valueParts, key+"=<absent>")
			continue
		}
		keyParts = append(keyParts, fmt.Sprintf("%s=%T", key, v))
		valueParts = append(valueParts, fmt.Sprintf("%s=%v", key, v))
	}
	return strings.Join(keyParts, ","), strings.Join(valueParts, ",")
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
