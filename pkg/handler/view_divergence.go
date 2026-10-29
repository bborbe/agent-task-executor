// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"sort"

	lib "github.com/bborbe/agent"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// maxDivergenceLogLines bounds how many individual ids one pass names. The
// counts are never truncated — the metric and the summary carry the full
// number — but a stale or empty index classifies every owned task, and naming
// thousands of ids once a minute is a worse fault than the divergence itself.
const maxDivergenceLogLines = 20

// viewDivergenceFinding is one owned task whose view state differs from the
// vault index, or that the index does not contain at all. Both sides of both
// fields are carried so a reader can see which one moved.
type viewDivergenceFinding struct {
	ID          string
	Absent      bool
	ViewStatus  string
	ViewPhase   string
	VaultStatus string
	VaultPhase  string
}

// viewDivergenceReport is one pass's comparison outcome. Compared counts ids
// actually joined, not owned ids: a failed or empty index reads as compared=0,
// never as agreement.
type viewDivergenceReport struct {
	Compared  int
	Divergent int
	Absent    int
	Findings  []viewDivergenceFinding
}

// compareViewToIndex joins the owned view snapshot to the vault index by task
// id and compares both status and phase. A difference in either field is a
// divergence; an owned id the index does not contain is reported as absent and
// is not counted as compared.
//
// Both sides are canonical: task.Frontmatter.Status() and taskPhaseString
// normalize legacy aliases, so the index side must too. Vault-api reads the
// same vault-cli domain values, which makes that hold in practice — but it is
// an assumption, not a contract. See docs/vault-api-task-index.md.
func compareViewToIndex(owned []lib.Task, index *claim.TaskIndex) viewDivergenceReport {
	report := viewDivergenceReport{}
	items := make(map[string]claim.TaskIndexItem, len(index.Items))
	for _, item := range index.Items {
		// A duplicate id keeps the last item: an arbitrary tie-break for a map
		// build, not a contract.
		items[item.ID] = item
	}
	for _, task := range owned {
		id := string(task.TaskIdentifier)
		viewStatus := string(task.Frontmatter.Status())
		viewPhase := taskPhaseString(task)
		item, ok := items[id]
		if !ok {
			report.Absent++
			report.Findings = append(report.Findings, viewDivergenceFinding{
				ID:         id,
				Absent:     true,
				ViewStatus: viewStatus,
				ViewPhase:  viewPhase,
			})
			continue
		}
		report.Compared++
		if viewStatus == item.Status && viewPhase == item.Phase {
			continue
		}
		report.Divergent++
		report.Findings = append(report.Findings, viewDivergenceFinding{
			ID:          id,
			ViewStatus:  viewStatus,
			ViewPhase:   viewPhase,
			VaultStatus: item.Status,
			VaultPhase:  item.Phase,
		})
	}
	// Deterministic emission order: the owned slice comes from map iteration.
	sort.Slice(report.Findings, func(i, j int) bool {
		return report.Findings[i].ID < report.Findings[j].ID
	})
	return report
}

// checkViewDivergence reads the vault index once and reports every owned task
// whose view state differs from it. It is advisory: it feeds nothing back into
// the pass, writes nothing, and a read failure is logged rather than returned,
// so ReconcileOnce keeps returning nil and service.Run cannot tear the executor
// down over a vault-api outage.
//
// A nil client means the deployment has no vault-api URL and the check is
// skipped without a call or a log line.
func (h *taskEventHandler) checkViewDivergence(ctx context.Context, owned []lib.Task) {
	if h.vaultIndexClient == nil {
		return
	}
	// The read is shared with the candidate gate through the pass cache.
	index, _, err := h.vaultIndexFor(ctx)
	if err != nil || index == nil {
		// A nil index is a read failure, not an empty index: dereferencing it
		// inside the pass is the one outcome the check must never produce.
		metrics.ViewDivergenceTotal.WithLabelValues("read_error").Inc()
		glog.Warningf("event=view_divergence_read_error error=%v", err)
		return
	}
	// `ok` means the read succeeded, not that there was no divergence: a pass
	// that finds findings still counts one successful read.
	metrics.ViewDivergenceTotal.WithLabelValues("ok").Inc()
	report := compareViewToIndex(owned, index)
	// A stale or empty index does not produce a handful of findings, it
	// produces one per owned task: an index that returns nothing classifies
	// every owned id as absent. Naming each of them would write thousands of
	// warning lines every pass, once a minute, which is a worse fault than the
	// divergence it reports. The counts stay exact in the metric and the
	// summary; only maxDivergenceLogLines ids are named.
	//
	// Divergences are emitted before absences, and the two are counted
	// separately rather than sharing one budget. A shared budget spends itself
	// on whichever ids sort first, and absences outnumber divergences by orders
	// of magnitude — measured on dev 2026-10-27, 74 absent against 3 divergent,
	// which left one of the three divergences unnamed. The whole point of the
	// check is to name divergences, so they cannot be the ones dropped.
	namedDivergent := 0
	for _, finding := range report.Findings {
		if finding.Absent {
			continue
		}
		metrics.ViewDivergenceTotal.WithLabelValues("mismatch").Inc()
		if namedDivergent < maxDivergenceLogLines {
			glog.Warningf(
				"event=view_divergence task=%s view_status=%s vault_status=%s view_phase=%s vault_phase=%s",
				finding.ID,
				finding.ViewStatus,
				finding.VaultStatus,
				finding.ViewPhase,
				finding.VaultPhase,
			)
			namedDivergent++
		}
	}
	namedAbsent := 0
	for _, finding := range report.Findings {
		if !finding.Absent {
			continue
		}
		metrics.ViewDivergenceTotal.WithLabelValues("absent").Inc()
		if namedAbsent < maxDivergenceLogLines {
			glog.Warningf(
				"event=view_divergence_absent task=%s view_status=%s view_phase=%s",
				finding.ID, finding.ViewStatus, finding.ViewPhase,
			)
			namedAbsent++
		}
	}
	glog.Infof(
		"event=view_divergence_summary compared=%d divergent=%d index_age_seconds=%d warnings=%d absent=%d named=%d named_absent=%d",
		report.Compared,
		report.Divergent,
		index.IndexAgeSeconds,
		index.Warnings,
		report.Absent,
		namedDivergent,
		namedAbsent,
	)
}
