// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"fmt"
	"strings"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// planSectionHeading is the markdown section a plan-consuming agent's execution
// phase reads as its input. The agent's own planning phase writes it.
const planSectionHeading = "## Plan"

// planRequiredTaskTypes lists the task_type values whose execution phase reads a
// "## Plan" section written by their own planning phase.
//
// It is an explicit allowlist, not an inference from the Config's trigger
// phases: almost every Config triggers on both planning and execution (measured
// on nuke dev 2026-11-19: 15 of 23), and only these pipelines treat the section
// as an input. A new plan-consuming agent adds its task_type here.
var planRequiredTaskTypes = map[lib.TaskType]struct{}{
	"strategy-prototype": {},
}

// planSectionRefusesSpawn reports whether the spawn must be refused because a
// plan-consuming task reached the execution phase without the "## Plan" section
// its execution step reads.
//
// Refusing rather than spawning is the point: the Job would start, find no
// section, and exit within seconds — the measured shape is an 8–22 s Job logging
// `read ## Plan: ## Plan section missing` — burning one of the agent's
// concurrency slots and a run-window slot while producing nothing. The
// agent-side fail-fast (bborbe/trading#581) makes such a run fail loudly
// instead of recording a success; this gate stops it running at all.
//
// Like every other admission refusal in this executor (the run window, the
// concurrency cap, the token ceiling), the refusal is a log line plus a metric
// and no write: the record keeps its status, phase, assignee and trigger_count,
// so it stays routed and is admitted automatically as soon as a "## Plan"
// appears — the missing section is a state the planning phase can still repair,
// and clearing the assignee would strand a task the pipeline can finish itself.
func planSectionRefusesSpawn(ctx context.Context, task lib.Task) bool {
	if !planSectionRequired(task) {
		return false
	}
	// The task view drops a body only for terminal-phase tasks, which never
	// reach here — but an empty body must never be read as "no ## Plan".
	// Refusing on a body this path cannot see would suppress a spawn for a task
	// that has the section.
	if strings.TrimSpace(string(task.Content)) == "" {
		return false
	}
	parsed, err := lib.ParseMarkdown(ctx, string(task.Content))
	if err != nil {
		// The parser is best-effort and this call cannot fail today; if it ever
		// does, an unreadable body is not evidence of a missing section.
		return false
	}
	if _, found := parsed.FindSection(planSectionHeading); found {
		return false
	}
	taskType := string(task.Frontmatter.TaskType())
	reason := fmt.Sprintf(
		"task_type %q at phase %s carries no %q section",
		taskType,
		domain.TaskPhaseExecution,
		planSectionHeading,
	)
	glog.V(1).Infof(
		"event=admission_refused_missing_plan task=%s task_type=%s phase=%s reason=%q",
		task.TaskIdentifier,
		taskType,
		domain.TaskPhaseExecution,
		reason,
	)
	metrics.TaskEventsTotal.WithLabelValues("skipped_missing_plan").Inc()
	return true
}

// planSectionRequired reports whether the task is a plan-consuming task at its
// execution phase. The phase is read through TaskFrontmatter.Phase, which
// normalizes the legacy in_progress alias onto execution.
func planSectionRequired(task lib.Task) bool {
	if _, ok := planRequiredTaskTypes[task.Frontmatter.TaskType()]; !ok {
		return false
	}
	phase := task.Frontmatter.Phase()
	return phase != nil && *phase == domain.TaskPhaseExecution
}
