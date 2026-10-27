// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"

	"github.com/bborbe/errors"
	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// inTypePreviousAssignee returns previousAssignee only when the Config it names
// declares taskType.
//
// previous_assignee is a tie-break among Configs that already declare the
// record's type, so an agent declaring none of them is not a candidate for that
// role: handing the name to ResolveByTaskType can only produce
// ErrConfigAmbiguous, and it produces it on the non-empty arm rather than the
// empty one. That is not a cosmetic difference. The non-empty arm reads as
// "several Configs, and the tie-break named none of them", while the truth is
// "no tie-break was available at all" — and the resolver consults the
// designated default only once preferAssignee is empty, so passing a name that
// cannot win hides the very input that resolves the record.
//
// An assignee with no Config in this stage is dropped for the same reason: there
// is no type set to check against, so the name cannot break a tie here. A store
// error is returned rather than swallowed, so the caller reports it as a
// resolve_error skip instead of routing on a partial read.
func (h *taskEventHandler) inTypePreviousAssignee(
	ctx context.Context,
	previousAssignee string,
	taskType string,
) (string, error) {
	if previousAssignee == "" {
		return "", nil
	}
	config, err := h.resolver.Resolve(ctx, previousAssignee)
	if err != nil {
		if stderrors.Is(err, pkg.ErrConfigNotFound) {
			glog.V(2).Infof(
				"ignore previous_assignee %s: no config in this stage",
				previousAssignee,
			)
			return "", nil
		}
		return "", errors.Wrapf(ctx, err, "resolve previous_assignee %q", previousAssignee)
	}
	effectiveTypes := pkg.EffectiveTaskTypes(config.TaskType, config.TaskTypes)
	if !pkg.TaskTypeInSet(taskType, effectiveTypes) {
		glog.V(2).Infof(
			"ignore previous_assignee %s: its config declares %v, not task_type %s",
			previousAssignee,
			effectiveTypes,
			taskType,
		)
		return "", nil
	}
	return previousAssignee, nil
}
