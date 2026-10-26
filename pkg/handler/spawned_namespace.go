// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	"github.com/golang/glog"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// spawnedNamespace returns the resolved namespace the Job was created in, for the
// spawn counter's label.
//
// The resolved value, never config.JobNamespace: empty means "the executor's own
// namespace", and labelling that "" would make a control-namespace spawn
// indistinguishable from a sandboxed one — falsifying exactly the query the label
// exists to answer.
//
// Resolution cannot fail for a namespace the executor has already used to create
// this Job, but the counter must never be the thing that breaks a successful spawn.
// An unresolvable value is reported as "unresolved" rather than collapsed into the
// executor namespace, so a broken read is visible as itself instead of reading as a
// control-namespace spawn.
func (h *taskEventHandler) spawnedNamespace(
	ctx context.Context,
	config *pkg.AgentConfiguration,
) string {
	namespace, err := pkg.ResolveJobNamespace(ctx, h.namespace, config.JobNamespace)
	if err != nil {
		glog.Warningf(
			"resolve job namespace for spawn counter failed (assignee=%s): %v",
			config.Assignee, err,
		)
		return "unresolved"
	}
	return namespace.String()
}
