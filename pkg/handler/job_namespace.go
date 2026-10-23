// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// jobNamespace resolves the namespace this Config's Jobs live in. A nil config
// has no Config-supplied namespace, so it uses the executor's own.
func (h *taskEventHandler) jobNamespace(
	ctx context.Context,
	config *pkg.AgentConfiguration,
) (libk8s.Namespace, error) {
	if config == nil {
		return h.namespace, nil
	}
	return pkg.ResolveJobNamespace(ctx, h.namespace, config.JobNamespace)
}

// jobNamespaceForTask resolves the Job namespace for a task and wraps a
// resolution failure with the task identifier.
func (h *taskEventHandler) jobNamespaceForTask(
	ctx context.Context,
	config *pkg.AgentConfiguration,
	taskID lib.TaskIdentifier,
) (libk8s.Namespace, error) {
	namespace, err := h.jobNamespace(ctx, config)
	if err != nil {
		return "", errors.Wrapf(ctx, err, "resolve job namespace for task %s", taskID)
	}
	return namespace, nil
}
