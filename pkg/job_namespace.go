// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
)

// ResolveJobNamespace returns the namespace in which a Config's Jobs must be
// created. An empty jobNamespace means the executor's own namespace, preserving
// the behaviour of every Config that does not set the field. A jobNamespace
// equal to the executor's own namespace is rejected: that would be an explicit
// opt-out of isolation, and no Config may author one.
func ResolveJobNamespace(
	ctx context.Context,
	executorNamespace libk8s.Namespace,
	jobNamespace string,
) (libk8s.Namespace, error) {
	if jobNamespace == "" {
		return executorNamespace, nil
	}
	if jobNamespace == executorNamespace.String() {
		return "", errors.Errorf(
			ctx,
			"jobNamespace %q may not equal the executor namespace",
			jobNamespace,
		)
	}
	return libk8s.Namespace(jobNamespace), nil
}
