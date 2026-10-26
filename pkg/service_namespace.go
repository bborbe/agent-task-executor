// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
)

// ResolveServiceNamespace returns the namespace in which a Config's service
// StatefulSet must be deployed. An empty serviceNamespace means the executor's
// own namespace. A value equal to the executor's own namespace is rejected:
// that would be an explicit opt-out of isolation.
func ResolveServiceNamespace(
	ctx context.Context,
	executorNamespace libk8s.Namespace,
	serviceNamespace string,
) (libk8s.Namespace, error) {
	return resolveNamespaceOutsideExecutor(
		ctx,
		executorNamespace,
		serviceNamespace,
		"serviceNamespace",
	)
}

// resolveNamespaceOutsideExecutor is the one rule shared by the job and service
// namespace resolution paths: an empty value falls back to the executor's own
// namespace, and a value equal to the executor's namespace is refused because it
// would be an explicit opt-out of isolation.
func resolveNamespaceOutsideExecutor(
	ctx context.Context,
	executorNamespace libk8s.Namespace,
	namespace string,
	fieldName string,
) (libk8s.Namespace, error) {
	if namespace == "" {
		return executorNamespace, nil
	}
	if namespace == executorNamespace.String() {
		return "", errors.Errorf(
			ctx,
			"%s %q may not equal the executor namespace",
			fieldName,
			namespace,
		)
	}
	return libk8s.Namespace(namespace), nil
}
