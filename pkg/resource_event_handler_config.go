// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	"github.com/bborbe/k8s"
	"k8s.io/client-go/tools/cache"

	agentv1 "github.com/bborbe/agent/task/executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

// NewResourceEventHandlerConfig adapts an EventHandlerConfig to the
// cache.ResourceEventHandler the informer expects.
func NewResourceEventHandlerConfig(
	ctx context.Context,
	handler EventHandlerConfig,
) cache.ResourceEventHandler {
	return k8s.NewResourceEventHandler[agentv1.Config](ctx, handler)
}
