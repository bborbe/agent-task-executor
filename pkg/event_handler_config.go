// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"github.com/bborbe/k8s"

	agentv1 "github.com/bborbe/agent/task/executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

// EventHandlerConfig is the typed in-memory event handler / store
// for Config resources. Backed by github.com/bborbe/k8s generics.
type EventHandlerConfig k8s.EventHandler[agentv1.Config]

// NewEventHandlerConfig returns an empty EventHandlerConfig.
func NewEventHandlerConfig() EventHandlerConfig {
	return k8s.NewEventHandler[agentv1.Config]()
}
