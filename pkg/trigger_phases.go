// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"github.com/bborbe/vault-cli/pkg/domain"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

// DefaultTriggerPhases is the fallback phase allow-list used when a Config
// carries no Trigger or an empty phase list. It holds every phase the executor
// runs: planning, execution and ai_review.
var DefaultTriggerPhases = domain.TaskPhases{
	domain.TaskPhasePlanning,
	domain.TaskPhaseExecution,
	domain.TaskPhaseAIReview,
}

// EffectiveTriggerPhases returns the phase allow-list a Config's trigger
// declares, falling back to DefaultTriggerPhases when the trigger is nil or its
// phase list is empty. It is the single definition of the effective set shared
// by the spawn gate and the liveness probe.
func EffectiveTriggerPhases(trigger *agentv1.Trigger) domain.TaskPhases {
	if trigger == nil || len(trigger.Phases) == 0 {
		return DefaultTriggerPhases
	}
	return trigger.Phases
}
