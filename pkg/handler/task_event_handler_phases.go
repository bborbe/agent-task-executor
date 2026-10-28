// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"github.com/bborbe/vault-cli/pkg/domain"
)

// terminalPhases is the set of phases that must never trigger a new spawn.
// Extending this set requires a follow-up spec if vault-cli adds new terminal phases.
var terminalPhases = map[domain.TaskPhase]struct{}{
	domain.TaskPhaseHumanReview: {},
	domain.TaskPhaseDone:        {},
}

// knownPhases contains all phase constants exported by vault-cli v0.64.3.
// Values outside this set trigger enum-drift logging (event=unknown_phase).
var knownPhases = map[domain.TaskPhase]struct{}{
	domain.TaskPhaseTodo:        {},
	domain.TaskPhasePlanning:    {},
	domain.TaskPhaseExecution:   {}, // canonical (was TaskPhaseInProgress)
	domain.TaskPhaseInProgress:  {}, // legacy alias — still a known phase string
	domain.TaskPhaseAIReview:    {},
	domain.TaskPhaseHumanReview: {},
	domain.TaskPhaseDone:        {},
}

// IsTerminal reports whether the given phase is in the terminal set.
// Tasks at a terminal phase must not be re-spawned; operator intervention is required.
func IsTerminal(phase domain.TaskPhase) bool {
	_, ok := terminalPhases[phase]
	return ok
}
