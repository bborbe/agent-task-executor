// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package v1

// LostClaimAction is what the executor should do with a task after another
// cluster has taken over this agent's expired claim.
type LostClaimAction string

const (
	// RedispatchAfterLostClaim runs the task again under the new owner. It is
	// the outcome for true and for an unset rerunOnLostClaim.
	RedispatchAfterLostClaim LostClaimAction = "redispatch"
	// ParkAfterLostClaim stops the task and parks it at phase human_review
	// for a person to decide, instead of running it a second time.
	ParkAfterLostClaim LostClaimAction = "park"
)

// RerunOnLostClaimEnabled reports whether a task should run again after
// another executor takes over this agent's expired claim. An unset field
// means true, so every Config that predates the field keeps re-dispatching.
func (s ConfigSpec) RerunOnLostClaimEnabled() bool {
	return s.RerunOnLostClaim == nil || *s.RerunOnLostClaim
}

// LostClaimAction maps rerunOnLostClaim to the action to take after a lost
// claim: RedispatchAfterLostClaim for true or unset, ParkAfterLostClaim for
// false. It is pure and is not called by any executor code yet.
func (s ConfigSpec) LostClaimAction() LostClaimAction {
	if s.RerunOnLostClaimEnabled() {
		return RedispatchAfterLostClaim
	}
	return ParkAfterLostClaim
}
