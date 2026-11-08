// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
)

var _ = Describe("EffectiveTriggerPhases", func() {
	It("falls back to the default list for a nil trigger", func() {
		Expect(pkg.EffectiveTriggerPhases(nil)).To(Equal(pkg.DefaultTriggerPhases))
	})

	It("falls back to the default list for empty phases", func() {
		Expect(
			pkg.EffectiveTriggerPhases(&agentv1.Trigger{Phases: domain.TaskPhases{}}),
		).To(Equal(pkg.DefaultTriggerPhases))
	})

	It("returns the trigger's phases when non-empty", func() {
		phases := domain.TaskPhases{domain.TaskPhasePlanning}
		Expect(pkg.EffectiveTriggerPhases(&agentv1.Trigger{Phases: phases})).To(Equal(phases))
	})

	It("defaults to planning, execution and ai_review", func() {
		Expect(pkg.DefaultTriggerPhases).To(Equal(domain.TaskPhases{
			domain.TaskPhasePlanning,
			domain.TaskPhaseExecution,
			domain.TaskPhaseAIReview,
		}))
	})
})
