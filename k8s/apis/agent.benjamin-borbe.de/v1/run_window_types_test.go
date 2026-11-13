// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package v1_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

var _ = Describe("ConfigSpec run window", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	valid := func() agentv1.ConfigSpec {
		return agentv1.ConfigSpec{
			Assignee:  "factory-agent",
			Image:     "agent-strategy-prototype",
			Heartbeat: "5m",
			TaskType:  "strategy-prototype",
			RunWindow: &agentv1.RunWindow{
				Timezone:      "Europe/Berlin",
				WeekdayStart:  "20:00",
				WeekdayEnd:    "06:00",
				WeekendAllDay: true,
			},
			RunBudgetSeconds: 7200,
		}
	}

	Describe("Equal", func() {
		It("compares RunWindow", func() {
			a := valid()
			b := valid()
			Expect(a.Equal(b)).To(BeTrue())

			b.RunWindow = &agentv1.RunWindow{
				Timezone:     "Europe/Berlin",
				WeekdayStart: "19:00",
				WeekdayEnd:   "06:00",
			}
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("compares RunBudgetSeconds", func() {
			a := valid()
			b := valid()
			b.RunBudgetSeconds = 3600
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("distinguishes a set window from an unset one", func() {
			a := valid()
			b := valid()
			b.RunWindow = nil
			Expect(a.Equal(b)).To(BeFalse())
		})
	})

	Describe("Validate", func() {
		It("accepts a well-formed window", func() {
			Expect(valid().Validate(ctx)).To(BeNil())
		})

		It("accepts an absent window", func() {
			spec := valid()
			spec.RunWindow = nil
			Expect(spec.Validate(ctx)).To(BeNil())
		})

		It("rejects an unresolvable timezone rather than silently using UTC", func() {
			spec := valid()
			spec.RunWindow.Timezone = "Europe/Nowhere"
			Expect(spec.Validate(ctx)).NotTo(BeNil())
		})

		It("rejects a clock that is not HH:MM", func() {
			spec := valid()
			spec.RunWindow.WeekdayStart = "8pm"
			Expect(spec.Validate(ctx)).NotTo(BeNil())
		})

		It("rejects an out-of-range hour", func() {
			spec := valid()
			spec.RunWindow.WeekdayEnd = "24:00"
			Expect(spec.Validate(ctx)).NotTo(BeNil())
		})
	})
})
