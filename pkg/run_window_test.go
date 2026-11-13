// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/pkg"
)

var _ = Describe("IsWithinRunWindow", func() {
	// The factory's window: Mon–Fri 20:00–06:00, Sat–Sun all day, Europe/Berlin.
	window := &agentv1.RunWindow{
		Timezone:      "Europe/Berlin",
		WeekdayStart:  "20:00",
		WeekdayEnd:    "06:00",
		WeekendAllDay: true,
	}

	// Build the instant IN Europe/Berlin, never in UTC then converted: the
	// conversion would shift every case by the zone offset and make the
	// boundary assertions test the wrong wall-clock time.
	berlin := func(year int, month time.Month, day, hour, minute int) time.Time {
		return time.Date(year, month, day, hour, minute, 0, 0, mustLocation("Europe/Berlin"))
	}

	DescribeTable("the boundary",
		func(now time.Time, allowed bool) {
			inside, _ := pkg.IsWithinRunWindow(now, window)
			Expect(inside).To(Equal(allowed))
		},
		Entry("Monday 19:59 is refused", berlin(2026, time.November, 16, 19, 59), false),
		Entry("Monday 20:01 is allowed", berlin(2026, time.November, 16, 20, 1), true),
		Entry("Tuesday 05:59 is allowed", berlin(2026, time.November, 17, 5, 59), true),
		Entry("Tuesday 06:01 is refused", berlin(2026, time.November, 17, 6, 1), false),
		Entry("Saturday midday is allowed", berlin(2026, time.November, 14, 12, 0), true),
		Entry("Sunday midday is allowed", berlin(2026, time.November, 15, 12, 0), true),
		Entry("Monday 06:01 is refused", berlin(2026, time.November, 16, 6, 1), false),
		Entry("Friday 23:00 is allowed", berlin(2026, time.November, 13, 23, 0), true),
		Entry("Saturday 05:00 is allowed via the weekend rule", berlin(2026, time.November, 14, 5, 0), true),
	)

	It("admits everything when no window is set", func() {
		inside, _ := pkg.IsWithinRunWindow(berlin(2026, time.November, 16, 12, 0), nil)
		Expect(inside).To(BeTrue())
	})

	It("fails closed on an unparseable clock", func() {
		broken := &agentv1.RunWindow{WeekdayStart: "twenty", WeekdayEnd: "06:00"}
		inside, _ := pkg.IsWithinRunWindow(berlin(2026, time.November, 16, 23, 0), broken)
		Expect(inside).To(BeFalse())
	})

	It("returns the next opening when the window is closed", func() {
		closed, nextOpen := pkg.IsWithinRunWindow(berlin(2026, time.November, 16, 19, 59), window)
		Expect(closed).To(BeFalse())
		Expect(nextOpen.Hour()).To(Equal(20))
		Expect(nextOpen.Minute()).To(Equal(0))
		inside, _ := pkg.IsWithinRunWindow(nextOpen, window)
		Expect(inside).To(BeTrue())
	})
})

var _ = Describe("SecondsUntilRunWindowClose", func() {
	window := &agentv1.RunWindow{
		Timezone:      "Europe/Berlin",
		WeekdayStart:  "20:00",
		WeekdayEnd:    "06:00",
		WeekendAllDay: true,
	}
	berlin := func(year int, month time.Month, day, hour, minute int) time.Time {
		return time.Date(year, month, day, hour, minute, 0, 0, mustLocation("Europe/Berlin"))
	}

	It("returns the time left before the morning close", func() {
		remaining, ok := pkg.SecondsUntilRunWindowClose(berlin(2026, time.November, 17, 5, 0), window)
		Expect(ok).To(BeTrue())
		Expect(remaining).To(Equal(int32(3600)))
	})

	It("returns the time left before the morning close from the evening half", func() {
		remaining, ok := pkg.SecondsUntilRunWindowClose(berlin(2026, time.November, 16, 21, 0), window)
		Expect(ok).To(BeTrue())
		Expect(remaining).To(Equal(int32(9 * 3600)))
	})

	It("never returns zero in the window's final second", func() {
		last := time.Date(2026, time.November, 17, 5, 59, 59, 900_000_000, mustLocation("Europe/Berlin"))
		remaining, ok := pkg.SecondsUntilRunWindowClose(last, window)
		Expect(ok).To(BeTrue())
		// Kubernetes rejects ActiveDeadlineSeconds: 0, so the truncating
		// conversion must clamp rather than hand back a zero.
		Expect(remaining).To(BeNumerically(">=", 1))
	})

	It("reports no bound when there is no window", func() {
		_, ok := pkg.SecondsUntilRunWindowClose(berlin(2026, time.November, 16, 21, 0), nil)
		Expect(ok).To(BeFalse())
	})

	It("reports no bound when the window is closed", func() {
		_, ok := pkg.SecondsUntilRunWindowClose(berlin(2026, time.November, 16, 12, 0), window)
		Expect(ok).To(BeFalse())
	})
})

var _ = Describe("AgentConfiguration.EffectiveRunDeadlineSeconds", func() {
	ptrInt32 := func(v int32) *int32 { return &v }

	It("returns the zombie timeout when no budget is set", func() {
		cfg := pkg.AgentConfiguration{ZombieJobTimeoutSeconds: ptrInt32(10800)}
		Expect(cfg.EffectiveRunDeadlineSeconds()).To(Equal(int32(10800)))
	})

	It("returns the budget when it is stricter than the zombie timeout", func() {
		cfg := pkg.AgentConfiguration{
			ZombieJobTimeoutSeconds: ptrInt32(10800),
			RunBudgetSeconds:        7200,
		}
		Expect(cfg.EffectiveRunDeadlineSeconds()).To(Equal(int32(7200)))
	})

	It("keeps the zombie timeout when the budget is looser", func() {
		cfg := pkg.AgentConfiguration{
			ZombieJobTimeoutSeconds: ptrInt32(1800),
			RunBudgetSeconds:        7200,
		}
		Expect(cfg.EffectiveRunDeadlineSeconds()).To(Equal(int32(1800)))
	})

	It("returns the default zombie timeout when neither is set", func() {
		Expect(pkg.AgentConfiguration{}.EffectiveRunDeadlineSeconds()).To(Equal(int32(1800)))
	})
})

func mustLocation(name string) *time.Location {
	location, err := time.LoadLocation(name)
	Expect(err).NotTo(HaveOccurred())
	return location
}
