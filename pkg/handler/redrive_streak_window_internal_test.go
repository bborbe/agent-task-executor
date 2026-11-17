// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// The in-memory floor is not a cache of the store: the cap check takes
// max(memory, stored), so a floor that never expires would keep a task capped
// after the store's window had already released it. These specs pin the floor
// to the same window.
var _ = Describe("redriveStreakExpired", func() {
	var now libtime.DateTime

	BeforeEach(func() {
		now = libtime.NewCurrentDateTime().Now()
	})

	It("keeps a streak younger than the window", func() {
		streak := redriveStreak{
			count: 3,
			phase: "ai_review",
			since: now.Add(-claim.RedriveStreakLeaseDuration + libtime.Second),
		}
		Expect(redriveStreakExpired(streak, now)).To(BeFalse())
	})

	It("expires a streak at the window boundary", func() {
		streak := redriveStreak{
			count: 3,
			phase: "ai_review",
			since: now.Add(-claim.RedriveStreakLeaseDuration),
		}
		Expect(redriveStreakExpired(streak, now)).To(BeTrue())
	})

	It("never expires a streak with no recorded start", func() {
		Expect(redriveStreakExpired(redriveStreak{count: 3}, now)).To(BeFalse())
	})
})
