// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"strconv"
	"strings"
	"time"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

// IsWithinRunWindow reports whether now falls inside the agent's run window,
// together with the next time the window opens. A nil window admits everything.
// A window whose times cannot be parsed admits nothing, so a typo fails closed
// rather than silently disabling the constraint.
func IsWithinRunWindow(now time.Time, window *agentv1.RunWindow) (bool, time.Time) {
	if window == nil {
		return true, time.Time{}
	}
	local := now.In(runWindowLocation(window.Timezone))
	if inRunWindowAt(local, window) {
		return true, time.Time{}
	}
	return false, NextRunWindowOpen(local, window)
}

// NextRunWindowOpen returns the first minute after now at which the window
// opens. The scan is bounded to eight days, which covers every weekly shape, so
// a malformed window cannot spin.
func NextRunWindowOpen(now time.Time, window *agentv1.RunWindow) time.Time {
	if window == nil {
		return now
	}
	local := now.In(runWindowLocation(window.Timezone))
	t := local.Truncate(time.Minute).Add(time.Minute)
	for i := 0; i < 8*24*60; i++ {
		if inRunWindowAt(t, window) {
			return t
		}
		t = t.Add(time.Minute)
	}
	return now.Add(7 * 24 * time.Hour)
}

// SecondsUntilRunWindowClose returns how long the window stays open from now.
// The caller caps a Job's deadline with it so a run admitted near the end of the
// window is stopped when the window closes rather than running into business
// hours. ok is false when there is no window, or when the window never closes
// within the eight-day scan (a permanently open configuration).
func SecondsUntilRunWindowClose(now time.Time, window *agentv1.RunWindow) (int32, bool) {
	if window == nil {
		return 0, false
	}
	local := now.In(runWindowLocation(window.Timezone))
	if !inRunWindowAt(local, window) {
		return 0, false
	}
	probe := local.Truncate(time.Minute)
	for i := 0; i < 8*24*60; i++ {
		probe = probe.Add(time.Minute)
		if !inRunWindowAt(probe, window) {
			remaining := int32(probe.Sub(local).Seconds())
			// Seconds() truncates, so a start in the window's final second yields
			// 0 — and JobSpec rejects a non-positive ActiveDeadlineSeconds, which
			// would turn a valid spawn into a failed one. One second is the
			// smallest deadline that is still a deadline.
			if remaining < 1 {
				remaining = 1
			}
			return remaining, true
		}
	}
	return 0, false
}

func runWindowLocation(name string) *time.Location {
	if name == "" {
		return time.UTC
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return time.UTC
	}
	return location
}

// inRunWindowAt decides one instant. A window that crosses midnight is
// attributed to the day it opened on, so a Friday 20:00–06:00 window admits
// Saturday 05:00 without needing WeekendAllDay.
func inRunWindowAt(t time.Time, window *agentv1.RunWindow) bool {
	weekday := t.Weekday()
	isWeekend := weekday == time.Saturday || weekday == time.Sunday
	if window.WeekendAllDay && isWeekend {
		return true
	}
	start, startOK := parseClock(window.WeekdayStart)
	end, endOK := parseClock(window.WeekdayEnd)
	if !startOK || !endOK {
		return false
	}
	minute := t.Hour()*60 + t.Minute()
	if start == end {
		return !isWeekend
	}
	if start < end {
		return !isWeekend && minute >= start && minute < end
	}
	// The window crosses midnight: the evening half belongs to today, the
	// morning half belongs to the previous day.
	if minute >= start {
		return !isWeekend
	}
	if minute < end {
		previous := time.Weekday((int(weekday) + 6) % 7)
		return previous != time.Saturday && previous != time.Sunday
	}
	return false
}

func parseClock(value string) (int, bool) {
	parts := strings.SplitN(strings.TrimSpace(value), ":", 2)
	if len(parts) != 2 {
		return 0, false
	}
	hour, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || hour < 0 || hour > 23 {
		return 0, false
	}
	minute, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || minute < 0 || minute > 59 {
		return 0, false
	}
	return hour*60 + minute, true
}
