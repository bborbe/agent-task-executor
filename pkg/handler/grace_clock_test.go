// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import "time"

// graceAnchor is the T+0 instant the deferred-respawn and index-terminal specs
// are written against.
var graceAnchor = time.Date(2026, 5, 17, 9, 34, 0, 0, time.UTC)

// graceOffset renders a timestamp relative to graceAnchor as RFC3339.
//
// The specs used to hardcode these as literals — T+299s for "one second before
// grace expiry", T+330s and T+360s for "past it". That silently asserted a
// 300 s SuccessGracePeriod: when the constant moved to 900 s every
// "past the grace" fixture landed *inside* the grace, and the suite waited on
// spawns that were now correctly suppressed. Deriving them from the constant
// keeps each fixture meaning what its name says at any grace value.
func graceOffset(offset time.Duration) string {
	return graceAnchor.Add(offset).Format(time.RFC3339)
}

// graceOffsetFrom renders base plus offset as RFC3339, for the specs that keep
// their own anchor instant.
func graceOffsetFrom(base string, offset time.Duration) string {
	at, err := time.Parse(time.RFC3339, base)
	if err != nil {
		panic(err)
	}
	return at.Add(offset).Format(time.RFC3339)
}
