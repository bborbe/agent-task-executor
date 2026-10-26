// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"sync"
	"time"

	libtime "github.com/bborbe/time"
)

// stepClock is a clock a test moves by hand. The external package has its own
// copy: the two test packages cannot share an unexported helper, and the shared
// claimtest package cannot be imported from here without an import cycle.
type stepClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *stepClock) get() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *stepClock) Now() libtime.DateTime {
	c.mu.Lock()
	defer c.mu.Unlock()
	return libtime.DateTime(c.now)
}
