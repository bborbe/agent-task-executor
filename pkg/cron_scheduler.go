// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
)

// CronScheduler runs a cron expression on a schedule.
type CronScheduler interface {
	Run(ctx context.Context) error
}
