// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	lib "github.com/bborbe/agent"
)

// ClaimReleaser releases this executor's held claim for a task, if any. The
// task event handler implements it and the Job watcher is injected with it, so
// a terminal Job can release the claim taken before the Job was created.
type ClaimReleaser interface {
	ReleaseTaskClaim(ctx context.Context, taskID lib.TaskIdentifier)
}
