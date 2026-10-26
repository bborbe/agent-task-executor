// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import "github.com/bborbe/agent-task-executor/pkg/claim"

// ClaimRefClientFactory builds the git-rest claim ref client. It is called at
// most once per consumer of it, and only when CLAIM_MODE is shadow or on.
//
// It lives in pkg rather than handler because both the spawn funnel (through
// handler) and the result publisher need it, and the publisher must not import
// handler. Each call returns a fresh client, so the publisher and the handler
// each build their own store over their own client.
type ClaimRefClientFactory func() claim.ClaimRefClient
