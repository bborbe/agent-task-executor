// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import "github.com/bborbe/agent-task-executor/pkg/claim"

// ClaimClientFactory builds the typed vault-api claim client. It is called at
// most once, by the lease manager's construction, and only when CLAIM_MODE is
// shadow or on.
//
// It is a separate factory from ClaimRefClientFactory rather than a second
// method on it because the two clients address different services with
// different credentials: the ref client still serves the redrive stores, which
// use git-rest refs as counters, while every lease is held through this one.
type ClaimClientFactory func() claim.ClaimClient
