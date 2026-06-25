// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"net/http"

	libhttp "github.com/bborbe/http"

	"github.com/bborbe/agent-task-executor/pkg/probe"
)

// NewHealthcheckTriggerHandler returns an HTTP handler that fires the healthcheck runner
// once per invocation with fire-and-forget + single-flight semantics.
// Concurrent invocations collapse into one in-flight run (second request is silently dropped).
//
// The handler uses context.Background() for its internal operations so it is not
// affected by the caller's context cancellation. The handler only stops when the
// HTTP server shuts down.
func NewHealthcheckTriggerHandler(
	runner probe.HealthcheckRunner,
) http.Handler {
	return libhttp.NewBackgroundRunHandler(context.Background(), runner.Run)
}
