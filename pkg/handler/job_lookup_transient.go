// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	stderrors "errors"
	"net"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// jobLookupTransientError marks a failed active-job read that may succeed on a
// later attempt: a 5xx, a 429, a timeout, a connection error, or a context
// deadline. It is the transient sibling of jobLookupDeniedError.
//
// Unlike a denial it is worth retrying, but retrying on the next
// deferred-respawn tick (or the next reconcile pass) is enough. It is a distinct
// type so evalDeferredRespawns can log and skip the entry instead of returning
// the error, which would end RunDeferredRespawnLoop and restart the executor pod
// (Sentry NUKE-DEV-GT: a kube-apiserver 503 during a deferred tick).
type jobLookupTransientError struct {
	err error
}

func (e *jobLookupTransientError) Error() string { return e.err.Error() }

func (e *jobLookupTransientError) Unwrap() error { return e.err }

// isJobLookupTransient reports whether err is a transient API-server failure: a
// condition that may clear on its own without operator action. It matches the
// apimachinery transient predicates (service-unavailable, timeout, server
// timeout, too-many-requests, internal error), any net.Error, and a context
// deadline. It deliberately does NOT match 401/403 (that is isJobLookupDenied),
// a 404 (the resource resolved), or an arbitrary programming error.
func isJobLookupTransient(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsServiceUnavailable(err) ||
		apierrors.IsTimeout(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) ||
		apierrors.IsInternalError(err) {
		return true
	}
	if stderrors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return stderrors.As(err, &netErr)
}
