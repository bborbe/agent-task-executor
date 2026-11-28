// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	stderrors "errors"
	"net"
	"net/http"
	"testing"

	libk8s "github.com/bborbe/k8s"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// timeoutNetError implements net.Error so isJobLookupTransient's net.Error arm
// can be exercised.
type timeoutNetError struct{}

var _ net.Error = timeoutNetError{}

func (timeoutNetError) Error() string { return "connection timed out" }

func (timeoutNetError) Timeout() bool { return true }

func (timeoutNetError) Temporary() bool { return true }

// timeoutStatusError is a hand-built 504: apimachinery has no NewTimeout
// constructor, so the predicate is exercised against the real StatusError.
func timeoutStatusError() *apierrors.StatusError {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Reason:  metav1.StatusReasonTimeout,
		Code:    http.StatusGatewayTimeout,
		Message: "timeout",
	}}
}

func TestIsJobLookupTransient(t *testing.T) {
	jobs := schema.GroupResource{Resource: "jobs"}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"service unavailable", apierrors.NewServiceUnavailable("apiserver not ready"), true},
		{"server timeout", apierrors.NewServerTimeout(jobs, "list", 1), true},
		{"gateway timeout", timeoutStatusError(), true},
		{"too many requests", apierrors.NewTooManyRequests("slow down", 1), true},
		{"internal error", apierrors.NewInternalError(stderrors.New("boom")), true},
		{"net error", timeoutNetError{}, true},
		{"context deadline", context.DeadlineExceeded, true},
		{"unauthorized", apierrors.NewUnauthorized("x"), false},
		{"forbidden", apierrors.NewForbidden(jobs, "x", stderrors.New("no grant")), false},
		{"not found", apierrors.NewNotFound(jobs, "x"), false},
		{"plain error", stderrors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isJobLookupTransient(tc.err); got != tc.want {
				t.Fatalf("isJobLookupTransient(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestWrapJobLookupErrorTypesTransient(t *testing.T) {
	ctx := context.Background()
	err := wrapJobLookupError(
		ctx,
		apierrors.NewServiceUnavailable("apiserver not ready"),
		"claude",
		libk8s.Namespace("dev"),
		"check active job",
	)

	var transient *jobLookupTransientError
	if !stderrors.As(err, &transient) {
		t.Fatalf("expected a *jobLookupTransientError, got %T: %v", err, err)
	}
	if isJobLookupDenied(err) {
		t.Fatalf("a transient error must not classify as a denial")
	}
}

func TestWrapJobLookupErrorKeepsNotFoundPlain(t *testing.T) {
	ctx := context.Background()
	err := wrapJobLookupError(
		ctx,
		apierrors.NewNotFound(schema.GroupResource{Resource: "jobs"}, "x"),
		"claude",
		libk8s.Namespace("dev"),
		"check active job",
	)

	var transient *jobLookupTransientError
	if stderrors.As(err, &transient) {
		t.Fatalf("a 404 must not become a transient error")
	}
	var denied *jobLookupDeniedError
	if stderrors.As(err, &denied) {
		t.Fatalf("a 404 must not become a denial")
	}
}

func TestHandleDeferredTickError(t *testing.T) {
	if stop := handleDeferredTickError(context.Background(), stderrors.New("boom")); stop {
		t.Fatalf("a non-context tick error must continue the loop")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if stop := handleDeferredTickError(ctx, stderrors.New("boom")); !stop {
		t.Fatalf("a cancelled context must stop the loop")
	}
}
