// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	"strings"
	"testing"

	libk8s "github.com/bborbe/k8s"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

func TestResolveJobNamespace(t *testing.T) {
	tests := []struct {
		name             string
		executor         libk8s.Namespace
		jobNamespace     string
		expected         libk8s.Namespace
		expectErr        bool
		expectedErrPiece string
	}{
		{
			name:         "empty defaults to executor namespace in dev",
			executor:     libk8s.Namespace("dev"),
			jobNamespace: "",
			expected:     libk8s.Namespace("dev"),
		},
		{
			name:         "empty defaults to executor namespace in prod",
			executor:     libk8s.Namespace("prod"),
			jobNamespace: "",
			expected:     libk8s.Namespace("prod"),
		},
		{
			name:         "explicit different namespace is used",
			executor:     libk8s.Namespace("dev"),
			jobNamespace: "dev-agents-sandbox",
			expected:     libk8s.Namespace("dev-agents-sandbox"),
		},
		{
			name:             "namespace equal to executor namespace is rejected",
			executor:         libk8s.Namespace("dev"),
			jobNamespace:     "dev",
			expectErr:        true,
			expectedErrPiece: "may not equal the executor namespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolved, err := pkg.ResolveJobNamespace(
				context.Background(),
				tt.executor,
				tt.jobNamespace,
			)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.expectedErrPiece) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.expectedErrPiece)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resolved != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, resolved)
			}
		})
	}
}
