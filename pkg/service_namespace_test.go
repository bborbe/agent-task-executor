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

func TestResolveServiceNamespace(t *testing.T) {
	tests := []struct {
		name             string
		executor         libk8s.Namespace
		serviceNamespace string
		expected         libk8s.Namespace
		expectErr        bool
		expectedErrPiece string
	}{
		{
			name:             "empty defaults to executor namespace agent",
			executor:         libk8s.Namespace("agent"),
			serviceNamespace: "",
			expected:         libk8s.Namespace("agent"),
		},
		{
			name:             "empty defaults to executor namespace dev",
			executor:         libk8s.Namespace("dev"),
			serviceNamespace: "",
			expected:         libk8s.Namespace("dev"),
		},
		{
			name:             "explicit different namespace is used",
			executor:         libk8s.Namespace("dev"),
			serviceNamespace: "agent-sandbox",
			expected:         libk8s.Namespace("agent-sandbox"),
		},
		{
			name:             "namespace equal to executor namespace is rejected",
			executor:         libk8s.Namespace("agent"),
			serviceNamespace: "agent",
			expectErr:        true,
			expectedErrPiece: "may not equal the executor namespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolved, err := pkg.ResolveServiceNamespace(
				context.Background(),
				tt.executor,
				tt.serviceNamespace,
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
