// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	"testing"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
)

func TestConvertCarriesServiceNamespace(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name             string
		serviceNamespace string
		expected         string
	}{
		{
			name:             "explicit serviceNamespace survives the resolver",
			serviceNamespace: "agent-sandbox",
			expected:         "agent-sandbox",
		},
		{
			name:             "unset serviceNamespace stays empty",
			serviceNamespace: "",
			expected:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &fakeProvider{
				items: []agentv1.Config{
					{
						Spec: agentv1.ConfigSpec{
							Assignee:         "claude",
							Image:            "foo/bar",
							Heartbeat:        "30m",
							ServiceNamespace: tt.serviceNamespace,
						},
					},
				},
			}

			resolved, err := pkg.NewConfigResolver(provider, "dev", "personal").
				Resolve(ctx, "claude")
			if err != nil {
				t.Fatalf("Resolve failed: %v", err)
			}
			if resolved.ServiceNamespace != tt.expected {
				t.Fatalf(
					"expected ServiceNamespace %q, got %q",
					tt.expected,
					resolved.ServiceNamespace,
				)
			}
		})
	}
}
