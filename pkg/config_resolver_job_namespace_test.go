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

func TestConvertCarriesJobNamespace(t *testing.T) {
	ctx := context.Background()

	provider := &fakeProvider{items: []agentv1.Config{{
		Spec: agentv1.ConfigSpec{
			Assignee:     "claude-personal",
			JobNamespace: "dev-agents-sandbox",
		},
	}}}
	resolver := pkg.NewConfigResolver(provider, "dev", "personal")

	config, err := resolver.Resolve(ctx, "claude")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.JobNamespace != "dev-agents-sandbox" {
		t.Fatalf("expected %q, got %q", "dev-agents-sandbox", config.JobNamespace)
	}

	provider.items[0].Spec.JobNamespace = ""
	config, err = resolver.Resolve(ctx, "claude")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.JobNamespace != "" {
		t.Fatalf("expected empty JobNamespace, got %q", config.JobNamespace)
	}
}
