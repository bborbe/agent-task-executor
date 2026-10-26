// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"testing"

	libk8s "github.com/bborbe/k8s"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// The spawn counter's label must carry the RESOLVED namespace, not the Config's
// raw field. An empty jobNamespace means "the executor's own namespace", so
// labelling it "" would make a control-namespace spawn indistinguishable from a
// sandboxed one — and the only question the label exists to answer is which
// namespace the Job landed in.
func TestSpawnedNamespaceResolvesEmptyToExecutorNamespace(t *testing.T) {
	h := &taskEventHandler{namespace: libk8s.Namespace("agent")}

	got := h.spawnedNamespace(context.Background(), &pkg.AgentConfiguration{
		Assignee:     "pr-reviewer-agent",
		JobNamespace: "",
	})

	if got != "agent" {
		t.Fatalf("expected the executor namespace, got %q", got)
	}
}

func TestSpawnedNamespaceCarriesTheSandbox(t *testing.T) {
	h := &taskEventHandler{namespace: libk8s.Namespace("agent")}

	got := h.spawnedNamespace(context.Background(), &pkg.AgentConfiguration{
		Assignee:     "pr-reviewer-agent",
		JobNamespace: "development-sandbox",
	})

	if got != "development-sandbox" {
		t.Fatalf("expected the sandbox namespace, got %q", got)
	}
}

// A value that cannot be resolved must not collapse into the executor namespace:
// that would read as a control-namespace spawn and silently falsify the query.
func TestSpawnedNamespaceReportsUnresolvedRatherThanGuessing(t *testing.T) {
	h := &taskEventHandler{namespace: libk8s.Namespace("agent")}

	got := h.spawnedNamespace(context.Background(), &pkg.AgentConfiguration{
		Assignee:     "pr-reviewer-agent",
		JobNamespace: "agent",
	})

	if got != "unresolved" {
		t.Fatalf("expected the unresolved sentinel, got %q", got)
	}
}
