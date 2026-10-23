// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"strings"
	"testing"

	libk8s "github.com/bborbe/k8s"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

func TestJobNamespaceHelper(t *testing.T) {
	ctx := context.Background()
	h := &taskEventHandler{namespace: libk8s.Namespace("dev")}

	t.Run("nil config uses executor namespace", func(t *testing.T) {
		got, err := h.jobNamespace(ctx, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != libk8s.Namespace("dev") {
			t.Fatalf("expected dev, got %q", got)
		}
	})

	t.Run("empty job namespace uses executor namespace", func(t *testing.T) {
		got, err := h.jobNamespace(ctx, &pkg.AgentConfiguration{JobNamespace: ""})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != libk8s.Namespace("dev") {
			t.Fatalf("expected dev, got %q", got)
		}
	})

	t.Run("explicit job namespace is used", func(t *testing.T) {
		got, err := h.jobNamespace(
			ctx,
			&pkg.AgentConfiguration{JobNamespace: "dev-agents-sandbox"},
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != libk8s.Namespace("dev-agents-sandbox") {
			t.Fatalf("expected dev-agents-sandbox, got %q", got)
		}
	})

	t.Run("job namespace equal to executor namespace is rejected", func(t *testing.T) {
		_, err := h.jobNamespace(ctx, &pkg.AgentConfiguration{JobNamespace: "dev"})
		if err == nil {
			t.Fatal("expected error when JobNamespace equals executor namespace")
		}
		if !strings.Contains(err.Error(), "may not equal the executor namespace") {
			t.Fatalf("unexpected error message: %v", err)
		}
	})
}
