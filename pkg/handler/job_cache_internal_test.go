// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"testing"

	lib "github.com/bborbe/agent"
	libk8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
)

// recordingJobSpawner records the namespace of every ListActiveJobs call.
type recordingJobSpawner struct {
	stubJobSpawner
	namespaces []libk8s.Namespace
}

func (s *recordingJobSpawner) ListActiveJobs(
	_ context.Context,
	namespace libk8s.Namespace,
	_ string,
) (map[lib.TaskIdentifier]bool, error) {
	s.namespaces = append(s.namespaces, namespace)
	return map[lib.TaskIdentifier]bool{}, nil
}

// TestActiveJobsForKeysCacheByNamespace pins the pass cache's key to the
// namespace as well as the assignee. Keyed by assignee alone, the second
// namespace of a pass is served the first namespace's active set: a Job in the
// sandbox then reads as absent, so the concurrency cap never fires and the
// spawn path duplicates work the cache exists to count.
func TestActiveJobsForKeysCacheByNamespace(t *testing.T) {
	spawner := &recordingJobSpawner{}
	h := &taskEventHandler{
		jobSpawner:      spawner,
		currentDateTime: libtime.NewCurrentDateTime(),
	}
	ctx := withJobCache(context.Background())

	for range 2 {
		for _, namespace := range []libk8s.Namespace{"test-ns", "dev-agents-sandbox"} {
			if _, err := h.activeJobsFor(ctx, namespace, "claude"); err != nil {
				t.Fatalf("activeJobsFor(%s): %v", namespace, err)
			}
		}
	}

	if len(spawner.namespaces) != 2 {
		t.Fatalf(
			"ListActiveJobs calls = %d, want 2 (one per namespace, then cached): %v",
			len(spawner.namespaces),
			spawner.namespaces,
		)
	}
	if spawner.namespaces[0] != "test-ns" || spawner.namespaces[1] != "dev-agents-sandbox" {
		t.Fatalf(
			"ListActiveJobs namespaces = %v, want [test-ns dev-agents-sandbox]",
			spawner.namespaces,
		)
	}
}
