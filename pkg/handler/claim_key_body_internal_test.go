// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus/testutil"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// captureGlogStderr runs fn with glog's stderr output redirected into a pipe and
// returns everything written. It restores the flag so later tests are not
// affected.
func captureGlogStderr(t *testing.T, fn func()) string {
	t.Helper()
	if err := flag.Set("logtostderr", "true"); err != nil {
		t.Fatalf("set logtostderr: %v", err)
	}
	defer func() {
		if err := flag.Set("logtostderr", "false"); err != nil {
			t.Errorf("reset logtostderr: %v", err)
		}
	}()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create pipe: %v", err)
	}
	os.Stderr = writer

	fn()
	glog.Flush()

	os.Stderr = original
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	return string(out)
}

// casClaimRefClient is an in-memory compare-and-swap ClaimRefClient.
type casClaimRefClient struct {
	mu   sync.Mutex
	refs map[string]claim.ClaimRef
	seq  int
	puts int
}

// Put applies the compare-and-swap and stores the resulting ref.
func (c *casClaimRefClient) Put(
	_ context.Context,
	key string,
	expectedSha string,
	cluster string,
	taskID string,
	leaseUntil libtime.DateTime,
) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts++
	existing, ok := c.refs[key]
	if expectedSha == "" {
		if ok {
			return "", claim.ErrLeaseConflict
		}
	} else if !ok || existing.Sha != expectedSha {
		return "", claim.ErrLeaseConflict
	}
	c.seq++
	sha := fmt.Sprintf("sha-%d", c.seq)
	c.refs[key] = claim.ClaimRef{
		Ref:        "refs/" + key,
		Sha:        sha,
		Cluster:    cluster,
		TaskID:     taskID,
		LeaseUntil: leaseUntil.Time().UTC().Format(time.RFC3339),
	}
	return sha, nil
}

// List returns every stored ref under prefix.
func (c *casClaimRefClient) List(_ context.Context, prefix string) ([]claim.ClaimRef, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	want := "refs/" + prefix
	refs := make([]claim.ClaimRef, 0, len(c.refs))
	for _, ref := range c.refs {
		if ref.Ref == want || strings.HasPrefix(ref.Ref, want+"/") {
			refs = append(refs, ref)
		}
	}
	return refs, nil
}

// Delete removes the ref when the expected sha matches.
func (c *casClaimRefClient) Delete(_ context.Context, key, expectedSha string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, ok := c.refs[key]
	if !ok || existing.Sha != expectedSha {
		return claim.ErrLeaseConflict
	}
	delete(c.refs, key)
	return nil
}

// prReviewClaimTask builds a pr-review task for repo bborbe/nuke.
func prReviewClaimTask(id, body string) lib.Task {
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier(id),
		Frontmatter: lib.TaskFrontmatter{
			"task_type": "pr-review",
			"repo":      "bborbe/nuke",
		},
		Content: lib.TaskContent(body),
	}
}

// TestAcquireSpawnClaimKeysPRReviewsPerPullRequest covers the dual-write era:
// the per-repo key is always primary, so different pull requests in one
// repository serialise on it for as long as both refs are written. The first
// task holds both refs; a second pull request in the same repository is
// refused because the shared per-repo primary is already held.
func TestAcquireSpawnClaimKeysPRReviewsPerPullRequest(t *testing.T) {
	ctx := context.Background()
	client := &casClaimRefClient{refs: map[string]claim.ClaimRef{}}
	h := newGateHandler(pkg.ClaimModeOn, client)

	body658 := "title\n\nhttps://github.com/bborbe/nuke/pull/658\n"
	body662 := "title\n\nhttps://github.com/bborbe/nuke/pull/662\n"
	if !h.acquireSpawnClaim(ctx, prReviewClaimTask("tid-658", body658), redispatchConfig()) {
		t.Fatal("expected the first pr-review to claim the pair")
	}

	manager, err := h.ClaimLeaseManager(ctx)
	if err != nil {
		t.Fatalf("lease manager: %v", err)
	}
	held := manager.HeldClaims()
	if len(held) != 2 {
		t.Fatalf("expected two held claims (the pair), got %d: %+v", len(held), held)
	}
	if held[0].Key != "claims/pr/bborbe-nuke-658" || held[0].TaskID != "tid-658" {
		t.Errorf("first held claim = %+v, want pr/658 owned by tid-658", held[0])
	}
	if held[1].Key != "claims/repo/bborbe-nuke" || held[1].TaskID != "tid-658" {
		t.Errorf("second held claim = %+v, want repo/bborbe-nuke owned by tid-658", held[1])
	}

	putsBefore := client.puts
	before := snapshotGateClaims()
	// The dual write takes the shared per-repo ref for every repo-keyed task, so a
	// different pull request of the same repo is blocked for as long as the dual
	// write exists. That is the accepted cost of the migration (spec 015 Non-goals):
	// it restores the per-repo serialisation that per-PR keying removed, and dropping
	// the dual write in a later release restores the concurrency.
	if h.acquireSpawnClaim(ctx, prReviewClaimTask("tid-662", body662), redispatchConfig()) {
		t.Fatal("expected the shared per-repo primary to block a different pull request")
	}
	expectGateClaimDelta(t, before, snapshotGateClaims(), map[string]float64{"conflict": 2})

	held = manager.HeldClaims()
	if len(held) != 2 {
		t.Fatalf("expected the refusal to leave the pair held, got %d", len(held))
	}
	if held[0].Key != "claims/pr/bborbe-nuke-658" || held[0].TaskID != "tid-658" {
		t.Errorf("held claim after refusal = %+v, want pr/658 owned by tid-658", held[0])
	}
	if held[1].Key != "claims/repo/bborbe-nuke" || held[1].TaskID != "tid-658" {
		t.Errorf("held claim after refusal = %+v, want repo owned by tid-658", held[1])
	}
	if client.puts != putsBefore {
		t.Errorf("expected no Put on the refusal, got %d", client.puts-putsBefore)
	}
}

func TestAcquireSpawnClaimKeysCloneURLPRReviewPerPullRequest(t *testing.T) {
	ctx := context.Background()
	client := &recordingClaimRefClient{}
	h := newGateHandler(pkg.ClaimModeOn, client)

	task := lib.Task{
		TaskIdentifier: "tid-clone-url",
		Frontmatter: lib.TaskFrontmatter{
			"task_type": "pr-review",
			"clone_url": "https://github.com/bborbe/nuke.git",
		},
		Content: "title\n\n\nhttps://github.com/bborbe/nuke/pull/658\n" +
			"\ncopied https://github.com/bborbe/nuke/pull/700\n",
	}
	if !h.acquireSpawnClaim(ctx, task, redispatchConfig()) {
		t.Fatal("expected the clone_url pr-review to claim")
	}
	if len(client.putCalls) != 2 {
		t.Fatalf("expected two Puts (per-repo then per-PR), got %d", len(client.putCalls))
	}
	if client.putCalls[0].key != "claims/repo/bborbe-nuke" {
		t.Errorf(
			"primary claim key = %q, want %q",
			client.putCalls[0].key,
			"claims/repo/bborbe-nuke",
		)
	}
	if client.putCalls[1].key != "claims/pr/bborbe-nuke-658" {
		t.Errorf(
			"secondary claim key = %q, want %q",
			client.putCalls[1].key,
			"claims/pr/bborbe-nuke-658",
		)
	}
}

func TestAcquireSpawnClaimFallsBackToRepoKey(t *testing.T) {
	ctx := context.Background()
	bodies := []string{
		"",
		"https://github.com/other/repo/pull/1",
		"https://github.com/bborbe/nuke/pull/abc",
	}
	for i, body := range bodies {
		body := body
		t.Run(fmt.Sprintf("body-%d", i), func(t *testing.T) {
			id := fmt.Sprintf("tid-fallback-%d", i)
			client := &recordingClaimRefClient{}
			h := newGateHandler(pkg.ClaimModeOn, client)

			var ok bool
			before := testutil.ToFloat64(metrics.ClaimKeyFallbackTotal)
			out := captureGlogStderr(t, func() {
				ok = h.acquireSpawnClaim(ctx, prReviewClaimTask(id, body), redispatchConfig())
			})
			after := testutil.ToFloat64(metrics.ClaimKeyFallbackTotal)

			if !ok {
				t.Fatal("expected the fallback to let the spawn proceed")
			}
			if len(client.putCalls) != 1 {
				t.Fatalf("expected one Put, got %d", len(client.putCalls))
			}
			if client.putCalls[0].key != "claims/repo/bborbe-nuke" {
				t.Errorf(
					"claim key = %q, want %q",
					client.putCalls[0].key,
					"claims/repo/bborbe-nuke",
				)
			}
			if delta := after - before; delta != 1 {
				t.Errorf("fallback counter delta = %v, want 1", delta)
			}
			want := "event=claim_key_fallback task=" + id + " repo=bborbe/nuke"
			if !strings.Contains(out, want) {
				t.Errorf("stderr = %q, want it to contain %q", out, want)
			}
		})
	}
}
