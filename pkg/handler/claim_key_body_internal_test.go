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

// TestAcquireSpawnClaimKeysPRReviewsPerPullRequest pins that a pr-review task
// is keyed per pull request and takes no shared per-repository ref, so two
// different pull requests of one repository both claim. Sharing the
// per-repository key here is the defect this test exists to prevent: the first
// holder refused every sibling review at takeover with a bare lease conflict
// for as long as it renewed.
func TestAcquireSpawnClaimKeysPRReviewsPerPullRequest(t *testing.T) {
	ctx := context.Background()
	client := &casClaimRefClient{refs: map[string]claim.ClaimRef{}}
	h := newGateHandler(pkg.ClaimModeOn, client)

	body658 := "title\n\nhttps://github.com/bborbe/nuke/pull/658\n"
	body662 := "title\n\nhttps://github.com/bborbe/nuke/pull/662\n"
	if !h.acquireSpawnClaim(ctx, prReviewClaimTask("tid-658", body658), redispatchConfig()) {
		t.Fatal("expected the first pr-review to claim")
	}

	manager, err := h.ClaimLeaseManager(ctx)
	if err != nil {
		t.Fatalf("lease manager: %v", err)
	}
	held := manager.HeldClaims()
	if len(held) != 1 {
		t.Fatalf("expected one held claim (no pair), got %d: %+v", len(held), held)
	}
	if held[0].Key != "claims/pr/bborbe/nuke/658" || held[0].TaskID != "tid-658" {
		t.Errorf("held claim = %+v, want pr/658 owned by tid-658", held[0])
	}

	putsBefore := client.puts
	if !h.acquireSpawnClaim(ctx, prReviewClaimTask("tid-662", body662), redispatchConfig()) {
		t.Fatal("expected a different pull request of the same repo to claim")
	}
	if client.puts != putsBefore+1 {
		t.Errorf("expected one Put for the second claim, got %d", client.puts-putsBefore)
	}

	keys := map[string]string{}
	for _, heldClaim := range manager.HeldClaims() {
		keys[heldClaim.Key] = heldClaim.TaskID
	}
	if len(keys) != 2 {
		t.Fatalf("expected both per-PR claims held, got %+v", keys)
	}
	if keys["claims/pr/bborbe/nuke/658"] != "tid-658" {
		t.Errorf("pr/658 owner = %q, want tid-658", keys["claims/pr/bborbe/nuke/658"])
	}
	if keys["claims/pr/bborbe/nuke/662"] != "tid-662" {
		t.Errorf("pr/662 owner = %q, want tid-662", keys["claims/pr/bborbe/nuke/662"])
	}
	if _, shared := keys["claims/repo/bborbe/nuke"]; shared {
		t.Error("a pr-review must not hold the shared per-repository key")
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
	if len(client.putCalls) != 1 {
		t.Fatalf("expected one Put (per-PR only), got %d", len(client.putCalls))
	}
	if client.putCalls[0].key != "claims/pr/bborbe/nuke/658" {
		t.Errorf(
			"claim key = %q, want %q",
			client.putCalls[0].key,
			"claims/pr/bborbe/nuke/658",
		)
	}
}

// TestAcquireSpawnClaimKeysURLlessPRReviewsShareNoKey pins that two URL-less
// pr-reviews of one repository claim against one shared store without
// contending: each takes its own task-id key and neither takes the
// per-repository key. Keying them by repository is the defect this covers —
// the first holder refused every sibling for as long as it renewed.
func TestAcquireSpawnClaimKeysURLlessPRReviewsShareNoKey(t *testing.T) {
	ctx := context.Background()
	client := &casClaimRefClient{refs: map[string]claim.ClaimRef{}}
	h := newGateHandler(pkg.ClaimModeOn, client)

	body := "no pull request url here"
	if !h.acquireSpawnClaim(ctx, prReviewClaimTask("tid-a", body), redispatchConfig()) {
		t.Fatal("expected the first URL-less pr-review to claim")
	}
	if !h.acquireSpawnClaim(ctx, prReviewClaimTask("tid-b", body), redispatchConfig()) {
		t.Fatal("expected a second URL-less pr-review of the same repo to claim")
	}

	manager, err := h.ClaimLeaseManager(ctx)
	if err != nil {
		t.Fatalf("lease manager: %v", err)
	}
	keys := map[string]string{}
	for _, heldClaim := range manager.HeldClaims() {
		keys[heldClaim.Key] = heldClaim.TaskID
	}
	if len(keys) != 2 {
		t.Fatalf("expected two task-id claims held, got %+v", keys)
	}
	if keys["claims/task/tid-a"] != "tid-a" {
		t.Errorf("tid-a claim = %+v, want its own task key", keys)
	}
	if keys["claims/task/tid-b"] != "tid-b" {
		t.Errorf("tid-b claim = %+v, want its own task key", keys)
	}
	if _, shared := keys["claims/repo/bborbe/nuke"]; shared {
		t.Error("a URL-less pr-review must not hold the shared per-repository key")
	}
}

func TestAcquireSpawnClaimFallsBackToTaskID(t *testing.T) {
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
			// A pr-review with no usable URL is keyed by its task id. Keying it
			// by the repository instead made every review of that repository
			// contend for one key.
			if want := "claims/task/" + id; client.putCalls[0].key != want {
				t.Errorf("claim key = %q, want %q", client.putCalls[0].key, want)
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
