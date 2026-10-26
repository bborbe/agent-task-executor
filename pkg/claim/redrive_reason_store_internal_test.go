// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"testing"
	"time"

	libtime "github.com/bborbe/time"
)

// TestParseRedriveReasonRefParsesSixSegments pins the key shape. The streak
// parser is frozen at five segments; the reason ref deliberately lives in its
// own prefix at six, so both must be parsed exactly.
func TestParseRedriveReasonRefParsesSixSegments(t *testing.T) {
	taskID, phase, reason, n, ok := parseRedriveReasonRef(
		"refs/claims/redrive-reason/tid-1/execution/pod_oom_killed/2",
	)
	if !ok {
		t.Fatal("expected the six-segment key to parse")
	}
	if taskID != "tid-1" || phase != "execution" || reason != "pod_oom_killed" || n != 2 {
		t.Fatalf("got (%q, %q, %q, %d); want (tid-1, execution, pod_oom_killed, 2)",
			taskID, phase, reason, n)
	}
}

// TestParseRedriveReasonRefRejectsOtherShapes keeps the parser total: a
// five-segment streak ref, a seven-segment key, a foreign prefix and a
// non-positive number all fail.
func TestParseRedriveReasonRefRejectsOtherShapes(t *testing.T) {
	cases := []string{
		"refs/claims/redrive/T/execution/1",
		"refs/claims/redrive-reason/T/execution/pod_oom_killed/1/extra",
		"refs/claims/redrive/T/execution/pod_oom_killed/1",
		"refs/claims/redrive-reason/T/execution/pod_oom_killed/0",
		"refs/claims/redrive-reason/T/execution/pod_oom_killed/x",
		"refs/claims/redrive-reason/T/execution/pod_oom_killed",
	}
	for _, ref := range cases {
		if _, _, _, _, ok := parseRedriveReasonRef(ref); ok {
			t.Fatalf("expected %q not to parse", ref)
		}
	}
}

// reasonRefRecordingClient records the Put it saw and answers List with a fixed
// ref set, so an internal test can inspect the key and sentinel without the
// external fake git-rest server.
type reasonRefRecordingClient struct {
	refs       []ClaimRef
	listCalls  int
	putCalls   int
	putKey     string
	putCluster string
	putTaskID  string
	putLease   libtime.DateTime
}

func (c *reasonRefRecordingClient) List(context.Context, string) ([]ClaimRef, error) {
	c.listCalls++
	return c.refs, nil
}

func (c *reasonRefRecordingClient) Put(
	_ context.Context,
	key string,
	_ string,
	cluster string,
	taskID string,
	leaseUntil libtime.DateTime,
) (string, error) {
	c.putCalls++
	c.putKey = key
	c.putCluster = cluster
	c.putTaskID = taskID
	c.putLease = leaseUntil
	return "sha", nil
}

func (c *reasonRefRecordingClient) Delete(context.Context, string, string) error { return nil }

// TestRedriveReasonStoreRecordUsesTheStreakSentinel pins the load-bearing
// detail: an older binary's lease-manager recovery is blind to these refs
// because they carry the RedriveStreakCluster sentinel, not because of the
// prefix. A new sentinel here would make them visible to that recovery.
func TestRedriveReasonStoreRecordUsesTheStreakSentinel(t *testing.T) {
	client := &reasonRefRecordingClient{}
	store := NewRedriveReasonStore(
		client,
		libtime.NewCurrentDateTime(),
	)

	if err := store.Record(context.Background(), "T", "execution", ""); err != nil {
		t.Fatalf("record: %v", err)
	}
	if client.putCluster != RedriveStreakCluster {
		t.Fatalf("cluster = %q; want %q", client.putCluster, RedriveStreakCluster)
	}
	if client.putTaskID != "T" {
		t.Fatalf("task id = %q; want T", client.putTaskID)
	}
	if client.putKey != "claims/redrive-reason/T/execution/unknown/1" {
		t.Fatalf("key = %q; want an empty reason recorded as unknown", client.putKey)
	}
}

// TestRedriveReasonStoreRecordRejectsUnsafeComponentsBeforeAnyRequest pins that
// a reason carrying a space or a parenthesis never reaches the network. The
// formatted backoffFailureReason form is normalised by the caller precisely
// because such a value would otherwise be an invalid key segment.
func TestRedriveReasonStoreRecordRejectsUnsafeComponentsBeforeAnyRequest(t *testing.T) {
	for _, reason := range []string{
		"pod oom",
		"pod_oom_killed (OOMKilled/137)",
		"a/b",
	} {
		client := &reasonRefRecordingClient{}
		store := NewRedriveReasonStore(client, libtime.NewCurrentDateTime())
		if err := store.Record(context.Background(), "T", "execution", reason); err == nil {
			t.Fatalf("expected %q to be rejected", reason)
		}
		if client.listCalls != 0 || client.putCalls != 0 {
			t.Fatalf("reason %q made %d list / %d put calls; want none",
				reason, client.listCalls, client.putCalls)
		}
	}
}

// TestRedriveReasonStoreUsesInjectedClock pins that lease_until comes from the
// injected clock, not the wall clock.
func TestRedriveReasonStoreUsesInjectedClock(t *testing.T) {
	fixed := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	client := &reasonRefRecordingClient{}
	store := NewRedriveReasonStore(client, &fixedReasonClock{now: fixed})
	if err := store.Record(context.Background(), "T", "execution", "pod_oom_killed"); err != nil {
		t.Fatalf("record: %v", err)
	}
	if client.putKey != "claims/redrive-reason/T/execution/pod_oom_killed/1" {
		t.Fatalf("key = %q", client.putKey)
	}
	want := libtime.DateTime(fixed).Add(RedriveStreakLeaseDuration)
	if client.putLease != want {
		t.Fatalf("lease_until = %v; want %v", client.putLease, want)
	}
}

// fixedReasonClock is a CurrentDateTimeGetter pinned to one instant.
type fixedReasonClock struct {
	now time.Time
}

func (c *fixedReasonClock) Now() libtime.DateTime { return libtime.DateTime(c.now) }
