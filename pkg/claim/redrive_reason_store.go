// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
)

const (
	// RedriveReasonPrefix is the key prefix of every re-drive reason ref.
	//
	// It is a separate namespace from the frozen RedriveStreakPrefix on purpose:
	// parseRedriveStreakRef is exactly five segments and refs written by the
	// deployed build must keep parsing. A sixth segment on the streak key would
	// have broken every one of them.
	RedriveReasonPrefix = "claims/redrive-reason/"
	// RedriveReasonUnknown is the reason recorded when a failure reason cannot be
	// parsed into the closed ZombieReason vocabulary. It is never an empty key
	// segment.
	RedriveReasonUnknown = "unknown"
)

//counterfeiter:generate -o ../../mocks/redrive_reason_store.go --fake-name FakeRedriveReasonStore . RedriveReasonStore

// RedriveReasonStore persists one failure reason per spawn attempt of a task at
// a phase, so the cap can tell a repeated broken signature from a transient
// failure instead of treating every three-times-failed record the same way.
//
// It shares the claim ref store with RedriveStreakStore and reuses the
// RedriveStreakCluster sentinel. That sentinel, not the prefix, is what makes
// these refs invisible to an older binary: its lease-manager recovery skips
// every ref carrying it.
type RedriveReasonStore interface {
	// Record appends one failure reason for taskID at phase. An empty reason is
	// recorded as RedriveReasonUnknown. Recording at a new phase deletes the
	// task's refs at every other phase, exactly as the streak store does.
	Record(ctx context.Context, taskID string, phase string, reason string) error
	// Reasons returns one element per ref at (taskID, phase), ordered by the
	// numeric suffix ascending. The order is load-bearing: the classifier reads
	// only the most recent count entries, so a reason left behind by an earlier
	// streak cannot skew the class.
	Reasons(ctx context.Context, taskID string, phase string) ([]string, error)
	// Snapshot returns every parsed reason ref as taskID → phase component →
	// reasons, from ONE list call over the whole reason prefix, each slice
	// ordered exactly as Reasons returns it. The reconcile pass feeds the same
	// classifier, so the two must agree on order.
	Snapshot(ctx context.Context) (map[string]map[string][]string, error)
	// Clear removes every reason ref of taskID, at every phase.
	Clear(ctx context.Context, taskID string) error
}

// NewRedriveReasonStore creates a RedriveReasonStore over refClient. The
// injected clock supplies lease_until; no wall clock is read.
func NewRedriveReasonStore(
	refClient ClaimRefClient,
	currentDateTime libtime.CurrentDateTimeGetter,
) RedriveReasonStore {
	return &redriveReasonStore{refClient: refClient, currentDateTime: currentDateTime}
}

type redriveReasonStore struct {
	refClient       ClaimRefClient
	currentDateTime libtime.CurrentDateTimeGetter
}

// redriveReasonEntry is one parsed ref's numeric position and reason, used only
// to sort a set of refs chronologically before dropping the numbers.
type redriveReasonEntry struct {
	n      int
	reason string
}

// parseRedriveReasonRef parses a full reason ref
// ("refs/claims/redrive-reason/<taskid>/<phase>/<reason>/<n>"). ok is false for
// any key that is not exactly six segments under the reason prefix, or whose
// last segment is not an integer >= 1.
func parseRedriveReasonRef(
	ref string,
) (taskID string, phase string, reason string, n int, ok bool) {
	key := strings.TrimPrefix(ref, "refs/")
	parts := strings.Split(key, "/")
	if len(parts) != 6 {
		return "", "", "", 0, false
	}
	if parts[0] != "claims" || parts[1] != "redrive-reason" {
		return "", "", "", 0, false
	}
	parsed, err := strconv.Atoi(parts[5])
	if err != nil || parsed < 1 {
		return "", "", "", 0, false
	}
	return parts[2], parts[3], parts[4], parsed, true
}

// Record appends one failure reason for taskID at phase, then deletes the
// task's refs at every other phase.
func (s *redriveReasonStore) Record(
	ctx context.Context,
	taskID string,
	phase string,
	reason string,
) error {
	component := RedriveStreakPhaseComponent(phase)
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return err
	}
	if err := validateClaimComponent(ctx, "phase", component); err != nil {
		return err
	}
	if reason == "" {
		reason = RedriveReasonUnknown
	}
	if err := validateClaimComponent(ctx, "reason", reason); err != nil {
		return err
	}
	return recordRedriveRef(
		ctx,
		s.refClient,
		s.currentDateTime,
		RedriveReasonPrefix,
		"redrive_reason_write_failed",
		"reason",
		taskID,
		component,
		parseRedriveReasonRef,
		func(next int) string {
			return RedriveReasonPrefix + taskID + "/" + component + "/" + reason + "/" +
				strconv.Itoa(next)
		},
	)
}

// Reasons returns one element per ref at (taskID, phase), ordered by the
// numeric suffix ascending.
func (s *redriveReasonStore) Reasons(
	ctx context.Context,
	taskID string,
	phase string,
) ([]string, error) {
	component := RedriveStreakPhaseComponent(phase)
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return nil, err
	}
	if err := validateClaimComponent(ctx, "phase", component); err != nil {
		return nil, err
	}
	refs, err := redriveListForTask(ctx, s.refClient, RedriveReasonPrefix, taskID)
	if err != nil {
		return nil, err
	}
	entries := make([]redriveReasonEntry, 0, len(refs))
	for _, ref := range refs {
		refTaskID, refPhase, reason, n, ok := parseRedriveReasonRef(ref.Ref)
		if !ok || refTaskID != taskID || refPhase != component {
			continue
		}
		entries = append(entries, redriveReasonEntry{n: n, reason: reason})
	}
	return sortedRedriveReasons(entries), nil
}

// Snapshot returns every parsed reason ref under the reason prefix, grouped per
// task and phase component, from one list call. Each slice is ordered by the
// numeric suffix ascending exactly as Reasons returns it.
func (s *redriveReasonStore) Snapshot(
	ctx context.Context,
) (map[string]map[string][]string, error) {
	refs, err := s.refClient.List(ctx, RedriveReasonPrefix)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "list all redrive reason refs")
	}
	byTask := make(map[string]map[string][]redriveReasonEntry, len(refs))
	// No ctx.Done() check in this loop, deliberately and for the same reason the
	// streak snapshot has none: refs is already in memory from the one list call
	// above, and folding it is pure CPU over a bounded slice.
	for _, ref := range refs {
		taskID, phase, reason, n, ok := parseRedriveReasonRef(ref.Ref)
		if !ok {
			continue
		}
		phases, found := byTask[taskID]
		if !found {
			phases = make(map[string][]redriveReasonEntry, 1)
			byTask[taskID] = phases
		}
		phases[phase] = append(phases[phase], redriveReasonEntry{n: n, reason: reason})
	}
	snapshot := make(map[string]map[string][]string, len(byTask))
	for taskID, phases := range byTask {
		out := make(map[string][]string, len(phases))
		for phase, entries := range phases {
			out[phase] = sortedRedriveReasons(entries)
		}
		snapshot[taskID] = out
	}
	return snapshot, nil
}

// Clear removes every reason ref of taskID, at every phase.
func (s *redriveReasonStore) Clear(ctx context.Context, taskID string) error {
	return clearRedriveRefs(
		ctx,
		s.refClient,
		RedriveReasonPrefix,
		"redrive_reason_write_failed",
		taskID,
		parseRedriveReasonRef,
	)
}

// sortedRedriveReasons orders entries by their numeric suffix and drops the
// numbers, so the newest attempt is last.
func sortedRedriveReasons(entries []redriveReasonEntry) []string {
	sort.Slice(entries, func(i, j int) bool { return entries[i].n < entries[j].n })
	reasons := make([]string, len(entries))
	for i, entry := range entries {
		reasons[i] = entry.reason
	}
	return reasons
}
