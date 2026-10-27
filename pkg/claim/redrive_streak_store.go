// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	"strconv"
	"strings"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
)

const (
	// RedriveStreakCluster is the sentinel cluster field on every streak ref.
	// git-rest refuses an empty cluster, and this value must never equal a real
	// executor cluster name: the lease manager's recovery and fence compare on
	// the cluster field.
	RedriveStreakCluster = "redrive-streak"
	// RedriveStreakPrefix is the frozen key prefix of every streak ref.
	// Changing it strands every existing ref.
	RedriveStreakPrefix = "claims/redrive/"
	// RedriveStreakPhaseNone is the phase component used when a task carries
	// no phase.
	RedriveStreakPhaseNone = "none"
	// RedriveStreakLeaseDuration is the window a streak ref counts toward the
	// cap. A ref whose lease_until has passed is ignored by Count and by
	// Snapshot, and Record prunes it, so the cap still refuses a fourth spawn
	// inside one window while a task capped by a past outage is released once
	// that window has passed — with no operator action.
	//
	// Nothing renews a ref, so expiry is the only release path the executor
	// controls. That is why the read side must honour it: a streak ref that
	// never expires makes the cap permanent, and a permanent cap turns a
	// systemic outage into a fleet that cannot recover after the cause is
	// fixed. The window is what keeps the cap a bound rather than an outage.
	RedriveStreakLeaseDuration = 6 * libtime.Hour
	// redriveStreakMaxCreateAttempts bounds the create retries on a 409.
	redriveStreakMaxCreateAttempts = 3
)

//counterfeiter:generate -o ../../mocks/redrive_streak_store.go --fake-name FakeRedriveStreakStore . RedriveStreakStore

// RedriveStreakStore persists the consecutive-spawn streak of a task per phase
// in the shared git-rest claim store, so every cluster and every restarted pod
// reads the same count.
type RedriveStreakStore interface {
	// Count returns how many streak refs taskID has at phase. Refs at other
	// phases, refs whose key does not parse, and refs whose lease_until has
	// passed are not counted.
	Count(ctx context.Context, taskID string, phase string) (int, error)
	// Snapshot returns every parsed streak ref as taskID → phase component →
	// count, from ONE list call over the whole streak prefix. The reconcile pass
	// reads it once and answers every candidate's cap check from the result;
	// Count issues one list per task, which is what put a pass over a large
	// candidate set over its budget (see handler/redrive_streak_cache.go).
	// Keys are normalized with RedriveStreakPhaseComponent.
	//
	// Expired refs are skipped exactly as Count skips them. That is load-bearing
	// rather than tidy: the pass answers the cap check from this map, so a
	// snapshot counting expired refs would keep every released task capped on
	// the path that actually re-drives it.
	Snapshot(ctx context.Context) (map[string]map[string]int, error)
	// Record appends one spawn of taskID at phase, then deletes the task's
	// refs at every other phase. Expired refs of taskID are pruned first, so a
	// released streak restarts at 1. A 409 create is retried on the next free
	// number at most redriveStreakMaxCreateAttempts times.
	Record(ctx context.Context, taskID string, phase string) error
	// Clear removes every parsed streak ref of taskID, at every phase.
	Clear(ctx context.Context, taskID string) error
}

// NewRedriveStreakStore creates a RedriveStreakStore over refClient. The
// injected clock supplies lease_until; no wall clock is read.
func NewRedriveStreakStore(
	refClient ClaimRefClient,
	currentDateTime libtime.CurrentDateTimeGetter,
) RedriveStreakStore {
	return &redriveStreakStore{refClient: refClient, currentDateTime: currentDateTime}
}

type redriveStreakStore struct {
	refClient       ClaimRefClient
	currentDateTime libtime.CurrentDateTimeGetter
}

// RedriveStreakPhaseComponent maps an empty phase to the frozen "none"
// component; every other value is used verbatim. Exported so a caller reading a
// Snapshot keys it exactly the way Count does.
func RedriveStreakPhaseComponent(phase string) string {
	if phase == "" {
		return RedriveStreakPhaseNone
	}
	return phase
}

// parseRedriveStreakRef parses a full ref ("refs/claims/redrive/<taskid>/<phase>/<n>").
// ok is false for any key that is not exactly five segments under the frozen
// prefix, or whose last segment is not an integer >= 1.
func parseRedriveStreakRef(ref string) (taskID string, phase string, n int, ok bool) {
	key := strings.TrimPrefix(ref, "refs/")
	parts := strings.Split(key, "/")
	if len(parts) != 5 {
		return "", "", 0, false
	}
	if parts[0] != "claims" || parts[1] != "redrive" {
		return "", "", 0, false
	}
	parsed, err := strconv.Atoi(parts[4])
	if err != nil || parsed < 1 {
		return "", "", 0, false
	}
	return parts[2], parts[3], parsed, true
}

// parseRedriveStreakRefForWalker adapts the frozen four-value parser to the
// shared ref walker's shape. A streak ref carries no detail segment.
func parseRedriveStreakRefForWalker(ref string) (taskID, phase, detail string, n int, ok bool) {
	taskID, phase, n, ok = parseRedriveStreakRef(ref)
	return taskID, phase, "", n, ok
}

// redriveStreakRefExpired reports whether ref's lease has passed at now, i.e.
// whether the spawn it counts happened longer than RedriveStreakLeaseDuration
// ago and no longer bears on the cap.
//
// An absent or unparseable lease_until counts as NOT expired. The cap is the
// behaviour that must never weaken, and git-rest writes the field on every
// create, so an unreadable value is a store anomaly rather than evidence that
// the streak is over. Expiring on a parse failure would release a genuinely
// failing task on a malformed ref, which is the exact failure this window must
// not introduce.
func redriveStreakRefExpired(ctx context.Context, now libtime.DateTime, ref ClaimRef) bool {
	if ref.LeaseUntil == "" {
		return false
	}
	leaseUntil, err := libtime.ParseDateTime(ctx, ref.LeaseUntil)
	if err != nil {
		glog.Warningf(
			"event=redrive_streak_lease_unparsed ref=%s lease_until=%q error=%v",
			ref.Ref,
			ref.LeaseUntil,
			err,
		)
		return false
	}
	return !now.Time().Before(leaseUntil.Time())
}

// Count returns how many streak refs taskID has at phase.
func (s *redriveStreakStore) Count(
	ctx context.Context,
	taskID string,
	phase string,
) (int, error) {
	component := RedriveStreakPhaseComponent(phase)
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return 0, err
	}
	if err := validateClaimComponent(ctx, "phase", component); err != nil {
		return 0, err
	}
	refs, err := redriveListForTask(ctx, s.refClient, RedriveStreakPrefix, taskID)
	if err != nil {
		return 0, err
	}
	count := 0
	now := s.currentDateTime.Now()
	for _, ref := range refs {
		refTaskID, refPhase, _, ok := parseRedriveStreakRef(ref.Ref)
		if !ok || refTaskID != taskID || refPhase != component {
			continue
		}
		if redriveStreakRefExpired(ctx, now, ref) {
			continue
		}
		count++
	}
	return count, nil
}

// Snapshot returns every parsed streak ref under the streak prefix, counted per
// task and phase component, from one list call.
//
// The reconcile pass needs the streak of every candidate it is about to drive.
// Count answers that with one list per task, so a pass with N candidates made N
// git-rest round-trips and spent its whole budget on them — measured on dev
// 2026-10-24: trading aborted every pass at 45 s having driven 16 of its 16
// candidates, openclaw every second pass at 30, while a pass with 4 candidates
// completed in a second. One prefix list answers the same question for all of
// them, which is the same trade ListActiveJobs makes for Jobs.
//
// Refs whose key does not parse are skipped, exactly as Count skips them.
func (s *redriveStreakStore) Snapshot(ctx context.Context) (map[string]map[string]int, error) {
	refs, err := s.refClient.List(ctx, RedriveStreakPrefix)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "list all redrive streak refs")
	}
	counts := make(map[string]map[string]int, len(refs))
	// No ctx.Done() check in this loop, deliberately and for the same reason
	// Count has none: refs is already in memory, returned by the one list call
	// above, and folding it is pure CPU over a bounded slice. Cancellation is
	// observed at the call that can actually block.
	//
	// Expired refs are skipped here for the same reason Count skips them, and
	// the filter is NOT optional on this path: the reconcile pass answers every
	// candidate's cap check from this snapshot (handler/redrive_streak_cache.go),
	// so a Snapshot that counted expired refs would leave the pass reporting
	// every capped task as capped forever and the release would never reach the
	// one path that actually re-drives a task.
	now := s.currentDateTime.Now()
	for _, ref := range refs {
		taskID, phase, _, ok := parseRedriveStreakRef(ref.Ref)
		if !ok {
			continue
		}
		if redriveStreakRefExpired(ctx, now, ref) {
			continue
		}
		phases, found := counts[taskID]
		if !found {
			phases = make(map[string]int, 1)
			counts[taskID] = phases
		}
		phases[phase]++
	}
	return counts, nil
}

// Record appends one spawn of taskID at phase, then deletes the task's refs at
// every other phase.
func (s *redriveStreakStore) Record(
	ctx context.Context,
	taskID string,
	phase string,
) error {
	component := RedriveStreakPhaseComponent(phase)
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return err
	}
	if err := validateClaimComponent(ctx, "phase", component); err != nil {
		return err
	}
	s.pruneExpired(ctx, taskID)
	return recordRedriveRef(
		ctx,
		s.refClient,
		s.currentDateTime,
		RedriveStreakPrefix,
		"redrive_streak_write_failed",
		"streak",
		taskID,
		component,
		parseRedriveStreakRefForWalker,
		func(next int) string {
			return RedriveStreakPrefix + taskID + "/" + component + "/" + strconv.Itoa(next)
		},
	)
}

// Clear removes every parsed streak ref of taskID, at every phase.
func (s *redriveStreakStore) Clear(ctx context.Context, taskID string) error {
	return clearRedriveRefs(
		ctx,
		s.refClient,
		RedriveStreakPrefix,
		"redrive_streak_write_failed",
		taskID,
		parseRedriveStreakRefForWalker,
	)
}

// pruneExpired deletes taskID's streak refs whose lease has passed, so the
// number sequence stays bounded and a task released by the window restarts at
// 1 rather than continuing a dead sequence.
//
// Scoped to this store's prefix AND to this one task. The reason store shares
// the redrive ref space (claims/redrive-reason/), and a prune that walked the
// streak prefix instead of one task's refs would delete reasons the classifier
// still needs. parseRedriveStreakRef already rejects a reason key on segment
// count, so the two cannot be confused; the task scoping is what keeps this
// from being an unbounded sweep on a path that runs after every spawn.
//
// Deletion is best-effort: a failure is logged and never fails the Record,
// because the spawn it is about to record has already happened.
func (s *redriveStreakStore) pruneExpired(ctx context.Context, taskID string) {
	refs, err := redriveListForTask(ctx, s.refClient, RedriveStreakPrefix, taskID)
	if err != nil {
		glog.Warningf(
			"event=redrive_streak_prune_failed task=%s error=%v", taskID, err,
		)
		return
	}
	now := s.currentDateTime.Now()
	for _, ref := range refs {
		refTaskID, _, _, ok := parseRedriveStreakRef(ref.Ref)
		if !ok || refTaskID != taskID {
			continue
		}
		if !redriveStreakRefExpired(ctx, now, ref) {
			continue
		}
		redriveDeleteRef(ctx, s.refClient, "redrive_streak_prune_failed", taskID, ref)
	}
}
