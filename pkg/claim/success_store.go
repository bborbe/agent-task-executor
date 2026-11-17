// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	stderrors "errors"
	"strconv"
	"strings"
	"time"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
)

const (
	// SuccessRecordPrefix is the frozen key prefix of every success record.
	// Changing it strands every existing record.
	SuccessRecordPrefix = "claims/success/"
	// SuccessRecordCluster is the sentinel cluster field on every success ref.
	// git-rest refuses an empty cluster, and this value must never equal a real
	// executor cluster name: the lease manager's recovery and fence compare on
	// the cluster field.
	SuccessRecordCluster = "success-record"
)

// SuccessStore persists the latest successful Job of a task per phase in the
// shared git-rest claim store, so a restarted pod — or a peer cluster — reads
// the same record.
//
// It exists because the spawn path's success record is per-pod memory: a fresh
// instance returns "never succeeded" for every task and re-opens the duplicate
// on every deploy. The claim store is the one artefact every cluster and every
// restarted pod shares, so the record lives beside the dispatch claims.
//
// It is deliberately NOT a lease on the dispatch claim. A claim carries no phase
// and is held to block a peer from running the task now; keeping it for the
// retention window would refuse every peer's claim for as long as the record
// lives. The success ref is a separate key that nothing claims against, so a
// peer can take the claim while still reading the success.
//
// Ref shape: `claims/success/<taskid>/<phase>/<unix seconds>`. The completion
// time rides in the key rather than in a body because the git-rest ref API
// carries no free-text payload — the PUT body holds only the compare-and-swap
// sha, the cluster, the task id and the lease.
//counterfeiter:generate -o ../../mocks/success_store.go --fake-name FakeSuccessStore . SuccessStore

type SuccessStore interface {
	// Record stores that a Job for taskID succeeded at phase at completedAt, and
	// leaves that one ref as the task's ONLY record: every other ref of the task,
	// at any phase and any age, is deleted.
	//
	// One record per task is what bounds the store. The key carries the completion
	// second, so a task that succeeds repeatedly — a recurring task over the
	// retention window — would otherwise accumulate one ref per completion, and
	// Snapshot lists the whole prefix on every reconcile pass. Deleting the
	// superseded refs on write also drops expired ones, so the store is bounded by
	// the task set rather than by the number of completions; the read-side expiry
	// filter stays as the belt for refs a failed delete left behind.
	//
	// Recording the same completion twice is not an error — the informer
	// re-delivers a terminal Job every few minutes, and two pods observe one
	// success. The key carries the completion second, so the second write is a
	// conflict that means "already recorded".
	Record(ctx context.Context, taskID string, phase string, completedAt libtime.DateTime) error
	// Lookup returns the completion time recorded for taskID at phase. A record
	// at another phase is not returned: the caller's guard is scoped by phase,
	// and a success at a phase the task has left says nothing about this one.
	Lookup(ctx context.Context, taskID string, phase string) (libtime.DateTime, bool, error)
	// Snapshot returns every live record as taskID → phase component →
	// completion time, from ONE list call over the whole prefix. The reconcile
	// pass answers every candidate's guard from it; Lookup issues one list per
	// task, which is what put a pass over a large candidate set over its budget
	// for the redrive streak. Keys are normalized with RedriveStreakPhaseComponent.
	Snapshot(ctx context.Context) (map[string]map[string]libtime.DateTime, error)
}

// NewSuccessStore creates a SuccessStore over refClient. retention is how long a
// record is kept: it is the caller's bound, because the caller is what knows how
// long the guard may need to read the record — the spawn path's retention covers
// the longest declared recurrence cadence. The injected clock supplies the
// expiry comparison; no wall clock is read.
func NewSuccessStore(
	refClient ClaimRefClient,
	currentDateTime libtime.CurrentDateTimeGetter,
	retention libtime.Duration,
) SuccessStore {
	return &successStore{
		refClient:       refClient,
		currentDateTime: currentDateTime,
		retention:       retention,
	}
}

type successStore struct {
	refClient       ClaimRefClient
	currentDateTime libtime.CurrentDateTimeGetter
	retention       libtime.Duration
}

// parseSuccessRef parses a full ref ("refs/claims/success/<taskid>/<phase>/<unix>").
// ok is false for any key that is not exactly five segments under the frozen
// prefix, or whose last segment is not a positive integer.
func parseSuccessRef(ref string) (taskID string, phase string, at libtime.DateTime, ok bool) {
	key := strings.TrimPrefix(ref, "refs/")
	parts := strings.Split(key, "/")
	if len(parts) != 5 || parts[0] != "claims" || parts[1] != "success" {
		return "", "", libtime.DateTime{}, false
	}
	unix, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil || unix < 1 {
		return "", "", libtime.DateTime{}, false
	}
	return parts[2], parts[3], libtime.DateTime(time.Unix(unix, 0).UTC()), true
}

// successRefExpired reports whether ref's lease has passed at now, i.e. whether
// the record is older than the retention the writer stamped on it.
//
// An absent or unparseable lease_until counts as NOT expired. git-rest writes the
// field on every create, so an unreadable value is a store anomaly rather than
// evidence that the record is spent — and treating it as spent would let a
// malformed ref re-open the duplicate this record exists to suppress.
func successRefExpired(ctx context.Context, now libtime.DateTime, ref ClaimRef) bool {
	if ref.LeaseUntil == "" {
		return false
	}
	leaseUntil, err := libtime.ParseDateTime(ctx, ref.LeaseUntil)
	if err != nil {
		glog.Warningf(
			"event=success_record_lease_unparsed ref=%s lease_until=%q error=%v",
			ref.Ref,
			ref.LeaseUntil,
			err,
		)
		return false
	}
	return !now.Time().Before(leaseUntil.Time())
}

// Record stores one success and drops the task's records at every other phase.
func (s *successStore) Record(
	ctx context.Context,
	taskID string,
	phase string,
	completedAt libtime.DateTime,
) error {
	component := RedriveStreakPhaseComponent(phase)
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return err
	}
	if err := validateClaimComponent(ctx, "phase", component); err != nil {
		return err
	}
	key := SuccessRecordPrefix + taskID + "/" + component + "/" +
		strconv.FormatInt(completedAt.Time().Unix(), 10)
	_, err := s.refClient.Put(
		ctx,
		key,
		"",
		SuccessRecordCluster,
		taskID,
		completedAt.Add(s.retention),
	)
	if err != nil {
		if !stderrors.Is(err, ErrLeaseConflict) {
			return errors.Wrapf(
				ctx, err, "record success for task %s at phase %s", taskID, component,
			)
		}
		// The same completion is already recorded: a re-delivered terminal Job,
		// or two pods observing one success. Idempotent by construction.
		glog.V(3).Infof(
			"event=success_record_exists task=%s phase=%s completed=%s",
			taskID, component, completedAt.Time().UTC().Format("2006-01-02T15:04:05Z"),
		)
	}
	s.keepOnly(ctx, taskID, key)
	return nil
}

// Lookup returns the latest live record of taskID at phase.
func (s *successStore) Lookup(
	ctx context.Context,
	taskID string,
	phase string,
) (libtime.DateTime, bool, error) {
	component := RedriveStreakPhaseComponent(phase)
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return libtime.DateTime{}, false, err
	}
	if err := validateClaimComponent(ctx, "phase", component); err != nil {
		return libtime.DateTime{}, false, err
	}
	refs, err := redriveListForTask(ctx, s.refClient, SuccessRecordPrefix, taskID)
	if err != nil {
		return libtime.DateTime{}, false, err
	}
	now := s.currentDateTime.Now()
	var latest libtime.DateTime
	found := false
	for _, ref := range refs {
		refTaskID, refPhase, at, ok := parseSuccessRef(ref.Ref)
		if !ok || refTaskID != taskID || refPhase != component {
			continue
		}
		if successRefExpired(ctx, now, ref) {
			continue
		}
		if !found || at.Time().After(latest.Time()) {
			latest = at
			found = true
		}
	}
	return latest, found, nil
}

// Snapshot returns every live record under the prefix, from one list call.
//
// The reconcile pass needs the record of every candidate it is about to drive.
// Lookup answers that with one list per task, so a pass with N candidates made N
// git-rest round-trips and spent its whole budget on them — the same measurement
// that forced RedriveStreakStore.Snapshot. One prefix list answers the question
// for all of them.
//
// Refs whose key does not parse are skipped, exactly as Lookup skips them.
func (s *successStore) Snapshot(
	ctx context.Context,
) (map[string]map[string]libtime.DateTime, error) {
	refs, err := s.refClient.List(ctx, SuccessRecordPrefix)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "list all success records")
	}
	records := make(map[string]map[string]libtime.DateTime, len(refs))
	// No ctx.Done() check in this loop, for the same reason Count and the streak
	// Snapshot have none: refs is already in memory from the one list above, and
	// folding it is pure CPU over a bounded slice.
	now := s.currentDateTime.Now()
	for _, ref := range refs {
		taskID, phase, at, ok := parseSuccessRef(ref.Ref)
		if !ok {
			continue
		}
		if successRefExpired(ctx, now, ref) {
			continue
		}
		phases, found := records[taskID]
		if !found {
			phases = make(map[string]libtime.DateTime, 1)
			records[taskID] = phases
		}
		if prev, exists := phases[phase]; !exists || at.Time().After(prev.Time()) {
			phases[phase] = at
		}
	}
	return records, nil
}

// keepOnly removes the records of taskID that `keep` supersedes, so the task
// holds one record and the newer completion is never destroyed.
//
// This is the store's bound, and it replaces a prune of expired refs alone: the
// key carries the completion second, so a recurring task would accumulate one ref
// per occurrence for the whole retention window, and Snapshot lists the entire
// prefix on every reconcile pass.
//
// Only a STRICTLY OLDER completion is deleted, and that distinction is the whole
// safety property. Deleting every other ref of the task destroys the record in
// two reachable interleavings: two pods recording C1 and C2 can each list after
// both Puts, so a "delete everything but mine" rule has each one delete the
// other's — leaving no record at all; and a stale caller recording an older
// completion (an informer re-delivering a pre-upgrade Job, whose missing phase
// annotation normalises to `none`) would wipe the live record at the real phase.
// Keeping anything at or after `keep` makes both harmless: the newest write
// survives whichever side lists last.
//
// Best-effort: a failed delete is logged and never fails the Record, because the
// success it records has already happened, and the read side filters expired refs
// if a delete is ever missed.
func (s *successStore) keepOnly(ctx context.Context, taskID string, keep string) {
	_, _, keepAt, keepOK := parseSuccessRef(keep)
	if !keepOK {
		return
	}
	refs, err := redriveListForTask(ctx, s.refClient, SuccessRecordPrefix, taskID)
	if err != nil {
		glog.Warningf(
			"event=success_record_prune_failed task=%s error=%v", taskID, err,
		)
		return
	}
	for _, ref := range refs {
		refTaskID, _, refAt, ok := parseSuccessRef(ref.Ref)
		if !ok || refTaskID != taskID {
			continue
		}
		if strings.TrimPrefix(ref.Ref, "refs/") == keep {
			continue
		}
		if !refAt.Time().Before(keepAt.Time()) {
			continue
		}
		redriveDeleteRef(ctx, s.refClient, "success_record_prune_failed", taskID, ref)
	}
}
