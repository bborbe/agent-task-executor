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
	// ParkRecordPrefix is the frozen key prefix of every park marker.
	// Changing it strands every existing marker.
	ParkRecordPrefix = "claims/park/"
	// ParkRecordCluster is the sentinel cluster field on every park ref.
	// git-rest refuses an empty cluster, and this value must never equal a
	// real executor cluster name.
	ParkRecordCluster = "park-record"
)

// ParkStore persists "a park for this success was already published" in the
// shared git-rest claim store, keyed by the success completion, so a restarted
// pod — or a peer cluster — reads the same marker.
//
// It exists because the park dedup was per-pod memory while the success record
// it dedups against lives in the shared claim store: a fresh pod sees a day-old
// success with no publish record and re-publishes the park, rewriting a
// deliberate re-arm back to completed. The marker travels beside the success
// record, under its own frozen prefix, so it cannot collide with a dispatch
// claim, a redrive-streak ref or a success record.
//
// Ref shape: `claims/park/<taskid>/<phase component>/<completed unix seconds>`.
// The completion time rides in the key rather than in a body because the
// git-rest ref API carries no free-text payload. The publish time rides in
// `lease_until`, which Record sets to publishedAt+retention and PublishedAt
// recovers by subtracting the same retention; both writer and reader use the
// store's one retention value, so the recovery is exact.
//
//counterfeiter:generate -o ../../mocks/park_store.go --fake-name FakeParkStore . ParkStore

type ParkStore interface {
	// Record stores that a park for the success completed at completedAt was
	// published at publishedAt, keyed by the completion, and deletes the task's
	// strictly older park refs.
	//
	// Recording the same completion twice is not an error: two clusters can
	// publish one park, and the second write is a conflict that means "already
	// recorded".
	Record(
		ctx context.Context,
		taskID string,
		phase string,
		completedAt libtime.DateTime,
		publishedAt libtime.DateTime,
	) error
	// PublishedAt returns when the park for the success completed at completedAt
	// was published, and whether one is recorded.
	PublishedAt(
		ctx context.Context,
		taskID string,
		completedAt libtime.DateTime,
	) (libtime.DateTime, bool, error)
}

// NewParkStore creates a ParkStore over refClient. retention is how long a
// marker is kept; it also encodes the publish time in the ref's lease, so both
// halves of the encoding use this one value. The injected clock supplies the
// expiry comparison; no wall clock is read.
func NewParkStore(
	refClient ClaimRefClient,
	currentDateTime libtime.CurrentDateTimeGetter,
	retention libtime.Duration,
) ParkStore {
	return &parkStore{
		refClient:       refClient,
		currentDateTime: currentDateTime,
		retention:       retention,
	}
}

type parkStore struct {
	refClient       ClaimRefClient
	currentDateTime libtime.CurrentDateTimeGetter
	retention       libtime.Duration
}

// parseParkRef parses a full ref
// ("refs/claims/park/<taskid>/<phase>/<unix>"). ok is false for any key that is
// not exactly five segments under the frozen prefix, or whose last segment is
// not a positive integer.
func parseParkRef(ref string) (taskID string, phase string, at libtime.DateTime, ok bool) {
	key := strings.TrimPrefix(ref, "refs/")
	parts := strings.Split(key, "/")
	if len(parts) != 5 || parts[0] != "claims" || parts[1] != "park" {
		return "", "", libtime.DateTime{}, false
	}
	unix, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil || unix < 1 {
		return "", "", libtime.DateTime{}, false
	}
	return parts[2], parts[3], libtime.DateTime(time.Unix(unix, 0).UTC()), true
}

// Record stores one park marker and drops the task's strictly older markers.
func (s *parkStore) Record(
	ctx context.Context,
	taskID string,
	phase string,
	completedAt libtime.DateTime,
	publishedAt libtime.DateTime,
) error {
	component := RedriveStreakPhaseComponent(phase)
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return err
	}
	if err := validateClaimComponent(ctx, "phase", component); err != nil {
		return err
	}
	key := ParkRecordPrefix + taskID + "/" + component + "/" +
		strconv.FormatInt(completedAt.Time().Unix(), 10)
	_, err := s.refClient.Put(
		ctx,
		key,
		"",
		ParkRecordCluster,
		taskID,
		publishedAt.Add(s.retention),
	)
	if err != nil {
		if !stderrors.Is(err, ErrLeaseConflict) {
			return errors.Wrapf(
				ctx, err, "record park for task %s at phase %s", taskID, component,
			)
		}
		// The same completion is already recorded: a re-delivery, or two
		// clusters publishing one park. Idempotent by construction.
		glog.V(3).Infof(
			"event=park_record_exists task=%s phase=%s completed=%s",
			taskID, component, completedAt.Time().UTC().Format(time.RFC3339),
		)
	}
	s.keepOnly(ctx, taskID, key)
	return nil
}

// PublishedAt returns the publish time recorded for the success that completed
// at completedAt.
//
// The completion second is the match, not the phase: a completion is unique per
// task, and a marker written at a phase the task has since left still answers
// for the completion it names.
func (s *parkStore) PublishedAt(
	ctx context.Context,
	taskID string,
	completedAt libtime.DateTime,
) (libtime.DateTime, bool, error) {
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return libtime.DateTime{}, false, err
	}
	refs, err := redriveListForTask(ctx, s.refClient, ParkRecordPrefix, taskID)
	if err != nil {
		return libtime.DateTime{}, false, errors.Wrapf(
			ctx, err, "list park records for task %s", taskID,
		)
	}
	now := s.currentDateTime.Now()
	for _, ref := range refs {
		refTaskID, refPhase, at, ok := parseParkRef(ref.Ref)
		if !ok || refTaskID != taskID {
			continue
		}
		if at.Time().Unix() != completedAt.Time().Unix() {
			continue
		}
		leaseUntil, parseErr := libtime.ParseDateTime(ctx, ref.LeaseUntil)
		if parseErr != nil {
			glog.Warningf(
				"event=park_record_lease_unparsed ref=%s phase=%s lease_until=%q error=%v",
				ref.Ref,
				refPhase,
				ref.LeaseUntil,
				parseErr,
			)
			continue
		}
		if !now.Time().Before(leaseUntil.Time()) {
			continue
		}
		return libtime.DateTime(leaseUntil.Time().Add(-s.retention.Duration())), true, nil
	}
	return libtime.DateTime{}, false, nil
}

// keepOnly removes the park markers of taskID whose completion is strictly
// older than keep's, so the task holds one marker and a stale completion can no
// longer re-emit a park.
//
// Only a STRICTLY OLDER completion is deleted: two clusters recording the same
// completion must not delete each other's ref, and a stale writer recording an
// older completion must not wipe the live marker.
//
// Best-effort: a failed delete is logged and never fails the Record, because the
// park it records has already gone out and the read side filters expired refs if
// a delete is ever missed.
func (s *parkStore) keepOnly(ctx context.Context, taskID string, keep string) {
	_, _, keepAt, keepOK := parseParkRef(keep)
	if !keepOK {
		return
	}
	refs, err := redriveListForTask(ctx, s.refClient, ParkRecordPrefix, taskID)
	if err != nil {
		glog.Warningf(
			"event=park_record_prune_failed task=%s error=%v", taskID, err,
		)
		return
	}
	for _, ref := range refs {
		refTaskID, _, refAt, ok := parseParkRef(ref.Ref)
		if !ok || refTaskID != taskID {
			continue
		}
		if strings.TrimPrefix(ref.Ref, "refs/") == keep {
			continue
		}
		if !refAt.Time().Before(keepAt.Time()) {
			continue
		}
		redriveDeleteRef(ctx, s.refClient, "park_record_prune_failed", taskID, ref)
	}
}
