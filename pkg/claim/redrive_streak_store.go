// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	stderrors "errors"
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
	// RedriveStreakLeaseDuration is the informational lease of a streak ref.
	// Nothing renews or expires it.
	RedriveStreakLeaseDuration = 24 * libtime.Hour
	// redriveStreakMaxCreateAttempts bounds the create retries on a 409.
	redriveStreakMaxCreateAttempts = 3
)

//counterfeiter:generate -o ../../mocks/redrive_streak_store.go --fake-name FakeRedriveStreakStore . RedriveStreakStore

// RedriveStreakStore persists the consecutive-spawn streak of a task per phase
// in the shared git-rest claim store, so every cluster and every restarted pod
// reads the same count.
type RedriveStreakStore interface {
	// Count returns how many streak refs taskID has at phase. Refs at other
	// phases and refs whose key does not parse are not counted.
	Count(ctx context.Context, taskID string, phase string) (int, error)
	// Record appends one spawn of taskID at phase, then deletes the task's
	// refs at every other phase. A 409 create is retried on the next free
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

// redriveStreakPhaseComponent maps an empty phase to the frozen "none"
// component; every other value is used verbatim.
func redriveStreakPhaseComponent(phase string) string {
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

// Count returns how many streak refs taskID has at phase.
func (s *redriveStreakStore) Count(
	ctx context.Context,
	taskID string,
	phase string,
) (int, error) {
	component := redriveStreakPhaseComponent(phase)
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return 0, err
	}
	if err := validateClaimComponent(ctx, "phase", component); err != nil {
		return 0, err
	}
	refs, err := s.list(ctx, taskID)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, ref := range refs {
		refTaskID, refPhase, _, ok := parseRedriveStreakRef(ref.Ref)
		if !ok || refTaskID != taskID || refPhase != component {
			continue
		}
		count++
	}
	return count, nil
}

// Record appends one spawn of taskID at phase, then deletes the task's refs at
// every other phase.
func (s *redriveStreakStore) Record(
	ctx context.Context,
	taskID string,
	phase string,
) error {
	component := redriveStreakPhaseComponent(phase)
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return err
	}
	if err := validateClaimComponent(ctx, "phase", component); err != nil {
		return err
	}
	refs, err := s.list(ctx, taskID)
	if err != nil {
		return err
	}
	leaseUntil := s.currentDateTime.Now().Add(RedriveStreakLeaseDuration)
	for attempt := 1; attempt <= redriveStreakMaxCreateAttempts; attempt++ {
		next := nextRedriveStreakNumber(refs, taskID, component)
		key := RedriveStreakPrefix + taskID + "/" + component + "/" + strconv.Itoa(next)
		_, putErr := s.refClient.Put(
			ctx,
			key,
			"",
			RedriveStreakCluster,
			taskID,
			leaseUntil,
		)
		if putErr == nil {
			s.deleteOtherPhases(ctx, taskID, component, refs)
			return nil
		}
		if !stderrors.Is(putErr, ErrLeaseConflict) {
			return errors.Wrapf(
				ctx,
				putErr,
				"record redrive streak for task %s at phase %s",
				taskID,
				component,
			)
		}
		if attempt == redriveStreakMaxCreateAttempts {
			return errors.Wrapf(
				ctx,
				putErr,
				"record redrive streak for task %s at phase %s after %d attempts",
				taskID,
				component,
				redriveStreakMaxCreateAttempts,
			)
		}
		refs, err = s.list(ctx, taskID)
		if err != nil {
			return err
		}
	}
	return errors.Errorf(
		ctx,
		"record redrive streak for task %s at phase %s exhausted %d attempts",
		taskID,
		component,
		redriveStreakMaxCreateAttempts,
	)
}

// Clear removes every parsed streak ref of taskID, at every phase.
func (s *redriveStreakStore) Clear(ctx context.Context, taskID string) error {
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return err
	}
	refs, err := s.list(ctx, taskID)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		refTaskID, _, _, ok := parseRedriveStreakRef(ref.Ref)
		if !ok || refTaskID != taskID {
			continue
		}
		s.deleteRef(ctx, taskID, ref)
	}
	return nil
}

// list returns every streak ref under taskID's prefix.
func (s *redriveStreakStore) list(ctx context.Context, taskID string) ([]ClaimRef, error) {
	refs, err := s.refClient.List(ctx, RedriveStreakPrefix+taskID)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "list redrive streak refs for task %s", taskID)
	}
	return refs, nil
}

// nextRedriveStreakNumber returns one more than the highest parsed number of
// taskID at component, or 1 when there is none.
func nextRedriveStreakNumber(refs []ClaimRef, taskID string, component string) int {
	highest := 0
	for _, ref := range refs {
		refTaskID, refPhase, n, ok := parseRedriveStreakRef(ref.Ref)
		if !ok || refTaskID != taskID || refPhase != component {
			continue
		}
		if n > highest {
			highest = n
		}
	}
	return highest + 1
}

// deleteOtherPhases deletes every parsed ref of taskID whose phase differs
// from component. It never returns an error.
func (s *redriveStreakStore) deleteOtherPhases(
	ctx context.Context,
	taskID string,
	component string,
	refs []ClaimRef,
) {
	for _, ref := range refs {
		refTaskID, refPhase, _, ok := parseRedriveStreakRef(ref.Ref)
		if !ok || refTaskID != taskID || refPhase == component {
			continue
		}
		s.deleteRef(ctx, taskID, ref)
	}
}

// deleteRef deletes one streak ref. A conflict or an already-gone ref is
// ignored; any other failure is logged and the caller continues.
func (s *redriveStreakStore) deleteRef(ctx context.Context, taskID string, ref ClaimRef) {
	key := strings.TrimPrefix(ref.Ref, "refs/")
	err := s.refClient.Delete(ctx, key, ref.Sha)
	if err == nil {
		return
	}
	if stderrors.Is(err, ErrLeaseConflict) || stderrors.Is(err, ErrRefNotFound) {
		return
	}
	glog.Warningf(
		"event=redrive_streak_write_failed task=%s key=%s error=%v",
		taskID,
		key,
		err,
	)
}
