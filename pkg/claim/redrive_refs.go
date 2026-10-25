// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	stderrors "errors"
	"strings"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
)

// redriveRefParser parses one redrive ref key. detail is the segment between
// the phase and the numeric suffix — the reason for a reason ref, empty for a
// streak ref. ok is false for any key that is not exactly the store's shape.
//
// Both redrive stores walk the same ref space, so the list/delete/retry
// mechanics live here once and each store supplies only its own parser and key
// builder. That is what keeps the two stores from becoming near-copies that
// drift apart.
type redriveRefParser func(ref string) (taskID, phase, detail string, n int, ok bool)

// redriveListForTask lists every redrive ref under taskID's prefix.
func redriveListForTask(
	ctx context.Context,
	refClient ClaimRefClient,
	prefix string,
	taskID string,
) ([]ClaimRef, error) {
	refs, err := refClient.List(ctx, prefix+taskID)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "list redrive refs for task %s", taskID)
	}
	return refs, nil
}

// nextRedriveNumber returns one more than the highest parsed number of taskID at
// component, or 1 when there is none.
//
// detail is ignored deliberately: the reason store numbers attempts across every
// reason, so a brand-new reason never restarts at 1 and sorts among the oldest
// refs. A per-reason counter would let a stale reason dominate the classifier's
// most-recent window.
func nextRedriveNumber(
	refs []ClaimRef,
	parse redriveRefParser,
	taskID string,
	component string,
) int {
	highest := 0
	for _, ref := range refs {
		refTaskID, refPhase, _, n, ok := parse(ref.Ref)
		if !ok || refTaskID != taskID || refPhase != component {
			continue
		}
		if n > highest {
			highest = n
		}
	}
	return highest + 1
}

// recordRedriveRef appends one ref under prefix, retrying a create conflict on
// the next free number at most redriveStreakMaxCreateAttempts times, then
// deletes the task's refs at every other phase.
func recordRedriveRef(
	ctx context.Context,
	refClient ClaimRefClient,
	currentDateTime libtime.CurrentDateTimeGetter,
	prefix string,
	event string,
	what string,
	taskID string,
	component string,
	parse redriveRefParser,
	buildKey func(next int) string,
) error {
	refs, err := redriveListForTask(ctx, refClient, prefix, taskID)
	if err != nil {
		return err
	}
	leaseUntil := currentDateTime.Now().Add(RedriveStreakLeaseDuration)
	for attempt := 1; attempt <= redriveStreakMaxCreateAttempts; attempt++ {
		next := nextRedriveNumber(refs, parse, taskID, component)
		_, putErr := refClient.Put(
			ctx,
			buildKey(next),
			"",
			RedriveStreakCluster,
			taskID,
			leaseUntil,
		)
		if putErr == nil {
			redriveDeleteOtherPhases(ctx, refClient, event, taskID, component, parse, refs)
			return nil
		}
		if !stderrors.Is(putErr, ErrLeaseConflict) {
			return errors.Wrapf(
				ctx,
				putErr,
				"record redrive %s for task %s at phase %s",
				what,
				taskID,
				component,
			)
		}
		if attempt == redriveStreakMaxCreateAttempts {
			return errors.Wrapf(
				ctx,
				putErr,
				"record redrive %s for task %s at phase %s after %d attempts",
				what,
				taskID,
				component,
				redriveStreakMaxCreateAttempts,
			)
		}
		refs, err = redriveListForTask(ctx, refClient, prefix, taskID)
		if err != nil {
			return err
		}
	}
	return errors.Errorf(
		ctx,
		"record redrive %s for task %s at phase %s exhausted %d attempts",
		what,
		taskID,
		component,
		redriveStreakMaxCreateAttempts,
	)
}

// clearRedriveRefs removes every parsed ref of taskID under prefix, at every
// phase.
func clearRedriveRefs(
	ctx context.Context,
	refClient ClaimRefClient,
	prefix string,
	event string,
	taskID string,
	parse redriveRefParser,
) error {
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return err
	}
	refs, err := redriveListForTask(ctx, refClient, prefix, taskID)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		refTaskID, _, _, _, ok := parse(ref.Ref)
		if !ok || refTaskID != taskID {
			continue
		}
		redriveDeleteRef(ctx, refClient, event, taskID, ref)
	}
	return nil
}

// redriveDeleteOtherPhases deletes every parsed ref of taskID whose phase
// differs from component. It never returns an error.
func redriveDeleteOtherPhases(
	ctx context.Context,
	refClient ClaimRefClient,
	event string,
	taskID string,
	component string,
	parse redriveRefParser,
	refs []ClaimRef,
) {
	for _, ref := range refs {
		refTaskID, refPhase, _, _, ok := parse(ref.Ref)
		if !ok || refTaskID != taskID || refPhase == component {
			continue
		}
		redriveDeleteRef(ctx, refClient, event, taskID, ref)
	}
}

// redriveDeleteRef deletes one redrive ref. A conflict or an already-gone ref is
// ignored; any other failure is logged and the caller continues.
func redriveDeleteRef(
	ctx context.Context,
	refClient ClaimRefClient,
	event string,
	taskID string,
	ref ClaimRef,
) {
	key := strings.TrimPrefix(ref.Ref, "refs/")
	err := refClient.Delete(ctx, key, ref.Sha)
	if err == nil {
		return
	}
	if stderrors.Is(err, ErrLeaseConflict) || stderrors.Is(err, ErrRefNotFound) {
		return
	}
	glog.Warningf("event=%s task=%s key=%s error=%v", event, taskID, key, err)
}
