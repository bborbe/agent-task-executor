// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"time"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// parkStore returns the shared park marker, constructing it on first use.
//
// It is built only in the modes that take a claim, matching successStore: claim
// mode off builds no ref client, so there is nothing here to build over. In
// claim mode off the park dedup is in-memory only, which is that configuration's
// stated limitation.
func (h *taskEventHandler) parkStore() claim.ParkStore {
	h.parkStoreOnce.Do(func() {
		// An injected store wins, so a caller can supply a fake without the factory.
		//
		// Both checks live INSIDE the Once for the same reason successStore's do:
		// reading the field before Do and writing it inside has no happens-before
		// edge, and the reconcile, the Kafka consumer and the deferred-respawn loop
		// can all reach this method for the first time concurrently.
		if h.parkRecordStore != nil {
			return
		}
		if !h.claimMode.TakesClaim() || h.claimRefClientFactory == nil {
			return
		}
		refClient := h.claimRefClientFactory()
		if refClient == nil {
			return
		}
		h.parkRecordStore = claim.NewParkStore(
			refClient,
			h.currentDateTime,
			libtime.Duration(successRecordRetention),
		)
	})
	return h.parkRecordStore
}

// parkPublishedAt returns when a park for the success completed at completedAt
// went out, and whether one is recorded.
//
// The shared marker is the authority, because it survives a restart and a peer
// cluster; the per-pod map is the floor. A failed read is logged and falls
// through to memory: a store that cannot be read is not evidence that no park
// went out, and treating it as one would re-publish the park the marker exists
// to suppress.
func (h *taskEventHandler) parkPublishedAt(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	completedAt time.Time,
) (time.Time, bool) {
	if store := h.parkStore(); store != nil {
		at, ok, err := store.PublishedAt(ctx, string(taskID), libtime.DateTime(completedAt))
		if err != nil {
			glog.Warningf("event=park_marker_read_failed task=%s error=%v", taskID, err)
		} else if ok {
			return at.Time(), true
		}
	}
	return h.taskStore.ParkPublishedAt(taskID, completedAt)
}

// latestParkPublishedAt returns when the task's most recent park went out, from
// the shared marker first and the per-pod floor second.
//
// It is parkPublishedAt's sibling with one difference that matters: it keys on
// the task alone, not on a completion. A caller that must hold a grace after a
// park needs the park's AGE, and the completion-keyed lookup misses exactly when
// the view has caught the terminal phase the park wrote while the re-armed task
// is at a non-terminal one. The authority order and failure handling are the
// same — shared store first, memory floor second, a read failure logged and
// never re-enabling a spawn this pod already parked.
func (h *taskEventHandler) latestParkPublishedAt(
	ctx context.Context,
	taskID lib.TaskIdentifier,
) (time.Time, bool) {
	if store := h.parkStore(); store != nil {
		at, ok, err := store.LatestPublishedAt(ctx, string(taskID))
		if err != nil {
			glog.Warningf("event=park_marker_read_failed task=%s error=%v", taskID, err)
		} else if ok {
			return at.Time(), true
		}
	}
	return h.taskStore.LatestParkPublishedAt(taskID)
}

// markParkPublished records that a park went out, in memory first and then in
// the shared store.
//
// The memory write is first so a failed shared write still suppresses a repeat
// on this pod. The shared write is best-effort: a marker failure is logged and
// never propagated, and it never re-enables the write-back path — the publish is
// what the caller has already decided, and the write-back is the irreversible
// step.
func (h *taskEventHandler) markParkPublished(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	phase string,
	completedAt time.Time,
	publishedAt time.Time,
) {
	h.taskStore.MarkParkPublished(taskID, completedAt, publishedAt)
	store := h.parkStore()
	if store == nil {
		return
	}
	if err := store.Record(
		ctx,
		string(taskID),
		phase,
		libtime.DateTime(completedAt),
		libtime.DateTime(publishedAt),
	); err != nil {
		glog.Warningf("event=park_marker_write_failed task=%s error=%v", taskID, err)
	}
}
