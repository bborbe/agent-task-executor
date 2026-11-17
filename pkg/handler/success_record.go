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

// successStore returns the shared success record, constructing it on first use.
//
// It is built only in the modes that take a claim, matching the transport's own
// contract: claim mode off builds no ref client and reads no gateway secret, so
// there is nothing here to build over. Every deployed executor runs `on` (read
// on nukedev and nukeprod 2026-10-28), so this costs the restart durability
// nothing where it matters. In claim mode off there is no shared store at all,
// and the suppression is in-memory only — a limitation of that configuration,
// stated rather than hidden.
func (h *taskEventHandler) successStore() claim.SuccessStore {
	h.successStoreOnce.Do(func() {
		// An injected store wins, so a caller can supply a fake without the factory.
		//
		// Both checks live INSIDE the Once. Reading the field before Do and writing
		// it inside has no happens-before edge between them, which is the classic
		// unsafe double-checked-locking race — and here it is a live one, because the
		// reconcile pass, the Kafka consumer and the deferred-respawn loop can all
		// reach this method for the first time concurrently. Calling Do
		// unconditionally and reading the field only afterwards is the shape
		// leaseManager already uses.
		if h.successRecordStore != nil {
			return
		}
		if !h.claimMode.TakesClaim() || h.claimRefClientFactory == nil {
			return
		}
		refClient := h.claimRefClientFactory()
		if refClient == nil {
			return
		}
		h.successRecordStore = claim.NewSuccessStore(
			refClient,
			h.currentDateTime,
			libtime.Duration(successRecordRetention),
		)
	})
	return h.successRecordStore
}

// recordTaskSuccess persists a succeeded Job's phase and completion time in the
// shared store, so a restarted pod and a peer cluster read the same record.
//
// Best-effort by design: the in-memory record already covers this pod and the
// claim hold already covers the peer window, so a failed write degrades to the
// behaviour before this change rather than failing a release that has already
// happened.
func (h *taskEventHandler) recordTaskSuccess(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	phase string,
	completedAt time.Time,
) {
	store := h.successStore()
	if store == nil {
		return
	}
	if err := store.Record(
		ctx,
		string(taskID),
		phase,
		libtime.DateTime(completedAt),
	); err != nil {
		glog.Warningf(
			"event=success_record_failed task=%s phase=%s error=%v",
			taskID, phase, err,
		)
		return
	}
	glog.V(2).Infof(
		"event=success_recorded task=%s phase=%s completed=%s",
		taskID, phase, completedAt.UTC().Format(time.RFC3339),
	)
}

// sharedSuccess returns the completion time recorded in the shared store for
// taskID at phase.
//
// Inside a reconcile pass it answers from the pass snapshot — one list for every
// candidate — because a per-candidate list is what put a large pass over its
// budget for the redrive streak. Outside a pass it reads live, so a spawn that
// just recorded a success is visible to the Kafka consumer immediately.
//
// A failed read is logged and reported as "no record": the guard then falls back
// to the in-memory map, which is exactly the behaviour before this change. A
// store that cannot be read is not evidence that the task has not succeeded, but
// neither is it evidence that it has, and suppressing a spawn on a failed read
// would strand a task on an unreachable store.
func (h *taskEventHandler) sharedSuccess(
	ctx context.Context,
	taskID lib.TaskIdentifier,
	phase string,
) (libtime.DateTime, bool) {
	store := h.successStore()
	if store == nil {
		return libtime.DateTime{}, false
	}
	if cache := successCacheFrom(ctx); cache != nil {
		at, ok, err := cache.lookup(ctx, store, string(taskID), phase)
		if err != nil {
			glog.Warningf(
				"event=success_record_snapshot_failed task=%s phase=%s error=%v",
				taskID, phase, err,
			)
			return libtime.DateTime{}, false
		}
		return at, ok
	}
	at, ok, err := store.Lookup(ctx, string(taskID), phase)
	if err != nil {
		glog.Warningf(
			"event=success_record_read_failed task=%s phase=%s error=%v",
			taskID, phase, err,
		)
		return libtime.DateTime{}, false
	}
	return at, ok
}
