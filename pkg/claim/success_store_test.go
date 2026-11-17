// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	"strconv"
	"time"

	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

var _ = Describe("SuccessStore", func() {
	const retention = 24 * libtime.Hour

	var (
		ctx   context.Context
		fake  *fakeGitRest
		store claim.SuccessStore
	)

	// successRef is the key a Record at completedAt produces.
	successRef := func(taskID, phase string, at time.Time) string {
		return "refs/" + claim.SuccessRecordPrefix + taskID + "/" + phase + "/" +
			strconv.FormatInt(at.Unix(), 10)
	}

	// seedSuccess seeds one record with an explicit lease, so a spec can model a
	// ref written by an earlier process at a known time.
	seedSuccess := func(taskID, phase string, at time.Time, leaseUntil libtime.DateTime) {
		fake.Seed(
			successRef(taskID, phase, at),
			"sha-"+taskID+"-"+phase,
			claim.SuccessRecordCluster,
			taskID,
			leaseUntil.Time().UTC().Format(time.RFC3339),
		)
	}

	BeforeEach(func() {
		ctx = context.Background()
		fake = newFakeGitRest()
		DeferCleanup(fake.Close)
		store = claim.NewSuccessStore(
			claim.NewClaimRefClient(fake.URL(), "", ""),
			newTestClock(leaseTestT0),
			retention,
		)
	})

	Describe("Record", func() {
		It("stores the completion and reports it back through Lookup", func() {
			at := leaseTestT0.Add(-2 * libtime.Hour).Time()

			Expect(store.Record(ctx, "tid-a", "execution", libtime.DateTime(at))).
				To(Succeed())

			got, ok, err := store.Lookup(ctx, "tid-a", "execution")
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(got.Time().Equal(at)).To(BeTrue())
		})

		It("keeps exactly one record per task across repeated completions", func() {
			// The bound the store lives or dies by: a recurring task succeeds at
			// one phase over and over, and every completion carries a distinct key.
			// Without this the prefix grows by one ref per completion and Snapshot
			// lists all of them on every pass.
			first := leaseTestT0.Add(-3 * libtime.Hour).Time()
			second := leaseTestT0.Add(-2 * libtime.Hour).Time()
			third := leaseTestT0.Add(-1 * libtime.Hour).Time()

			for _, at := range []time.Time{first, second, third} {
				Expect(store.Record(ctx, "tid-a", "execution", libtime.DateTime(at))).
					To(Succeed())
			}

			_, olderExists := fake.Ref(successRef("tid-a", "execution", first))
			Expect(olderExists).To(BeFalse())
			_, middleExists := fake.Ref(successRef("tid-a", "execution", second))
			Expect(middleExists).To(BeFalse())
			_, newestExists := fake.Ref(successRef("tid-a", "execution", third))
			Expect(newestExists).To(BeTrue())
		})

		It("drops the record at the phase it superseded", func() {
			planning := leaseTestT0.Add(-3 * libtime.Hour).Time()
			execution := leaseTestT0.Add(-2 * libtime.Hour).Time()
			seedSuccess(
				"tid-a", "planning", planning, leaseTestT0.Add(retention),
			)

			Expect(store.Record(ctx, "tid-a", "execution", libtime.DateTime(execution))).
				To(Succeed())

			_, planningExists := fake.Ref(successRef("tid-a", "planning", planning))
			Expect(planningExists).To(BeFalse())
		})

		It("keeps a newer record a peer wrote, rather than deleting it", func() {
			// Two pods can each list after both Puts. A "delete everything but mine"
			// rule has each side delete the other's record, leaving the task with none
			// at all — so only a strictly OLDER completion is superseded. The same
			// guard covers a stale writer: an informer re-delivering a pre-upgrade Job
			// records an older completion at a normalised phase, and it must not wipe
			// the live record at the real phase.
			newer := leaseTestT0.Add(-1 * libtime.Hour).Time()
			older := leaseTestT0.Add(-2 * libtime.Hour).Time()
			seedSuccess("tid-a", "execution", newer, leaseTestT0.Add(retention))

			Expect(store.Record(ctx, "tid-a", "execution", libtime.DateTime(older))).
				To(Succeed())

			_, newerExists := fake.Ref(successRef("tid-a", "execution", newer))
			Expect(newerExists).To(BeTrue())
			_, olderExists := fake.Ref(successRef("tid-a", "execution", older))
			Expect(olderExists).To(BeTrue())

			got, ok, err := store.Lookup(ctx, "tid-a", "execution")
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(got.Time().Equal(newer)).To(BeTrue())
		})

		It("treats a repeated completion as already recorded, not an error", func() {
			// The informer re-delivers a terminal Job every few minutes and two pods
			// can observe one success; the second write must not fail the caller.
			at := leaseTestT0.Add(-2 * libtime.Hour).Time()

			Expect(store.Record(ctx, "tid-a", "execution", libtime.DateTime(at))).
				To(Succeed())
			Expect(store.Record(ctx, "tid-a", "execution", libtime.DateTime(at))).
				To(Succeed())

			_, exists := fake.Ref(successRef("tid-a", "execution", at))
			Expect(exists).To(BeTrue())
		})

		It("rejects a task id that would escape the prefix", func() {
			Expect(store.Record(ctx, "bad/id", "execution", leaseTestT0)).
				To(HaveOccurred())
		})
	})

	Describe("Lookup", func() {
		It("ignores a record at another phase", func() {
			seedSuccess(
				"tid-a", "planning", leaseTestT0.Add(-2*libtime.Hour).Time(),
				leaseTestT0.Add(retention),
			)

			_, ok, err := store.Lookup(ctx, "tid-a", "execution")
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeFalse())
		})

		It("ignores a record whose lease has passed", func() {
			seedSuccess(
				"tid-a", "execution", leaseTestT0.Add(-2*libtime.Hour).Time(),
				leaseTestT0.Add(-libtime.Hour),
			)

			_, ok, err := store.Lookup(ctx, "tid-a", "execution")
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeFalse())
		})

		It("keeps a record whose lease is unreadable", func() {
			// The fail direction preserves the suppression the store exists for: a
			// malformed lease is a store anomaly, not evidence the record is spent.
			fake.Seed(
				successRef("tid-a", "execution", leaseTestT0.Add(-2*libtime.Hour).Time()),
				"sha-tid-a-execution",
				claim.SuccessRecordCluster,
				"tid-a",
				"not-a-timestamp",
			)

			_, ok, err := store.Lookup(ctx, "tid-a", "execution")
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeTrue())
		})

		It("skips keys that do not parse", func() {
			// A key with the wrong segment count, and one whose last segment is not
			// a number, must not be read as a record.
			fake.Seed(
				"refs/"+claim.SuccessRecordPrefix+"tid-a/execution",
				"sha-short", claim.SuccessRecordCluster, "tid-a",
				leaseTestT0.Add(retention).Time().UTC().Format(time.RFC3339),
			)
			fake.Seed(
				"refs/"+claim.SuccessRecordPrefix+"tid-a/execution/not-a-number",
				"sha-bad", claim.SuccessRecordCluster, "tid-a",
				leaseTestT0.Add(retention).Time().UTC().Format(time.RFC3339),
			)

			_, ok, err := store.Lookup(ctx, "tid-a", "execution")
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeFalse())
		})

		It("returns the newest of several records at one phase", func() {
			older := leaseTestT0.Add(-3 * libtime.Hour).Time()
			newer := leaseTestT0.Add(-1 * libtime.Hour).Time()
			seedSuccess("tid-a", "execution", older, leaseTestT0.Add(retention))
			seedSuccess("tid-a", "execution", newer, leaseTestT0.Add(retention))

			got, ok, err := store.Lookup(ctx, "tid-a", "execution")
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(got.Time().Equal(newer)).To(BeTrue())
		})
	})

	Describe("Snapshot", func() {
		It("answers every task and phase from one list", func() {
			execution := leaseTestT0.Add(-2 * libtime.Hour).Time()
			planning := leaseTestT0.Add(-4 * libtime.Hour).Time()
			seedSuccess("tid-a", "execution", execution, leaseTestT0.Add(retention))
			seedSuccess("tid-b", "planning", planning, leaseTestT0.Add(retention))

			snapshot, err := store.Snapshot(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(
				snapshot["tid-a"][claim.RedriveStreakPhaseComponent("execution")].Time().
					Equal(execution),
			).To(BeTrue())
			Expect(
				snapshot["tid-b"][claim.RedriveStreakPhaseComponent("planning")].Time().
					Equal(planning),
			).To(BeTrue())
		})

		It("skips expired records and malformed keys", func() {
			live := leaseTestT0.Add(-1 * libtime.Hour).Time()
			seedSuccess("tid-a", "execution", live, leaseTestT0.Add(retention))
			seedSuccess(
				"tid-b", "execution", leaseTestT0.Add(-2*libtime.Hour).Time(),
				leaseTestT0.Add(-libtime.Hour),
			)
			fake.Seed(
				"refs/"+claim.SuccessRecordPrefix+"tid-c/execution/not-a-number",
				"sha-bad", claim.SuccessRecordCluster, "tid-c",
				leaseTestT0.Add(retention).Time().UTC().Format(time.RFC3339),
			)

			snapshot, err := store.Snapshot(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(snapshot).To(HaveKey("tid-a"))
			Expect(snapshot).ToNot(HaveKey("tid-b"))
			Expect(snapshot).ToNot(HaveKey("tid-c"))
		})
	})
})
