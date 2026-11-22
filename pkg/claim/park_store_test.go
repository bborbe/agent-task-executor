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

var _ = Describe("ParkStore", func() {
	const retention = 24 * libtime.Hour

	var (
		ctx   context.Context
		fake  *fakeGitRest
		clock libtime.CurrentDateTime
		store claim.ParkStore
	)

	// parkRef is the key a Record at completedAt produces.
	parkRef := func(taskID, phase string, at time.Time) string {
		return "refs/" + claim.ParkRecordPrefix + taskID + "/" + phase + "/" +
			strconv.FormatInt(at.Unix(), 10)
	}

	// seedPark seeds one marker with an explicit lease, so a spec can model a ref
	// written by an earlier process at a known time.
	seedPark := func(taskID, phase string, at time.Time, leaseUntil libtime.DateTime) {
		fake.Seed(
			parkRef(taskID, phase, at),
			"sha-"+taskID+"-"+phase,
			claim.ParkRecordCluster,
			taskID,
			leaseUntil.Time().UTC().Format(time.RFC3339),
		)
	}

	BeforeEach(func() {
		ctx = context.Background()
		fake = newFakeGitRest()
		DeferCleanup(fake.Close)
		clock = newTestClock(leaseTestT0)
		store = claim.NewParkStore(
			claim.NewClaimRefClient(fake.URL(), "", ""),
			clock,
			retention,
		)
	})

	Describe("Record", func() {
		It("round-trips the publish time through the ref", func() {
			completed := leaseTestT0.Add(-2 * libtime.Hour).Time()
			published := leaseTestT0.Add(-1 * libtime.Hour).Time()

			Expect(store.Record(
				ctx,
				"tid-a",
				"execution",
				libtime.DateTime(completed),
				libtime.DateTime(published),
			)).To(Succeed())

			got, ok, err := store.PublishedAt(ctx, "tid-a", libtime.DateTime(completed))
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(got.Time().Equal(published)).To(BeTrue())
		})

		It("survives a freshly constructed store", func() {
			// The store has no memory: a second instance reads only the shared refs,
			// which is the restart proof.
			completed := leaseTestT0.Add(-2 * libtime.Hour).Time()
			published := leaseTestT0.Add(-1 * libtime.Hour).Time()

			Expect(store.Record(
				ctx,
				"tid-a",
				"execution",
				libtime.DateTime(completed),
				libtime.DateTime(published),
			)).To(Succeed())

			fresh := claim.NewParkStore(
				claim.NewClaimRefClient(fake.URL(), "", ""),
				clock,
				retention,
			)
			got, ok, err := fresh.PublishedAt(ctx, "tid-a", libtime.DateTime(completed))
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(got.Time().Equal(published)).To(BeTrue())
		})

		It("advances the marker to a later completion", func() {
			firstCompleted := leaseTestT0.Add(-3 * libtime.Hour).Time()
			firstPublished := firstCompleted.Add(5 * time.Minute)
			secondCompleted := leaseTestT0.Add(-1 * libtime.Hour).Time()
			secondPublished := secondCompleted.Add(5 * time.Minute)

			Expect(store.Record(
				ctx,
				"tid-a",
				"execution",
				libtime.DateTime(firstCompleted),
				libtime.DateTime(firstPublished),
			)).To(Succeed())
			Expect(store.Record(
				ctx,
				"tid-a",
				"execution",
				libtime.DateTime(secondCompleted),
				libtime.DateTime(secondPublished),
			)).To(Succeed())

			_, ok, err := store.PublishedAt(
				ctx, "tid-a", libtime.DateTime(firstCompleted),
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeFalse())

			got, ok, err := store.PublishedAt(
				ctx, "tid-a", libtime.DateTime(secondCompleted),
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(got.Time().Equal(secondPublished)).To(BeTrue())
		})

		It("keeps exactly one ref per task", func() {
			firstCompleted := leaseTestT0.Add(-3 * libtime.Hour).Time()
			secondCompleted := leaseTestT0.Add(-1 * libtime.Hour).Time()

			Expect(store.Record(
				ctx,
				"tid-a",
				"execution",
				libtime.DateTime(firstCompleted),
				libtime.DateTime(firstCompleted.Add(time.Minute)),
			)).To(Succeed())
			Expect(store.Record(
				ctx,
				"tid-a",
				"execution",
				libtime.DateTime(secondCompleted),
				libtime.DateTime(secondCompleted.Add(time.Minute)),
			)).To(Succeed())

			_, firstExists := fake.Ref(parkRef("tid-a", "execution", firstCompleted))
			Expect(firstExists).To(BeFalse())
			_, secondExists := fake.Ref(parkRef("tid-a", "execution", secondCompleted))
			Expect(secondExists).To(BeTrue())
		})

		It("treats the same completion twice as already recorded", func() {
			// The two-cluster case: both clusters observe one success, and the second
			// Put is a 409 no-op. The first publish time is what the marker keeps.
			completed := leaseTestT0.Add(-2 * libtime.Hour).Time()
			firstPublished := leaseTestT0.Add(-1 * libtime.Hour).Time()
			secondPublished := firstPublished.Add(10 * time.Minute)

			Expect(store.Record(
				ctx,
				"tid-a",
				"execution",
				libtime.DateTime(completed),
				libtime.DateTime(firstPublished),
			)).To(Succeed())
			Expect(store.Record(
				ctx,
				"tid-a",
				"execution",
				libtime.DateTime(completed),
				libtime.DateTime(secondPublished),
			)).To(Succeed())

			_, exists := fake.Ref(parkRef("tid-a", "execution", completed))
			Expect(exists).To(BeTrue())

			got, ok, err := store.PublishedAt(ctx, "tid-a", libtime.DateTime(completed))
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(got.Time().Equal(firstPublished)).To(BeTrue())
		})

		It("rejects a task id that would escape the prefix", func() {
			Expect(store.Record(
				ctx,
				"bad/id",
				"execution",
				leaseTestT0,
				leaseTestT0,
			)).To(HaveOccurred())

			_, _, err := store.PublishedAt(ctx, "bad/id", leaseTestT0)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("PublishedAt", func() {
		It("skips a malformed ref", func() {
			fake.Seed(
				"refs/"+claim.ParkRecordPrefix+"tid-a/execution",
				"sha-short",
				claim.ParkRecordCluster,
				"tid-a",
				leaseTestT0.Add(retention).Time().UTC().Format(time.RFC3339),
			)
			fake.Seed(
				"refs/"+claim.ParkRecordPrefix+"tid-a/execution/not-a-number",
				"sha-bad",
				claim.ParkRecordCluster,
				"tid-a",
				leaseTestT0.Add(retention).Time().UTC().Format(time.RFC3339),
			)

			_, ok, err := store.PublishedAt(
				ctx,
				"tid-a",
				libtime.DateTime(leaseTestT0.Add(-2*libtime.Hour).Time()),
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeFalse())
		})

		It("ignores a marker whose lease has passed", func() {
			completed := leaseTestT0.Add(-2 * libtime.Hour).Time()
			seedPark("tid-a", "execution", completed, leaseTestT0.Add(-libtime.Hour))

			_, ok, err := store.PublishedAt(ctx, "tid-a", libtime.DateTime(completed))
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeFalse())
		})
	})
})
