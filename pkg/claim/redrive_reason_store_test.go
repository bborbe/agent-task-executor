// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	"net/http"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// newReasonStore builds a reason store over the fake with the shared test clock.
func newReasonStore(fake *fakeGitRest) claim.RedriveReasonStore {
	return claim.NewRedriveReasonStore(
		claim.NewClaimRefClient(fake.URL(), "", ""),
		newTestClock(leaseTestT0),
	)
}

// seedReason seeds one reason ref in the fake.
func seedReason(fake *fakeGitRest, taskID, phase, reason string, n int) {
	fake.Seed(
		"refs/"+claim.RedriveReasonPrefix+taskID+"/"+phase+"/"+reason+"/"+strconv.Itoa(n),
		"sha-"+taskID+"-"+phase+"-"+reason+"-"+strconv.Itoa(n),
		claim.RedriveStreakCluster,
		taskID,
		rfc3339(leaseTestT0),
	)
}

var _ = Describe("RedriveReasonStore", func() {
	var (
		ctx  context.Context
		fake *fakeGitRest
	)

	BeforeEach(func() {
		ctx = context.Background()
		fake = newFakeGitRest()
	})

	AfterEach(func() {
		fake.Close()
	})

	It("records the same reason twice and returns it twice, oldest first", func() {
		store := newReasonStore(fake)
		Expect(store.Record(ctx, "T", "execution", "pod_oom_killed")).To(Succeed())
		Expect(store.Record(ctx, "T", "execution", "pod_oom_killed")).To(Succeed())

		reasons, err := store.Reasons(ctx, "T", "execution")
		Expect(err).NotTo(HaveOccurred())
		Expect(reasons).To(Equal([]string{"pod_oom_killed", "pod_oom_killed"}))

		_, ok1 := fake.Ref("refs/claims/redrive-reason/T/execution/pod_oom_killed/1")
		_, ok2 := fake.Ref("refs/claims/redrive-reason/T/execution/pod_oom_killed/2")
		Expect(ok1).To(BeTrue())
		Expect(ok2).To(BeTrue())
	})

	It("numbers attempts across reasons, so the newest attempt sorts last", func() {
		store := newReasonStore(fake)
		Expect(store.Record(ctx, "T", "execution", "pod_oom_killed")).To(Succeed())
		Expect(store.Record(ctx, "T", "execution", "pod_crash_no_stdout")).To(Succeed())
		Expect(store.Record(ctx, "T", "execution", "pod_oom_killed")).To(Succeed())

		reasons, err := store.Reasons(ctx, "T", "execution")
		Expect(err).NotTo(HaveOccurred())
		Expect(reasons).To(Equal([]string{
			"pod_oom_killed",
			"pod_crash_no_stdout",
			"pod_oom_killed",
		}))
	})

	It("records an empty reason as unknown", func() {
		store := newReasonStore(fake)
		Expect(store.Record(ctx, "T", "execution", "")).To(Succeed())

		_, ok := fake.Ref("refs/claims/redrive-reason/T/execution/unknown/1")
		Expect(ok).To(BeTrue())
	})

	It("records every ref with the streak sentinel cluster", func() {
		store := newReasonStore(fake)
		Expect(store.Record(ctx, "T", "execution", "deadline_exceeded")).To(Succeed())

		ref, ok := fake.Ref("refs/claims/redrive-reason/T/execution/deadline_exceeded/1")
		Expect(ok).To(BeTrue())
		Expect(ref.Cluster).To(Equal(claim.RedriveStreakCluster))
		Expect(ref.TaskID).To(Equal("T"))
	})

	It("resets the reasons on a phase change and keeps other tasks", func() {
		seedReason(fake, "OTHER", "execution", "pod_oom_killed", 1)
		store := newReasonStore(fake)
		Expect(store.Record(ctx, "T", "execution", "pod_oom_killed")).To(Succeed())
		Expect(store.Record(ctx, "T", "execution", "pod_oom_killed")).To(Succeed())
		Expect(store.Record(ctx, "T", "planning", "pod_oom_killed")).To(Succeed())

		_, old1 := fake.Ref("refs/claims/redrive-reason/T/execution/pod_oom_killed/1")
		_, old2 := fake.Ref("refs/claims/redrive-reason/T/execution/pod_oom_killed/2")
		_, planning := fake.Ref("refs/claims/redrive-reason/T/planning/pod_oom_killed/1")
		Expect(old1).To(BeFalse())
		Expect(old2).To(BeFalse())
		Expect(planning).To(BeTrue())

		_, other := fake.Ref("refs/claims/redrive-reason/OTHER/execution/pod_oom_killed/1")
		Expect(other).To(BeTrue())
	})

	It("clears every phase of one task and leaves other tasks", func() {
		seedReason(fake, "OTHER", "execution", "pod_oom_killed", 1)
		store := newReasonStore(fake)
		Expect(store.Record(ctx, "T", "execution", "pod_oom_killed")).To(Succeed())
		Expect(store.Record(ctx, "T", "planning", "deadline_exceeded")).To(Succeed())
		Expect(store.Clear(ctx, "T")).To(Succeed())

		_, execution := fake.Ref("refs/claims/redrive-reason/T/execution/pod_oom_killed/1")
		_, planning := fake.Ref("refs/claims/redrive-reason/T/planning/deadline_exceeded/1")
		Expect(execution).To(BeFalse())
		Expect(planning).To(BeFalse())

		_, other := fake.Ref("refs/claims/redrive-reason/OTHER/execution/pod_oom_killed/1")
		Expect(other).To(BeTrue())
	})

	It("snapshots every task and phase from one list call, ordered by attempt", func() {
		seedReason(fake, "T", "execution", "pod_oom_killed", 1)
		seedReason(fake, "T", "execution", "pod_crash_no_stdout", 2)
		seedReason(fake, "T", "planning", "deadline_exceeded", 1)
		seedReason(fake, "U", "execution", "unknown", 1)
		store := newReasonStore(fake)

		snapshot, err := store.Snapshot(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(snapshot["T"]["execution"]).To(Equal([]string{
			"pod_oom_killed",
			"pod_crash_no_stdout",
		}))
		Expect(snapshot["T"]["planning"]).To(Equal([]string{"deadline_exceeded"}))
		Expect(snapshot["U"]["execution"]).To(Equal([]string{"unknown"}))
	})

	It("agrees with Reasons for every task it holds", func() {
		seedReason(fake, "T", "execution", "pod_oom_killed", 1)
		seedReason(fake, "T", "execution", "pod_oom_killed", 2)
		seedReason(fake, "U", claim.RedriveStreakPhaseNone, "pod_evicted", 1)
		store := newReasonStore(fake)

		snapshot, err := store.Snapshot(ctx)
		Expect(err).NotTo(HaveOccurred())
		for taskID, phases := range snapshot {
			for phase, want := range phases {
				reasons, reasonsErr := store.Reasons(ctx, taskID, phase)
				Expect(reasonsErr).NotTo(HaveOccurred())
				Expect(reasons).To(Equal(want))
			}
		}
	})

	It("skips an unparsable ref in a snapshot and in Reasons", func() {
		seedReason(fake, "T", "execution", "pod_oom_killed", 1)
		fake.Seed(
			"refs/claims/redrive-reason/T/execution/pod_oom_killed/not-a-number",
			"sha-odd",
			claim.RedriveStreakCluster,
			"T",
			rfc3339(leaseTestT0),
		)
		store := newReasonStore(fake)

		reasons, err := store.Reasons(ctx, "T", "execution")
		Expect(err).NotTo(HaveOccurred())
		Expect(reasons).To(Equal([]string{"pod_oom_killed"}))

		snapshot, err := store.Snapshot(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(snapshot["T"]["execution"]).To(Equal([]string{"pod_oom_killed"}))
	})

	It("maps an empty phase to the frozen none component", func() {
		store := newReasonStore(fake)
		Expect(store.Record(ctx, "T", "", "pod_oom_killed")).To(Succeed())

		_, ok := fake.Ref("refs/claims/redrive-reason/T/none/pod_oom_killed/1")
		Expect(ok).To(BeTrue())

		reasons, err := store.Reasons(ctx, "T", "")
		Expect(err).NotTo(HaveOccurred())
		Expect(reasons).To(Equal([]string{"pod_oom_killed"}))
	})

	It("rejects invalid task ids, phases and reasons before any request", func() {
		store := newReasonStore(fake)

		Expect(store.Record(ctx, "bad/id", "execution", "pod_oom_killed")).To(HaveOccurred())
		Expect(store.Record(ctx, "T", "bad/phase", "pod_oom_killed")).To(HaveOccurred())
		Expect(store.Record(ctx, "T", "execution", "pod oom")).To(HaveOccurred())
		Expect(store.Record(ctx, "T", "execution", "pod_oom_killed (OOMKilled/137)")).To(
			HaveOccurred(),
		)
		_, err := store.Reasons(ctx, "bad/id", "execution")
		Expect(err).To(HaveOccurred())
		Expect(store.Clear(ctx, "bad/id")).To(HaveOccurred())

		Expect(fake.Requests()).To(BeEmpty())
	})

	It("retries a lost create race on the next free number", func() {
		first := true
		fake.SetFault(func(request fakeRequest) *fakeFault {
			if request.Method == http.MethodPut && first {
				first = false
				return &fakeFault{Status: http.StatusConflict, Apply: true}
			}
			return nil
		})
		store := newReasonStore(fake)
		Expect(store.Record(ctx, "T", "execution", "pod_oom_killed")).To(Succeed())

		_, ok1 := fake.Ref("refs/claims/redrive-reason/T/execution/pod_oom_killed/1")
		_, ok2 := fake.Ref("refs/claims/redrive-reason/T/execution/pod_oom_killed/2")
		Expect(ok1).To(BeTrue())
		Expect(ok2).To(BeTrue())
	})

	It("fails Reasons, Record and Clear when the list fails", func() {
		seedReason(fake, "T", "execution", "pod_oom_killed", 1)
		fake.SetFault(faultOnMethod(http.MethodGet, http.StatusInternalServerError))
		store := newReasonStore(fake)

		_, err := store.Reasons(ctx, "T", "execution")
		Expect(err).To(HaveOccurred())
		Expect(store.Record(ctx, "T", "execution", "pod_oom_killed")).To(HaveOccurred())
		Expect(store.Clear(ctx, "T")).To(HaveOccurred())

		Expect(
			requestsMatching(fake.Requests(), http.MethodPut, "claims/redrive-reason/"),
		).To(BeEmpty())
	})
})
