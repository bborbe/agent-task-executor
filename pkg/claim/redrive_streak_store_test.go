// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	"flag"
	"io"
	"net/http"
	"os"
	"strconv"

	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// captureStderr runs fn with glog's stderr output redirected into a pipe and
// returns everything written. Specs in this suite run serially, so the global
// os.Stderr swap cannot race another spec's logging.
func captureStderr(fn func()) string {
	Expect(flag.Set("logtostderr", "true")).To(Succeed())
	defer func() {
		Expect(flag.Set("logtostderr", "false")).To(Succeed())
	}()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	Expect(err).ToNot(HaveOccurred())
	os.Stderr = writer

	fn()
	glog.Flush()

	os.Stderr = original
	Expect(writer.Close()).To(Succeed())
	out, err := io.ReadAll(reader)
	Expect(err).ToNot(HaveOccurred())
	Expect(reader.Close()).To(Succeed())
	return string(out)
}

// newStreakStore builds a store over the fake with the shared test clock.
func newStreakStore(fake *fakeGitRest) claim.RedriveStreakStore {
	return claim.NewRedriveStreakStore(
		claim.NewClaimRefClient(fake.URL(), "", ""),
		newTestClock(leaseTestT0),
	)
}

// seedStreak seeds one streak ref in the fake, with the lease a write at the
// test clock's instant would carry.
func seedStreak(fake *fakeGitRest, taskID, phase string, n int) {
	seedStreakLeased(
		fake,
		taskID,
		phase,
		n,
		leaseTestT0.Add(claim.RedriveStreakLeaseDuration),
	)
}

// seedStreakLeased seeds one streak ref with an explicit lease deadline, so a
// spec can model a ref written by an earlier process at a known time.
func seedStreakLeased(
	fake *fakeGitRest,
	taskID, phase string,
	n int,
	leaseUntil libtime.DateTime,
) {
	fake.Seed(
		"refs/"+claim.RedriveStreakPrefix+taskID+"/"+phase+"/"+strconv.Itoa(n),
		"sha-"+taskID+"-"+phase+"-"+strconv.Itoa(n),
		claim.RedriveStreakCluster,
		taskID,
		rfc3339(leaseUntil),
	)
}

// newStreakStoreAt builds a store over the fake with a settable clock, so a
// spec can advance past the release window without sleeping.
func newStreakStoreAt(
	fake *fakeGitRest,
	clock libtime.CurrentDateTime,
) claim.RedriveStreakStore {
	return claim.NewRedriveStreakStore(
		claim.NewClaimRefClient(fake.URL(), "", ""),
		clock,
	)
}

// faultOnMethod answers status for every request with method.
func faultOnMethod(method string, status int) func(fakeRequest) *fakeFault {
	return func(request fakeRequest) *fakeFault {
		if request.Method == method {
			return &fakeFault{Status: status}
		}
		return nil
	}
}

var _ = Describe("RedriveStreakStore", func() {
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

	It("counts refs of one task at one phase across independent instances", func() {
		seedStreak(fake, "T", "ai_review", 1)
		seedStreak(fake, "T", "ai_review", 2)
		store := newStreakStore(fake)

		count, err := store.Count(ctx, "T", "ai_review")
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(2))

		other, err := store.Count(ctx, "T", "planning")
		Expect(err).NotTo(HaveOccurred())
		Expect(other).To(Equal(0))
	})

	It("answers every task and phase from one snapshot call", func() {
		seedStreak(fake, "T", "ai_review", 1)
		seedStreak(fake, "T", "ai_review", 2)
		seedStreak(fake, "T", "planning", 1)
		seedStreak(fake, "U", "execution", 1)
		store := newStreakStore(fake)

		snapshot, err := store.Snapshot(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(snapshot["T"]["ai_review"]).To(Equal(2))
		Expect(snapshot["T"]["planning"]).To(Equal(1))
		Expect(snapshot["U"]["execution"]).To(Equal(1))
	})

	It("agrees with Count for every task it holds", func() {
		seedStreak(fake, "T", "ai_review", 1)
		seedStreak(fake, "T", "ai_review", 2)
		seedStreak(fake, "U", claim.RedriveStreakPhaseNone, 1)
		store := newStreakStore(fake)

		snapshot, err := store.Snapshot(ctx)
		Expect(err).NotTo(HaveOccurred())

		for taskID, phases := range snapshot {
			for phase, want := range phases {
				count, countErr := store.Count(ctx, taskID, phase)
				Expect(countErr).NotTo(HaveOccurred())
				Expect(count).To(Equal(want))
			}
		}
	})

	It("skips an unparsable ref in a snapshot", func() {
		seedStreak(fake, "T", "ai_review", 1)
		fake.Seed(
			"refs/claims/redrive/T/ai_review/not-a-number",
			"sha-odd",
			claim.RedriveStreakCluster,
			"T",
			rfc3339(leaseTestT0),
		)
		store := newStreakStore(fake)

		snapshot, err := store.Snapshot(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(snapshot["T"]["ai_review"]).To(Equal(1))
	})

	It("fails the snapshot when the list fails", func() {
		seedStreak(fake, "T", "ai_review", 1)
		fake.SetFault(faultOnMethod(http.MethodGet, http.StatusInternalServerError))
		store := newStreakStore(fake)

		_, err := store.Snapshot(ctx)
		Expect(err).To(HaveOccurred())
	})

	It("shares the recorded streak between separately constructed stores", func() {
		storeA := newStreakStore(fake)
		Expect(storeA.Record(ctx, "U", "ai_review")).To(Succeed())
		Expect(storeA.Record(ctx, "U", "ai_review")).To(Succeed())

		storeB := newStreakStore(fake)
		count, err := storeB.Count(ctx, "U", "ai_review")
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(2))
	})

	It("records the exact stored shape with an empty expected sha", func() {
		store := newStreakStore(fake)
		Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())

		ref, ok := fake.Ref("refs/claims/redrive/T/ai_review/1")
		Expect(ok).To(BeTrue())
		Expect(ref.Cluster).To(Equal("redrive-streak"))
		Expect(ref.TaskID).To(Equal("T"))
		Expect(ref.LeaseUntil).To(Equal(rfc3339(leaseTestT0.Add(6 * libtime.Hour))))

		puts := requestsMatching(fake.Requests(), http.MethodPut, "claims/redrive/")
		Expect(puts).NotTo(BeEmpty())
		for _, request := range puts {
			Expect(putExpectedSha(request)).To(Equal(""))
		}
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
		store := newStreakStore(fake)
		Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())

		_, ok1 := fake.Ref("refs/claims/redrive/T/ai_review/1")
		_, ok2 := fake.Ref("refs/claims/redrive/T/ai_review/2")
		Expect(ok1).To(BeTrue())
		Expect(ok2).To(BeTrue())

		puts := requestsMatching(fake.Requests(), http.MethodPut, "claims/redrive/")
		Expect(puts).To(HaveLen(2))
		Expect(puts[1].Path).To(HaveSuffix("claims/redrive/T/ai_review/2"))
	})

	It("stops after three conflicting creates", func() {
		fake.SetFault(faultAllPuts(http.StatusConflict, false))
		store := newStreakStore(fake)

		err := store.Record(ctx, "T", "ai_review")
		Expect(err).To(HaveOccurred())

		puts := requestsMatching(fake.Requests(), http.MethodPut, "claims/redrive/")
		Expect(puts).To(HaveLen(3))
	})

	It("resets the streak on a phase change and keeps other tasks", func() {
		seedStreak(fake, "OTHER", "ai_review", 1)
		store := newStreakStore(fake)
		Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())
		Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())
		Expect(store.Record(ctx, "T", "planning")).To(Succeed())

		_, ok1 := fake.Ref("refs/claims/redrive/T/ai_review/1")
		_, ok2 := fake.Ref("refs/claims/redrive/T/ai_review/2")
		_, planning := fake.Ref("refs/claims/redrive/T/planning/1")
		Expect(ok1).To(BeFalse())
		Expect(ok2).To(BeFalse())
		Expect(planning).To(BeTrue())

		_, other := fake.Ref("refs/claims/redrive/OTHER/ai_review/1")
		Expect(other).To(BeTrue())
	})

	It("clears every phase of one task and leaves other tasks", func() {
		seedStreak(fake, "OTHER", "ai_review", 1)
		store := newStreakStore(fake)
		Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())
		Expect(store.Record(ctx, "T", "planning")).To(Succeed())
		Expect(store.Clear(ctx, "T")).To(Succeed())

		_, ok1 := fake.Ref("refs/claims/redrive/T/ai_review/1")
		_, ok2 := fake.Ref("refs/claims/redrive/T/planning/1")
		Expect(ok1).To(BeFalse())
		Expect(ok2).To(BeFalse())

		_, other := fake.Ref("refs/claims/redrive/OTHER/ai_review/1")
		Expect(other).To(BeTrue())
	})

	It("ignores an unparsable ref when counting and recording", func() {
		fake.Seed(
			"refs/claims/redrive/T/ai_review/x",
			"sha-bad",
			claim.RedriveStreakCluster,
			"T",
			rfc3339(leaseTestT0),
		)
		store := newStreakStore(fake)

		count, err := store.Count(ctx, "T", "ai_review")
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(0))

		Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())

		_, bad := fake.Ref("refs/claims/redrive/T/ai_review/x")
		_, first := fake.Ref("refs/claims/redrive/T/ai_review/1")
		Expect(bad).To(BeTrue())
		Expect(first).To(BeTrue())
	})

	It("rejects invalid task ids and phases before any request", func() {
		store := newStreakStore(fake)

		_, err := store.Count(ctx, "bad/id", "ai_review")
		Expect(err).To(HaveOccurred())
		err = store.Record(ctx, "bad/id", "ai_review")
		Expect(err).To(HaveOccurred())
		err = store.Clear(ctx, "bad/id")
		Expect(err).To(HaveOccurred())

		_, err = store.Count(ctx, "T", "ai/review")
		Expect(err).To(HaveOccurred())
		err = store.Record(ctx, "T", "ai/review")
		Expect(err).To(HaveOccurred())

		Expect(fake.Requests()).To(BeEmpty())
	})

	It("maps an empty phase to the frozen none component", func() {
		store := newStreakStore(fake)
		Expect(store.Record(ctx, "T", "")).To(Succeed())

		_, ok := fake.Ref("refs/claims/redrive/T/none/1")
		Expect(ok).To(BeTrue())

		count, err := store.Count(ctx, "T", "")
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(1))
	})

	It("does not retry a non-conflict create error", func() {
		fake.SetFault(faultAllPuts(http.StatusInternalServerError, false))
		store := newStreakStore(fake)

		err := store.Record(ctx, "T", "ai_review")
		Expect(err).To(HaveOccurred())

		puts := requestsMatching(fake.Requests(), http.MethodPut, "claims/redrive/")
		Expect(puts).To(HaveLen(1))
	})

	It("treats an already-gone delete as success", func() {
		seedStreak(fake, "T", "ai_review", 1)
		fake.SetFault(faultOnMethod(http.MethodDelete, http.StatusNotFound))
		store := newStreakStore(fake)

		Expect(store.Clear(ctx, "T")).To(Succeed())
		_, ok := fake.Ref("refs/claims/redrive/T/ai_review/1")
		Expect(ok).To(BeTrue())
	})

	It("logs a failed delete and keeps attempting the rest", func() {
		store := newStreakStore(fake)
		Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())
		Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())
		fake.SetFault(faultOnMethod(http.MethodDelete, http.StatusInternalServerError))
		fake.ResetRequests()

		out := captureStderr(func() {
			Expect(store.Record(ctx, "T", "planning")).To(Succeed())
		})
		Expect(out).To(ContainSubstring("event=redrive_streak_write_failed"))

		deletes := requestsMatching(fake.Requests(), http.MethodDelete, "claims/redrive/")
		Expect(deletes).To(HaveLen(2))

		out = captureStderr(func() {
			Expect(store.Clear(ctx, "T")).To(Succeed())
		})
		Expect(out).To(ContainSubstring("event=redrive_streak_write_failed"))
	})

	It("fails count, record and clear when the list fails", func() {
		seedStreak(fake, "T", "ai_review", 1)
		fake.SetFault(faultOnMethod(http.MethodGet, http.StatusInternalServerError))
		store := newStreakStore(fake)

		_, err := store.Count(ctx, "T", "ai_review")
		Expect(err).To(HaveOccurred())
		err = store.Record(ctx, "T", "ai_review")
		Expect(err).To(HaveOccurred())
		err = store.Clear(ctx, "T")
		Expect(err).To(HaveOccurred())

		Expect(requestsMatching(fake.Requests(), http.MethodPut, "claims/redrive/")).To(BeEmpty())
		Expect(
			requestsMatching(fake.Requests(), http.MethodDelete, "claims/redrive/"),
		).To(BeEmpty())
	})

	It("refuses the cap inside one window and releases it after", func() {
		clock := newTestClock(leaseTestT0)
		store := newStreakStoreAt(fake, clock)
		for i := 0; i < 3; i++ {
			Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())
		}

		count, err := store.Count(ctx, "T", "ai_review")
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(
			Equal(3),
			"a still-failing task must stay capped inside one window",
		)

		advance(clock, claim.RedriveStreakLeaseDuration-libtime.Second)
		count, err = store.Count(ctx, "T", "ai_review")
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(3))

		advance(clock, 2*libtime.Second)
		count, err = store.Count(ctx, "T", "ai_review")
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(
			Equal(0),
			"a task capped by a past outage must be released by the window",
		)
	})

	It("releases the snapshot on the same window as Count", func() {
		clock := newTestClock(leaseTestT0)
		store := newStreakStoreAt(fake, clock)
		for i := 0; i < 3; i++ {
			Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())
		}
		advance(clock, claim.RedriveStreakLeaseDuration)

		snapshot, err := store.Snapshot(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(snapshot["T"]["ai_review"]).To(
			Equal(0),
			"the reconcile pass answers every cap check from this map, so an "+
				"expired ref left in it would keep the task capped forever",
		)
	})

	It("prunes expired refs so a released streak restarts at one", func() {
		clock := newTestClock(leaseTestT0)
		store := newStreakStoreAt(fake, clock)
		Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())
		Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())
		advance(clock, claim.RedriveStreakLeaseDuration+libtime.Second)

		Expect(store.Record(ctx, "T", "ai_review")).To(Succeed())

		_, kept := fake.Ref("refs/claims/redrive/T/ai_review/1")
		_, pruned := fake.Ref("refs/claims/redrive/T/ai_review/2")
		Expect(kept).To(BeTrue())
		Expect(pruned).To(BeFalse())

		count, err := store.Count(ctx, "T", "ai_review")
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(1))
	})

	It("keeps a ref whose lease cannot be read, so the cap cannot weaken", func() {
		seedStreakLeased(fake, "T", "ai_review", 1, leaseTestT0)
		fake.Seed(
			"refs/claims/redrive/T/ai_review/2",
			"sha-2",
			claim.RedriveStreakCluster,
			"T",
			"not-a-timestamp",
		)
		clock := newTestClock(leaseTestT0.Add(claim.RedriveStreakLeaseDuration + libtime.Hour))
		store := newStreakStoreAt(fake, clock)

		count, err := store.Count(ctx, "T", "ai_review")
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(
			Equal(1),
			"the expired ref drops out and the unreadable one stays counted",
		)
	})
})
