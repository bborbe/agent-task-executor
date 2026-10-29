// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	stderrors "errors"
	"net/http"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

var _ = Describe("SchemeGuard", func() {
	var (
		ctx  context.Context
		fake *fakeGitRest
	)

	BeforeEach(func() {
		ctx = context.Background()
		fake = newFakeGitRest()
		DeferCleanup(fake.Close)
	})

	// newGuard builds a guard over the fake with the shared test clock.
	newGuard := func() claim.SchemeGuard {
		return claim.NewSchemeGuard(
			claim.NewClaimRefClient(fake.URL(), "", ""),
			"nuke-prod",
			newTestClock(leaseTestT0),
		)
	}

	// schemeCount reads one series of the guard's counter.
	schemeCount := func(result string) float64 {
		return testutil.ToFloat64(metrics.ClaimSchemeTotal.WithLabelValues(result))
	}

	// expectNoClaimRefWritten asserts the guard never wrote a ref that could act
	// as a claim. A guard that refuses and still writes is worse than no guard:
	// it holds the work it refused to run.
	expectNoClaimRefWritten := func() {
		for _, req := range fake.Requests() {
			if req.Method != http.MethodPut {
				continue
			}
			key := strings.TrimPrefix(req.Path, "/api/v1/refs/")
			Expect(key).To(
				Equal(claim.ClaimSchemeRef),
				"the guard must never write a claim ref, wrote %q",
				key,
			)
		}
	}

	Context("when the fleet declares the scheme this binary derives", func() {
		BeforeEach(func() {
			fake.Seed(
				"refs/"+claim.ClaimSchemeRef,
				"aaaa",
				"nuke-dev",
				claim.ClaimKeySchemeVersion,
				"2099-01-01T00:00:00Z",
			)
		})

		It("admits the claim", func() {
			Expect(newGuard().Check(ctx)).To(Succeed())
			expectNoClaimRefWritten()
		})

		It("counts the check as confirmed, not as absent", func() {
			before := schemeCount("confirmed")
			Expect(newGuard().Check(ctx)).To(Succeed())
			Expect(schemeCount("confirmed") - before).To(Equal(1.0))
		})
	})

	Context("when the fleet declares a different scheme", func() {
		BeforeEach(func() {
			fake.Seed(
				"refs/"+claim.ClaimSchemeRef,
				"bbbb",
				"nuke-dev",
				"v1",
				"2099-01-01T00:00:00Z",
			)
		})

		It("refuses with ErrSchemeMismatch", func() {
			err := newGuard().Check(ctx)
			Expect(err).To(HaveOccurred())
			Expect(stderrors.Is(err, claim.ErrSchemeMismatch)).To(
				BeTrue(),
				"a refusal must be identifiable with errors.Is, got %v",
				err,
			)
		})

		It("writes no claim ref while refusing", func() {
			Expect(newGuard().Check(ctx)).ToNot(Succeed())
			expectNoClaimRefWritten()
		})

		It("leaves the declared value untouched", func() {
			Expect(newGuard().Check(ctx)).ToNot(Succeed())
			ref, found := fake.Ref("refs/" + claim.ClaimSchemeRef)
			Expect(found).To(BeTrue())
			Expect(ref.TaskID).To(Equal("v1"))
		})

		It("counts the refusal as a mismatch", func() {
			before := schemeCount("mismatch")
			Expect(newGuard().Check(ctx)).ToNot(Succeed())
			Expect(schemeCount("mismatch") - before).To(Equal(1.0))
		})
	})

	Context("when no declaration exists", func() {
		It("bootstraps this binary's scheme and admits the claim", func() {
			Expect(newGuard().Check(ctx)).To(Succeed())
			ref, found := fake.Ref("refs/" + claim.ClaimSchemeRef)
			Expect(found).To(BeTrue())
			Expect(ref.TaskID).To(Equal(claim.ClaimKeySchemeVersion))
			Expect(ref.Cluster).To(Equal("nuke-prod"))
		})

		It("bootstraps only once across two checks", func() {
			guard := newGuard()
			Expect(guard.Check(ctx)).To(Succeed())
			Expect(guard.Check(ctx)).To(Succeed())
			puts := 0
			for _, req := range fake.Requests() {
				if req.Method == http.MethodPut {
					puts++
				}
			}
			Expect(puts).To(Equal(1), "the second check must read, not re-declare")
		})
	})

	Context("when the declaration cannot be read", func() {
		BeforeEach(func() {
			fake.SetFault(func(req fakeRequest) *fakeFault {
				if req.Method == http.MethodGet {
					return &fakeFault{
						Status: http.StatusInternalServerError,
						Body:   `{"error":"boom"}`,
					}
				}
				return nil
			})
		})

		It("admits the claim rather than stopping the fleet", func() {
			Expect(newGuard().Check(ctx)).To(Succeed())
		})

		It("writes nothing, so an unreadable store is not overwritten", func() {
			Expect(newGuard().Check(ctx)).To(Succeed())
			expectNoClaimRefWritten()
			_, found := fake.Ref("refs/" + claim.ClaimSchemeRef)
			Expect(found).To(BeFalse())
		})
	})

	Context("when a peer wins the bootstrap race with a different scheme", func() {
		// The first list reports an empty store, the peer's declaration then
		// appears, and this guard's create-only write loses the compare-and-swap.
		// This is the window the guard exists for: two clusters upgrading at once
		// must converge on one declaration instead of each believing it declared.
		BeforeEach(func() {
			listCalls := 0
			fake.SetFault(func(req fakeRequest) *fakeFault {
				if req.Method == http.MethodGet {
					listCalls++
					if listCalls == 1 {
						return &fakeFault{Status: http.StatusOK, Body: `[]`}
					}
				}
				return nil
			})
			fake.Seed(
				"refs/"+claim.ClaimSchemeRef,
				"cccc",
				"nuke-dev",
				"v1",
				"2099-01-01T00:00:00Z",
			)
		})

		It("judges the peer's value instead of assuming it declared", func() {
			err := newGuard().Check(ctx)
			Expect(err).To(HaveOccurred())
			Expect(stderrors.Is(err, claim.ErrSchemeMismatch)).To(BeTrue())
		})
	})
})
