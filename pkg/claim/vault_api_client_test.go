// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	stderrors "errors"
	"io"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

var _ = Describe("VaultAPIClaimClient", func() {
	var (
		ctx        context.Context
		server     *httptest.Server
		client     claim.ClaimClient
		lastMethod string
		lastPath   string
		lastBody   []byte
		lastHeader http.Header
		status     int
		body       string
	)

	BeforeEach(func() {
		ctx = context.Background()
		lastMethod, lastPath, lastBody, lastHeader = "", "", nil, nil
		status, body = http.StatusOK, ""
		server = httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				lastMethod = r.Method
				lastPath = r.URL.Path
				lastHeader = r.Header.Clone()
				lastBody, _ = io.ReadAll(r.Body)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(body))
			},
		))
		client = claim.NewVaultAPIClaimClient(server.URL, "secret-value", "agent-task-executor")
	})

	AfterEach(func() { server.Close() })

	Context("Acquire", func() {
		BeforeEach(func() {
			status = http.StatusCreated
			body = `{"scope":"task","id":"t-1","holder":"holder-a",` +
				`"lease_until":"2026-10-25T19:56:34Z","renew_before":"2026-10-25T19:55:34Z",` +
				`"lease_token":"tok-1"}`
		})

		It("posts the holder and TTL to the task's claim route", func() {
			acquired, err := client.Acquire(ctx, claim.NewTaskClaim("t-1"), "holder-a", 120)
			Expect(err).To(BeNil())
			Expect(acquired).NotTo(BeNil())
			Expect(acquired.LeaseToken).To(Equal("tok-1"))
			Expect(acquired.Holder).To(Equal("holder-a"))
			Expect(acquired.Scope).To(Equal("task"))
			Expect(acquired.LeaseUntil.IsZero()).To(BeFalse())

			Expect(lastMethod).To(Equal(http.MethodPost))
			Expect(lastPath).To(Equal("/api/v1/tasks/t-1/claim"))
			Expect(string(lastBody)).To(ContainSubstring(`"holder":"holder-a"`))
			Expect(string(lastBody)).To(ContainSubstring(`"ttl_seconds":120`))
			// A missing holder is how the API would read an empty one, so the
			// field must be present rather than omitted.
			Expect(lastBody).NotTo(ContainSubstring("lease_token"))
		})

		It("sends the gateway headers", func() {
			_, err := client.Acquire(ctx, claim.NewTaskClaim("t-1"), "holder-a", 120)
			Expect(err).To(BeNil())
			Expect(lastHeader.Get("X-Gateway-Secret")).To(Equal("secret-value"))
			Expect(lastHeader.Get("X-Gateway-Initator")).To(Equal("agent-task-executor"))
		})

		It("addresses a repository claim by owner and repo", func() {
			_, err := client.Acquire(ctx, claim.NewRepoClaim("bborbe", "nuke"), "holder-a", 120)
			Expect(err).To(BeNil())
			Expect(lastPath).To(Equal("/api/v1/repos/bborbe/nuke/claim"))
		})

		It("names the holder when the claim is held", func() {
			status = http.StatusConflict
			body = `{"error":"claim_held","holder":"holder-b",` +
				`"lease_until":"2026-10-25T19:56:34Z"}`

			_, err := client.Acquire(ctx, claim.NewTaskClaim("t-1"), "holder-a", 120)
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrClaimHeld)).To(BeTrue())

			var held *claim.ClaimHeldError
			Expect(stderrors.As(err, &held)).To(BeTrue())
			Expect(held.Holder).To(Equal("holder-b"))
			Expect(held.LeaseUntil.IsZero()).To(BeFalse())
		})

		It("does not echo the response body into the error", func() {
			status = http.StatusInternalServerError
			body = `{"error":"boom","lease_token":"secret-token"}`

			_, err := client.Acquire(ctx, claim.NewTaskClaim("t-1"), "holder-a", 120)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).NotTo(ContainSubstring("secret-token"))
			Expect(err.Error()).To(ContainSubstring("500"))
		})
	})

	Context("Renew", func() {
		It("posts the token to the renew route", func() {
			status = http.StatusOK
			body = `{"scope":"task","id":"t-1","holder":"holder-a",` +
				`"lease_until":"2026-10-25T20:00:00Z","renew_before":"2026-10-25T19:59:00Z",` +
				`"lease_token":"tok-1"}`

			renewed, err := client.Renew(ctx, claim.NewTaskClaim("t-1"), "tok-1", 60)
			Expect(err).To(BeNil())
			Expect(renewed.LeaseToken).To(Equal("tok-1"))
			Expect(lastPath).To(Equal("/api/v1/tasks/t-1/claim/renew"))
			Expect(string(lastBody)).To(ContainSubstring(`"lease_token":"tok-1"`))
		})

		It("returns ErrLeaseLost rather than retrying a stale token", func() {
			status = http.StatusConflict
			body = `{"error":"lease_lost"}`

			_, err := client.Renew(ctx, claim.NewTaskClaim("t-1"), "stale", 60)
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrLeaseLost)).To(BeTrue())
		})
	})

	Context("Release", func() {
		It("deletes with the token in the body", func() {
			status = http.StatusNoContent
			body = ""

			Expect(client.Release(ctx, claim.NewTaskClaim("t-1"), "tok-1")).To(BeNil())
			Expect(lastMethod).To(Equal(http.MethodDelete))
			Expect(lastPath).To(Equal("/api/v1/tasks/t-1/claim"))
			// A token sent as a header answers 400 invalid_body, so the body
			// placement is part of the contract.
			Expect(string(lastBody)).To(ContainSubstring(`"lease_token":"tok-1"`))
		})

		It("returns ErrLeaseLost for a token that is no longer current", func() {
			status = http.StatusConflict
			body = `{"error":"lease_lost"}`

			err := client.Release(ctx, claim.NewTaskClaim("t-1"), "stale")
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrLeaseLost)).To(BeTrue())
		})
	})

	Context("Read", func() {
		It("returns the claim", func() {
			status = http.StatusOK
			body = `{"scope":"task","id":"t-1","holder":"holder-a",` +
				`"lease_until":"2026-10-25T19:56:34Z","renew_before":"2026-10-25T19:55:34Z",` +
				`"lease_token":"tok-1"}`

			read, err := client.Read(ctx, claim.NewTaskClaim("t-1"))
			Expect(err).To(BeNil())
			Expect(read.Holder).To(Equal("holder-a"))
			Expect(lastMethod).To(Equal(http.MethodGet))
		})

		It("reports a free claim as ErrClaimFree", func() {
			status = http.StatusNotFound
			body = `{"error":"claim_free"}`

			_, err := client.Read(ctx, claim.NewTaskClaim("t-1"))
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrClaimFree)).To(BeTrue())
		})
	})

	Context("an unaddressable target", func() {
		It("is refused locally by Acquire, without a request", func() {
			_, err := client.Acquire(ctx, claim.ClaimTarget{}, "holder-a", 120)
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrClaimTargetInvalid)).To(BeTrue())
			Expect(lastMethod).To(Equal(""))
		})

		// The regression this case exists for: renewPath() was path()+"/renew",
		// which for an empty target is "/renew" — non-empty, so the guard never
		// fired and the client posted to baseURL+"/renew" instead of refusing.
		It("is refused locally by Renew, without a request", func() {
			_, err := client.Renew(ctx, claim.ClaimTarget{}, "tok-1", 60)
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrClaimTargetInvalid)).To(BeTrue())
			Expect(lastMethod).To(Equal(""))
			Expect(lastPath).To(Equal(""))
		})

		It("is refused locally by Release, without a request", func() {
			err := client.Release(ctx, claim.ClaimTarget{}, "tok-1")
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrClaimTargetInvalid)).To(BeTrue())
			Expect(lastMethod).To(Equal(""))
		})

		It("is refused locally by Read, without a request", func() {
			_, err := client.Read(ctx, claim.ClaimTarget{})
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrClaimTargetInvalid)).To(BeTrue())
			Expect(lastMethod).To(Equal(""))
		})
	})

	Context("a repository target", func() {
		It("addresses renew, release and read by owner and repo", func() {
			status = http.StatusOK
			body = `{"scope":"repo","id":"bborbe/nuke","holder":"holder-a",` +
				`"lease_until":"2026-10-25T20:00:00Z","renew_before":"2026-10-25T19:59:00Z",` +
				`"lease_token":"tok-1"}`
			_, err := client.Renew(ctx, claim.NewRepoClaim("bborbe", "nuke"), "tok-1", 60)
			Expect(err).To(BeNil())
			Expect(lastPath).To(Equal("/api/v1/repos/bborbe/nuke/claim/renew"))

			status = http.StatusNoContent
			body = ""
			Expect(client.Release(ctx, claim.NewRepoClaim("bborbe", "nuke"), "tok-1")).To(BeNil())
			Expect(lastPath).To(Equal("/api/v1/repos/bborbe/nuke/claim"))

			status = http.StatusOK
			body = `{"scope":"repo","id":"bborbe/nuke","holder":"holder-a",` +
				`"lease_until":"2026-10-25T20:00:00Z","renew_before":"2026-10-25T19:59:00Z",` +
				`"lease_token":"tok-1"}`
			_, err = client.Read(ctx, claim.NewRepoClaim("bborbe", "nuke"))
			Expect(err).To(BeNil())
			Expect(lastMethod).To(Equal(http.MethodGet))
		})
	})

	Context("a malformed response", func() {
		It("fails on a body that is not JSON", func() {
			status = http.StatusOK
			body = `not json at all`

			_, err := client.Read(ctx, claim.NewTaskClaim("t-1"))
			Expect(err).NotTo(BeNil())
		})

		// A zero LeaseUntil would read as "already expired" to a caller
		// comparing against the clock, so a bad timestamp must fail loudly
		// rather than silently become the zero value.
		It("fails on an unparsable lease_until", func() {
			status = http.StatusOK
			body = `{"scope":"task","id":"t-1","holder":"holder-a",` +
				`"lease_until":"not-a-timestamp","renew_before":"2026-10-25T19:59:00Z",` +
				`"lease_token":"tok-1"}`

			_, err := client.Read(ctx, claim.NewTaskClaim("t-1"))
			Expect(err).NotTo(BeNil())
		})

		// A response missing lease_token cannot be renewed or released, so it is
		// schema drift and must fail rather than yield a half-usable Claim.
		It("fails when lease_token is absent", func() {
			status = http.StatusOK
			body = `{"scope":"task","id":"t-1","holder":"holder-a",` +
				`"lease_until":"2026-10-25T19:56:34Z","renew_before":"2026-10-25T19:55:34Z"}`

			_, err := client.Read(ctx, claim.NewTaskClaim("t-1"))
			Expect(err).NotTo(BeNil())
		})

		It("fails when lease_until is absent", func() {
			status = http.StatusOK
			body = `{"scope":"task","id":"t-1","holder":"holder-a","lease_token":"tok-1"}`

			_, err := client.Read(ctx, claim.NewTaskClaim("t-1"))
			Expect(err).NotTo(BeNil())
		})
	})

	Context("an unparsable held body", func() {
		// A zero LeaseUntil reads as "already expired", the opposite of held,
		// so a malformed timestamp must fail rather than fall back silently.
		It("fails instead of returning a zero lease", func() {
			status = http.StatusConflict
			body = `{"error":"claim_held","holder":"holder-b","lease_until":"nonsense"}`

			_, err := client.Acquire(ctx, claim.NewTaskClaim("t-1"), "holder-a", 120)
			Expect(err).NotTo(BeNil())
		})
	})

	Context("an unexpected status", func() {
		It("exposes the status code on Renew, Release and Read", func() {
			status = http.StatusBadGateway
			body = `{"error":"upstream"}`

			_, err := client.Renew(ctx, claim.NewTaskClaim("t-1"), "tok-1", 60)
			var renewErr *claim.StatusError
			Expect(stderrors.As(err, &renewErr)).To(BeTrue())
			Expect(renewErr.Status).To(Equal(http.StatusBadGateway))
			Expect(renewErr.Retryable()).To(BeTrue())

			err = client.Release(ctx, claim.NewTaskClaim("t-1"), "tok-1")
			var releaseErr *claim.StatusError
			Expect(stderrors.As(err, &releaseErr)).To(BeTrue())
			Expect(releaseErr.Status).To(Equal(http.StatusBadGateway))

			_, err = client.Read(ctx, claim.NewTaskClaim("t-1"))
			var readErr *claim.StatusError
			Expect(stderrors.As(err, &readErr)).To(BeTrue())
			Expect(readErr.Status).To(Equal(http.StatusBadGateway))
		})

		It("marks a 4xx as not retryable", func() {
			status = http.StatusBadRequest
			body = `{"error":"invalid_body"}`

			_, err := client.Read(ctx, claim.NewTaskClaim("t-1"))
			var readErr *claim.StatusError
			Expect(stderrors.As(err, &readErr)).To(BeTrue())
			Expect(readErr.Retryable()).To(BeFalse())
		})
	})

	Context("an ambiguous target", func() {
		// Both forms set is a caller bug; picking the task silently would act on
		// a different object than the one named.
		It("is refused locally rather than resolved to the task", func() {
			ambiguous := claim.ClaimTarget{TaskID: "t-1", Owner: "bborbe", Repo: "nuke"}

			_, err := client.Acquire(ctx, ambiguous, "holder-a", 120)
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrClaimTargetInvalid)).To(BeTrue())
			Expect(lastMethod).To(Equal(""))
		})
	})

	Context("an empty gateway secret", func() {
		It("sends no auth headers", func() {
			noAuth := claim.NewVaultAPIClaimClient(server.URL, "", "")
			status = http.StatusOK
			body = `{"scope":"task","id":"t-1","holder":"holder-a",` +
				`"lease_until":"2026-10-25T19:56:34Z","renew_before":"2026-10-25T19:55:34Z",` +
				`"lease_token":"tok-1"}`

			_, err := noAuth.Read(ctx, claim.NewTaskClaim("t-1"))
			Expect(err).To(BeNil())
			Expect(lastHeader.Get("X-Gateway-Secret")).To(BeEmpty())
			Expect(lastHeader.Get("X-Gateway-Initator")).To(BeEmpty())
		})
	})

	Context("a baseURL with a trailing slash", func() {
		It("does not double the slash in the path", func() {
			slashed := claim.NewVaultAPIClaimClient(server.URL+"/", "s", "i")
			status = http.StatusOK
			body = `{"scope":"task","id":"t-1","holder":"holder-a",` +
				`"lease_until":"2026-10-25T19:56:34Z","renew_before":"2026-10-25T19:55:34Z",` +
				`"lease_token":"tok-1"}`

			_, err := slashed.Read(ctx, claim.NewTaskClaim("t-1"))
			Expect(err).To(BeNil())
			Expect(lastPath).To(Equal("/api/v1/tasks/t-1/claim"))
		})
	})
})
