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
			acquired, err := client.Acquire(ctx, claim.TaskClaim("t-1"), "holder-a", 120)
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
			_, err := client.Acquire(ctx, claim.TaskClaim("t-1"), "holder-a", 120)
			Expect(err).To(BeNil())
			Expect(lastHeader.Get("X-Gateway-Secret")).To(Equal("secret-value"))
			Expect(lastHeader.Get("X-Gateway-Initator")).To(Equal("agent-task-executor"))
		})

		It("addresses a repository claim by owner and repo", func() {
			_, err := client.Acquire(ctx, claim.RepoClaim("bborbe", "nuke"), "holder-a", 120)
			Expect(err).To(BeNil())
			Expect(lastPath).To(Equal("/api/v1/repos/bborbe/nuke/claim"))
		})

		It("names the holder when the claim is held", func() {
			status = http.StatusConflict
			body = `{"error":"claim_held","holder":"holder-b",` +
				`"lease_until":"2026-10-25T19:56:34Z"}`

			_, err := client.Acquire(ctx, claim.TaskClaim("t-1"), "holder-a", 120)
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

			_, err := client.Acquire(ctx, claim.TaskClaim("t-1"), "holder-a", 120)
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

			renewed, err := client.Renew(ctx, claim.TaskClaim("t-1"), "tok-1", 60)
			Expect(err).To(BeNil())
			Expect(renewed.LeaseToken).To(Equal("tok-1"))
			Expect(lastPath).To(Equal("/api/v1/tasks/t-1/claim/renew"))
			Expect(string(lastBody)).To(ContainSubstring(`"lease_token":"tok-1"`))
		})

		It("returns ErrLeaseLost rather than retrying a stale token", func() {
			status = http.StatusConflict
			body = `{"error":"lease_lost"}`

			_, err := client.Renew(ctx, claim.TaskClaim("t-1"), "stale", 60)
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrLeaseLost)).To(BeTrue())
		})
	})

	Context("Release", func() {
		It("deletes with the token in the body", func() {
			status = http.StatusNoContent
			body = ""

			Expect(client.Release(ctx, claim.TaskClaim("t-1"), "tok-1")).To(BeNil())
			Expect(lastMethod).To(Equal(http.MethodDelete))
			Expect(lastPath).To(Equal("/api/v1/tasks/t-1/claim"))
			// A token sent as a header answers 400 invalid_body, so the body
			// placement is part of the contract.
			Expect(string(lastBody)).To(ContainSubstring(`"lease_token":"tok-1"`))
		})

		It("returns ErrLeaseLost for a token that is no longer current", func() {
			status = http.StatusConflict
			body = `{"error":"lease_lost"}`

			err := client.Release(ctx, claim.TaskClaim("t-1"), "stale")
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

			read, err := client.Read(ctx, claim.TaskClaim("t-1"))
			Expect(err).To(BeNil())
			Expect(read.Holder).To(Equal("holder-a"))
			Expect(lastMethod).To(Equal(http.MethodGet))
		})

		It("reports a free claim as ErrClaimFree", func() {
			status = http.StatusNotFound
			body = `{"error":"claim_free"}`

			_, err := client.Read(ctx, claim.TaskClaim("t-1"))
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrClaimFree)).To(BeTrue())
		})
	})

	Context("an unaddressable target", func() {
		It("is refused locally, without a request", func() {
			_, err := client.Acquire(ctx, claim.ClaimTarget{}, "holder-a", 120)
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrClaimTargetInvalid)).To(BeTrue())
			// An empty path segment would reach the server as a 404 that reads
			// like a missing task, which is the confusion this refusal prevents.
			Expect(lastMethod).To(Equal(""))
		})
	})
})
