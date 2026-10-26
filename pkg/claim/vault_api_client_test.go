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
		lastQuery  string
		lastBody   []byte
		lastHeader http.Header
		status     int
		body       string
	)

	BeforeEach(func() {
		ctx = context.Background()
		lastMethod, lastPath, lastQuery, lastBody, lastHeader = "", "", "", nil, nil
		status, body = http.StatusOK, ""
		server = httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				lastMethod = r.Method
				lastPath = r.URL.Path
				lastQuery = r.URL.RawQuery
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
			acquired, err := client.Acquire(ctx, claim.NewTaskClaim("t-1"), "holder-a", "", 120)
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
			_, err := client.Acquire(ctx, claim.NewTaskClaim("t-1"), "holder-a", "", 120)
			Expect(err).To(BeNil())
			Expect(lastHeader.Get("X-Gateway-Secret")).To(Equal("secret-value"))
			Expect(lastHeader.Get("X-Gateway-Initator")).To(Equal("agent-task-executor"))
		})

		It("addresses a repository claim by owner and repo, carrying the claimant task id", func() {
			// The server requires task_id on a repository write and answers 400
			// missing_task_id without it, because the transport stores a task id on
			// every ref. Omitting it here would make every repo claim fail at its
			// first write, and the client would report a status error rather than
			// the real cause.
			_, err := client.Acquire(
				ctx,
				claim.NewRepoClaim("bborbe", "nuke"),
				"holder-a",
				"claimant-task",
				120,
			)
			Expect(err).To(BeNil())
			Expect(lastPath).To(Equal("/api/v1/repos/bborbe/nuke/claim"))
			Expect(string(lastBody)).To(ContainSubstring(`"task_id":"claimant-task"`))
		})

		It("names the holder when the claim is held", func() {
			status = http.StatusConflict
			body = `{"error":"claim_held","holder":"holder-b",` +
				`"lease_until":"2026-10-25T19:56:34Z"}`

			_, err := client.Acquire(ctx, claim.NewTaskClaim("t-1"), "holder-a", "", 120)
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

			_, err := client.Acquire(ctx, claim.NewTaskClaim("t-1"), "holder-a", "", 120)
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

			renewed, err := client.Renew(ctx, claim.NewTaskClaim("t-1"), "tok-1", "", 60)
			Expect(err).To(BeNil())
			Expect(renewed.LeaseToken).To(Equal("tok-1"))
			Expect(lastPath).To(Equal("/api/v1/tasks/t-1/claim/renew"))
			Expect(string(lastBody)).To(ContainSubstring(`"lease_token":"tok-1"`))
		})

		It("returns ErrLeaseLost rather than retrying a stale token", func() {
			status = http.StatusConflict
			body = `{"error":"lease_lost"}`

			_, err := client.Renew(ctx, claim.NewTaskClaim("t-1"), "stale", "", 60)
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
			_, err := client.Acquire(ctx, claim.ClaimTarget{}, "holder-a", "", 120)
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, claim.ErrClaimTargetInvalid)).To(BeTrue())
			Expect(lastMethod).To(Equal(""))
		})

		// The regression this case exists for: renewPath() was path()+"/renew",
		// which for an empty target is "/renew" — non-empty, so the guard never
		// fired and the client posted to baseURL+"/renew" instead of refusing.
		It("is refused locally by Renew, without a request", func() {
			_, err := client.Renew(ctx, claim.ClaimTarget{}, "tok-1", "", 60)
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
			// Renew carries the claimant task id too: the server routes renew and
			// acquire through the same task-id check, so an empty one here is the
			// same 400 the acquire would get.
			_, err := client.Renew(
				ctx,
				claim.NewRepoClaim("bborbe", "nuke"),
				"tok-1",
				"claimant-task",
				60,
			)
			Expect(err).To(BeNil())
			Expect(lastPath).To(Equal("/api/v1/repos/bborbe/nuke/claim/renew"))
			Expect(string(lastBody)).To(ContainSubstring(`"task_id":"claimant-task"`))

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

			_, err := client.Acquire(ctx, claim.NewTaskClaim("t-1"), "holder-a", "", 120)
			Expect(err).NotTo(BeNil())
		})
	})

	Context("an unexpected status", func() {
		It("exposes the status code on Renew, Release and Read", func() {
			status = http.StatusBadGateway
			body = `{"error":"upstream"}`

			_, err := client.Renew(ctx, claim.NewTaskClaim("t-1"), "tok-1", "", 60)
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

			_, err := client.Acquire(ctx, ambiguous, "holder-a", "", 120)
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

	Describe("per-PR target", func() {
		It("addresses one pull request rather than the whole repository", func() {
			status = http.StatusCreated
			body = `{"scope":"pr","id":"bborbe/nuke/7","holder":"holder-a",` +
				`"task_id":"claimant-task","lease_until":"2026-10-25T19:56:34Z",` +
				`"renew_before":"2026-10-25T19:55:34Z","lease_token":"tok-1"}`

			_, err := client.Acquire(
				ctx,
				claim.NewPRClaim("bborbe", "nuke", "7"),
				"holder-a",
				"claimant-task",
				120,
			)
			Expect(err).To(BeNil())
			Expect(lastPath).To(Equal("/api/v1/repos/bborbe/nuke/pulls/7/claim"))
			Expect(string(lastBody)).To(ContainSubstring(`"task_id":"claimant-task"`))
		})

		It("refuses a target that sets both a task and a pull request", func() {
			// The regression this exists for: isRepo() is false when only Number is
			// set, so this target failed the TaskID+repo ambiguity guard and then
			// matched the task branch — silently claiming a task the caller never
			// named, which is exactly what the type promises never to do.
			_, err := client.Acquire(
				ctx,
				claim.ClaimTarget{TaskID: "t-1", Number: "7"},
				"holder-a",
				"",
				120,
			)
			Expect(err).NotTo(BeNil())
			Expect(lastPath).To(BeEmpty())
		})

		It("refuses a number that is not digits, because PathEscape leaves .. alone", func() {
			// url.PathEscape does not escape ".", so a number of ".." would survive
			// as /pulls/../claim and a server that cleans paths could resolve it to
			// the repository route — claiming a different object than named.
			_, err := client.Acquire(
				ctx,
				claim.NewPRClaim("bborbe", "nuke", ".."),
				"holder-a",
				"claimant-task",
				120,
			)
			Expect(err).NotTo(BeNil())
			Expect(lastPath).To(BeEmpty())
		})

		It("refuses a task claim that carries a claimant task id", func() {
			// The server cannot catch this half: it derives the task id from the
			// address, so a supplied one is either ignored or accepted. Refused here
			// or nowhere.
			_, err := client.Acquire(
				ctx,
				claim.NewTaskClaim("t-1"),
				"holder-a",
				"someone-elses-task",
				120,
			)
			Expect(err).NotTo(BeNil())
			Expect(lastPath).To(BeEmpty())
		})

		It("refuses a repository claim that carries no claimant task id", func() {
			_, err := client.Acquire(ctx, claim.NewRepoClaim("bborbe", "nuke"), "holder-a", "", 120)
			Expect(err).NotTo(BeNil())
			Expect(lastPath).To(BeEmpty())
		})

		It("refuses a number with no repository instead of inventing a path", func() {
			// The route has no shape for a number alone. Guessing one would claim a
			// key the server never serves, and the caller would read the resulting
			// 404 as "somebody else holds it".
			_, err := client.Acquire(
				ctx,
				claim.ClaimTarget{Number: "7"},
				"holder-a",
				"claimant-task",
				120,
			)
			Expect(err).NotTo(BeNil())
			Expect(lastPath).To(BeEmpty())
		})
	})

	Describe("list", func() {
		It("enumerates claims without a target, because there is no object to name", func() {
			status = http.StatusOK
			body = `{"items":[{"scope":"repo","id":"bborbe/nuke","holder":"holder-a",` +
				`"task_id":"task-9","lease_until":"2026-10-25T19:56:34Z",` +
				`"renew_before":"2026-10-25T19:55:34Z","lease_token":"tok-1"}],` +
				`"total":1,"skipped":2}`

			claims, err := client.List(ctx, "")
			Expect(err).To(BeNil())
			Expect(lastPath).To(Equal("/api/v1/claims"))
			Expect(claims).To(HaveLen(1))
			// task_id is what makes a listed claim groupable: recovery pairs a repo
			// claim with its per-PR claim by it, so dropping it here would make the
			// listing useless to its only caller.
			Expect(claims[0].TaskID).To(Equal("task-9"))
			Expect(claims[0].Scope).To(Equal("repo"))
			Expect(claims[0].LeaseToken).To(Equal("tok-1"))
		})

		It("narrows to one holder", func() {
			status = http.StatusOK
			body = `{"items":[],"total":0,"skipped":0}`

			_, err := client.List(ctx, "holder a")
			Expect(err).To(BeNil())
			Expect(lastPath).To(Equal("/api/v1/claims"))
			// Escaped, because a holder is an opaque string and an unescaped space
			// or ampersand would silently change which claims come back.
			Expect(lastQuery).To(Equal("holder=holder+a"))
		})

		It("escapes an ampersand in the holder, which would otherwise split the query", func() {
			status = http.StatusOK
			body = `{"items":[],"total":0,"skipped":0}`

			_, err := client.List(ctx, "a&b=c")
			Expect(err).To(BeNil())
			// Unescaped, this would arrive as two parameters and the server would
			// filter on "a" — a different holder, silently.
			Expect(lastQuery).To(Equal("holder=a%26b%3Dc"))
		})

		It("fails the whole listing when an item is missing its lease", func() {
			// The point of sharing claimFromResponse: a listing item must not default
			// a missing lease and hand back a claim that can be neither renewed nor
			// trusted. This exercises that guarantee through List, not through Read.
			status = http.StatusOK
			body = `{"items":[{"scope":"repo","id":"bborbe/nuke","holder":"holder-a",` +
				`"lease_token":"tok-1"}],"total":1,"skipped":0}`

			_, err := client.List(ctx, "holder-a")
			Expect(err).NotTo(BeNil())
		})

		It("fails rather than returning an empty list when the server errors", func() {
			// An empty list reads as "this holder holds nothing", which is exactly
			// the wrong conclusion for a restarting executor: it would skip
			// recovering claims it still holds.
			status = http.StatusInternalServerError
			body = `{"error":"claim_read_failed"}`

			_, err := client.List(ctx, "holder-a")
			Expect(err).NotTo(BeNil())
		})
	})
})
