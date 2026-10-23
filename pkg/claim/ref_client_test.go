// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

var _ = Describe("ClaimRefClient", func() {
	var (
		ctx    context.Context
		server *httptest.Server
		client claim.ClaimRefClient
	)

	BeforeEach(func() {
		ctx = context.Background()
		server = nil
	})

	AfterEach(func() {
		if server != nil {
			server.Close()
		}
	})

	Describe("List", func() {
		Context("empty result", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						Expect(r.Method).To(Equal(http.MethodGet))
						Expect(r.URL.Path).To(Equal("/api/v1/refs"))
						Expect(r.URL.Query().Get("prefix")).To(Equal("claims/"))
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`[]`))
					}),
				)
				client = claim.NewClaimRefClient(server.URL, "", "agent-task-executor")
			})

			It("returns a zero-length slice and nil error", func() {
				refs, err := client.List(ctx, "claims/")
				Expect(err).NotTo(HaveOccurred())
				Expect(refs).To(BeEmpty())
			})
		})

		Context("two entries", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`[
							{"ref":"refs/claims/task/a","sha":"S0","cluster":"nuke","task_id":"a","lease_until":"2026-04-18T12:00:00Z"},
							{"ref":"refs/claims/task/b","sha":"S1","cluster":"sun","task_id":"b","lease_until":"2026-04-18T13:00:00Z"}
						]`))
					}),
				)
				client = claim.NewClaimRefClient(server.URL, "", "agent-task-executor")
			})

			It("decodes every field", func() {
				refs, err := client.List(ctx, "claims/")
				Expect(err).NotTo(HaveOccurred())
				Expect(refs).To(HaveLen(2))
				Expect(refs[0]).To(Equal(claim.ClaimRef{
					Ref:        "refs/claims/task/a",
					Sha:        "S0",
					Cluster:    "nuke",
					TaskID:     "a",
					LeaseUntil: "2026-04-18T12:00:00Z",
				}))
				Expect(refs[1].Ref).To(Equal("refs/claims/task/b"))
				Expect(refs[1].Sha).To(Equal("S1"))
				Expect(refs[1].Cluster).To(Equal("sun"))
				Expect(refs[1].TaskID).To(Equal("b"))
				Expect(refs[1].LeaseUntil).To(Equal("2026-04-18T13:00:00Z"))
			})
		})
	})

	Describe("Put", func() {
		Context("200 response", func() {
			var leaseUntil libtime.DateTime

			BeforeEach(func() {
				leaseUntil = libtimetest.ParseDateTime("2026-04-18T12:00:00Z")
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						Expect(r.Method).To(Equal(http.MethodPut))
						Expect(r.URL.Path).To(Equal("/api/v1/refs/claims/task/abc123"))
						body, err := io.ReadAll(r.Body)
						Expect(err).NotTo(HaveOccurred())
						var payload map[string]json.RawMessage
						Expect(json.Unmarshal(body, &payload)).To(Succeed())
						Expect(payload).To(HaveLen(4))
						for _, key := range []string{
							"expected_sha", "cluster", "task_id", "lease_until",
						} {
							Expect(payload).To(HaveKey(key))
						}
						var lease string
						Expect(json.Unmarshal(payload["lease_until"], &lease)).To(Succeed())
						parsed, err := time.Parse(time.RFC3339, lease)
						Expect(err).NotTo(HaveOccurred())
						Expect(parsed.Equal(leaseUntil.Time().UTC())).To(BeTrue())
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`{"ref":"refs/claims/task/abc123","sha":"S1"}`))
					}),
				)
				client = claim.NewClaimRefClient(server.URL, "", "agent-task-executor")
			})

			It("sends exactly the four CAS fields and returns the new sha", func() {
				sha, err := client.Put(ctx, "claims/task/abc123", "", "nuke", "abc123", leaseUntil)
				Expect(err).NotTo(HaveOccurred())
				Expect(sha).To(Equal("S1"))
			})
		})

		Context("key with a space in a segment", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						Expect(r.URL.EscapedPath()).To(ContainSubstring("%20"))
						Expect(r.URL.Path).To(Equal("/api/v1/refs/claims/repo/a b"))
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`{"ref":"refs/claims/repo/a b","sha":"S1"}`))
					}),
				)
				client = claim.NewClaimRefClient(server.URL, "", "agent-task-executor")
			})

			It("escapes the segment but keeps the separators", func() {
				_, err := client.Put(
					ctx,
					"claims/repo/a b",
					"",
					"nuke",
					"abc123",
					libtimetest.ParseDateTime("2026-04-18T12:00:00Z"),
				)
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})

	Describe("Delete", func() {
		Context("200 response", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						Expect(r.Method).To(Equal(http.MethodDelete))
						Expect(r.URL.Path).To(Equal("/api/v1/refs/claims/task/abc123"))
						Expect(r.URL.Query().Get("expected_sha")).To(Equal("S0"))
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`{"ok":true}`))
					}),
				)
				client = claim.NewClaimRefClient(server.URL, "", "agent-task-executor")
			})

			It("sends expected_sha and returns nil", func() {
				Expect(client.Delete(ctx, "claims/task/abc123", "S0")).To(Succeed())
			})
		})
	})

	Describe("auth headers", func() {
		var (
			seenSecret    string
			seenInitiator string
		)

		BeforeEach(func() {
			server = httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					seenSecret = r.Header.Get("X-Gateway-Secret")
					seenInitiator = r.Header.Get("X-Gateway-Initator")
					switch r.Method {
					case http.MethodPut:
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`{"ref":"refs/x","sha":"S1"}`))
					case http.MethodDelete:
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`{"ok":true}`))
					default:
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`[]`))
					}
				}),
			)
		})

		Context("secret configured", func() {
			BeforeEach(func() {
				client = claim.NewClaimRefClient(server.URL, "s3cret", "agent-task-executor")
			})

			It("sends both headers on every call", func() {
				calls := []func() error{
					func() error {
						_, err := client.List(ctx, "claims/")
						return err
					},
					func() error {
						_, err := client.Put(
							ctx,
							"claims/task/abc123",
							"",
							"nuke",
							"abc123",
							libtimetest.ParseDateTime("2026-04-18T12:00:00Z"),
						)
						return err
					},
					func() error {
						return client.Delete(ctx, "claims/task/abc123", "S0")
					},
				}
				for _, call := range calls {
					Expect(call()).To(Succeed())
					Expect(seenSecret).To(Equal("s3cret"))
					Expect(seenInitiator).To(Equal("agent-task-executor"))
				}
			})
		})

		Context("secret empty", func() {
			BeforeEach(func() {
				client = claim.NewClaimRefClient(server.URL, "", "agent-task-executor")
			})

			It("sends neither header on any call", func() {
				calls := []func() error{
					func() error {
						_, err := client.List(ctx, "claims/")
						return err
					},
					func() error {
						_, err := client.Put(
							ctx,
							"claims/task/abc123",
							"",
							"nuke",
							"abc123",
							libtimetest.ParseDateTime("2026-04-18T12:00:00Z"),
						)
						return err
					},
					func() error {
						return client.Delete(ctx, "claims/task/abc123", "S0")
					},
				}
				for _, call := range calls {
					Expect(call()).To(Succeed())
					Expect(seenSecret).To(BeEmpty())
					Expect(seenInitiator).To(BeEmpty())
				}
			})
		})
	})

	Describe("conflict mapping", func() {
		BeforeEach(func() {
			server = httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusConflict)
					_, _ = w.Write([]byte(`{"error":"lease mismatch"}`))
				}),
			)
			client = claim.NewClaimRefClient(server.URL, "", "agent-task-executor")
		})

		It("maps PUT 409 to ErrLeaseConflict", func() {
			_, err := client.Put(
				ctx,
				"claims/task/abc123",
				"S0",
				"nuke",
				"abc123",
				libtimetest.ParseDateTime("2026-04-18T12:00:00Z"),
			)
			Expect(err).To(HaveOccurred())
			Expect(stderrors.Is(err, claim.ErrLeaseConflict)).To(BeTrue())
		})

		It("maps DELETE 409 to ErrLeaseConflict", func() {
			err := client.Delete(ctx, "claims/task/abc123", "S0")
			Expect(err).To(HaveOccurred())
			Expect(stderrors.Is(err, claim.ErrLeaseConflict)).To(BeTrue())
		})
	})

	Describe("non-conflict failures", func() {
		It("returns an error naming the status code", func() {
			cases := []struct {
				method string
				status int
				body   string
				code   string
			}{
				{method: http.MethodPut, status: http.StatusBadRequest, body: "bad", code: "400"},
				{
					method: http.MethodPut,
					status: http.StatusInternalServerError,
					body:   "boom",
					code:   "500",
				},
				{
					method: http.MethodDelete,
					status: http.StatusInternalServerError,
					body:   "boom",
					code:   "500",
				},
				{method: http.MethodPut, status: http.StatusOK, body: "not json", code: "200"},
			}
			for _, tc := range cases {
				method := tc.method
				status := tc.status
				body := tc.body
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(status)
						_, _ = w.Write([]byte(body))
					}),
				)
				client = claim.NewClaimRefClient(server.URL, "", "agent-task-executor")
				var err error
				if method == http.MethodPut {
					_, err = client.Put(
						ctx,
						"claims/task/abc123",
						"",
						"nuke",
						"abc123",
						libtimetest.ParseDateTime("2026-04-18T12:00:00Z"),
					)
				} else {
					err = client.Delete(ctx, "claims/task/abc123", "S0")
				}
				Expect(err).To(HaveOccurred())
				Expect(stderrors.Is(err, claim.ErrLeaseConflict)).To(BeFalse())
				Expect(err.Error()).To(ContainSubstring(tc.code))
				server.Close()
				server = nil
			}
		})
	})

	Describe("no retry", func() {
		var count atomic.Int32

		BeforeEach(func() {
			count.Store(0)
			server = httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					count.Add(1)
					switch r.Method {
					case http.MethodPut:
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = w.Write([]byte("boom"))
					case http.MethodDelete:
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = w.Write([]byte("boom"))
					default:
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`[]`))
					}
				}),
			)
			client = claim.NewClaimRefClient(server.URL, "", "agent-task-executor")
		})

		It("issues exactly one request per call, successful or not", func() {
			_, _ = client.List(ctx, "claims/")
			Expect(count.Load()).To(Equal(int32(1)))
			_, _ = client.Put(
				ctx,
				"claims/task/abc123",
				"",
				"nuke",
				"abc123",
				libtimetest.ParseDateTime("2026-04-18T12:00:00Z"),
			)
			Expect(count.Load()).To(Equal(int32(2)))
			_ = client.Delete(ctx, "claims/task/abc123", "S0")
			Expect(count.Load()).To(Equal(int32(3)))
		})
	})

	Describe("secret leakage", func() {
		BeforeEach(func() {
			server = httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte("s3cret"))
				}),
			)
			client = claim.NewClaimRefClient(server.URL, "s3cret", "agent-task-executor")
		})

		It("never puts the secret in the error string", func() {
			_, err := client.Put(
				ctx,
				"claims/task/abc123",
				"",
				"nuke",
				"abc123",
				libtimetest.ParseDateTime("2026-04-18T12:00:00Z"),
			)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).NotTo(ContainSubstring("s3cret"))
		})
	})

	Describe("caller deadline", func() {
		var (
			blocked atomic.Bool
			count   atomic.Int32
		)

		BeforeEach(func() {
			blocked.Store(false)
			count.Store(0)
			server = httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					count.Add(1)
					if blocked.Load() {
						<-r.Context().Done()
						return
					}
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`[]`))
				}),
			)
			client = claim.NewClaimRefClient(server.URL, "", "agent-task-executor")
		})

		It("returns DeadlineExceeded and never retries", func() {
			// Warm up the keep-alive connection so the 1ms deadline below is
			// spent on the handler, not on a fresh TCP dial.
			_, err := client.List(ctx, "claims/")
			Expect(err).NotTo(HaveOccurred())
			count.Store(0)
			blocked.Store(true)

			deadlineCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
			defer cancel()
			_, err = client.List(deadlineCtx, "claims/")
			Expect(err).To(HaveOccurred())
			Expect(stderrors.Is(err, context.DeadlineExceeded)).To(BeTrue())
			Expect(count.Load()).To(Equal(int32(1)))
		})
	})
})
