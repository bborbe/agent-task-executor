// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gitrestclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/gitrestclient"
)

var _ = Describe("GitRestClient", func() {
	var (
		ctx    context.Context
		server *httptest.Server
		client gitrestclient.GitRestClient
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

	Describe("Get", func() {
		Context("200 response", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						Expect(r.Method).To(Equal(http.MethodGet))
						Expect(r.URL.Path).To(Equal("/api/v1/files/tasks/foo.md"))
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte("---\nfoo: bar\n---\nbody"))
					}),
				)
				client = gitrestclient.NewGitRestClient(server.URL, "", "agent-task-executor")
			})

			It("returns the exact body bytes and nil error", func() {
				content, err := client.Get(ctx, "tasks/foo.md")
				Expect(err).NotTo(HaveOccurred())
				Expect(content).To(Equal([]byte("---\nfoo: bar\n---\nbody")))
			})
		})

		Context("404 response", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusNotFound)
						_, _ = w.Write([]byte("file not found"))
					}),
				)
				client = gitrestclient.NewGitRestClient(server.URL, "", "agent-task-executor")
			})

			It("returns an error whose message contains the status code", func() {
				content, err := client.Get(ctx, "tasks/missing.md")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("404"))
				Expect(content).To(BeNil())
			})
		})

		Context("gateway secret configured", func() {
			var capturedSecret, capturedInitiator string

			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						capturedSecret = r.Header.Get("X-Gateway-Secret")
						capturedInitiator = r.Header.Get("X-Gateway-Initator")
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte("body"))
					}),
				)
				client = gitrestclient.NewGitRestClient(server.URL, "s3cret", "agent-task-executor")
			})

			It("sends X-Gateway-Secret and X-Gateway-Initator with the configured values", func() {
				_, err := client.Get(ctx, "tasks/foo.md")
				Expect(err).NotTo(HaveOccurred())
				Expect(capturedSecret).To(Equal("s3cret"))
				Expect(capturedInitiator).To(Equal("agent-task-executor"))
			})
		})

		Context("empty gateway secret", func() {
			var capturedSecret, capturedInitiator string

			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						capturedSecret = r.Header.Get("X-Gateway-Secret")
						capturedInitiator = r.Header.Get("X-Gateway-Initator")
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte("body"))
					}),
				)
				client = gitrestclient.NewGitRestClient(server.URL, "", "agent-task-executor")
			})

			It("sends neither auth header (auth-disabled git-rest still works)", func() {
				_, err := client.Get(ctx, "tasks/foo.md")
				Expect(err).NotTo(HaveOccurred())
				Expect(capturedSecret).To(BeEmpty())
				Expect(capturedInitiator).To(BeEmpty())
			})
		})

		Context("relPath with spaces is percent-escaped", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						// r.URL.Path is the decoded path — the space and the segment
						// separators survive as separate path elements.
						Expect(r.URL.Path).To(Equal("/api/v1/files/24 Tasks/a b.md"))
						// url.PathEscape escapes the space in the URL, so the
						// escaped form carries %20.
						Expect(r.URL.EscapedPath()).To(ContainSubstring("%20"))
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte("ok"))
					}),
				)
				client = gitrestclient.NewGitRestClient(server.URL, "", "agent-task-executor")
			})

			It("escapes the path and succeeds", func() {
				content, err := client.Get(ctx, "24 Tasks/a b.md")
				Expect(err).NotTo(HaveOccurred())
				Expect(content).To(Equal([]byte("ok")))
			})
		})
	})

	Describe("List", func() {
		Context("2 paths returned", func() {
			var receivedGlob string

			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						Expect(r.Method).To(Equal(http.MethodGet))
						receivedGlob = r.URL.Query().Get("glob")
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`["24 Tasks/a.md","24 Tasks/b.md"]`))
					}),
				)
				client = gitrestclient.NewGitRestClient(server.URL, "", "agent-task-executor")
			})

			It("returns exactly those two paths and propagates the glob query param", func() {
				paths, err := client.List(ctx, "24 Tasks/*.md")
				Expect(err).NotTo(HaveOccurred())
				Expect(paths).To(Equal([]string{"24 Tasks/a.md", "24 Tasks/b.md"}))
				Expect(receivedGlob).To(Equal("24 Tasks/*.md"))
			})
		})

		Context("non-200 response", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = w.Write([]byte("internal error"))
					}),
				)
				client = gitrestclient.NewGitRestClient(server.URL, "", "agent-task-executor")
			})

			It("returns an error containing the status code", func() {
				paths, err := client.List(ctx, "24 Tasks/*.md")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("500"))
				Expect(paths).To(BeNil())
			})
		})

		Context("unparseable body", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte("not json"))
					}),
				)
				client = gitrestclient.NewGitRestClient(server.URL, "", "agent-task-executor")
			})

			It("returns nil and a non-nil error (no panic)", func() {
				paths, err := client.List(ctx, "24 Tasks/*.md")
				Expect(err).To(HaveOccurred())
				Expect(paths).To(BeNil())
			})
		})
	})

	Describe("IsReady", func() {
		Context("200 response", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						Expect(r.URL.Path).To(Equal("/readiness"))
						w.WriteHeader(http.StatusOK)
					}),
				)
				client = gitrestclient.NewGitRestClient(server.URL, "", "agent-task-executor")
			})

			It("returns true, nil", func() {
				ready, err := client.IsReady(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(ready).To(BeTrue())
			})
		})

		Context("503 response", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusServiceUnavailable)
					}),
				)
				client = gitrestclient.NewGitRestClient(server.URL, "", "agent-task-executor")
			})

			It("returns false, nil — a valid not-ready state, not an error", func() {
				ready, err := client.IsReady(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(ready).To(BeFalse())
			})
		})

		Context("500 response", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusInternalServerError)
					}),
				)
				client = gitrestclient.NewGitRestClient(server.URL, "", "agent-task-executor")
			})

			It("returns false, error", func() {
				ready, err := client.IsReady(ctx)
				Expect(err).To(HaveOccurred())
				Expect(ready).To(BeFalse())
			})
		})

		Context("network error (stopped server)", func() {
			BeforeEach(func() {
				server = httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
				)
				client = gitrestclient.NewGitRestClient(server.URL, "", "agent-task-executor")
				server.Close()
				server = nil
			})

			It("returns false, error on network failure", func() {
				ready, err := client.IsReady(ctx)
				Expect(err).To(HaveOccurred())
				Expect(ready).To(BeFalse())
			})
		})
	})
})
