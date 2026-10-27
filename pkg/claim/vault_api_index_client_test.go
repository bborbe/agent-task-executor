// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// zeroReader yields an unbounded stream of zero bytes, so the oversized-body
// test can exceed the 32 MiB bound without allocating a 32 MiB string.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

var _ = Describe("VaultAPIIndexClient", func() {
	const maxIndexResponseBytes = 32 * 1024 * 1024

	var (
		ctx          context.Context
		server       *httptest.Server
		client       claim.VaultAPIIndexClient
		lastMethod   string
		lastPath     string
		lastQuery    string
		lastHeader   http.Header
		requestCount int
		status       int
		body         string
		streamBytes  int64
	)

	BeforeEach(func() {
		ctx = context.Background()
		lastMethod, lastPath, lastQuery, lastHeader = "", "", "", nil
		requestCount = 0
		status, body, streamBytes = http.StatusOK, "", 0
		server = httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requestCount++
				lastMethod = r.Method
				lastPath = r.URL.Path
				lastQuery = r.URL.RawQuery
				lastHeader = r.Header.Clone()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				if streamBytes > 0 {
					_, _ = io.CopyN(w, zeroReader{}, streamBytes)
					return
				}
				_, _ = w.Write([]byte(body))
			},
		))
		client = claim.NewVaultAPIIndexClient(server.URL, "secret-value", "agent-task-executor")
	})

	AfterEach(func() { server.Close() })

	Context("a successful read", func() {
		BeforeEach(func() {
			status = http.StatusOK
			body = `{"items":[` +
				`{"id":"t-1","status":"in_progress","phase":"execution"},` +
				`{"id":"t-2","status":"completed","phase":"done"},` +
				`{"id":"t-3","status":"planning","phase":"planning"}],` +
				`"total":3,"warnings":2,"index_age_seconds":42}`
		})

		It("issues one unfiltered GET to the task index", func() {
			_, err := client.Read(ctx)
			Expect(err).To(Succeed())
			Expect(requestCount).To(Equal(1))
			Expect(lastMethod).To(Equal(http.MethodGet))
			Expect(lastPath).To(Equal("/api/v1/tasks"))
			// A cluster= filter would omit files with no cluster frontmatter and
			// manufacture absent divergences, so the request must carry no query.
			Expect(lastQuery).To(Equal(""))
		})

		It("sends the gateway headers", func() {
			_, err := client.Read(ctx)
			Expect(err).To(Succeed())
			Expect(lastHeader.Get("X-Gateway-Secret")).To(Equal("secret-value"))
			Expect(lastHeader.Get("X-Gateway-Initator")).To(Equal("agent-task-executor"))
		})

		It("decodes every envelope field", func() {
			index, err := client.Read(ctx)
			Expect(err).To(Succeed())
			Expect(index.Items).To(HaveLen(3))
			Expect(index.Items[0]).To(Equal(claim.TaskIndexItem{
				ID: "t-1", Status: "in_progress", Phase: "execution",
			}))
			Expect(index.Items[1]).To(Equal(claim.TaskIndexItem{
				ID: "t-2", Status: "completed", Phase: "done",
			}))
			Expect(index.Items[2]).To(Equal(claim.TaskIndexItem{
				ID: "t-3", Status: "planning", Phase: "planning",
			}))
			Expect(index.Total).To(Equal(3))
			Expect(index.Warnings).To(Equal(2))
			Expect(index.IndexAgeSeconds).To(Equal(42))
		})
	})

	Context("an empty index", func() {
		It("reads items: [] as zero items, not as a failure", func() {
			body = `{"items":[],"index_age_seconds":0}`
			index, err := client.Read(ctx)
			Expect(err).To(Succeed())
			Expect(index.Items).To(HaveLen(0))
		})

		It("reads items: null as zero items, not as a failure", func() {
			// A Go server marshalling a nil slice writes null; that is a
			// legitimate empty index, not a missing field.
			body = `{"items":null,"index_age_seconds":0}`
			index, err := client.Read(ctx)
			Expect(err).To(Succeed())
			Expect(index.Items).To(HaveLen(0))
		})

		It("defaults an absent total and warnings to zero", func() {
			body = `{"items":[],"index_age_seconds":0}`
			index, err := client.Read(ctx)
			Expect(err).To(Succeed())
			Expect(index.Total).To(Equal(0))
			Expect(index.Warnings).To(Equal(0))
		})
	})

	Context("a malformed envelope", func() {
		It("fails when the items key is absent", func() {
			body = `{"index_age_seconds":0}`
			_, err := client.Read(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("missing items")))
		})

		It("fails when index_age_seconds is absent", func() {
			body = `{"items":[]}`
			_, err := client.Read(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("missing index_age_seconds")))
		})

		It("fails on truncated JSON", func() {
			body = `{"items": [`
			_, err := client.Read(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("decode vault-api task index response")))
		})

		It("fails when an item's id is not a string", func() {
			body = `{"items": [{"id": 1}], "index_age_seconds": 0}`
			_, err := client.Read(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("decode vault-api task index items")))
		})
	})

	Context("a non-200 status", func() {
		It("fails on a 429", func() {
			status = http.StatusTooManyRequests
			body = `{"error":"rate_limited"}`
			_, err := client.Read(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("429")))
		})
	})

	Context("an oversized body", func() {
		It("fails rather than truncating and parsing", func() {
			streamBytes = int64(maxIndexResponseBytes) + 1
			_, err := client.Read(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("exceeded")))
		})
	})

	Context("a closed listener", func() {
		It("fails on the transport error", func() {
			closed := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {},
			))
			url := closed.URL
			closed.Close()

			deadClient := claim.NewVaultAPIIndexClient(url, "s", "i")
			_, err := deadClient.Read(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("read vault-api task index")))
		})
	})

	Context("an empty baseURL", func() {
		It("is refused before any request", func() {
			noURL := claim.NewVaultAPIIndexClient("", "s", "i")
			_, err := noURL.Read(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("base URL is empty")))
			Expect(requestCount).To(Equal(0))
		})
	})

	Context("a trailing-slash baseURL", func() {
		It("does not double the slash", func() {
			body = `{"items":[],"index_age_seconds":0}`
			slashed := claim.NewVaultAPIIndexClient(server.URL+"/", "s", "i")
			_, err := slashed.Read(ctx)
			Expect(err).To(Succeed())
			Expect(requestCount).To(Equal(1))
			Expect(lastPath).To(Equal("/api/v1/tasks"))
		})
	})

	Context("an empty gateway secret", func() {
		It("sends neither header", func() {
			body = `{"items":[],"index_age_seconds":0}`
			noAuth := claim.NewVaultAPIIndexClient(server.URL, "", "")
			_, err := noAuth.Read(ctx)
			Expect(err).To(Succeed())
			Expect(lastHeader.Get("X-Gateway-Secret")).To(BeEmpty())
			Expect(lastHeader.Get("X-Gateway-Initator")).To(BeEmpty())
		})
	})
})
