// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/pkg/handler"
)

var _ = Describe("AgentsHandler", func() {
	var (
		providerFake *fakeProviderImpl
		h            http.Handler
	)

	BeforeEach(func() {
		providerFake = &fakeProviderImpl{}
		h = handler.NewAgentsHandler(providerFake, "")
	})

	Context("GET request", func() {
		It("returns HTTP 200 with JSON array", func() {
			providerFake.configs = []*agentv1.Config{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-agent",
					},
					Spec: agentv1.ConfigSpec{
						Assignee: "claude",
						Image:    "test-image:latest",
					},
				},
			}

			rec := &responseRecorder{}
			req := httptest.NewRequest(http.MethodGet, "/agents", nil)
			h.ServeHTTP(rec, req)

			Expect(rec.statusCode).To(Equal(http.StatusOK))
			Expect(rec.header.Get("Content-Type")).To(Equal("application/json"))
			Expect(rec.body).To(ContainSubstring(`"name":"test-agent"`))
		})

		It("returns empty array when no agents", func() {
			providerFake.configs = []*agentv1.Config{}

			rec := &responseRecorder{}
			req := httptest.NewRequest(http.MethodGet, "/agents", nil)
			h.ServeHTTP(rec, req)

			Expect(rec.statusCode).To(Equal(http.StatusOK))
			Expect(rec.body).To(ContainSubstring("[]"))
		})

		It("returns HTTP 500 when provider fails", func() {
			providerFake.err = errors.New("provider error")

			rec := &responseRecorder{}
			req := httptest.NewRequest(http.MethodGet, "/agents", nil)
			h.ServeHTTP(rec, req)

			Expect(rec.statusCode).To(Equal(http.StatusInternalServerError))
		})

		It("returns HTTP 500 when encoding fails", func() {
			providerFake.configs = []*agentv1.Config{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-agent",
					},
					Spec: agentv1.ConfigSpec{
						Assignee: "claude",
						Image:    "test-image:latest",
					},
				},
			}

			rec := &errorResponseRecorder{
				writeErr: errors.New("connection reset"),
			}
			req := httptest.NewRequest(http.MethodGet, "/agents", nil)
			h.ServeHTTP(rec, req)

			Expect(rec.statusCode).To(Equal(http.StatusInternalServerError))
		})
	})

	Context("authentication", func() {
		var providerFake *fakeProviderImpl

		BeforeEach(func() {
			providerFake = &fakeProviderImpl{}
		})

		It("returns HTTP 401 without X-Agent-Auth header when secret is set", func() {
			h = handler.NewAgentsHandler(providerFake, "secret123")

			rec := &responseRecorder{}
			req := httptest.NewRequest(http.MethodGet, "/agents", nil)
			h.ServeHTTP(rec, req)

			Expect(rec.statusCode).To(Equal(http.StatusUnauthorized))
		})

		It("returns HTTP 401 with wrong X-Agent-Auth header value", func() {
			h = handler.NewAgentsHandler(providerFake, "secret123")

			rec := &responseRecorder{}
			req := httptest.NewRequest(http.MethodGet, "/agents", nil)
			req.Header.Set("X-Agent-Auth", "wrong-secret")
			h.ServeHTTP(rec, req)

			Expect(rec.statusCode).To(Equal(http.StatusUnauthorized))
		})

		It("returns HTTP 200 with correct X-Agent-Auth header", func() {
			providerFake.configs = []*agentv1.Config{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-agent",
					},
					Spec: agentv1.ConfigSpec{
						Assignee: "claude",
						Image:    "test-image:latest",
					},
				},
			}
			h = handler.NewAgentsHandler(providerFake, "secret123")

			rec := &responseRecorder{}
			req := httptest.NewRequest(http.MethodGet, "/agents", nil)
			req.Header.Set("X-Agent-Auth", "secret123")
			h.ServeHTTP(rec, req)

			Expect(rec.statusCode).To(Equal(http.StatusOK))
			Expect(rec.body).To(ContainSubstring(`"name":"test-agent"`))
		})
	})
})

// fakeProviderImpl implements the Provider interface for agentv1.Config.
type fakeProviderImpl struct {
	configs []*agentv1.Config
	err     error
}

func (p *fakeProviderImpl) Get(ctx context.Context) ([]agentv1.Config, error) {
	result := make([]agentv1.Config, len(p.configs))
	for i, c := range p.configs {
		result[i] = *c
	}
	return result, p.err
}

// responseRecorder implements http.ResponseWriter for capturing responses.
type responseRecorder struct {
	headerCalled bool
	statusCode   int
	header       http.Header
	body         string
}

func (r *responseRecorder) Header() http.Header {
	if r.header == nil {
		r.header = make(http.Header)
	}
	return r.header
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.headerCalled = true
	r.statusCode = statusCode
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	if !r.headerCalled {
		r.statusCode = http.StatusOK
	}
	r.body = string(p)
	return len(p), nil
}

// errorResponseRecorder simulates a connection that fails on first write.
type errorResponseRecorder struct {
	writeErr   error
	statusCode int
	header     http.Header
}

func (e *errorResponseRecorder) Header() http.Header {
	if e.header == nil {
		e.header = make(http.Header)
	}
	return e.header
}

func (e *errorResponseRecorder) WriteHeader(statusCode int) {
	e.statusCode = statusCode
}

func (e *errorResponseRecorder) Write(p []byte) (int, error) {
	if e.statusCode == 0 {
		e.statusCode = http.StatusOK
	}
	return 0, e.writeErr
}
