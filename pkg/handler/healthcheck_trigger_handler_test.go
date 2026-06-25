// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent/task/executor/pkg/handler"
	"github.com/bborbe/agent/task/executor/pkg/probe/mocks"
)

var _ = Describe("HealthcheckTriggerHandler", func() {
	var (
		fakeRunner *mocks.FakeHealthcheckRunner
		h          http.Handler
	)

	BeforeEach(func() {
		fakeRunner = new(mocks.FakeHealthcheckRunner)
		h = handler.NewHealthcheckTriggerHandler(fakeRunner)
	})

	Context("POST request", func() {
		It("returns HTTP 200", func() {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/healthcheck/trigger", nil)
			h.ServeHTTP(w, req)
			Expect(w.Code).To(Equal(http.StatusOK))
		})

		It("triggers the runner exactly once", func() {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/healthcheck/trigger", nil)
			h.ServeHTTP(w, req)
			Eventually(fakeRunner.RunCallCount).Should(Equal(1))
		})

		It("returns before the runner finishes (fire-and-forget)", func() {
			firstCallUnblock := make(chan struct{})
			firstCallDone := make(chan struct{})
			fakeRunner.RunStub = func(ctx context.Context) error {
				<-firstCallUnblock
				close(firstCallDone)
				return nil
			}
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/healthcheck/trigger", nil)
			// ServeHTTP returns immediately even though the runner is blocked
			h.ServeHTTP(w, req)
			Expect(w.Code).To(Equal(http.StatusOK))
			// Unblock and wait for goroutine to fully exit the stub body before the test ends
			close(firstCallUnblock)
			Eventually(firstCallDone).Should(BeClosed())
		})
	})

	Context("single-flight: second invocation while first is in-flight", func() {
		// Each It block uses local variables so no closure captures are shared across tests.
		//
		// The stub uses atomic.Bool (via CompareAndSwap) so that if Go's scheduler delays G7
		// until after G6 has finished (and the parallelSkipper lock is free), G7 can acquire
		// the lock and enter the stub without panicking — it simply returns immediately.
		// The key assertion is Consistently during G6's blocking period, which proves G7 did
		// not call the runner while G6 was in-flight.

		It("does not invoke the runner a second time", func() {
			firstCallStarted := make(chan struct{})
			firstCallUnblock := make(chan struct{})
			firstCallDone := make(chan struct{})
			var entered atomic.Bool

			fakeRunner.RunStub = func(ctx context.Context) error {
				if !entered.CompareAndSwap(false, true) {
					// A late goroutine ran after the first call finished — return cleanly.
					return nil
				}
				close(firstCallStarted)
				<-firstCallUnblock
				close(firstCallDone)
				return nil
			}

			// First request — fires runner in background, blocks on firstCallUnblock
			h.ServeHTTP(
				httptest.NewRecorder(),
				httptest.NewRequest(http.MethodPost, "/healthcheck/trigger", nil),
			)
			Eventually(firstCallStarted).Should(BeClosed())

			// Second request while first is still in-flight
			h.ServeHTTP(
				httptest.NewRecorder(),
				httptest.NewRequest(http.MethodPost, "/healthcheck/trigger", nil),
			)

			// While G6 is still blocking, the runner must not be invoked a second time
			Consistently(fakeRunner.RunCallCount, "200ms", "20ms").Should(Equal(1))

			// Release G6 and wait for it to finish
			close(firstCallUnblock)
			Eventually(firstCallDone).Should(BeClosed())
		})

		It("returns HTTP 200 for the second request too", func() {
			firstCallStarted := make(chan struct{})
			firstCallUnblock := make(chan struct{})
			firstCallDone := make(chan struct{})
			var entered atomic.Bool

			fakeRunner.RunStub = func(ctx context.Context) error {
				if !entered.CompareAndSwap(false, true) {
					return nil
				}
				close(firstCallStarted)
				<-firstCallUnblock
				close(firstCallDone)
				return nil
			}

			h.ServeHTTP(
				httptest.NewRecorder(),
				httptest.NewRequest(http.MethodPost, "/healthcheck/trigger", nil),
			)
			Eventually(firstCallStarted).Should(BeClosed())

			w2 := httptest.NewRecorder()
			h.ServeHTTP(w2, httptest.NewRequest(http.MethodPost, "/healthcheck/trigger", nil))
			Expect(w2.Code).To(Equal(http.StatusOK))

			close(firstCallUnblock)
			Eventually(firstCallDone).Should(BeClosed())
		})
	})
})
