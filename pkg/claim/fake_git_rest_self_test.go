// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"io"
	"net/http"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const identicalPutBody = `{"expected_sha":"","cluster":"nuke","task_id":"t","lease_until":"2026-04-18T12:00:00Z"}`

func doFakeRequest(method, target, body string) (int, []byte) {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, target, reader)
	Expect(err).NotTo(HaveOccurred())
	resp, err := http.DefaultClient.Do(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	return resp.StatusCode, respBody
}

var _ = Describe("fakeGitRest", func() {
	var fake *fakeGitRest

	BeforeEach(func() {
		fake = newFakeGitRest()
	})

	AfterEach(func() {
		fake.Close()
	})

	It("rejects a create on an existing ref with 409", func() {
		fake.Seed("refs/claims/task/a", "S0", "nuke", "a", "2026-04-18T12:00:00Z")
		status, _ := doFakeRequest(
			http.MethodPut,
			fake.URL()+"/api/v1/refs/claims/task/a",
			identicalPutBody,
		)
		Expect(status).To(Equal(http.StatusConflict))
	})

	It("rejects a stale expected_sha and leaves the ref unchanged", func() {
		fake.Seed("refs/claims/task/a", "S0", "nuke", "a", "2026-04-18T12:00:00Z")
		status, _ := doFakeRequest(
			http.MethodPut,
			fake.URL()+"/api/v1/refs/claims/task/a",
			`{"expected_sha":"stale","cluster":"nuke","task_id":"a","lease_until":"2026-04-18T12:00:00Z"}`,
		)
		Expect(status).To(Equal(http.StatusConflict))
		ref, ok := fake.Ref("refs/claims/task/a")
		Expect(ok).To(BeTrue())
		Expect(ref.Sha).To(Equal("S0"))
	})

	It("rejects deleting an absent ref with 409", func() {
		status, _ := doFakeRequest(
			http.MethodDelete,
			fake.URL()+"/api/v1/refs/claims/task/absent?expected_sha=S0",
			"",
		)
		Expect(status).To(Equal(http.StatusConflict))
	})

	It("returns different shas for byte-identical creates on different keys", func() {
		statusA, bodyA := doFakeRequest(
			http.MethodPut,
			fake.URL()+"/api/v1/refs/claims/task/a",
			identicalPutBody,
		)
		statusB, bodyB := doFakeRequest(
			http.MethodPut,
			fake.URL()+"/api/v1/refs/claims/task/b",
			identicalPutBody,
		)
		Expect(statusA).To(Equal(http.StatusOK))
		Expect(statusB).To(Equal(http.StatusOK))
		Expect(bodyA).NotTo(Equal(bodyB))
		refA, _ := fake.Ref("refs/claims/task/a")
		refB, _ := fake.Ref("refs/claims/task/b")
		Expect(refA.Sha).NotTo(Equal(refB.Sha))
	})

	It("matches whole path segments only", func() {
		fake.Seed("refs/claims/task/1", "S0", "nuke", "1", "2026-04-18T12:00:00Z")
		fake.Seed("refs/claims/task/10", "S1", "nuke", "10", "2026-04-18T12:00:00Z")
		status, body := doFakeRequest(
			http.MethodGet,
			fake.URL()+"/api/v1/refs?prefix=claims/task/1",
			"",
		)
		Expect(status).To(Equal(http.StatusOK))
		Expect(string(body)).To(ContainSubstring(`"refs/claims/task/1"`))
		Expect(string(body)).NotTo(ContainSubstring(`"refs/claims/task/10"`))
	})

	It("encodes an empty list as [] not null", func() {
		status, body := doFakeRequest(
			http.MethodGet,
			fake.URL()+"/api/v1/refs?prefix=claims/absent",
			"",
		)
		Expect(status).To(Equal(http.StatusOK))
		Expect(string(body)).To(Equal("[]"))
	})

	It("leaves the ref unchanged when a fault does not apply", func() {
		fake.Seed("refs/claims/task/a", "S0", "nuke", "a", "2026-04-18T12:00:00Z")
		fake.SetFault(func(fakeRequest) *fakeFault {
			return &fakeFault{Status: http.StatusInternalServerError, Body: "boom"}
		})
		status, _ := doFakeRequest(
			http.MethodPut,
			fake.URL()+"/api/v1/refs/claims/task/a",
			`{"expected_sha":"S0","cluster":"nuke","task_id":"a","lease_until":"2026-04-19T12:00:00Z"}`,
		)
		Expect(status).To(Equal(http.StatusInternalServerError))
		ref, _ := fake.Ref("refs/claims/task/a")
		Expect(ref.Sha).To(Equal("S0"))
		Expect(ref.LeaseUntil).To(Equal("2026-04-18T12:00:00Z"))
	})

	It("applies the write when the fault says Apply", func() {
		fake.Seed("refs/claims/task/a", "S0", "nuke", "a", "2026-04-18T12:00:00Z")
		fake.SetFault(func(fakeRequest) *fakeFault {
			return &fakeFault{
				Status: http.StatusInternalServerError,
				Body:   "boom",
				Apply:  true,
			}
		})
		status, _ := doFakeRequest(
			http.MethodPut,
			fake.URL()+"/api/v1/refs/claims/task/a",
			`{"expected_sha":"S0","cluster":"nuke","task_id":"a","lease_until":"2026-04-19T12:00:00Z"}`,
		)
		Expect(status).To(Equal(http.StatusInternalServerError))
		ref, _ := fake.Ref("refs/claims/task/a")
		Expect(ref.Sha).NotTo(Equal("S0"))
		Expect(ref.LeaseUntil).To(Equal("2026-04-19T12:00:00Z"))
	})

	It("lets exactly one of two concurrent creates win", func() {
		fake.SetPutBarrier(2)
		statuses := make(chan int, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				status, _ := doFakeRequest(
					http.MethodPut,
					fake.URL()+"/api/v1/refs/claims/task/race",
					identicalPutBody,
				)
				statuses <- status
			}()
		}
		wg.Wait()
		close(statuses)
		var got []int
		for status := range statuses {
			got = append(got, status)
		}
		Expect(got).To(ConsistOf(http.StatusOK, http.StatusConflict))
		ref, ok := fake.Ref("refs/claims/task/race")
		Expect(ok).To(BeTrue())
		Expect(ref.Sha).NotTo(BeEmpty())
	})
})
