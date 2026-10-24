// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// fakeRef is one stored ref in the fake git-rest.
type fakeRef struct {
	Sha        string
	Cluster    string
	TaskID     string
	LeaseUntil string
}

// fakeRequest is one recorded request. Status is the status actually written.
type fakeRequest struct {
	Method   string
	Path     string
	RawQuery string
	Header   http.Header
	Body     []byte
	Status   int
}

// fakeFault is a one-off answer injected by SetFault.
type fakeFault struct {
	Status int
	Body   string
	// Apply (PUT only) performs the CAS write, when it would succeed, before
	// answering Status.
	Apply bool
	// AfterApply runs after the CAS write has been stored and the store lock
	// released, but before Status is written. Use it to act on a state the
	// caller must be able to observe — cancelling a context once the write is
	// visible, without racing the write itself.
	AfterApply func()
}

type fakePutBody struct {
	ExpectedSha string `json:"expected_sha"`
	Cluster     string `json:"cluster"`
	TaskID      string `json:"task_id"`
	LeaseUntil  string `json:"lease_until"`
}

// fakeGitRest is a reusable in-process fake of git-rest's leased claim-ref
// API (git-rest spec 018). It is test-only.
type fakeGitRest struct {
	mu         sync.Mutex
	server     *httptest.Server
	refs       map[string]fakeRef
	requests   []fakeRequest
	fault      func(fakeRequest) *fakeFault
	shaSeq     int
	putBarrier int
	putWaiting int
	putRelease chan struct{}
}

func newFakeGitRest() *fakeGitRest {
	f := &fakeGitRest{refs: map[string]fakeRef{}}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	return f
}

func (f *fakeGitRest) Close() {
	f.server.Close()
}

func (f *fakeGitRest) URL() string {
	return f.server.URL
}

func (f *fakeGitRest) Seed(ref, sha, cluster, taskID, leaseUntil string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refs[ref] = fakeRef{Sha: sha, Cluster: cluster, TaskID: taskID, LeaseUntil: leaseUntil}
}

func (f *fakeGitRest) Ref(ref string) (fakeRef, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.refs[ref]
	return r, ok
}

func (f *fakeGitRest) DeleteRef(ref string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.refs, ref)
}

func (f *fakeGitRest) Requests() []fakeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeRequest, len(f.requests))
	copy(out, f.requests)
	return out
}

func (f *fakeGitRest) ResetRequests() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = nil
}

func (f *fakeGitRest) SetFault(fn func(fakeRequest) *fakeFault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fault = fn
}

// SetPutBarrier makes every PUT wait until n PUTs are waiting, then releases
// all of them. The barrier is one-shot. SetPutBarrier(0) disables it.
func (f *fakeGitRest) SetPutBarrier(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putBarrier = n
	f.putWaiting = 0
	if n > 0 {
		f.putRelease = make(chan struct{})
	} else {
		f.putRelease = nil
	}
}

func (f *fakeGitRest) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024))
	req := fakeRequest{
		Method:   r.Method,
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
		Header:   r.Header.Clone(),
		Body:     body,
	}
	status, respBody := f.process(req)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(respBody)
	req.Status = status
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.mu.Unlock()
}

func (f *fakeGitRest) process(req fakeRequest) (int, []byte) {
	f.mu.Lock()
	faultFn := f.fault
	f.mu.Unlock()
	var fault *fakeFault
	if faultFn != nil {
		fault = faultFn(req)
	}
	if fault != nil && !fault.Apply {
		return fault.Status, []byte(fault.Body)
	}
	if req.Method == http.MethodPut {
		f.awaitPutBarrier()
	}
	status, body := f.handleNormal(req)
	if fault != nil {
		if fault.AfterApply != nil {
			fault.AfterApply()
		}
		return fault.Status, []byte(fault.Body)
	}
	return status, body
}

func (f *fakeGitRest) awaitPutBarrier() {
	f.mu.Lock()
	if f.putBarrier <= 0 {
		f.mu.Unlock()
		return
	}
	f.putWaiting++
	if f.putWaiting >= f.putBarrier {
		close(f.putRelease)
		f.putBarrier = 0
		f.putWaiting = 0
		f.putRelease = nil
		f.mu.Unlock()
		return
	}
	release := f.putRelease
	f.mu.Unlock()
	<-release
}

func (f *fakeGitRest) handleNormal(req fakeRequest) (int, []byte) {
	key := strings.TrimPrefix(req.Path, "/api/v1/refs")
	key = strings.TrimPrefix(key, "/")
	switch req.Method {
	case http.MethodGet:
		if key != "" {
			return http.StatusNotFound, []byte(`{"error":"not found"}`)
		}
		return f.list(req.RawQuery)
	case http.MethodPut:
		return f.put(key, req.Body)
	case http.MethodDelete:
		return f.delete(key, req.RawQuery)
	default:
		return http.StatusMethodNotAllowed, []byte(`{"error":"method not allowed"}`)
	}
}

func (f *fakeGitRest) list(rawQuery string) (int, []byte) {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return http.StatusBadRequest, []byte(`{"error":"bad query"}`)
	}
	prefix := values.Get("prefix")
	f.mu.Lock()
	refs := make([]claim.ClaimRef, 0, len(f.refs))
	for ref, stored := range f.refs {
		if matchesPrefix(ref, prefix) {
			refs = append(refs, claim.ClaimRef{
				Ref:        ref,
				Sha:        stored.Sha,
				Cluster:    stored.Cluster,
				TaskID:     stored.TaskID,
				LeaseUntil: stored.LeaseUntil,
			})
		}
	}
	f.mu.Unlock()
	sort.Slice(refs, func(i, j int) bool { return refs[i].Ref < refs[j].Ref })
	body, err := json.Marshal(refs)
	if err != nil {
		return http.StatusInternalServerError, []byte(`{"error":"marshal"}`)
	}
	return http.StatusOK, body
}

func matchesPrefix(ref, prefix string) bool {
	if strings.HasSuffix(prefix, "/") {
		return strings.HasPrefix(ref, "refs/"+prefix)
	}
	return ref == "refs/"+prefix || strings.HasPrefix(ref, "refs/"+prefix+"/")
}

func (f *fakeGitRest) put(key string, body []byte) (int, []byte) {
	var req fakePutBody
	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, []byte(`{"error":"bad body"}`)
	}
	fullRef := "refs/" + key
	f.mu.Lock()
	defer f.mu.Unlock()
	existing, exists := f.refs[fullRef]
	if req.ExpectedSha == "" {
		if exists {
			return http.StatusConflict, []byte(`{"error":"conflict"}`)
		}
	} else if !exists || existing.Sha != req.ExpectedSha {
		return http.StatusConflict, []byte(`{"error":"conflict"}`)
	}
	f.shaSeq++
	sha := fmt.Sprintf("%040x", f.shaSeq)
	f.refs[fullRef] = fakeRef{
		Sha:        sha,
		Cluster:    req.Cluster,
		TaskID:     req.TaskID,
		LeaseUntil: req.LeaseUntil,
	}
	response, err := json.Marshal(map[string]string{"ref": fullRef, "sha": sha})
	if err != nil {
		return http.StatusInternalServerError, []byte(`{"error":"marshal"}`)
	}
	return http.StatusOK, response
}

func (f *fakeGitRest) delete(key, rawQuery string) (int, []byte) {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return http.StatusBadRequest, []byte(`{"error":"bad query"}`)
	}
	fullRef := "refs/" + key
	f.mu.Lock()
	defer f.mu.Unlock()
	existing, exists := f.refs[fullRef]
	if !exists || existing.Sha != values.Get("expected_sha") {
		return http.StatusConflict, []byte(`{"error":"conflict"}`)
	}
	delete(f.refs, fullRef)
	return http.StatusOK, []byte(`{"ok":true}`)
}
