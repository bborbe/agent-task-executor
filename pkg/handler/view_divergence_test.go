// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

var _ = Describe("reconcile pass view/vault divergence check", func() {
	var (
		ctx                 context.Context
		fakeSpawner         *mocks.FakeJobSpawner
		fakeResolver        *mocks.FakeConfigResolver
		fakeResultPublisher *mocks.FakeResultPublisher
		fakeView            *mocks.FakeTaskView
		taskStore           *pkg.TaskStore
		currentDateTime     libtime.CurrentDateTime

		// The index server runs on its own goroutine, so the last request is
		// guarded rather than relying on the response's happens-before edge.
		indexMu     sync.Mutex
		lastMethod  string
		lastPath    string
		lastQuery   string
		lastHeaders *http.Header
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeSpawner = new(mocks.FakeJobSpawner)
		fakeResolver = &mocks.FakeConfigResolver{}
		fakeResultPublisher = &mocks.FakeResultPublisher{}
		fakeView = &mocks.FakeTaskView{}
		fakeView.ReadyReturns(true)
		fakeView.SnapshotReturns(nil)
		fakeView.GetReturns(lib.Task{}, false)
		taskStore = pkg.NewTaskStore()
		currentDateTime = libtime.NewCurrentDateTime()
		indexMu.Lock()
		lastMethod, lastPath, lastQuery = "", "", ""
		lastHeaders = nil
		indexMu.Unlock()
	})

	// newIndexServer serves body with 200 and records the last request. The
	// counter is atomic because the handler may be read from the test goroutine.
	newIndexServer := func(body string) (*httptest.Server, *int32, *http.Header) {
		var count int32
		headers := new(http.Header)
		server := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&count, 1)
				indexMu.Lock()
				lastMethod = r.Method
				lastPath = r.URL.Path
				lastQuery = r.URL.RawQuery
				*headers = r.Header.Clone()
				lastHeaders = headers
				indexMu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(body))
			},
		))
		DeferCleanup(server.Close)
		return server, &count, headers
	}

	indexBody := func(items []map[string]any, age, warnings int) string {
		raw, err := json.Marshal(map[string]any{
			"items":             items,
			"total":             len(items),
			"warnings":          warnings,
			"index_age_seconds": age,
		})
		Expect(err).To(BeNil())
		return string(raw)
	}

	ownedTask := func(id, status, phase string) lib.Task {
		return lib.Task{
			TaskIdentifier: lib.TaskIdentifier(id),
			Vault:          testTaskVault,
			Frontmatter: lib.TaskFrontmatter{
				"status": status,
				"phase":  phase,
			},
		}
	}

	indexItem := func(id, status, phase string) map[string]any {
		return map[string]any{"id": id, "status": status, "phase": phase}
	}

	// buildHandler mirrors the reconcile fixture's construction. Callers supply
	// the view; the index client is optional so the no-URL deployment's shape is
	// reachable without a second constructor.
	buildHandler := func(
		view handler.TaskView,
		opts ...handler.HandlerOption,
	) handler.ClaimLifecycleEventHandler {
		return handler.NewTaskEventHandler(
			fakeSpawner,
			"dev",
			base.Branch("prod"),
			"",
			true,
			true,
			fakeResolver,
			fakeResultPublisher,
			taskStore,
			currentDateTime,
			view,
			testTaskVault,
			handler.DefaultReconcilePassTimeout,
			handler.DefaultReconcileInterval,
			pkg.ClaimModeOff,
			nil,
			opts...,
		)
	}

	newDivergenceHandler := func(
		indexClient claim.VaultAPIIndexClient,
		view handler.TaskView,
	) handler.ClaimLifecycleEventHandler {
		return buildHandler(view, handler.WithVaultAPIIndexClient(indexClient))
	}

	// recordSnapshot copies the last request under the lock the handler wrote it
	// with, so a later read can never observe a torn header map.
	recordSnapshot := func() (string, string, string, http.Header) {
		indexMu.Lock()
		defer indexMu.Unlock()
		var headers http.Header
		if lastHeaders != nil {
			headers = *lastHeaders
		}
		return lastMethod, lastPath, lastQuery, headers
	}

	terminalOwnedSet := func() []lib.Task {
		return []lib.Task{
			ownedTask("tid-a", "completed", "done"),
			ownedTask("tid-b", "completed", "done"),
			ownedTask("tid-c", "completed", "done"),
		}
	}

	// divergenceCase is one row of the comparison table. hiddenTitle and
	// hiddenAssignee are only set on the entry that proves the log line carries
	// ids, statuses and phases rather than the whole frontmatter.
	type divergenceCase struct {
		vaultStatus    string
		vaultPhase     string
		warnings       int
		wantDivergent  int
		wantLine       string
		hiddenTitle    string
		hiddenAssignee string
	}

	DescribeTable(
		"reports a divergence in either field and stays silent on agreement",
		func(c divergenceCase) {
			server, requestCount, _ := newIndexServer(indexBody([]map[string]any{
				indexItem("tid-a", c.vaultStatus, c.vaultPhase),
				indexItem("tid-b", "completed", "done"),
				indexItem("tid-c", "completed", "done"),
			}, 12, c.warnings))
			client := claim.NewVaultAPIIndexClient(
				server.URL, "secret", "agent-task-executor",
			)

			divergent := ownedTask("tid-a", "completed", "done")
			if c.hiddenTitle != "" {
				divergent.Frontmatter["title"] = c.hiddenTitle
				divergent.Frontmatter["assignee"] = c.hiddenAssignee
			}
			fakeView.SnapshotReturns([]lib.Task{
				divergent,
				ownedTask("tid-b", "completed", "done"),
				ownedTask("tid-c", "completed", "done"),
			})
			h := newDivergenceHandler(client, fakeView)

			out := captureStderr(func() {
				Expect(h.ReconcileOnce(ctx)).To(Succeed())
			})

			if c.wantDivergent == 1 {
				Expect(out).To(ContainSubstring(c.wantLine))
				Expect(out).To(ContainSubstring(fmt.Sprintf(
					"event=view_divergence_summary compared=3 divergent=1 index_age_seconds=12 warnings=%d",
					c.warnings,
				)))
			} else {
				Expect(out).NotTo(ContainSubstring("event=view_divergence task=tid-a"))
				Expect(out).To(ContainSubstring(fmt.Sprintf(
					"event=view_divergence_summary compared=3 divergent=0 index_age_seconds=12 warnings=%d",
					c.warnings,
				)))
			}
			// The matching ids never produce a finding.
			Expect(out).NotTo(ContainSubstring("event=view_divergence task=tid-b"))
			Expect(out).NotTo(ContainSubstring("event=view_divergence task=tid-c"))

			// The line carries ids, statuses and phases only.
			if c.hiddenTitle != "" {
				Expect(out).NotTo(ContainSubstring(c.hiddenTitle))
				Expect(out).NotTo(ContainSubstring(c.hiddenAssignee))
			}

			// The pass sends exactly one unfiltered, authenticated read.
			Expect(atomic.LoadInt32(requestCount)).To(Equal(int32(1)))
			method, path, query, gotHeaders := recordSnapshot()
			Expect(method).To(Equal(http.MethodGet))
			Expect(path).To(Equal("/api/v1/tasks"))
			Expect(query).To(Equal(""))
			Expect(gotHeaders.Get("X-Gateway-Secret")).To(Equal("secret"))
			Expect(gotHeaders.Get("X-Gateway-Initator")).To(Equal("agent-task-executor"))
		},
		Entry("status differs", divergenceCase{
			vaultStatus:    "aborted",
			vaultPhase:     "done",
			warnings:       3,
			wantDivergent:  1,
			wantLine:       "event=view_divergence task=tid-a view_status=completed vault_status=aborted view_phase=done vault_phase=done",
			hiddenTitle:    "distinctive-title",
			hiddenAssignee: "distinctive-assignee",
		}),
		Entry("phase differs", divergenceCase{
			vaultStatus:   "completed",
			vaultPhase:    "human_review",
			warnings:      0,
			wantDivergent: 1,
			wantLine:      "event=view_divergence task=tid-a view_status=completed vault_status=completed view_phase=done vault_phase=human_review",
		}),
		Entry("both agree", divergenceCase{
			vaultStatus:   "completed",
			vaultPhase:    "done",
			warnings:      0,
			wantDivergent: 0,
		}),
	)

	It("reports an owned id missing from the index as absent", func() {
		server, requestCount, _ := newIndexServer(indexBody([]map[string]any{
			indexItem("tid-a", "completed", "done"),
			indexItem("tid-c", "completed", "done"),
		}, 12, 0))
		client := claim.NewVaultAPIIndexClient(
			server.URL, "secret", "agent-task-executor",
		)
		fakeView.SnapshotReturns(terminalOwnedSet())
		h := newDivergenceHandler(client, fakeView)

		beforeAbsent := testutil.ToFloat64(
			metrics.ViewDivergenceTotal.WithLabelValues("absent"),
		)
		beforeMismatch := testutil.ToFloat64(
			metrics.ViewDivergenceTotal.WithLabelValues("mismatch"),
		)

		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(out).To(ContainSubstring("event=view_divergence_absent task=tid-b"))
		Expect(out).NotTo(ContainSubstring("event=view_divergence task=tid-b"))
		Expect(out).To(ContainSubstring(
			"event=view_divergence_summary compared=2 divergent=0",
		))
		Expect(
			testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("absent")),
		).To(Equal(beforeAbsent + 1))
		Expect(
			testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("mismatch")),
		).To(Equal(beforeMismatch))
		Expect(atomic.LoadInt32(requestCount)).To(Equal(int32(1)))
	})

	DescribeTable("an empty index reports every owned id as absent",
		func() {
			server, requestCount, _ := newIndexServer(
				indexBody([]map[string]any{}, 12, 0),
			)
			client := claim.NewVaultAPIIndexClient(
				server.URL, "secret", "agent-task-executor",
			)
			fakeView.SnapshotReturns(terminalOwnedSet())
			h := newDivergenceHandler(client, fakeView)

			out := captureStderr(func() {
				Expect(h.ReconcileOnce(ctx)).To(Succeed())
			})

			Expect(out).To(ContainSubstring(
				"event=view_divergence_summary compared=0 divergent=0",
			))
			Expect(strings.Count(out, "event=view_divergence_absent")).To(Equal(3))
			Expect(atomic.LoadInt32(requestCount)).To(Equal(int32(1)))
		},
		Entry("empty items"),
	)

	It("caps the per-id lines and keeps the full count in the summary", func() {
		// An index that returns nothing classifies every owned task as absent.
		// Naming all of them would write one warning per owned task per pass,
		// once a minute — a worse fault than the divergence it reports. The
		// counts stay exact; only the first maxDivergenceLogLines ids are named.
		server, _, _ := newIndexServer(indexBody([]map[string]any{}, 12, 0))
		client := claim.NewVaultAPIIndexClient(
			server.URL, "secret", "agent-task-executor",
		)
		owned := make([]lib.Task, 0, 40)
		for i := 0; i < 40; i++ {
			owned = append(owned, ownedTask(fmt.Sprintf("tid-%02d", i), "completed", "done"))
		}
		fakeView.SnapshotReturns(owned)
		h := newDivergenceHandler(client, fakeView)

		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(strings.Count(out, "event=view_divergence_absent")).To(Equal(20))
		Expect(out).To(ContainSubstring("absent=40 named=0 named_absent=20"))
	})

	It("names a divergence even when absent ids would exhaust the budget", func() {
		// Absences outnumber divergences by orders of magnitude — measured on dev
		// 2026-10-27, 74 against 3. A budget shared between the two is spent on
		// whichever ids sort first, which left one of those three divergences
		// unnamed. Divergences are the finding the check exists to report, so they
		// are named first and counted separately.
		server, _, _ := newIndexServer(indexBody([]map[string]any{
			{"id": "tid-39", "status": "in_progress", "phase": "done"},
		}, 12, 0))
		client := claim.NewVaultAPIIndexClient(
			server.URL, "secret", "agent-task-executor",
		)
		owned := make([]lib.Task, 0, 40)
		for i := 0; i < 40; i++ {
			owned = append(owned, ownedTask(fmt.Sprintf("tid-%02d", i), "completed", "done"))
		}
		fakeView.SnapshotReturns(owned)
		h := newDivergenceHandler(client, fakeView)

		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(out).To(ContainSubstring("event=view_divergence task=tid-39"))
		Expect(strings.Count(out, "event=view_divergence_absent")).To(Equal(20))
		Expect(out).To(ContainSubstring("named=1 named_absent=20"))
	})

	It("makes one index request per pass regardless of task count", func() {
		const taskCount = 50
		items := make([]map[string]any, 0, taskCount)
		tasks := make([]lib.Task, 0, taskCount)
		for i := 0; i < taskCount; i++ {
			id := fmt.Sprintf("tid-%d", i)
			items = append(items, indexItem(id, "completed", "done"))
			tasks = append(tasks, ownedTask(id, "completed", "done"))
		}
		server, requestCount, _ := newIndexServer(indexBody(items, 12, 0))
		client := claim.NewVaultAPIIndexClient(
			server.URL, "secret", "agent-task-executor",
		)
		fakeView.SnapshotReturns(tasks)
		h := newDivergenceHandler(client, fakeView)

		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(atomic.LoadInt32(requestCount)).To(Equal(int32(1)))
		Expect(out).To(ContainSubstring(
			"event=view_divergence_summary compared=50 divergent=0",
		))
	})

	It("a failed index read does not stop the pass", func() {
		// A closed port is the closed-port evidence: the read fails, the pass
		// still drives its candidates and logs the failure.
		server := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {},
		))
		url := server.URL
		server.Close()

		client := claim.NewVaultAPIIndexClient(url, "", "")
		fakeResolver.ResolveReturns(
			pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"},
			nil,
		)
		fakeSpawner.IsJobActiveReturns(false, nil)
		fakeView.SnapshotReturns([]lib.Task{{
			TaskIdentifier: lib.TaskIdentifier("tid-a"),
			Vault:          testTaskVault,
			Frontmatter: lib.TaskFrontmatter{
				"status":   "in_progress",
				"phase":    "execution",
				"assignee": "claude",
				"stage":    "prod",
			},
		}})
		h := newDivergenceHandler(client, fakeView)

		beforeReadError := testutil.ToFloat64(
			metrics.ViewDivergenceTotal.WithLabelValues("read_error"),
		)
		beforeOK := testutil.ToFloat64(
			metrics.ViewDivergenceTotal.WithLabelValues("ok"),
		)

		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(out).To(ContainSubstring("event=view_divergence_read_error"))
		Expect(out).To(ContainSubstring("event=reconcile evaluated="))
		Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
		Expect(
			testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("read_error")),
		).To(Equal(beforeReadError + 1))
		Expect(
			testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("ok")),
		).To(Equal(beforeOK))
	})

	It("a deployment with no index client emits nothing", func() {
		fakeView.SnapshotReturns(terminalOwnedSet())
		h := buildHandler(fakeView)

		before := map[string]float64{}
		for _, result := range []string{"ok", "mismatch", "absent", "read_error"} {
			before[result] = testutil.ToFloat64(
				metrics.ViewDivergenceTotal.WithLabelValues(result),
			)
		}

		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(out).NotTo(ContainSubstring("event=view_divergence"))
		for result, want := range before {
			Expect(
				testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues(result)),
			).To(Equal(want), result)
		}
	})

	It("a healthy pass counts one summary and one evaluated line", func() {
		server, _, _ := newIndexServer(indexBody([]map[string]any{
			indexItem("tid-a", "aborted", "done"),
			indexItem("tid-b", "completed", "done"),
			indexItem("tid-c", "completed", "done"),
		}, 12, 0))
		client := claim.NewVaultAPIIndexClient(
			server.URL, "secret", "agent-task-executor",
		)
		fakeView.SnapshotReturns(terminalOwnedSet())
		h := newDivergenceHandler(client, fakeView)

		beforeOK := testutil.ToFloat64(
			metrics.ViewDivergenceTotal.WithLabelValues("ok"),
		)

		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		Expect(strings.Count(out, "event=view_divergence_summary")).To(Equal(1))
		Expect(strings.Count(out, "event=reconcile evaluated=")).To(Equal(1))
		Expect(
			testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("ok")),
		).To(Equal(beforeOK + 1))
	})

	DescribeTable("a pass that returns before the check makes no index request",
		func(tasks []lib.Task, ready, expire bool, wantLine string) {
			server, requestCount, _ := newIndexServer(indexBody([]map[string]any{
				indexItem("tid-a", "completed", "done"),
				indexItem("tid-b", "completed", "done"),
				indexItem("tid-c", "completed", "done"),
			}, 12, 0))
			client := claim.NewVaultAPIIndexClient(
				server.URL, "secret", "agent-task-executor",
			)
			fakeView.ReadyReturns(ready)
			fakeView.SnapshotReturns(tasks)
			h := newDivergenceHandler(client, fakeView)

			runCtx := ctx
			if expire {
				var cancel context.CancelFunc
				runCtx, cancel = context.WithCancel(ctx)
				cancel()
			}

			out := captureStderr(func() {
				Expect(h.ReconcileOnce(runCtx)).To(Succeed())
			})

			Expect(atomic.LoadInt32(requestCount)).To(Equal(int32(0)))
			Expect(out).NotTo(ContainSubstring("event=view_divergence"))
			Expect(out).To(ContainSubstring(wantLine))
		},
		Entry(
			"view not ready",
			terminalOwnedSet(),
			false,
			false,
			"event=reconcile_view_not_ready",
		),
		Entry(
			"task vault unstamped",
			[]lib.Task{{
				TaskIdentifier: lib.TaskIdentifier("tid-a"),
				Vault:          "",
				Frontmatter: lib.TaskFrontmatter{
					"status": "completed",
					"phase":  "done",
				},
			}},
			true,
			false,
			"event=task_vault_unstamped",
		),
		Entry(
			"task vault mismatch",
			[]lib.Task{{
				TaskIdentifier: lib.TaskIdentifier("tid-a"),
				Vault:          "other-vault",
				Frontmatter: lib.TaskFrontmatter{
					"status": "completed",
					"phase":  "done",
				},
			}},
			true,
			false,
			"event=task_vault_mismatch",
		),
		Entry(
			"context already expired",
			terminalOwnedSet(),
			true,
			true,
			"event=reconcile_aborted",
		),
	)
})
