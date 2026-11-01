// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	stderrors "errors"
	"fmt"
	"regexp"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// acceptedSeries is one gathered executor_config_accepted sample.
type acceptedSeries struct {
	phase    string
	taskType string
	vault    string
	value    float64
}

// acceptedTestProvider is a controllable k8s.Provider[agentv1.Config].
type acceptedTestProvider struct {
	configs []agentv1.Config
	err     error
}

func (p *acceptedTestProvider) Get(_ context.Context) ([]agentv1.Config, error) {
	out := make([]agentv1.Config, len(p.configs))
	copy(out, p.configs)
	return out, p.err
}

// configAcceptedSummary matches the one-line per-pass summary the refresh logs.
var configAcceptedSummary = regexp.MustCompile(
	`event=config_accepted_refreshed configs=(\d+) pairs=(\d+) deleted=(\d+)`,
)

// acceptedDefaultPhases is the phase allow-list a Config without a Trigger
// receives, mirrored here so the fixtures spell out the expected tuples.
var acceptedDefaultPhases = domain.TaskPhases{
	domain.TaskPhasePlanning,
	domain.TaskPhaseExecution,
	domain.TaskPhaseAIReview,
}

// gatherConfigAccepted reads every published accepted-pair series from the
// default registry.
func gatherConfigAccepted() []acceptedSeries {
	mfs, err := prometheus.DefaultGatherer.Gather()
	Expect(err).NotTo(HaveOccurred())

	for _, mf := range mfs {
		if mf.GetName() != "executor_config_accepted" {
			continue
		}
		series := make([]acceptedSeries, 0, len(mf.GetMetric()))
		for _, metric := range mf.GetMetric() {
			sample := acceptedSeries{value: metric.GetGauge().GetValue()}
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "phase":
					sample.phase = label.GetValue()
				case "task_type":
					sample.taskType = label.GetValue()
				case "vault":
					sample.vault = label.GetValue()
				}
			}
			series = append(series, sample)
		}
		return series
	}
	return nil
}

// clearConfigAccepted deletes every published series by label values. It never
// calls Reset(): the production contract forbids it, so the fixtures must not
// rely on it either.
func clearConfigAccepted() {
	for _, sample := range gatherConfigAccepted() {
		metrics.ConfigAccepted.DeleteLabelValues(sample.phase, sample.taskType, sample.vault)
	}
}

// seriesForVault narrows a gather to one fixture's vault slug.
func seriesForVault(series []acceptedSeries, vault string) []acceptedSeries {
	result := make([]acceptedSeries, 0, len(series))
	for _, sample := range series {
		if sample.vault == vault {
			result = append(result, sample)
		}
	}
	return result
}

// acceptedForTypes expands a phase allow-list crossed with task types.
func acceptedForTypes(
	vault string,
	phases domain.TaskPhases,
	taskTypes ...string,
) []acceptedSeries {
	result := make([]acceptedSeries, 0, len(phases)*len(taskTypes))
	for _, phase := range phases {
		for _, taskType := range taskTypes {
			result = append(result, acceptedSeries{
				phase:    string(phase),
				taskType: taskType,
				vault:    vault,
				value:    1,
			})
		}
	}
	return result
}

// acceptedConfig builds a Config fixture declaring taskTypes under trigger.
func acceptedConfig(name string, trigger *agentv1.Trigger, taskTypes ...string) agentv1.Config {
	return agentv1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: agentv1.ConfigSpec{
			Assignee:  name,
			TaskTypes: taskTypes,
			Trigger:   trigger,
		},
	}
}

// executionOnlyTrigger restricts a Config to the execution phase.
func executionOnlyTrigger() *agentv1.Trigger {
	return &agentv1.Trigger{Phases: domain.TaskPhases{domain.TaskPhaseExecution}}
}

// acceptedTestView returns a ready view with an empty snapshot.
func acceptedTestView() *mocks.FakeTaskView {
	view := &mocks.FakeTaskView{}
	view.ReadyReturns(true)
	view.SnapshotReturns(nil)
	view.GetReturns(lib.Task{}, false)
	return view
}

// newAcceptedTestHandler builds a handler with the accepted-set provider wired.
func newAcceptedTestHandler(
	provider *acceptedTestProvider,
	view handler.TaskView,
	taskVault string,
) handler.ClaimLifecycleEventHandler {
	return handler.NewTaskEventHandler(
		new(mocks.FakeJobSpawner),
		"dev",
		base.Branch("prod"),
		"",
		true,
		true,
		&mocks.FakeConfigResolver{},
		&mocks.FakeResultPublisher{},
		pkg.NewTaskStore(),
		libtime.NewCurrentDateTime(),
		view,
		taskVault,
		handler.DefaultReconcilePassTimeout,
		handler.DefaultReconcileInterval,
		pkg.ClaimModeOff,
		nil,
		handler.WithConfigProvider(provider),
	)
}

// acceptedFixture is one provider snapshot and the tuples it must publish.
type acceptedFixture struct {
	vault    string
	initial  []agentv1.Config
	after    []agentv1.Config
	rerun    bool
	expected []acceptedSeries
	deleted  int
}

var _ = Describe("Config accepted metric", func() {
	BeforeEach(func() {
		clearConfigAccepted()
	})

	DescribeTable(
		"publishes the accepted set",
		func(fixture acceptedFixture) {
			ctx := context.Background()
			provider := &acceptedTestProvider{configs: fixture.initial}
			h := newAcceptedTestHandler(provider, acceptedTestView(), fixture.vault)

			Expect(h.ReconcileOnce(ctx)).To(Succeed())

			if fixture.rerun {
				provider.configs = fixture.after
				readErrorsBefore := testutil.ToFloat64(
					metrics.ConfigAcceptedRefreshTotal.WithLabelValues("read_error"),
				)
				out := captureStderr(func() {
					Expect(h.ReconcileOnce(ctx)).To(Succeed())
				})
				Expect(testutil.ToFloat64(
					metrics.ConfigAcceptedRefreshTotal.WithLabelValues("read_error"),
				)).To(Equal(readErrorsBefore))
				matches := configAcceptedSummary.FindAllStringSubmatch(out, -1)
				Expect(matches).To(HaveLen(1))
				Expect(matches[0][3]).To(Equal(fmt.Sprintf("%d", fixture.deleted)))
			}

			gathered := seriesForVault(gatherConfigAccepted(), fixture.vault)
			for _, sample := range gathered {
				Expect(sample.taskType).NotTo(BeEmpty())
				Expect(sample.value).To(Equal(float64(1)))
			}
			if len(fixture.expected) == 0 {
				Expect(gathered).To(BeEmpty())
				return
			}
			Expect(gathered).To(ConsistOf(fixture.expected))
		},
		Entry("unions both Configs' pairs", acceptedFixture{
			vault: "cfgtest-a",
			initial: []agentv1.Config{
				acceptedConfig("a", executionOnlyTrigger(), "github-update-go"),
				acceptedConfig("b", nil, "probe", "review"),
			},
			expected: append(
				acceptedForTypes(
					"cfgtest-a",
					domain.TaskPhases{domain.TaskPhaseExecution},
					"github-update-go",
				),
				acceptedForTypes("cfgtest-a", acceptedDefaultPhases, "probe", "review")...,
			),
		}),
		Entry(
			"removes exactly the pairs of a dropped type and keeps survivors at 1",
			acceptedFixture{
				vault: "cfgtest-b",
				initial: []agentv1.Config{
					acceptedConfig("a", executionOnlyTrigger(), "github-update-go"),
					acceptedConfig("b", nil, "probe", "review"),
				},
				after: []agentv1.Config{
					acceptedConfig("a", executionOnlyTrigger(), "github-update-go"),
					acceptedConfig("b", nil, "probe"),
				},
				rerun:   true,
				deleted: 3,
				expected: append(
					acceptedForTypes(
						"cfgtest-b",
						domain.TaskPhases{domain.TaskPhaseExecution},
						"github-update-go",
					),
					acceptedForTypes("cfgtest-b", acceptedDefaultPhases, "probe")...,
				),
			},
		),
		Entry("dedupes a pair two Configs declare", acceptedFixture{
			vault: "cfgtest-c",
			initial: []agentv1.Config{
				acceptedConfig("a", executionOnlyTrigger(), "probe"),
				acceptedConfig("b", executionOnlyTrigger(), "probe"),
			},
			expected: acceptedForTypes(
				"cfgtest-c",
				domain.TaskPhases{domain.TaskPhaseExecution},
				"probe",
			),
		}),
		Entry("empties the family when every Config is gone", acceptedFixture{
			vault: "cfgtest-d",
			initial: []agentv1.Config{
				acceptedConfig("a", executionOnlyTrigger(), "github-update-go"),
				acceptedConfig("b", nil, "probe", "review"),
			},
			after:   []agentv1.Config{},
			rerun:   true,
			deleted: 7,
		}),
		Entry("contributes nothing for a Config with no task types", acceptedFixture{
			vault: "cfgtest-e",
			initial: []agentv1.Config{{
				ObjectMeta: metav1.ObjectMeta{Name: "empty"},
				Spec:       agentv1.ConfigSpec{Assignee: "claude"},
			}},
		}),
	)

	It("distinguishes the three refresh outcomes", func() {
		ctx := context.Background()
		provider := &acceptedTestProvider{configs: []agentv1.Config{
			acceptedConfig("a", executionOnlyTrigger(), "probe"),
		}}
		h := newAcceptedTestHandler(provider, acceptedTestView(), "cfgtest-f")
		Expect(h.ReconcileOnce(ctx)).To(Succeed())

		baseline := seriesForVault(gatherConfigAccepted(), "cfgtest-f")
		Expect(baseline).To(ConsistOf(acceptedForTypes(
			"cfgtest-f",
			domain.TaskPhases{domain.TaskPhaseExecution},
			"probe",
		)))

		// Success arm: exactly one `ok`, no read_error, same set.
		okBefore := testutil.ToFloat64(metrics.ConfigAcceptedRefreshTotal.WithLabelValues("ok"))
		readErrorBefore := testutil.ToFloat64(
			metrics.ConfigAcceptedRefreshTotal.WithLabelValues("read_error"),
		)
		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(testutil.ToFloat64(
			metrics.ConfigAcceptedRefreshTotal.WithLabelValues("ok"),
		)).To(Equal(okBefore + 1))
		Expect(testutil.ToFloat64(
			metrics.ConfigAcceptedRefreshTotal.WithLabelValues("read_error"),
		)).To(Equal(readErrorBefore))
		Expect(seriesForVault(gatherConfigAccepted(), "cfgtest-f")).To(ConsistOf(baseline))

		// Cancelled arm: counts as a read error and publishes nothing.
		okBefore = testutil.ToFloat64(metrics.ConfigAcceptedRefreshTotal.WithLabelValues("ok"))
		readErrorBefore = testutil.ToFloat64(
			metrics.ConfigAcceptedRefreshTotal.WithLabelValues("read_error"),
		)
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		_ = h.ReconcileOnce(cancelled)
		Expect(testutil.ToFloat64(
			metrics.ConfigAcceptedRefreshTotal.WithLabelValues("read_error"),
		)).To(Equal(readErrorBefore + 1))
		Expect(testutil.ToFloat64(
			metrics.ConfigAcceptedRefreshTotal.WithLabelValues("ok"),
		)).To(Equal(okBefore))
		Expect(seriesForVault(gatherConfigAccepted(), "cfgtest-f")).To(ConsistOf(baseline))

		// Failed-read arm: the pass continues, the previous set survives.
		provider.err = stderrors.New("boom")
		okBefore = testutil.ToFloat64(metrics.ConfigAcceptedRefreshTotal.WithLabelValues("ok"))
		readErrorBefore = testutil.ToFloat64(
			metrics.ConfigAcceptedRefreshTotal.WithLabelValues("read_error"),
		)
		Expect(h.ReconcileOnce(ctx)).To(Succeed())
		Expect(testutil.ToFloat64(
			metrics.ConfigAcceptedRefreshTotal.WithLabelValues("read_error"),
		)).To(Equal(readErrorBefore + 1))
		Expect(testutil.ToFloat64(
			metrics.ConfigAcceptedRefreshTotal.WithLabelValues("ok"),
		)).To(Equal(okBefore))
		Expect(seriesForVault(gatherConfigAccepted(), "cfgtest-f")).To(ConsistOf(baseline))
	})

	It("labels the published set with the injected task vault", func() {
		ctx := context.Background()
		provider := &acceptedTestProvider{configs: []agentv1.Config{
			acceptedConfig("a", executionOnlyTrigger(), "probe"),
		}}

		shared := newAcceptedTestHandler(provider, acceptedTestView(), "shared")
		Expect(shared.ReconcileOnce(ctx)).To(Succeed())
		Expect(seriesForVault(gatherConfigAccepted(), "shared")).To(ConsistOf(
			acceptedForTypes(
				"shared",
				domain.TaskPhases{domain.TaskPhaseExecution},
				"probe",
			),
		))

		clearConfigAccepted()

		personal := newAcceptedTestHandler(provider, acceptedTestView(), "personal")
		Expect(personal.ReconcileOnce(ctx)).To(Succeed())
		Expect(seriesForVault(gatherConfigAccepted(), "personal")).To(ConsistOf(
			acceptedForTypes(
				"personal",
				domain.TaskPhases{domain.TaskPhaseExecution},
				"probe",
			),
		))
	})

	It("publishes from a pass that aborts on view-not-ready and logs one summary", func() {
		ctx := context.Background()
		provider := &acceptedTestProvider{configs: []agentv1.Config{
			acceptedConfig("a", nil, "probe"),
			acceptedConfig("b", nil, "review"),
		}}
		view := acceptedTestView()
		view.ReadyReturns(false)
		h := newAcceptedTestHandler(provider, view, "cfgtest-g")

		out := captureStderr(func() {
			Expect(h.ReconcileOnce(ctx)).To(Succeed())
		})

		gathered := seriesForVault(gatherConfigAccepted(), "cfgtest-g")
		Expect(gathered).To(HaveLen(6))
		matches := configAcceptedSummary.FindAllStringSubmatch(out, -1)
		Expect(matches).To(HaveLen(1))
		Expect(matches[0][1]).To(Equal("2"))
		Expect(matches[0][2]).To(Equal(fmt.Sprintf("%d", len(gathered))))
	})

	It("publishes nothing when the reconcile pass never runs", func() {
		provider := &acceptedTestProvider{configs: []agentv1.Config{
			acceptedConfig("a", executionOnlyTrigger(), "probe"),
		}}
		out := captureStderr(func() {
			_ = newAcceptedTestHandler(provider, acceptedTestView(), "cfgtest-h")
		})

		Expect(out).NotTo(ContainSubstring("event=config_accepted_refreshed"))
		Expect(seriesForVault(gatherConfigAccepted(), "cfgtest-h")).To(BeEmpty())
	})
})
