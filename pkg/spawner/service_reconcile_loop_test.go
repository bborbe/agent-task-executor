// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner_test

import (
	"context"

	"github.com/bborbe/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/mocks"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/spawner"
)

// fakeProviderImpl satisfies k8s.Provider[agentv1.Config] for the loop specs.
type fakeProviderImpl struct {
	configs []agentv1.Config
	err     error
}

func (p *fakeProviderImpl) Get(_ context.Context) ([]agentv1.Config, error) {
	result := make([]agentv1.Config, len(p.configs))
	copy(result, p.configs)
	return result, p.err
}

var _ = Describe("ServiceReconcileLoop", func() {
	var (
		ctx        context.Context
		provider   *fakeProviderImpl
		resolver   *mocks.FakeConfigResolver
		reconciler *mocks.FakeServiceReconciler
		loop       spawner.ServiceReconcileLoop
	)

	serviceConfig := func(name string) agentv1.Config {
		return agentv1.Config{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "dev"},
			Spec: agentv1.ConfigSpec{
				Assignee: name,
				Image:    "docker.io/bborbe/agent-pi:v0.1.7",
				Type:     agentv1.AgentTypeService,
			},
		}
	}

	BeforeEach(func() {
		ctx = context.Background()
		provider = &fakeProviderImpl{}
		resolver = &mocks.FakeConfigResolver{}
		reconciler = &mocks.FakeServiceReconciler{}
		resolver.ResolveReturns(pkg.AgentConfiguration{
			Assignee: "identity",
			Type:     agentv1.AgentTypeService,
			Image:    "docker.io/bborbe/agent-pi:v0.1.7",
		}, nil)
		loop = spawner.NewServiceReconcileLoop(provider, resolver, reconciler, 0)
	})

	It("reconciles a service Config", func() {
		provider.configs = []agentv1.Config{serviceConfig("identity")}

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		Expect(reconciler.ReconcileServiceCallCount()).To(Equal(1))
	})

	It("does not deploy a job Config, and removes any StatefulSet it once owned", func() {
		job := serviceConfig("claude")
		job.Spec.Type = agentv1.AgentTypeJob
		provider.configs = []agentv1.Config{job}

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		Expect(reconciler.ReconcileServiceCallCount()).To(Equal(0))
		Expect(reconciler.UndeployServiceCallCount()).To(Equal(1))
		_, name := reconciler.UndeployServiceArgsForCall(0)
		Expect(name).To(Equal("claude"))
	})

	It("treats an unset type as job, matching the CRD contract, and undeploys it", func() {
		unset := serviceConfig("claude")
		unset.Spec.Type = ""
		provider.configs = []agentv1.Config{unset}

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		Expect(reconciler.ReconcileServiceCallCount()).To(Equal(0))
		Expect(reconciler.UndeployServiceCallCount()).To(Equal(1))
	})

	It("does not undeploy a Config that is still a service", func() {
		provider.configs = []agentv1.Config{serviceConfig("identity")}

		Expect(loop.ReconcileOnce(ctx)).To(Succeed())
		Expect(reconciler.UndeployServiceCallCount()).To(Equal(0))
	})

	It("reports an undeploy failure without aborting the pass", func() {
		job := serviceConfig("claude")
		job.Spec.Type = agentv1.AgentTypeJob
		provider.configs = []agentv1.Config{job, serviceConfig("identity")}
		reconciler.UndeployServiceReturns(errors.Errorf(ctx, "undeploy failed"))

		Expect(loop.ReconcileOnce(ctx)).To(HaveOccurred())
		Expect(reconciler.ReconcileServiceCallCount()).To(Equal(1))
	})

	It("keeps going when one Config fails, so a single bad agent cannot stop the rest", func() {
		provider.configs = []agentv1.Config{serviceConfig("broken"), serviceConfig("identity")}
		reconciler.ReconcileServiceReturnsOnCall(
			0,
			errors.Errorf(ctx, "deploy failed"),
		)

		err := loop.ReconcileOnce(ctx)
		Expect(err).To(HaveOccurred())
		Expect(reconciler.ReconcileServiceCallCount()).To(Equal(2))
	})

	It("reports a resolve failure without aborting the pass", func() {
		provider.configs = []agentv1.Config{serviceConfig("identity")}
		resolver.ResolveReturns(pkg.AgentConfiguration{}, errors.Errorf(ctx, "no config"))

		Expect(loop.ReconcileOnce(ctx)).To(HaveOccurred())
		Expect(reconciler.ReconcileServiceCallCount()).To(Equal(0))
	})

	It("returns the provider error", func() {
		provider.err = errors.Errorf(ctx, "store unavailable")

		Expect(loop.ReconcileOnce(ctx)).To(HaveOccurred())
	})

	It("stops the pass when the context is cancelled", func() {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		provider.configs = []agentv1.Config{serviceConfig("identity")}

		Expect(loop.ReconcileOnce(cancelled)).To(HaveOccurred())
		Expect(reconciler.ReconcileServiceCallCount()).To(Equal(0))
	})
})
