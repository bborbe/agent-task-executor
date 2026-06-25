// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"

	"github.com/bborbe/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	agentv1 "github.com/bborbe/agent/task/executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent/task/executor/pkg"
)

// fakeProvider is a simple in-memory Provider[agentv1.Config] for tests.
type fakeProvider struct {
	items []agentv1.Config
	err   error
}

func (f *fakeProvider) Get(_ context.Context) ([]agentv1.Config, error) {
	return f.items, f.err
}

var _ = Describe("ConfigResolver", func() {
	var (
		ctx      context.Context
		provider *fakeProvider
		resolver pkg.ConfigResolver
	)

	BeforeEach(func() {
		ctx = context.Background()
		provider = &fakeProvider{}
		resolver = pkg.NewConfigResolver(provider, "dev")
	})

	It("returns converted AgentConfiguration with image tag appended", func() {
		provider.items = []agentv1.Config{
			{
				Spec: agentv1.ConfigSpec{
					Assignee:        "claude",
					Image:           "foo/bar",
					Heartbeat:       "30m",
					Env:             map[string]string{"KEY": "val"},
					SecretName:      "my-secret",
					VolumeClaim:     "my-pvc",
					VolumeMountPath: "/mnt/data",
					Resources: &agentv1.AgentResources{
						Requests: agentv1.AgentResourceList{
							CPU:              "500m",
							Memory:           "1Gi",
							EphemeralStorage: "2Gi",
						},
						Limits: agentv1.AgentResourceList{
							CPU:              "1",
							Memory:           "2Gi",
							EphemeralStorage: "4Gi",
						},
					},
				},
			},
		}
		config, err := resolver.Resolve(ctx, "claude")
		Expect(err).To(BeNil())
		Expect(config.Assignee).To(Equal("claude"))
		Expect(config.Image).To(Equal("foo/bar:dev"))
		Expect(config.Env).To(Equal(map[string]string{"KEY": "val"}))
		Expect(config.SecretName).To(Equal("my-secret"))
		Expect(config.VolumeClaim).To(Equal("my-pvc"))
		Expect(config.VolumeMountPath).To(Equal("/mnt/data"))
		Expect(config.Resources).NotTo(BeNil())
		Expect(config.Resources.Requests.CPU).To(Equal("500m"))
		Expect(config.Resources.Requests.Memory).To(Equal("1Gi"))
		Expect(config.Resources.Requests.EphemeralStorage).To(Equal("2Gi"))
		Expect(config.Resources.Limits.CPU).To(Equal("1"))
		Expect(config.Resources.Limits.Memory).To(Equal("2Gi"))
		Expect(config.Resources.Limits.EphemeralStorage).To(Equal("4Gi"))
	})

	It("leaves Resources nil when Spec.Resources is nil", func() {
		provider.items = []agentv1.Config{
			{
				Spec: agentv1.ConfigSpec{
					Assignee:  "claude",
					Image:     "foo/bar",
					Heartbeat: "30m",
				},
			},
		}
		config, err := resolver.Resolve(ctx, "claude")
		Expect(err).To(BeNil())
		Expect(config.Resources).To(BeNil())
	})

	It("returns ErrConfigNotFound when no item matches", func() {
		provider.items = []agentv1.Config{
			{Spec: agentv1.ConfigSpec{Assignee: "other-agent", Image: "img", Heartbeat: "1m"}},
		}
		_, err := resolver.Resolve(ctx, "claude")
		Expect(err).NotTo(BeNil())
		Expect(errors.Is(err, pkg.ErrConfigNotFound)).To(BeTrue())
	})

	It("returns ErrConfigNotFound when store is empty", func() {
		provider.items = []agentv1.Config{}
		_, err := resolver.Resolve(ctx, "claude")
		Expect(err).NotTo(BeNil())
		Expect(errors.Is(err, pkg.ErrConfigNotFound)).To(BeTrue())
	})

	It("returns a wrapped error when provider.Get fails", func() {
		provider.err = errors.Errorf(ctx, "storage unavailable")
		_, err := resolver.Resolve(ctx, "claude")
		Expect(err).NotTo(BeNil())
		Expect(errors.Is(err, pkg.ErrConfigNotFound)).To(BeFalse())
	})

	It(
		"defensively copies env map — mutation after Resolve does not affect returned config",
		func() {
			originalEnv := map[string]string{"KEY": "val"}
			provider.items = []agentv1.Config{
				{
					Spec: agentv1.ConfigSpec{
						Assignee:  "claude",
						Image:     "img",
						Heartbeat: "1m",
						Env:       originalEnv,
					},
				},
			}
			config, err := resolver.Resolve(ctx, "claude")
			Expect(err).To(BeNil())
			originalEnv["KEY"] = "mutated"
			Expect(config.Env["KEY"]).To(Equal("val"))
		},
	)

	It("branch tagging: given branch=dev and Image=foo/bar, result has Image==foo/bar:dev", func() {
		provider.items = []agentv1.Config{
			{Spec: agentv1.ConfigSpec{Assignee: "claude", Image: "foo/bar", Heartbeat: "1m"}},
		}
		config, err := resolver.Resolve(ctx, "claude")
		Expect(err).To(BeNil())
		Expect(config.Image).To(Equal("foo/bar:dev"))
	})
})
