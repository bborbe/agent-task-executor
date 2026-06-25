// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	"github.com/bborbe/agent-task-executor/pkg"
)

func TestPkg(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pkg Suite")
}

var _ = Describe("AgentConfiguration", func() {
	Describe("EffectiveZombieJobTimeoutSeconds", func() {
		ptrInt32 := func(v int32) *int32 { return &v }

		It("returns default when ZombieJobTimeoutSeconds is nil", func() {
			cfg := pkg.AgentConfiguration{}
			Expect(cfg.EffectiveZombieJobTimeoutSeconds()).To(Equal(int32(1800)))
		})

		It("returns configured value when set", func() {
			cfg := pkg.AgentConfiguration{ZombieJobTimeoutSeconds: ptrInt32(900)}
			Expect(cfg.EffectiveZombieJobTimeoutSeconds()).To(Equal(int32(900)))
		})
	})
})

var _ = Describe("AgentConfigurations", func() {
	var configs pkg.AgentConfigurations

	BeforeEach(func() {
		configs = pkg.AgentConfigurations{
			{
				Assignee: "claude",
				Image:    "registry/agent-claude",
				Env:      map[string]string{},
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
			{
				Assignee: "backtest-agent",
				Image:    "registry/agent-backtest",
				Env:      map[string]string{"GEMINI_API_KEY": "test-key"},
			},
		}
	})

	Describe("FindByAssignee", func() {
		It("returns config when found", func() {
			config, ok := configs.FindByAssignee("claude")
			Expect(ok).To(BeTrue())
			Expect(config.Assignee).To(Equal("claude"))
			Expect(config.Image).To(Equal("registry/agent-claude"))
		})

		It("returns backtest config when found", func() {
			config, ok := configs.FindByAssignee("backtest-agent")
			Expect(ok).To(BeTrue())
			Expect(config.Assignee).To(Equal("backtest-agent"))
			Expect(config.Env["GEMINI_API_KEY"]).To(Equal("test-key"))
		})

		It("returns false when not found", func() {
			_, ok := configs.FindByAssignee("unknown-agent")
			Expect(ok).To(BeFalse())
		})

		It("returns zero value config when not found", func() {
			config, ok := configs.FindByAssignee("unknown-agent")
			Expect(ok).To(BeFalse())
			Expect(config.Assignee).To(Equal(""))
			Expect(config.Image).To(Equal(""))
		})
	})

	Describe("TaggedConfigurations", func() {
		It("appends branch as tag to all images", func() {
			tagged := configs.TaggedConfigurations("dev")
			Expect(tagged[0].Image).To(Equal("registry/agent-claude:dev"))
			Expect(tagged[1].Image).To(Equal("registry/agent-backtest:dev"))
		})

		It("preserves assignee", func() {
			tagged := configs.TaggedConfigurations("prod")
			Expect(tagged[0].Assignee).To(Equal("claude"))
			Expect(tagged[1].Assignee).To(Equal("backtest-agent"))
		})

		It("preserves env", func() {
			tagged := configs.TaggedConfigurations("prod")
			Expect(tagged[1].Env["GEMINI_API_KEY"]).To(Equal("test-key"))
		})

		It("returns same length as input", func() {
			tagged := configs.TaggedConfigurations("dev")
			Expect(tagged).To(HaveLen(len(configs)))
		})

		It("preserves resource requests and limits independently", func() {
			result := configs.TaggedConfigurations("prod")
			Expect(result[0].Resources).NotTo(BeNil())
			Expect(result[0].Resources.Requests.CPU).To(Equal("500m"))
			Expect(result[0].Resources.Limits.CPU).To(Equal("1"))
			Expect(result[0].Resources.Requests.Memory).To(Equal("1Gi"))
			Expect(result[0].Resources.Limits.Memory).To(Equal("2Gi"))
			Expect(result[0].Resources.Requests.EphemeralStorage).To(Equal("2Gi"))
			Expect(result[0].Resources.Limits.EphemeralStorage).To(Equal("4Gi"))
		})

		It("deep-copies Resources so mutating output does not affect input", func() {
			result := configs.TaggedConfigurations("prod")
			Expect(result[0].Resources).NotTo(BeNil())
			Expect(result[0].Resources).NotTo(BeIdenticalTo(configs[0].Resources))
			result[0].Resources.Requests.CPU = "999m"
			Expect(configs[0].Resources.Requests.CPU).To(Equal("500m"))
		})

		It("preserves nil Resources for configs without resources", func() {
			result := configs.TaggedConfigurations("prod")
			Expect(result[1].Resources).To(BeNil())
		})

		It("preserves ZombieJobTimeoutSeconds", func() {
			ptr := int32(900)
			configs[0].ZombieJobTimeoutSeconds = &ptr
			result := configs.TaggedConfigurations("prod")
			Expect(result[0].ZombieJobTimeoutSeconds).NotTo(BeNil())
			Expect(*result[0].ZombieJobTimeoutSeconds).To(Equal(int32(900)))
			Expect(result[1].ZombieJobTimeoutSeconds).To(BeNil())
		})
	})
})
