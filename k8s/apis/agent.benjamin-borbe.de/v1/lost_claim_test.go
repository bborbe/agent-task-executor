// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package v1_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

func boolPtr(b bool) *bool { return &b }

var _ = Describe("LostClaimAction", func() {
	DescribeTable("maps rerunOnLostClaim to the takeover action",
		func(field *bool, expected agentv1.LostClaimAction) {
			spec := agentv1.ConfigSpec{RerunOnLostClaim: field}
			Expect(spec.LostClaimAction()).To(Equal(expected))
		},
		Entry("unset re-dispatches", nil, agentv1.RedispatchAfterLostClaim),
		Entry("true re-dispatches", boolPtr(true), agentv1.RedispatchAfterLostClaim),
		Entry("false parks at human_review", boolPtr(false), agentv1.ParkAfterLostClaim),
	)
})

var _ = Describe("rerunOnLostClaim JSON", func() {
	It("treats an absent field as nil and true", func() {
		var spec agentv1.ConfigSpec
		Expect(json.Unmarshal([]byte(`{}`), &spec)).To(Succeed())
		Expect(spec.RerunOnLostClaim).To(BeNil())
		Expect(spec.RerunOnLostClaimEnabled()).To(BeTrue())
	})

	It("decodes an explicit false", func() {
		var spec agentv1.ConfigSpec
		Expect(json.Unmarshal([]byte(`{"rerunOnLostClaim":false}`), &spec)).To(Succeed())
		Expect(spec.RerunOnLostClaim).ToNot(BeNil())
		Expect(*spec.RerunOnLostClaim).To(BeFalse())
		Expect(spec.RerunOnLostClaimEnabled()).To(BeFalse())
	})

	It("decodes an explicit true", func() {
		var spec agentv1.ConfigSpec
		Expect(json.Unmarshal([]byte(`{"rerunOnLostClaim":true}`), &spec)).To(Succeed())
		Expect(spec.RerunOnLostClaim).ToNot(BeNil())
		Expect(*spec.RerunOnLostClaim).To(BeTrue())
		Expect(spec.RerunOnLostClaimEnabled()).To(BeTrue())
	})

	It("omits the field when unset", func() {
		spec := agentv1.ConfigSpec{
			Assignee:  "claude",
			Image:     "registry/agent-claude",
			Heartbeat: "30m",
		}
		data, err := json.Marshal(spec)
		Expect(err).ToNot(HaveOccurred())
		Expect(string(data)).NotTo(ContainSubstring("rerunOnLostClaim"))
	})
})

var _ = Describe("Equal - rerunOnLostClaim", func() {
	DescribeTable("compares the field by value",
		func(a, b *bool, expected bool) {
			specA := agentv1.ConfigSpec{
				Assignee:         "claude",
				Image:            "registry/agent-claude",
				Heartbeat:        "30m",
				RerunOnLostClaim: a,
			}
			specB := agentv1.ConfigSpec{
				Assignee:         "claude",
				Image:            "registry/agent-claude",
				Heartbeat:        "30m",
				RerunOnLostClaim: b,
			}
			Expect(specA.Equal(specB)).To(Equal(expected))
		},
		Entry("nil vs false differs", nil, boolPtr(false), false),
		Entry("true vs false differs", boolPtr(true), boolPtr(false), false),
		Entry("nil vs nil is equal", nil, nil, true),
		Entry("false vs false is equal", boolPtr(false), boolPtr(false), true),
	)
})

var _ = Describe("rerunOnLostClaim deep copy", func() {
	It("copies the pointed-to value", func() {
		spec := agentv1.ConfigSpec{
			Assignee:         "claude",
			Image:            "registry/agent-claude",
			Heartbeat:        "30m",
			RerunOnLostClaim: boolPtr(false),
		}
		copied := spec.DeepCopy()
		Expect(copied.RerunOnLostClaim).ToNot(BeNil())
		*copied.RerunOnLostClaim = true
		Expect(*spec.RerunOnLostClaim).To(BeFalse())
		Expect(*copied.RerunOnLostClaim).To(BeTrue())
	})

	It("keeps an unset field nil", func() {
		spec := agentv1.ConfigSpec{}
		copied := spec.DeepCopy()
		Expect(copied.RerunOnLostClaim).To(BeNil())
	})
})
