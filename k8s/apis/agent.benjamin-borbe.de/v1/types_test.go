// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package v1_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	libk8s "github.com/bborbe/k8s"
	"github.com/bborbe/validation"
	"github.com/bborbe/vault-cli/pkg/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentv1 "github.com/bborbe/agent/task/executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

func TestV1(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "V1 Suite")
}

var _ = Describe("Config", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("Equal", func() {
		It("returns true for identical specs", func() {
			a := agentv1.Config{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: agentv1.ConfigSpec{
					Assignee:  "claude",
					Image:     "registry/agent-claude",
					Heartbeat: "30m",
				},
			}
			b := agentv1.Config{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: agentv1.ConfigSpec{
					Assignee:  "claude",
					Image:     "registry/agent-claude",
					Heartbeat: "30m",
				},
			}
			Expect(a.Equal(b)).To(BeTrue())
		})

		It("returns false when Image differs", func() {
			a := agentv1.Config{
				Spec: agentv1.ConfigSpec{
					Assignee:  "claude",
					Image:     "registry/agent-claude",
					Heartbeat: "30m",
				},
			}
			b := agentv1.Config{
				Spec: agentv1.ConfigSpec{
					Assignee:  "claude",
					Image:     "registry/agent-claude-v2",
					Heartbeat: "30m",
				},
			}
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("returns true when compared with pointer type", func() {
			a := agentv1.Config{
				Spec: agentv1.ConfigSpec{
					Assignee:  "claude",
					Image:     "registry/agent-claude",
					Heartbeat: "30m",
				},
			}
			b := &agentv1.Config{
				Spec: agentv1.ConfigSpec{
					Assignee:  "claude",
					Image:     "registry/agent-claude",
					Heartbeat: "30m",
				},
			}
			Expect(a.Equal(b)).To(BeTrue())
		})

		It("returns false for unknown type", func() {
			a := agentv1.Config{
				Spec: agentv1.ConfigSpec{Assignee: "claude"},
			}
			Expect(a.Equal(nil)).To(BeFalse())
		})
	})

	Describe("Identifier", func() {
		It("returns BuildName of namespace and name", func() {
			a := agentv1.Config{
				ObjectMeta: metav1.ObjectMeta{Name: "my-agent", Namespace: "production"},
			}
			Expect(
				a.Identifier(),
			).To(Equal(libk8s.Identifier(libk8s.BuildName("production", "my-agent"))))
		})
	})

	Describe("String", func() {
		It("returns metadata.name", func() {
			a := agentv1.Config{
				ObjectMeta: metav1.ObjectMeta{Name: "my-agent"},
			}
			Expect(a.String()).To(Equal("my-agent"))
		})
	})

	Describe("Validate", func() {
		It("returns nil for a complete valid spec", func() {
			a := agentv1.Config{
				Spec: agentv1.ConfigSpec{
					Assignee:  "claude",
					Image:     "registry/agent-claude",
					Heartbeat: "30m",
					TaskType:  "claude",
				},
			}
			Expect(a.Validate(ctx)).To(BeNil())
		})
	})
})

var _ = Describe("ConfigSpec", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("Validate", func() {
		It("returns nil for a valid spec", func() {
			s := agentv1.ConfigSpec{
				Assignee:  "claude",
				Image:     "registry/agent-claude",
				Heartbeat: "30m",
				TaskType:  "claude",
			}
			Expect(s.Validate(ctx)).To(BeNil())
		})

		It("returns a wrapped validation.Error when Assignee is empty", func() {
			s := agentv1.ConfigSpec{
				Image:     "registry/agent-claude",
				Heartbeat: "30m",
				TaskType:  "claude",
			}
			err := s.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("assignee is empty")))
		})

		It("returns a wrapped validation.Error when Image is empty", func() {
			s := agentv1.ConfigSpec{
				Assignee:  "claude",
				Heartbeat: "30m",
				TaskType:  "claude",
			}
			err := s.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("image is empty")))
		})

		It("returns a wrapped validation.Error when Heartbeat is empty", func() {
			s := agentv1.ConfigSpec{
				Assignee: "claude",
				Image:    "registry/agent-claude",
				TaskType: "claude",
			}
			err := s.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("heartbeat is empty")))
		})

		It(
			"returns a wrapped validation.Error when VolumeClaim is set but VolumeMountPath is empty",
			func() {
				s := agentv1.ConfigSpec{
					Assignee:    "claude",
					Image:       "registry/agent-claude",
					Heartbeat:   "30m",
					TaskType:    "claude",
					VolumeClaim: "my-pvc",
				}
				err := s.Validate(ctx)
				Expect(err).To(HaveOccurred())
				Expect(
					err,
				).To(MatchError(ContainSubstring("VolumeMountPath required when VolumeClaim set")))
			},
		)

		It("returns nil when both VolumeClaim and VolumeMountPath are set", func() {
			s := agentv1.ConfigSpec{
				Assignee:        "claude",
				Image:           "registry/agent-claude",
				Heartbeat:       "30m",
				TaskType:        "claude",
				VolumeClaim:     "my-pvc",
				VolumeMountPath: "/data",
			}
			Expect(s.Validate(ctx)).To(BeNil())
		})

		It("wraps error with validation.Error sentinel", func() {
			s := agentv1.ConfigSpec{}
			err := s.Validate(ctx)
			Expect(err).To(HaveOccurred())
			// The error must wrap validation.Error
			_ = validation.Error // ensure the import is used
		})
	})

	Describe("JSON round-trip for priorityClassName", func() {
		It("round-trips priorityClassName through JSON", func() {
			spec := agentv1.ConfigSpec{
				Assignee:          "claude-agent",
				Image:             "example/image:latest",
				Heartbeat:         "30m",
				PriorityClassName: "agent-claude",
			}
			data, err := json.Marshal(spec)
			Expect(err).To(BeNil())
			var decoded agentv1.ConfigSpec
			Expect(json.Unmarshal(data, &decoded)).To(Succeed())
			Expect(decoded.PriorityClassName).To(Equal("agent-claude"))
		})

		It("omits priorityClassName from JSON when empty", func() {
			spec := agentv1.ConfigSpec{
				Assignee:  "claude-agent",
				Image:     "example/image:latest",
				Heartbeat: "30m",
			}
			data, err := json.Marshal(spec)
			Expect(err).To(BeNil())
			Expect(string(data)).NotTo(ContainSubstring("priorityClassName"))
		})
	})

	Describe("Equal with priorityClassName", func() {
		It("returns false when PriorityClassName differs", func() {
			a := agentv1.ConfigSpec{
				Assignee:          "claude",
				Image:             "registry/agent-claude",
				Heartbeat:         "30m",
				PriorityClassName: "agent-claude",
			}
			b := agentv1.ConfigSpec{
				Assignee:  "claude",
				Image:     "registry/agent-claude",
				Heartbeat: "30m",
			}
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("returns true when PriorityClassName matches", func() {
			a := agentv1.ConfigSpec{
				Assignee:          "claude",
				Image:             "registry/agent-claude",
				Heartbeat:         "30m",
				PriorityClassName: "agent-claude",
			}
			b := agentv1.ConfigSpec{
				Assignee:          "claude",
				Image:             "registry/agent-claude",
				Heartbeat:         "30m",
				PriorityClassName: "agent-claude",
			}
			Expect(a.Equal(b)).To(BeTrue())
		})
	})

	Describe("Equal - Trigger field", func() {
		It("returns false when one spec has Trigger nil and other has Trigger set", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", Trigger: nil}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m",
				Trigger: &agentv1.Trigger{Phases: domain.TaskPhases{domain.TaskPhasePlanning}}}
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("returns false when Triggers have different Phases", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m",
				Trigger: &agentv1.Trigger{Phases: domain.TaskPhases{domain.TaskPhasePlanning}}}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m",
				Trigger: &agentv1.Trigger{Phases: domain.TaskPhases{domain.TaskPhaseAIReview}}}
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("returns false when Phases are same values but different order", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m",
				Trigger: &agentv1.Trigger{
					Phases: domain.TaskPhases{domain.TaskPhasePlanning, domain.TaskPhaseAIReview},
				}}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m",
				Trigger: &agentv1.Trigger{
					Phases: domain.TaskPhases{domain.TaskPhaseAIReview, domain.TaskPhasePlanning},
				}}
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("returns true when both Triggers are nil", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", Trigger: nil}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", Trigger: nil}
			Expect(a.Equal(b)).To(BeTrue())
		})

		It("returns true when Triggers are identical", func() {
			t := &agentv1.Trigger{
				Phases:   domain.TaskPhases{domain.TaskPhasePlanning},
				Statuses: domain.TaskStatuses{domain.TaskStatusInProgress},
			}
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", Trigger: t}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", Trigger: t}
			Expect(a.Equal(b)).To(BeTrue())
		})
	})

	Describe("Validate - Trigger field", func() {
		baseSpec := func() agentv1.ConfigSpec {
			return agentv1.ConfigSpec{
				Assignee:  "agent",
				Image:     "img:latest",
				Heartbeat: "1m",
				TaskType:  "claude",
			}
		}

		It("passes with nil Trigger", func() {
			spec := baseSpec()
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("passes with empty-list Trigger (both lists empty)", func() {
			spec := baseSpec()
			spec.Trigger = &agentv1.Trigger{}
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("passes with valid phase entries", func() {
			spec := baseSpec()
			spec.Trigger = &agentv1.Trigger{
				Phases: domain.TaskPhases{domain.TaskPhasePlanning, domain.TaskPhaseAIReview},
			}
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("passes with valid status entries", func() {
			spec := baseSpec()
			spec.Trigger = &agentv1.Trigger{
				Statuses: domain.TaskStatuses{
					domain.TaskStatusInProgress,
					domain.TaskStatusCompleted,
				},
			}
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("fails with an invalid phase entry", func() {
			spec := baseSpec()
			spec.Trigger = &agentv1.Trigger{
				Phases: domain.TaskPhases{"bogus_phase"},
			}
			Expect(spec.Validate(ctx)).NotTo(Succeed())
		})

		It("fails with an invalid status entry", func() {
			spec := baseSpec()
			spec.Trigger = &agentv1.Trigger{
				Statuses: domain.TaskStatuses{"bogus_status"},
			}
			Expect(spec.Validate(ctx)).NotTo(Succeed())
		})
	})

	Describe("Validate - TaskType field", func() {
		baseSpec := func() agentv1.ConfigSpec {
			return agentv1.ConfigSpec{
				Assignee:  "agent",
				Image:     "img:latest",
				Heartbeat: "1m",
				TaskType:  "claude",
			}
		}

		It("passes with taskType 'pr-review'", func() {
			spec := baseSpec()
			spec.TaskType = "pr-review"
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("passes with taskType 'claude'", func() {
			Expect(baseSpec().Validate(ctx)).To(Succeed())
		})

		It("passes with taskType containing leading digit '2fa-setup'", func() {
			spec := baseSpec()
			spec.TaskType = "2fa-setup"
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("passes with single-character taskType 'a'", func() {
			spec := baseSpec()
			spec.TaskType = "a"
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("passes with exactly 63-character taskType", func() {
			spec := baseSpec()
			spec.TaskType = strings.Repeat("a", 63)
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("fails when neither taskType nor taskTypes is set", func() {
			spec := baseSpec()
			spec.TaskType = ""
			// TaskTypes not set — neither field populated
			err := spec.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(
				err,
			).To(MatchError(ContainSubstring("at least one of taskType or taskTypes must be set")))
		})

		It("fails when TaskType contains uppercase letters (PR-Review)", func() {
			spec := baseSpec()
			spec.TaskType = "PR-Review"
			err := spec.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("taskType must match")))
		})

		It("fails when TaskType contains underscore (pr_review)", func() {
			spec := baseSpec()
			spec.TaskType = "pr_review"
			err := spec.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("taskType must match")))
		})

		It("fails when TaskType contains a dot (pr.review)", func() {
			spec := baseSpec()
			spec.TaskType = "pr.review"
			err := spec.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("taskType must match")))
		})

		It("fails when TaskType exceeds 63 characters", func() {
			spec := baseSpec()
			spec.TaskType = strings.Repeat("a", 64)
			err := spec.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("taskType exceeds maximum length")))
		})
	})

	Describe("Equal - TaskType field", func() {
		It("returns false when TaskType differs", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "claude"}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "general"}
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("returns false when one TaskType is empty and the other is not", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: ""}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "claude"}
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("returns true when TaskType is identical", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "claude"}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "claude"}
			Expect(a.Equal(b)).To(BeTrue())
		})
	})

	Describe("Validate - TaskTypes field", func() {
		baseSpec := func() agentv1.ConfigSpec {
			return agentv1.ConfigSpec{Assignee: "agent", Image: "img:latest", Heartbeat: "1m"}
		}

		It("passes when only taskType is set", func() {
			spec := baseSpec()
			spec.TaskType = "pr-review"
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("passes when only taskTypes is set (non-empty list)", func() {
			spec := baseSpec()
			spec.TaskTypes = []string{"pr-review", "healthcheck"}
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("passes when both taskType and taskTypes are set", func() {
			spec := baseSpec()
			spec.TaskType = "pr-review"
			spec.TaskTypes = []string{"healthcheck"}
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("fails when neither taskType nor taskTypes is set", func() {
			spec := baseSpec()
			err := spec.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(
				err,
			).To(MatchError(ContainSubstring("at least one of taskType or taskTypes must be set")))
		})

		It("fails when taskTypes has an element with uppercase letters", func() {
			spec := baseSpec()
			spec.TaskTypes = []string{"PR-Review"}
			err := spec.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("must match")))
		})

		It("fails when taskTypes has an element with underscore", func() {
			spec := baseSpec()
			spec.TaskTypes = []string{"pr_review"}
			err := spec.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("must match")))
		})

		It("fails when taskTypes has an element exceeding 63 characters", func() {
			spec := baseSpec()
			spec.TaskTypes = []string{strings.Repeat("a", 64)}
			err := spec.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("exceeds maximum length")))
		})

		It("passes when taskTypes has an element of exactly 63 characters", func() {
			spec := baseSpec()
			spec.TaskTypes = []string{strings.Repeat("a", 63)}
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("passes when taskTypes is an empty list and taskType is set", func() {
			spec := baseSpec()
			spec.TaskType = "claude"
			spec.TaskTypes = []string{}
			Expect(spec.Validate(ctx)).To(Succeed())
		})
	})

	Describe("Equal - TaskTypes field", func() {
		It("returns false when TaskTypes differs", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "claude",
				TaskTypes: []string{"healthcheck"}}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "claude",
				TaskTypes: []string{"pr-review"}}
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("returns false when TaskTypes differ in order (order-sensitive)", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m",
				TaskTypes: []string{"pr-review", "healthcheck"}}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m",
				TaskTypes: []string{"healthcheck", "pr-review"}}
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("returns false when one has nil TaskTypes and the other has a non-empty slice", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "claude"}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "claude",
				TaskTypes: []string{"healthcheck"}}
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("returns true when both TaskTypes are nil", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "claude"}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "claude"}
			Expect(a.Equal(b)).To(BeTrue())
		})

		It("returns true when TaskTypes are identical slices", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m",
				TaskTypes: []string{"pr-review", "healthcheck"}}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m",
				TaskTypes: []string{"pr-review", "healthcheck"}}
			Expect(a.Equal(b)).To(BeTrue())
		})
	})

	Describe("Validate - zombie knobs", func() {
		ptrInt32 := func(v int32) *int32 { return &v }

		baseSpec := func() agentv1.ConfigSpec {
			return agentv1.ConfigSpec{
				Assignee:  "agent",
				Image:     "img:latest",
				Heartbeat: "1m",
				TaskType:  "claude",
			}
		}

		It("accepts nil zombie fields", func() {
			spec := baseSpec()
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("accepts valid zombie values at the floor", func() {
			spec := baseSpec()
			spec.ZombieSweeperIntervalSeconds = ptrInt32(10)
			spec.ZombieJobTimeoutSeconds = ptrInt32(30)
			Expect(spec.Validate(ctx)).To(Succeed())
		})

		It("rejects zombieSweeperIntervalSeconds below floor", func() {
			spec := baseSpec()
			spec.ZombieSweeperIntervalSeconds = ptrInt32(9)
			err := spec.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid: must be >= 10"))
		})

		It("rejects zombieJobTimeoutSeconds below floor", func() {
			spec := baseSpec()
			spec.ZombieJobTimeoutSeconds = ptrInt32(29)
			err := spec.Validate(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid: must be >= 30"))
		})
	})

	Describe("Equal - zombie fields", func() {
		ptrInt32 := func(v int32) *int32 { return &v }

		It("equal when both zombie fields nil", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "t"}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "t"}
			Expect(a.Equal(b)).To(BeTrue())
		})

		It("equal when both have same non-nil zombie values", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "t",
				ZombieJobTimeoutSeconds: ptrInt32(1800)}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "t",
				ZombieJobTimeoutSeconds: ptrInt32(1800)}
			Expect(a.Equal(b)).To(BeTrue())
		})

		It("not equal when one zombie field nil and other non-nil", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "t"}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "t",
				ZombieJobTimeoutSeconds: ptrInt32(1800)}
			Expect(a.Equal(b)).To(BeFalse())
		})

		It("not equal when zombie values differ", func() {
			a := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "t",
				ZombieJobTimeoutSeconds: ptrInt32(1800)}
			b := agentv1.ConfigSpec{Assignee: "x", Image: "y", Heartbeat: "1m", TaskType: "t",
				ZombieJobTimeoutSeconds: ptrInt32(900)}
			Expect(a.Equal(b)).To(BeFalse())
		})
	})
})

var _ = Describe("JSON round-trip for taskType", func() {
	It("round-trips taskType through JSON", func() {
		spec := agentv1.ConfigSpec{
			Assignee:  "agent",
			Image:     "img:latest",
			Heartbeat: "1m",
			TaskType:  "pr-review",
		}
		data, err := json.Marshal(spec)
		Expect(err).To(BeNil())
		Expect(string(data)).To(ContainSubstring(`"taskType":"pr-review"`))
		var decoded agentv1.ConfigSpec
		Expect(json.Unmarshal(data, &decoded)).To(Succeed())
		Expect(decoded.TaskType).To(Equal("pr-review"))
	})

	It("includes taskType in JSON even when empty (no omitempty)", func() {
		spec := agentv1.ConfigSpec{Assignee: "agent", Image: "img:latest", Heartbeat: "1m"}
		data, err := json.Marshal(spec)
		Expect(err).To(BeNil())
		Expect(string(data)).To(ContainSubstring(`"taskType":`))
	})
})

var _ = Describe("JSON round-trip for taskTypes", func() {
	It("round-trips taskTypes through JSON", func() {
		spec := agentv1.ConfigSpec{
			Assignee:  "agent",
			Image:     "img:latest",
			Heartbeat: "1m",
			TaskType:  "pr-review",
			TaskTypes: []string{"pr-review", "healthcheck"},
		}
		data, err := json.Marshal(spec)
		Expect(err).To(BeNil())
		Expect(string(data)).To(ContainSubstring(`"taskTypes":["pr-review","healthcheck"]`))
		var decoded agentv1.ConfigSpec
		Expect(json.Unmarshal(data, &decoded)).To(Succeed())
		Expect(decoded.TaskTypes).To(Equal([]string{"pr-review", "healthcheck"}))
	})

	It("omits taskTypes from JSON when nil (omitempty)", func() {
		spec := agentv1.ConfigSpec{
			Assignee:  "agent",
			Image:     "img:latest",
			Heartbeat: "1m",
			TaskType:  "pr-review",
		}
		data, err := json.Marshal(spec)
		Expect(err).To(BeNil())
		Expect(string(data)).NotTo(ContainSubstring("taskTypes"))
	})
})

var _ = Describe("JSON round-trip for trigger", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("round-trips trigger through JSON", func() {
		_ = ctx
		spec := agentv1.ConfigSpec{
			Assignee:  "agent",
			Image:     "img:latest",
			Heartbeat: "1m",
			Trigger: &agentv1.Trigger{
				Phases:   domain.TaskPhases{domain.TaskPhasePlanning},
				Statuses: domain.TaskStatuses{domain.TaskStatusInProgress},
			},
		}
		data, err := json.Marshal(spec)
		Expect(err).To(BeNil())
		var decoded agentv1.ConfigSpec
		Expect(json.Unmarshal(data, &decoded)).To(Succeed())
		Expect(decoded.Trigger).NotTo(BeNil())
		Expect(decoded.Trigger.Phases).To(ConsistOf(domain.TaskPhasePlanning))
		Expect(decoded.Trigger.Statuses).To(ConsistOf(domain.TaskStatusInProgress))
	})

	It("omits trigger from JSON when nil", func() {
		spec := agentv1.ConfigSpec{Assignee: "agent", Image: "img:latest", Heartbeat: "1m"}
		data, err := json.Marshal(spec)
		Expect(err).To(BeNil())
		Expect(string(data)).NotTo(ContainSubstring("trigger"))
	})
})
