// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

// AgentConfiguration defines the container image and environment for one agent type.
type AgentConfiguration struct {
	// Assignee is the task frontmatter assignee value that routes to this agent.
	Assignee string
	// TaskType is the singular task_type value from ConfigSpec.TaskType.
	// Deprecated in favour of TaskTypes; stays functional.
	TaskType string
	// TaskTypes is the list of task_type values from ConfigSpec.TaskTypes.
	// Nil when the CRD only sets the singular TaskType field.
	TaskTypes []string
	// Image is the container image base name (without tag). Tag is appended at runtime from branch.
	Image string
	// Env holds per-agent environment variables (e.g. API keys, config).
	// These are merged with shared env vars (TASK_CONTENT, TASK_ID, KAFKA_BROKERS, BRANCH)
	// when spawning the K8s Job.
	Env map[string]string
	// VolumeClaim is the name of an existing PVC to mount into the container.
	// Empty means no volume mount.
	VolumeClaim string
	// VolumeMountPath is the container path where the PVC is mounted.
	// Required when VolumeClaim is set.
	VolumeMountPath string
	// SecretName is the name of a K8s Secret to mount as envFrom on the container.
	// Empty means no secret is mounted.
	SecretName string
	// Resources declares optional resource requests and limits for the agent container.
	// Nil means "do not set, keep the k8s builder default".
	Resources *agentv1.AgentResources
	// PriorityClassName is the Kubernetes PriorityClass name to stamp onto spawned Job PodTemplates.
	PriorityClassName string
	// ImagePullSecret is the name of the K8s Secret used for image pulls.
	// Empty uses the cluster default (typically "docker").
	ImagePullSecret string
	// Trigger declares the per-agent phase and status conditions under which the executor spawns a Job.
	Trigger *agentv1.Trigger
	// ZombieJobTimeoutSeconds mirrors ConfigSpec.ZombieJobTimeoutSeconds. The
	// spawner stamps this value onto Job.Spec.ActiveDeadlineSeconds; the sweeper
	// uses it as the elapsed-time threshold. nil means "use the default
	// DefaultZombieJobTimeoutSeconds from the CRD types package".
	ZombieJobTimeoutSeconds *int32
}

// EffectiveZombieJobTimeoutSeconds returns the effective deadline in seconds:
// the configured value when non-nil, else agentv1.DefaultZombieJobTimeoutSeconds.
func (a AgentConfiguration) EffectiveZombieJobTimeoutSeconds() int32 {
	if a.ZombieJobTimeoutSeconds != nil {
		return *a.ZombieJobTimeoutSeconds
	}
	return agentv1.DefaultZombieJobTimeoutSeconds
}

// AgentConfigurations is a list of agent configurations.
type AgentConfigurations []AgentConfiguration

// FindByAssignee returns the configuration for the given assignee name.
// Returns the config and true if found, zero value and false otherwise.
func (a AgentConfigurations) FindByAssignee(assignee string) (AgentConfiguration, bool) {
	for _, c := range a {
		if c.Assignee == assignee {
			return c, true
		}
	}
	return AgentConfiguration{}, false
}

// TaggedConfigurations returns a new AgentConfigurations with the branch appended
// to each image as a tag (e.g. "registry/image" + ":" + "dev" → "registry/image:dev").
func (a AgentConfigurations) TaggedConfigurations(branch string) AgentConfigurations {
	result := make(AgentConfigurations, len(a))
	for i, c := range a {
		result[i] = AgentConfiguration{
			Assignee:                c.Assignee,
			TaskType:                c.TaskType,
			TaskTypes:               append([]string(nil), c.TaskTypes...),
			Image:                   c.Image + ":" + branch,
			Env:                     c.Env,
			VolumeClaim:             c.VolumeClaim,
			VolumeMountPath:         c.VolumeMountPath,
			SecretName:              c.SecretName,
			Resources:               c.Resources.DeepCopy(),
			PriorityClassName:       c.PriorityClassName,
			ImagePullSecret:         c.ImagePullSecret,
			Trigger:                 c.Trigger,
			ZombieJobTimeoutSeconds: c.ZombieJobTimeoutSeconds,
		}
	}
	return result
}
