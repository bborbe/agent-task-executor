// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package v1

import (
	"context"
	"reflect"
	"regexp"

	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
	"github.com/bborbe/validation"
	"github.com/bborbe/vault-cli/pkg/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// var _ k8s.Type = Config{} ensures Config implements k8s.Type at compile time.
var _ libk8s.Type = Config{}

// Defaults and validation floors for the zombie-detection knobs.
// Floors prevent thrash (sweeper) and pathological short-deadline kills (timeout).
const (
	DefaultZombieSweeperIntervalSeconds int32 = 60
	MinZombieSweeperIntervalSeconds     int32 = 10
	DefaultZombieJobTimeoutSeconds      int32 = 1800
	MinZombieJobTimeoutSeconds          int32 = 30
)

var taskTypePattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Config declares a single agent type that the executor can spawn.
type Config struct {
	metav1.TypeMeta   `           json:",inline"`
	metav1.ObjectMeta `           json:"metadata,omitempty"`
	// Spec holds the configuration for this agent type.
	Spec ConfigSpec `json:"spec"`
}

// Trigger declares the per-agent phase and status conditions under which the executor spawns a Job.
// Absent or empty lists fall back to the default allow-list (phases: planning/execution/ai_review; statuses: in_progress).
type Trigger struct {
	Phases   domain.TaskPhases   `json:"phases,omitempty"`
	Statuses domain.TaskStatuses `json:"statuses,omitempty"`
}

// ConfigSpec defines the desired state of a Config.
type ConfigSpec struct {
	// Assignee is the task frontmatter assignee value that routes to this agent.
	Assignee string `json:"assignee"`
	// Image is the container image base name (without tag).
	Image string `json:"image"`
	// Heartbeat is the interval at which the agent re-spawns (e.g. "30m").
	Heartbeat string `json:"heartbeat"`
	// Deprecated: prefer TaskTypes (list). Stays functional indefinitely; use taskTypes for new agents.
	// TaskType is the task_type value in task frontmatter that routes to this agent.
	TaskType string `json:"taskType"`
	// TaskTypes is the list of task_type values in task frontmatter that route to this agent.
	// Optional when taskType is set; at least one of taskType or taskTypes must be non-empty.
	TaskTypes []string `json:"taskTypes,omitempty"`
	// Resources holds optional resource requests for the agent container.
	Resources *AgentResources `json:"resources,omitempty"`
	// Env holds per-agent environment variables.
	Env map[string]string `json:"env,omitempty"`
	// SecretName is the name of a K8s Secret to mount as envFrom.
	SecretName string `json:"secretName,omitempty"`
	// VolumeClaim is the name of an existing PVC to mount.
	VolumeClaim string `json:"volumeClaim,omitempty"`
	// VolumeMountPath is the container path where the PVC is mounted.
	VolumeMountPath string `json:"volumeMountPath,omitempty"`
	// PriorityClassName is the Kubernetes PriorityClass name to stamp onto spawned Job PodTemplates.
	PriorityClassName string `json:"priorityClassName,omitempty"`
	// Trigger declares the per-agent phase and status conditions under which the executor spawns a Job.
	Trigger *Trigger `json:"trigger,omitempty"`
	// ZombieSweeperIntervalSeconds is how often the executor's deadline sweeper
	// walks the TaskStore looking for zombie jobs. Optional; when nil, the executor
	// uses DefaultZombieSweeperIntervalSeconds (60). Values below
	// MinZombieSweeperIntervalSeconds (10) are rejected at admission to prevent
	// sweeper thrash. Pointer-typed so "unset" is distinguishable from "0".
	ZombieSweeperIntervalSeconds *int32 `json:"zombieSweeperIntervalSeconds,omitempty"`
	// ZombieJobTimeoutSeconds is the deadline applied to every spawned Job (via
	// Job.Spec.ActiveDeadlineSeconds) AND the elapsed-time threshold the sweeper
	// uses when classifying zombies. Optional; when nil, the executor uses
	// DefaultZombieJobTimeoutSeconds (1800 — 30 minutes). Values below
	// MinZombieJobTimeoutSeconds (30) are rejected at admission to prevent
	// pathological short-deadline kills. Pointer-typed so "unset" is
	// distinguishable from "0".
	ZombieJobTimeoutSeconds *int32 `json:"zombieJobTimeoutSeconds,omitempty"`
}

// AgentResources holds optional resource requests and limits for the agent container.
type AgentResources struct {
	// Requests declares the minimum resources the container needs.
	Requests AgentResourceList `json:"requests,omitempty"`
	// Limits declares the maximum resources the container may use.
	Limits AgentResourceList `json:"limits,omitempty"`
}

// AgentResourceList describes a CPU / memory / ephemeral-storage triple
// used by both Requests and Limits on AgentResources.
type AgentResourceList struct {
	// CPU is the CPU resource value (e.g. "500m").
	CPU string `json:"cpu,omitempty"`
	// Memory is the memory resource value (e.g. "256Mi").
	Memory string `json:"memory,omitempty"`
	// EphemeralStorage is the ephemeral-storage resource value (e.g. "1Gi").
	EphemeralStorage string `json:"ephemeral-storage,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ConfigList is a list of Config resources.
type ConfigList struct {
	metav1.TypeMeta `         json:",inline"`
	metav1.ListMeta `         json:"metadata"`
	// Items is the list of Config resources.
	Items []Config `json:"items"`
}

// Equal returns true if this Config has the same spec as other.
func (a Config) Equal(other libk8s.Type) bool {
	switch o := other.(type) {
	case Config:
		return a.Spec.Equal(o.Spec)
	case *Config:
		return a.Spec.Equal(o.Spec)
	default:
		return false
	}
}

// Identifier returns a unique identifier for this Config.
func (a Config) Identifier() libk8s.Identifier {
	return libk8s.Identifier(libk8s.BuildName(a.Namespace, a.Name))
}

// Validate validates the Config spec.
func (a Config) Validate(ctx context.Context) error {
	return a.Spec.Validate(ctx)
}

// String returns the name of the Config.
func (a Config) String() string {
	return a.Name
}

// Equal returns true if the two ConfigSpec values are identical.
func (s ConfigSpec) Equal(o ConfigSpec) bool {
	return s.Assignee == o.Assignee &&
		s.Image == o.Image &&
		s.Heartbeat == o.Heartbeat &&
		s.TaskType == o.TaskType &&
		reflect.DeepEqual(s.TaskTypes, o.TaskTypes) &&
		s.SecretName == o.SecretName &&
		s.VolumeClaim == o.VolumeClaim &&
		s.VolumeMountPath == o.VolumeMountPath &&
		s.PriorityClassName == o.PriorityClassName &&
		reflect.DeepEqual(s.Env, o.Env) &&
		reflect.DeepEqual(s.Resources, o.Resources) &&
		reflect.DeepEqual(s.Trigger, o.Trigger) &&
		reflect.DeepEqual(s.ZombieSweeperIntervalSeconds, o.ZombieSweeperIntervalSeconds) &&
		reflect.DeepEqual(s.ZombieJobTimeoutSeconds, o.ZombieJobTimeoutSeconds)
}

// Validate validates the ConfigSpec fields.
func (s ConfigSpec) Validate(ctx context.Context) error {
	if s.Assignee == "" {
		return errors.Wrapf(ctx, validation.Error, "assignee is empty")
	}
	if s.Image == "" {
		return errors.Wrapf(ctx, validation.Error, "image is empty")
	}
	if s.Heartbeat == "" {
		return errors.Wrapf(ctx, validation.Error, "heartbeat is empty")
	}
	if s.VolumeClaim != "" && s.VolumeMountPath == "" {
		return errors.Wrapf(ctx, validation.Error, "VolumeMountPath required when VolumeClaim set")
	}
	if err := validateTrigger(ctx, s.Trigger); err != nil {
		return err
	}
	if s.TaskType == "" && len(s.TaskTypes) == 0 {
		return errors.Wrapf(
			ctx,
			validation.Error,
			"at least one of taskType or taskTypes must be set",
		)
	}
	if err := validateTaskTypeValue(ctx, s.TaskType); err != nil {
		return err
	}
	if err := validateTaskTypesList(ctx, s.TaskTypes); err != nil {
		return err
	}
	if err := validateZombieSweeperInterval(ctx, s.ZombieSweeperIntervalSeconds); err != nil {
		return err
	}
	return validateZombieJobTimeout(ctx, s.ZombieJobTimeoutSeconds)
}

func validateZombieSweeperInterval(ctx context.Context, v *int32) error {
	if v == nil {
		return nil
	}
	if *v < MinZombieSweeperIntervalSeconds {
		return errors.Wrapf(
			ctx,
			validation.Error,
			"zombieSweeperIntervalSeconds invalid: must be >= %d",
			MinZombieSweeperIntervalSeconds,
		)
	}
	return nil
}

func validateZombieJobTimeout(ctx context.Context, v *int32) error {
	if v == nil {
		return nil
	}
	if *v < MinZombieJobTimeoutSeconds {
		return errors.Wrapf(
			ctx,
			validation.Error,
			"zombieJobTimeoutSeconds invalid: must be >= %d",
			MinZombieJobTimeoutSeconds,
		)
	}
	return nil
}

func validateTrigger(ctx context.Context, trigger *Trigger) error {
	if trigger == nil {
		return nil
	}
	for _, phase := range trigger.Phases {
		if err := phase.Validate(ctx); err != nil {
			return errors.Wrapf(ctx, err, "invalid trigger phase %q", phase)
		}
	}
	for _, status := range trigger.Statuses {
		if err := status.Validate(ctx); err != nil {
			return errors.Wrapf(ctx, err, "invalid trigger status %q", status)
		}
	}
	return nil
}

func validateTaskTypeValue(ctx context.Context, taskType string) error {
	if taskType == "" {
		return nil
	}
	if !taskTypePattern.MatchString(taskType) {
		return errors.Wrapf(ctx, validation.Error, "taskType must match ^[a-z0-9-]+$")
	}
	if len(taskType) > 63 {
		return errors.Wrapf(ctx, validation.Error, "taskType exceeds maximum length of 63")
	}
	return nil
}

func validateTaskTypesList(ctx context.Context, types []string) error {
	for _, t := range types {
		if !taskTypePattern.MatchString(t) {
			return errors.Wrapf(
				ctx,
				validation.Error,
				"taskTypes element %q must match ^[a-z0-9-]+$",
				t,
			)
		}
		if len(t) > 63 {
			return errors.Wrapf(
				ctx,
				validation.Error,
				"taskTypes element %q exceeds maximum length of 63",
				t,
			)
		}
	}
	return nil
}
