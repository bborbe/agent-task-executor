// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	stderrors "errors"

	"github.com/bborbe/cqrs/base"
	"github.com/bborbe/errors"
	"github.com/bborbe/k8s"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

// ErrConfigNotFound is returned by ConfigResolver.Resolve when no
// Config in the store has a matching Spec.Assignee.
var ErrConfigNotFound = stderrors.New("config not found")

//counterfeiter:generate -o ../mocks/config_resolver.go --fake-name FakeConfigResolver . ConfigResolver

// ConfigResolver looks up the AgentConfiguration for an assignee by
// iterating the in-memory Config store and converting the matching entry.
type ConfigResolver interface {
	Resolve(ctx context.Context, assignee string) (AgentConfiguration, error)
}

// NewConfigResolver returns a ConfigResolver backed by the given
// typed store. The branch is captured here and appended as the image tag at
// resolution time.
func NewConfigResolver(
	provider k8s.Provider[agentv1.Config],
	branch base.Branch,
) ConfigResolver {
	return &configResolver{provider: provider, branch: branch}
}

type configResolver struct {
	provider k8s.Provider[agentv1.Config]
	branch   base.Branch
}

func (r *configResolver) Resolve(
	ctx context.Context,
	assignee string,
) (AgentConfiguration, error) {
	items, err := r.provider.Get(ctx)
	if err != nil {
		return AgentConfiguration{}, errors.Wrapf(ctx, err, "list agent configs")
	}
	for _, it := range items {
		if it.Spec.Assignee == assignee {
			return convert(it, r.branch.String()), nil
		}
	}
	return AgentConfiguration{}, errors.Wrapf(
		ctx,
		ErrConfigNotFound,
		"find assignee %q",
		assignee,
	)
}

func convert(obj agentv1.Config, branch string) AgentConfiguration {
	return AgentConfiguration{
		Assignee:                obj.Spec.Assignee,
		TaskType:                obj.Spec.TaskType,
		TaskTypes:               append([]string(nil), obj.Spec.TaskTypes...),
		Image:                   appendBranchTag(obj.Spec.Image, branch),
		Env:                     copyEnv(obj.Spec.Env),
		SecretName:              obj.Spec.SecretName,
		VolumeClaim:             obj.Spec.VolumeClaim,
		VolumeMountPath:         obj.Spec.VolumeMountPath,
		Resources:               obj.Spec.Resources.DeepCopy(),
		PriorityClassName:       obj.Spec.PriorityClassName,
		Trigger:                 obj.Spec.Trigger,
		ZombieJobTimeoutSeconds: obj.Spec.ZombieJobTimeoutSeconds,
	}
}

func copyEnv(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
