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
// resolution time. Resolutions are scoped to the given vault: each task's
// plain assignee is matched against the composed {assignee}-{vaultName}
// Config assignee, so per-vault Config CRs apply per executor instance. When
// no composed Config exists, Resolve falls back to the plain assignee, so the
// singular shared-topic executor (v0.6.x Config CRs without a vault suffix)
// keeps resolving on the v0.7+ line.
func NewConfigResolver(
	provider k8s.Provider[agentv1.Config],
	branch base.Branch,
	vaultName string,
) ConfigResolver {
	return &configResolver{provider: provider, branch: branch, vaultName: vaultName}
}

type configResolver struct {
	provider  k8s.Provider[agentv1.Config]
	branch    base.Branch
	vaultName string
}

func (r *configResolver) Resolve(
	ctx context.Context,
	assignee string,
) (AgentConfiguration, error) {
	items, err := r.provider.Get(ctx)
	if err != nil {
		return AgentConfiguration{}, errors.Wrapf(ctx, err, "list agent configs")
	}
	composed := assignee + "-" + r.vaultName
	for _, it := range items {
		select {
		case <-ctx.Done():
			return AgentConfiguration{}, errors.Wrapf(
				ctx,
				ctx.Err(),
				"resolve config %q cancelled",
				composed,
			)
		default:
		}
		if it.Spec.Assignee == composed {
			return convert(ctx, it, r.branch.String())
		}
	}
	// No {assignee}-{vaultName} Config: fall back to the plain assignee so the
	// singular executor's un-suffixed Config CRs (v0.6.x naming) keep resolving.
	// Per-vault installs still match composed first, so vault scoping wins when
	// a composed Config exists.
	for _, it := range items {
		select {
		case <-ctx.Done():
			return AgentConfiguration{}, errors.Wrapf(
				ctx,
				ctx.Err(),
				"resolve config %q cancelled",
				assignee,
			)
		default:
		}
		if it.Spec.Assignee == assignee {
			return convert(ctx, it, r.branch.String())
		}
	}
	return AgentConfiguration{}, errors.Wrapf(
		ctx,
		ErrConfigNotFound,
		"find composed assignee %q",
		composed,
	)
}

func convert(ctx context.Context, obj agentv1.Config, branch string) (AgentConfiguration, error) {
	env, err := copyEnv(ctx, obj.Spec.Env)
	if err != nil {
		return AgentConfiguration{}, errors.Wrapf(ctx, err, "convert config %q", obj.Spec.Assignee)
	}
	return AgentConfiguration{
		Assignee:                obj.Spec.Assignee,
		TaskType:                obj.Spec.TaskType,
		TaskTypes:               append([]string(nil), obj.Spec.TaskTypes...),
		Image:                   appendBranchTag(obj.Spec.Image, branch),
		Env:                     env,
		SecretName:              obj.Spec.SecretName,
		VolumeClaim:             obj.Spec.VolumeClaim,
		VolumeMountPath:         obj.Spec.VolumeMountPath,
		Resources:               obj.Spec.Resources.DeepCopy(),
		MaxConcurrentJobs:       obj.Spec.MaxConcurrentJobs,
		PriorityClassName:       obj.Spec.PriorityClassName,
		Trigger:                 obj.Spec.Trigger,
		ZombieJobTimeoutSeconds: obj.Spec.ZombieJobTimeoutSeconds,
	}, nil
}

func copyEnv(ctx context.Context, in map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(in))
	for k, v := range in {
		select {
		case <-ctx.Done():
			return nil, errors.Wrapf(ctx, ctx.Err(), "copy env cancelled")
		default:
		}
		out[k] = v
	}
	return out, nil
}
