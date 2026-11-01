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
	"github.com/golang/glog"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

// ErrConfigNotFound is returned by ConfigResolver.Resolve when no
// Config in the store has a matching Spec.Assignee.
var ErrConfigNotFound = stderrors.New("config not found")

// ErrConfigAmbiguous is returned by ConfigResolver.ResolveByTaskType when more
// than one Config in the store declares the requested task_type. The executor
// does not guess between them: the ambiguous task is left untouched and the
// skip is logged, so an operator can narrow the Configs' taskTypes.
var ErrConfigAmbiguous = stderrors.New("config ambiguous")

//counterfeiter:generate -o ../mocks/config_resolver.go --fake-name FakeConfigResolver . ConfigResolver

// ConfigResolver looks up the AgentConfiguration for an assignee, and — on
// the recovery path — for a task's task_type.
type ConfigResolver interface {
	Resolve(ctx context.Context, assignee string) (AgentConfiguration, error)
	// ResolveByTaskType looks up the single AgentConfiguration whose effective
	// task-type set ({Spec.TaskType} ∪ Spec.TaskTypes) contains taskType. It is
	// the recovery path's routing lookup for a task whose assignee was cleared:
	// the task's own task_type still identifies exactly one agent.
	//
	// When more than one Config declares taskType, a non-empty preferAssignee
	// narrows the matches to the ones Resolve would pick for that assignee
	// ({preferAssignee}-{vaultName} first, then the plain preferAssignee); a
	// single survivor is returned.
	//
	// When several Configs declare taskType and preferAssignee does not narrow
	// them to one, the single Config that names taskType in
	// Spec.DefaultForTaskTypes is returned instead. That is an explicit
	// declaration on the Config, not a guess: zero or several Configs declaring
	// it leave the ambiguity refusal in place.
	//
	// Returns ErrConfigNotFound when no Config declares taskType, and
	// ErrConfigAmbiguous when more than one does and neither preferAssignee nor
	// a unique designated default narrows them to exactly one.
	ResolveByTaskType(
		ctx context.Context,
		taskType string,
		preferAssignee string,
	) (AgentConfiguration, error)
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

// ResolveByTaskType returns the single AgentConfiguration whose effective
// task-type set contains taskType. It is deliberately vault-agnostic: the
// lookup scans every Config the executor's informer holds, keyed on the task
// type rather than the assignee. preferAssignee only breaks a tie between
// several type matches; it never selects a Config that does not declare
// taskType. When it does not break the tie either, the one match that declares
// itself the designated default for taskType (Spec.DefaultForTaskTypes) is
// returned — again only among Configs that declare taskType, and only when
// exactly one of them declares the default.
func (r *configResolver) ResolveByTaskType(
	ctx context.Context,
	taskType string,
	preferAssignee string,
) (AgentConfiguration, error) {
	items, err := r.provider.Get(ctx)
	if err != nil {
		return AgentConfiguration{}, errors.Wrapf(ctx, err, "list agent configs")
	}
	var matches []agentv1.Config
	for _, it := range items {
		select {
		case <-ctx.Done():
			return AgentConfiguration{}, errors.Wrapf(
				ctx,
				ctx.Err(),
				"resolve config by task_type %q cancelled",
				taskType,
			)
		default:
		}
		if TaskTypeInSet(taskType, EffectiveTaskTypes(it.Spec.TaskType, it.Spec.TaskTypes)) {
			matches = append(matches, it)
		}
	}
	switch len(matches) {
	case 0:
		return AgentConfiguration{}, errors.Wrapf(
			ctx,
			ErrConfigNotFound,
			"find config for task_type %q",
			taskType,
		)
	case 1:
		return convert(ctx, matches[0], r.branch.String())
	default:
		if preferred, ok := r.narrowByAssignee(matches, preferAssignee); ok {
			glog.V(2).Infof(
				"task_type %s matches %d configs: previous_assignee %s picks config %s (assignee %s)",
				taskType,
				len(matches),
				preferAssignee,
				preferred.Name,
				preferred.Spec.Assignee,
			)
			return convert(ctx, preferred, r.branch.String())
		}
		if designated, ok := narrowByDefault(matches, taskType); ok {
			glog.V(2).Infof(
				"task_type %s matches %d configs: config %s declares itself the designated default (assignee %s)",
				taskType,
				len(matches),
				designated.Name,
				designated.Spec.Assignee,
			)
			return convert(ctx, designated, r.branch.String())
		}
		if preferAssignee != "" {
			return AgentConfiguration{}, errors.Wrapf(
				ctx,
				ErrConfigAmbiguous,
				"task_type %q matches %d configs and previous_assignee %q names none of them uniquely",
				taskType,
				len(matches),
				preferAssignee,
			)
		}
		return AgentConfiguration{}, errors.Wrapf(
			ctx,
			ErrConfigAmbiguous,
			"task_type %q matches %d configs",
			taskType,
			len(matches),
		)
	}
}

// narrowByAssignee picks the single match Resolve would pick for assignee, in
// Resolve's own order: the composed {assignee}-{vaultName} Config first, then
// the plain assignee. It reports false when assignee is empty or the candidate
// tier it lands on does not hold exactly one match.
func (r *configResolver) narrowByAssignee(
	matches []agentv1.Config,
	assignee string,
) (agentv1.Config, bool) {
	if assignee == "" {
		return agentv1.Config{}, false
	}
	for _, candidate := range []string{assignee + "-" + r.vaultName, assignee} {
		var hits []agentv1.Config
		for _, it := range matches {
			if it.Spec.Assignee == candidate {
				hits = append(hits, it)
			}
		}
		switch len(hits) {
		case 0:
			continue
		case 1:
			return hits[0], true
		default:
			return agentv1.Config{}, false
		}
	}
	return agentv1.Config{}, false
}

// narrowByDefault picks the single match that declares itself the designated
// default for taskType in Spec.DefaultForTaskTypes. It reports false when no
// match declares it or when several do: a default that is not unique is no
// better than none, so the ambiguity refusal stands. It only ever considers
// matches, so it can never select a Config that does not declare taskType.
func narrowByDefault(matches []agentv1.Config, taskType string) (agentv1.Config, bool) {
	var hits []agentv1.Config
	for _, it := range matches {
		if TaskTypeInSet(taskType, it.Spec.DefaultForTaskTypes) {
			hits = append(hits, it)
		}
	}
	if len(hits) == 1 {
		return hits[0], true
	}
	return agentv1.Config{}, false
}

func convert(ctx context.Context, obj agentv1.Config, branch string) (AgentConfiguration, error) {
	env, err := copyEnv(ctx, obj.Spec.Env)
	if err != nil {
		return AgentConfiguration{}, errors.Wrapf(ctx, err, "convert config %q", obj.Spec.Assignee)
	}
	return AgentConfiguration{
		Assignee:                obj.Spec.Assignee,
		Type:                    obj.Spec.Type,
		TaskType:                obj.Spec.TaskType,
		TaskTypes:               append([]string(nil), obj.Spec.TaskTypes...),
		DefaultForTaskTypes:     append([]string(nil), obj.Spec.DefaultForTaskTypes...),
		Image:                   appendBranchTag(obj.Spec.Image, branch),
		Env:                     env,
		SecretName:              obj.Spec.SecretName,
		VolumeClaim:             obj.Spec.VolumeClaim,
		VolumeMountPath:         obj.Spec.VolumeMountPath,
		ConfigMapName:           obj.Spec.ConfigMapName,
		ConfigMapMountPath:      obj.Spec.ConfigMapMountPath,
		ConfigMapItems:          obj.Spec.ConfigMapItems,
		Resources:               obj.Spec.Resources.DeepCopy(),
		MaxConcurrentJobs:       obj.Spec.MaxConcurrentJobs,
		PriorityClassName:       obj.Spec.PriorityClassName,
		JobNamespace:            obj.Spec.JobNamespace,
		ServiceNamespace:        obj.Spec.ServiceNamespace,
		Trigger:                 obj.Spec.Trigger,
		RerunOnLostClaim:        obj.Spec.RerunOnLostClaim,
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
