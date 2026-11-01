// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"

	"github.com/bborbe/errors"
	"github.com/golang/glog"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// configAcceptedPair is one (phase, task_type) pair the Configs accept.
type configAcceptedPair struct {
	phase    string
	taskType string
}

// configAcceptedPairs returns the union over configs of (effective trigger
// phases) x (effective task types). The union is namespace-wide by design:
// every executor in the namespace sees every Config and publishes the same
// pairs; only the vault label differs between instances.
func configAcceptedPairs(items []agentv1.Config) map[configAcceptedPair]struct{} {
	pairs := make(map[configAcceptedPair]struct{})
	for _, item := range items {
		phases := effectiveTriggerPhases(&pkg.AgentConfiguration{Trigger: item.Spec.Trigger})
		for _, phase := range phases {
			for _, taskType := range pkg.EffectiveTaskTypes(item.Spec.TaskType, item.Spec.TaskTypes) {
				pairs[configAcceptedPair{phase: string(phase), taskType: taskType}] = struct{}{}
			}
		}
	}
	return pairs
}

// refreshConfigAccepted publishes the accepted (phase, task_type) set for this
// executor's vault. Set-then-delete, under one lock: a refresh interrupted
// after the deletes but before the sets would leave the family missing pairs
// it accepts, which is the under-publish the consumer's `unless` turns into an
// all-records alert. Interrupted the other way round the worst outcome is a
// stale pair surviving one interval — a superset, never a subset.
func (h *taskEventHandler) refreshConfigAccepted(ctx context.Context) error {
	if h.configProvider == nil {
		glog.V(2).Infof("event=config_accepted_refresh_skipped reason=no_provider")
		return nil
	}
	items, err := h.configProvider.Get(ctx)
	if err != nil {
		return errors.Wrapf(ctx, err, "list agent configs for accepted set")
	}
	if err := ctx.Err(); err != nil {
		return errors.Wrapf(ctx, err, "refresh accepted config set cancelled")
	}
	pairs := configAcceptedPairs(items)
	vault := h.vaultMatcher.Vault()

	h.configAcceptedMu.Lock()
	defer h.configAcceptedMu.Unlock()
	for pair := range pairs {
		metrics.ConfigAccepted.WithLabelValues(pair.phase, pair.taskType, vault).Set(1)
	}
	deleted := 0
	for pair := range h.configAcceptedPairs {
		if _, ok := pairs[pair]; ok {
			continue
		}
		metrics.ConfigAccepted.DeleteLabelValues(pair.phase, pair.taskType, vault)
		deleted++
	}
	h.configAcceptedPairs = pairs

	glog.Infof(
		"event=config_accepted_refreshed configs=%d pairs=%d deleted=%d",
		len(items), len(pairs), deleted,
	)
	return nil
}
