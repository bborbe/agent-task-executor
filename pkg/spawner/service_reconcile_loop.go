// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner

import (
	"context"
	"time"

	"github.com/bborbe/errors"
	k8s "github.com/bborbe/k8s"
	"github.com/golang/glog"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// defaultServiceReconcileInterval is how often the loop re-ensures service
// StatefulSets. It matches the task reconcile floor: the loop is a backstop for
// drift, not the primary mechanism — the StatefulSet controller owns pod
// replacement, and the ownerRef owns garbage collection.
const defaultServiceReconcileInterval = time.Minute

// ServiceReconcileLoop ensures one StatefulSet exists per Config whose
// spec.type is service, and removes the StatefulSet when a Config stops being one.
//
// It is deliberately a periodic pass rather than an informer hook: the Config
// informer already exists to populate the resolver's store, and a loop keeps the
// two concerns separate while giving the same convergence. A Config deleted
// entirely is collected by the ownerRef, so only the "stopped being a service"
// transition needs the explicit undeploy.
type ServiceReconcileLoop interface {
	// ReconcileOnce runs one full pass over the known Configs.
	ReconcileOnce(ctx context.Context) error
	// Run reconciles once immediately, then on every tick until ctx is cancelled.
	Run(ctx context.Context) error
}

// NewServiceReconcileLoop returns a loop that reconciles service Configs read
// from provider, resolving each one through resolver.
func NewServiceReconcileLoop(
	provider k8s.Provider[agentv1.Config],
	resolver pkg.ConfigResolver,
	reconciler ServiceReconciler,
	interval time.Duration,
) ServiceReconcileLoop {
	if interval <= 0 {
		interval = defaultServiceReconcileInterval
	}
	return &serviceReconcileLoop{
		provider:   provider,
		resolver:   resolver,
		reconciler: reconciler,
		interval:   interval,
	}
}

type serviceReconcileLoop struct {
	provider   k8s.Provider[agentv1.Config]
	resolver   pkg.ConfigResolver
	reconciler ServiceReconciler
	interval   time.Duration
}

func (l *serviceReconcileLoop) Run(ctx context.Context) error {
	if err := l.ReconcileOnce(ctx); err != nil {
		glog.Errorf("event=service_reconcile initial pass failed: %v", err)
	}
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := l.ReconcileOnce(ctx); err != nil {
				glog.Errorf("event=service_reconcile tick failed: %v", err)
			}
		}
	}
}

// ReconcileOnce reconciles every service Config.
//
// A single failing Config does not abort the pass — one broken agent must not stop
// every other service from converging — but the failures are collected and
// returned so the caller logs them.
func (l *serviceReconcileLoop) ReconcileOnce(ctx context.Context) error {
	configs, err := l.provider.Get(ctx)
	if err != nil {
		return errors.Wrapf(ctx, err, "list agent configs")
	}

	var failures []error
	services := 0
	for _, config := range configs {
		if agentTypeOrDefault(config.Spec.Type) != agentv1.AgentTypeService {
			continue
		}
		services++
		if err := l.reconcileConfig(ctx, config); err != nil {
			failures = append(failures, err)
		}
	}

	glog.V(2).
		Infof("event=service_reconcile done configs=%d services=%d failures=%d", len(configs), services, len(failures))
	if len(failures) > 0 {
		return errors.Wrapf(
			ctx,
			failures[0],
			"reconcile %d of %d service configs failed",
			len(failures),
			services,
		)
	}
	return nil
}

func (l *serviceReconcileLoop) reconcileConfig(
	ctx context.Context,
	config agentv1.Config,
) error {
	resolved, err := l.resolver.Resolve(ctx, config.Spec.Assignee)
	if err != nil {
		return errors.Wrapf(ctx, err, "resolve config %s", config.Name)
	}
	if err := l.reconciler.ReconcileService(ctx, config, resolved); err != nil {
		return errors.Wrapf(ctx, err, "reconcile config %s", config.Name)
	}
	return nil
}
