// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner

import (
	"context"

	"github.com/bborbe/errors"
	k8s "github.com/bborbe/k8s"
	"github.com/golang/glog"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// serviceDatadirName is the volumeClaimTemplate the StatefulSet builder emits, and
// the volume a service agent's session storage is mounted from. Kubernetes rejects
// any change to volumeClaimTemplates on update, so this name is fixed for the life
// of the StatefulSet — renaming it would require deleting and recreating.
const serviceDatadirName = "datadir"

// serviceDefaultMountPath is where the session volume is mounted when a service
// Config declares no VolumeMountPath of its own. It is pi's own agent dir, which
// holds settings.json, auth.json, session storage and skills; the Pi runner
// resolves it as $PI_CODING_AGENT_DIR or this path.
const serviceDefaultMountPath = "/home/pi/.pi"

// serviceStorageSize is the size requested for a service agent's session volume.
// A session is text; 1Gi is the chart's own default for agent volumes.
const serviceStorageSize = "1Gi"

// agentTypeEnvKey carries the Config's type into the agent container so a runner
// can tell a long-running identity agent from a task-routed one. It is stamped by
// the executor from spec.type rather than hand-written into the CR, so the type
// stays the single source of truth.
const agentTypeEnvKey = "AGENT_TYPE"

//counterfeiter:generate -o ../../mocks/service_reconciler.go --fake-name FakeServiceReconciler . ServiceReconciler

// ServiceReconciler reconciles one long-running StatefulSet per Config whose
// spec.type is service.
//
// It is the identity-shape counterpart to JobSpawner. Where that creates a
// short-lived Job per task and phase, this creates a workload that outlives any
// single task and is addressed directly. Both read the same Config CR, and the
// discriminator between them is ConfigSpec.Type.
type ServiceReconciler interface {
	// ReconcileService creates or updates the StatefulSet for a service Config.
	// It is idempotent: an existing StatefulSet has only its mutable fields
	// merged, so an image change rolls the pod and an unchanged Config is a no-op.
	ReconcileService(
		ctx context.Context,
		config agentv1.Config,
		resolved pkg.AgentConfiguration,
	) error
	// UndeployService removes the StatefulSet a Config owns, in the reconciler's
	// own namespace — but only if the executor actually owns it.
	//
	// The ownership check is not defensive decoration. The loop calls this for
	// every Config that is not a service, and a StatefulSet's name here is just
	// the Config's name, which any other chart in the namespace may also use
	// (recurring-task-creator is itself a StatefulSet). Deleting by name alone
	// would therefore let a job Config tear down an unrelated workload.
	//
	// Idempotent and honest: a missing StatefulSet is not an error (the ownerRef
	// may already have collected it), and a StatefulSet this executor does not
	// own is left untouched and reported as such, never as a removal.
	UndeployService(ctx context.Context, name string) error
}

// NewServiceReconciler returns a ServiceReconciler that deploys StatefulSets
// through the given deployer.
func NewServiceReconciler(
	deployer k8s.StatefulSetDeployer,
	statefulSets k8s.StatefulSetInterface,
	namespace k8s.Namespace,
	storageClass string,
) ServiceReconciler {
	return &serviceReconciler{
		deployer:     deployer,
		statefulSets: statefulSets,
		namespace:    namespace,
		storageClass: storageClass,
	}
}

type serviceReconciler struct {
	deployer     k8s.StatefulSetDeployer
	statefulSets k8s.StatefulSetInterface
	namespace    k8s.Namespace
	storageClass string
}

func (r *serviceReconciler) ReconcileService(
	ctx context.Context,
	config agentv1.Config,
	resolved pkg.AgentConfiguration,
) error {
	// Guard rather than trust the caller: a job Config reaching this path would
	// otherwise get a StatefulSet alongside its Jobs, and the Job path must stay
	// untouched.
	if agentTypeOrDefault(resolved.Type) != agentv1.AgentTypeService {
		return nil
	}

	statefulSet, err := r.buildStatefulSet(ctx, config, resolved)
	if err != nil {
		return err
	}
	if err := r.deployer.Deploy(ctx, *statefulSet); err != nil {
		return errors.Wrapf(ctx, err, "deploy statefulset %s", statefulSet.Name)
	}
	glog.V(2).
		Infof("reconciled service statefulset %s for assignee %s with image %s", statefulSet.Name, resolved.Assignee, resolved.Image)
	return nil
}

func (r *serviceReconciler) UndeployService(
	ctx context.Context,
	name string,
) error {
	existing, err := r.statefulSets.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Nothing to remove — the ownerRef may already have collected it, and
			// the loop calls this for every non-service Config, most of which never
			// had a StatefulSet at all. Silent, and *not* reported as a removal.
			return nil
		}
		return errors.Wrapf(ctx, err, "get statefulset %s", name)
	}
	if !ownedByConfig(existing) {
		// Someone else's workload, sharing this Config's name. Leave it alone and
		// say so — reporting it as a removal is how a no-op becomes a false alarm.
		glog.V(2).
			Infof("kept statefulset %s: not owned by a Config, refusing to undeploy", name)
		return nil
	}
	if err := r.deployer.Undeploy(ctx, r.namespace, k8s.Name(name)); err != nil {
		return errors.Wrapf(ctx, err, "undeploy statefulset %s", name)
	}
	glog.V(2).Infof("removed service statefulset %s", name)
	return nil
}

// ownedByConfig reports whether a StatefulSet was created by this executor for a
// Config. buildStatefulSet sets the Config as the controller owner, so that
// ownerRef is the executor's own mark: a StatefulSet without it belongs to
// another chart in the namespace that happens to use the same name.
func ownedByConfig(statefulSet *appsv1.StatefulSet) bool {
	for _, owner := range statefulSet.OwnerReferences {
		if owner.Kind == "Config" && owner.APIVersion == agentv1.SchemeGroupVersion.String() {
			return true
		}
	}
	return false
}

// buildStatefulSet renders the StatefulSet for one service Config.
func (r *serviceReconciler) buildStatefulSet(
	ctx context.Context,
	config agentv1.Config,
	resolved pkg.AgentConfiguration,
) (*appsv1.StatefulSet, error) {
	mountPath := resolved.VolumeMountPath
	if mountPath == "" {
		mountPath = serviceDefaultMountPath
	}

	containerBuilder := k8s.NewContainerBuilder()
	containerBuilder.SetName(k8s.Name("agent"))
	containerBuilder.SetImage(resolved.Image)
	containerBuilder.SetEnvBuilder(buildServiceEnvBuilder(resolved))
	containerBuilder.AddVolumeMounts(corev1.VolumeMount{
		Name:      serviceDatadirName,
		MountPath: mountPath,
	})
	applyCPUMemoryResources(resolved, containerBuilder)

	containersBuilder := k8s.NewContainersBuilder()
	containersBuilder.SetContainerBuilders([]k8s.HasBuildContainer{containerBuilder})

	objectMetaBuilder := k8s.NewObjectMetaBuilder()
	objectMetaBuilder.SetName(k8s.Name(config.Name))
	objectMetaBuilder.SetNamespace(r.namespace)

	statefulSetBuilder := k8s.NewStatefulSetBuilder()
	statefulSetBuilder.SetObjectMetaBuilder(objectMetaBuilder)
	statefulSetBuilder.SetContainersBuilder(containersBuilder)
	statefulSetBuilder.SetName(k8s.Name(config.Name))
	statefulSetBuilder.SetReplicas(1)
	statefulSetBuilder.SetDatadirSize(serviceStorageSize)
	// Always set it, including when empty: the builder otherwise hardcodes
	// "standard", whereas an empty StorageClassName means "use the cluster's
	// default class" — which is what the chart's own PVC does.
	statefulSetBuilder.SetStorageClass(r.storageClass)
	statefulSetBuilder.AddImagePullSecrets(imagePullSecretName(resolved))
	statefulSetBuilder.AddLabel(assigneeLabelKey, resolved.Assignee)

	statefulSet, err := statefulSetBuilder.Build(ctx)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "build statefulset for config %s", config.Name)
	}

	// The Config owns the workload: deleting the CR garbage-collects the
	// StatefulSet, which is SC4's "CR delete → cleanup" half.
	statefulSet.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(&config, agentv1.SchemeGroupVersion.WithKind("Config")),
	}

	// A StatefulSet's volumeClaimTemplates are RETAINED by default, so without
	// this the session PVC would outlive the CR and "no resources left" would
	// never be true.
	statefulSet.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
	}

	applyServiceSecretEnvFrom(resolved, statefulSet)
	return statefulSet, nil
}

// buildServiceEnvBuilder renders the env for a service agent's container. Unlike a
// Job, a service agent has no task, so there is no TASK_CONTENT/TASK_ID/PHASE —
// only the Config's own env plus the type stamp.
func buildServiceEnvBuilder(resolved pkg.AgentConfiguration) k8s.EnvBuilder {
	envBuilder := k8s.NewEnvBuilder()
	envBuilder.Add(agentTypeEnvKey, string(agentTypeOrDefault(resolved.Type)))
	for key, value := range resolved.Env {
		envBuilder.Add(key, value)
	}
	return envBuilder
}

// agentTypeOrDefault resolves the empty type to job, matching the CRD's documented
// "absent means job" contract.
func agentTypeOrDefault(t agentv1.AgentType) agentv1.AgentType {
	if t == "" {
		return agentv1.AgentTypeJob
	}
	return t
}

// applyServiceSecretEnvFrom mounts the Config's Secret as envFrom on the container,
// mirroring applySecretEnvFrom on the Job path. ContainerBuilder exposes no envFrom
// setter, so it is applied to the built pod template — same as the Job path does.
func applyServiceSecretEnvFrom(
	resolved pkg.AgentConfiguration,
	statefulSet *appsv1.StatefulSet,
) {
	if resolved.SecretName == "" {
		return
	}
	if len(statefulSet.Spec.Template.Spec.Containers) == 0 {
		return
	}
	statefulSet.Spec.Template.Spec.Containers[0].EnvFrom = append(
		statefulSet.Spec.Template.Spec.Containers[0].EnvFrom,
		corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: resolved.SecretName},
			},
		},
	)
}
