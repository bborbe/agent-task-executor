// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"time"

	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsv1client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	versioned "github.com/bborbe/agent-task-executor/k8s/client/clientset/versioned"
	agentinformers "github.com/bborbe/agent-task-executor/k8s/client/informers/externalversions"
)

// crdMinZero is the lower bound for integer CRD fields that count things.
// apiextensionsv1 takes *float64, so it needs an addressable value.
var crdMinZero = float64(0)

// crdMinZombieSweeperInterval / crdMinZombieJobTimeout mirror the chart's
// config-crd.yaml minimums and the admission guards in the v1 types package.
var (
	crdMinZombieSweeperInterval = float64(agentv1.MinZombieSweeperIntervalSeconds)
	crdMinZombieJobTimeout      = float64(agentv1.MinZombieJobTimeoutSeconds)
)

//counterfeiter:generate -o ../mocks/k8s_connector.go --fake-name FakeK8sConnector . K8sConnector

// K8sConnector installs the Config CRD and starts an informer.
type K8sConnector interface {
	SetupCustomResourceDefinition(ctx context.Context) error
	Listen(
		ctx context.Context,
		namespace libk8s.Namespace,
		handler cache.ResourceEventHandler,
	) error
}

// CRDClientBuilder constructs the apiextensions clientset from a rest.Config.
// Injected to allow fake clientsets in tests.
type CRDClientBuilder func(*rest.Config) (apiextensionsclient.Interface, error)

// DefaultCRDClientBuilder wraps apiextensionsclient.NewForConfig to satisfy CRDClientBuilder.
func DefaultCRDClientBuilder(c *rest.Config) (apiextensionsclient.Interface, error) {
	return apiextensionsclient.NewForConfig(c)
}

// NewK8sConnector returns a K8sConnector using the given rest config and builder.
// Production callers pass DefaultCRDClientBuilder as the builder.
func NewK8sConnector(config *rest.Config, crdBuilder CRDClientBuilder) K8sConnector {
	return &k8sConnector{config: config, crdBuilder: crdBuilder}
}

// k8sConnector implements K8sConnector using a REST config and a CRD client builder.
type k8sConnector struct {
	config     *rest.Config
	crdBuilder CRDClientBuilder
}

// crdName is the cluster-scoped Config CRD. Every executor instance
// (openclaw/personal/trading) installs this same object on start.
const crdName = "configs.agent.benjamin-borbe.de"

// crdDNS1123Label is the DNS-1123 label shape Kubernetes requires of a
// namespace name. jobNamespace is used verbatim as a target namespace, so an
// unvalidated value would pass admission, resolve cleanly, and fail only at
// Jobs(...).Create with a 422 the operator has to correlate by hand.
const crdDNS1123Label = "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"

// SetupCustomResourceDefinition installs or updates the Config CRD on the cluster.
func (c *k8sConnector) SetupCustomResourceDefinition(ctx context.Context) error {
	clientset, err := c.crdBuilder(c.config)
	if err != nil {
		return errors.Wrapf(ctx, err, "build apiextensions clientset")
	}
	return ensureCRD(ctx, clientset.ApiextensionsV1().CustomResourceDefinitions())
}

// ensureCRD creates the Config CRD when the cluster has none, and updates it when the
// stored spec differs from the one this executor installs.
//
// The Get->Update runs inside RetryOnConflict because the CRD is cluster-scoped and
// shared: every executor instance installs it on start, so a simultaneous rollout
// leaves the losers writing against a resourceVersion the winner already bumped. That
// was a fatal 409 "object has been modified" during startup, and the pod restarted.
// Each attempt re-Gets, so the retry carries a fresh resourceVersion instead of
// replaying the stale one.
//
// The Update is skipped when the specs already match: a restart of an otherwise
// unchanged executor then issues no write at all, and fewer writes mean fewer
// resourceVersion bumps — which makes the conflict rare rather than merely survivable.
func ensureCRD(
	ctx context.Context,
	crds apiextensionsv1client.CustomResourceDefinitionInterface,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existing, err := crds.Get(ctx, crdName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			if _, err := crds.Create(ctx, desiredCRD(), metav1.CreateOptions{}); err != nil {
				return errors.Wrapf(ctx, err, "create CRD")
			}
			return nil
		}
		if err != nil {
			return errors.Wrapf(ctx, err, "get CRD")
		}
		desired := desiredCRD()
		if crdSpecEqual(existing.Spec, desired.Spec) {
			return nil
		}
		existing.Spec = desired.Spec
		if _, err := crds.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
			return errors.Wrapf(ctx, err, "update CRD")
		}
		return nil
	})
}

// desiredCRD is the Config CRD as this executor installs it, with the API server's own
// CRD defaults already applied.
//
// The defaulting is load-bearing for the no-op check in ensureCRD, not cosmetic: the
// server stores `spec.conversion.strategy: None` on create, while desiredCRDSpec leaves
// Conversion nil. Comparing a stored spec against an undefaulted desired one would
// therefore never be equal, and the Update would be re-issued on every single executor
// start — exactly the needless write the no-op check exists to prevent.
func desiredCRD() *apiextensionsv1.CustomResourceDefinition {
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: crdName},
		Spec:       desiredCRDSpec(),
	}
	apiextensionsv1.SetDefaults_CustomResourceDefinitionSpec(&crd.Spec)
	return crd
}

// crdSpecEqual reports whether the cluster's stored CRD spec already matches the one
// this executor installs. Both sides are run through the API server's CRD defaults
// first, so the comparison is between two copies of the form the server stores.
func crdSpecEqual(existing, desired apiextensionsv1.CustomResourceDefinitionSpec) bool {
	existingDefaults := existing.DeepCopy()
	apiextensionsv1.SetDefaults_CustomResourceDefinitionSpec(existingDefaults)
	desiredDefaults := desired.DeepCopy()
	apiextensionsv1.SetDefaults_CustomResourceDefinitionSpec(desiredDefaults)
	return equality.Semantic.DeepEqual(*existingDefaults, *desiredDefaults)
}

// defaultResync is the re-sync period for the shared informer factory.
const defaultResync = 5 * time.Minute

// Listen starts a Kubernetes informer for Config resources in the given namespace
// and dispatches events to the provided handler until ctx is cancelled.
func (c *k8sConnector) Listen(
	ctx context.Context,
	namespace libk8s.Namespace,
	handler cache.ResourceEventHandler,
) error {
	clientset, err := versioned.NewForConfig(c.config)
	if err != nil {
		return errors.Wrapf(ctx, err, "build agentconfig clientset")
	}
	factory := agentinformers.NewSharedInformerFactoryWithOptions(
		clientset,
		defaultResync,
		agentinformers.WithNamespace(namespace.String()),
	)
	informer := factory.Agent().V1().Configs().Informer()
	if _, err := informer.AddEventHandler(handler); err != nil {
		return errors.Wrapf(ctx, err, "add event handler")
	}
	stopCh := make(chan struct{})
	factory.Start(stopCh)
	defer close(stopCh)
	select {
	case <-ctx.Done():
	case <-stopCh:
	}
	return nil
}

func desiredCRDSpec() apiextensionsv1.CustomResourceDefinitionSpec {
	return apiextensionsv1.CustomResourceDefinitionSpec{
		Group: "agent.benjamin-borbe.de",
		Names: apiextensionsv1.CustomResourceDefinitionNames{
			Kind:       "Config",
			ListKind:   "ConfigList",
			Plural:     "configs",
			Singular:   "config",
			ShortNames: []string{"cfg"},
		},
		Scope: apiextensionsv1.NamespaceScoped,
		Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
			{
				Name:    "v1",
				Served:  true,
				Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": configSpecSchema(),
						},
					},
				},
			},
		},
		PreserveUnknownFields: false,
	}
}

// configSpecSchema is the authoritative schema for Config.spec — SetupCustomResourceDefinition
// writes it into the cluster CRD whenever the stored spec differs from it.
//
// Any field added to AgentConfiguration must be added here too, or the API server prunes it
// from every Config within seconds of the next restart. The failure is silent and reads as
// "the operator never set it": v0.5.0 shipped maxConcurrentJobs in AgentConfiguration and in
// the Helm chart's crds/ but not here, so the field was pruned everywhere, a pruned integer
// read as 0, and the cap treated 0 as unlimited — the feature was inert in prod for weeks
// while hand-applied CRD fixes appeared to "revert" (each revert was just the next pod start).
//
// Keep in sync with helm/crds/config-crd.yaml in bborbe/agent.
func configSpecSchema() apiextensionsv1.JSONSchemaProps {
	return apiextensionsv1.JSONSchemaProps{
		Type:         "object",
		Required:     []string{"assignee", "image", "heartbeat"},
		XValidations: configSpecValidations(),
		Properties:   configSpecProperties(),
	}
}

func configSpecValidations() apiextensionsv1.ValidationRules {
	return apiextensionsv1.ValidationRules{
		{
			// A service agent is addressed directly and is never task-routed, so it
			// carries no taskType/taskTypes and the requirement applies to job agents
			// only. Only an explicit `type: service` is exempt: a Config that omits
			// `type` resolves to job and must still carry a taskType, so the clause
			// is `has(self.type) && self.type == 'service'` rather than a
			// `!has(self.type) ||` leading guard, which would wrongly let an
			// untyped job Config through. Keep in sync with ConfigSpec.Validate.
			Rule:    "(has(self.type) && self.type == 'service') || (has(self.taskType) && size(self.taskType) > 0) || (has(self.taskTypes) && size(self.taskTypes) > 0)",
			Message: "at least one of spec.taskType or spec.taskTypes must be non-empty (unless spec.type is service)",
		},
	}
}

// agentTypeSchema is the OpenAPI schema for ConfigSpec.type. Extracted from
// configSpecProperties to keep that function under the funlen limit, mirroring
// configMapItemsSchema.
//
// The enum mirrors AvailableAgentTypes in k8s/apis/agent.benjamin-borbe.de/v1/types.go.
// Absent means "job", so existing Configs are unaffected; an unknown value is
// rejected at admission rather than silently routed down the Job path.
func agentTypeSchema() apiextensionsv1.JSONSchemaProps {
	return apiextensionsv1.JSONSchemaProps{
		Type: "string",
		Enum: []apiextensionsv1.JSON{
			{Raw: []byte(`"job"`)},
			{Raw: []byte(`"service"`)},
		},
	}
}

// configMapItemsSchema is the OpenAPI schema for ConfigSpec.configMapItems.
// Extracted from configSpecProperties to keep that function under the funlen
// limit.
func configMapItemsSchema() apiextensionsv1.JSONSchemaProps {
	return apiextensionsv1.JSONSchemaProps{
		Type: "array",
		Items: &apiextensionsv1.JSONSchemaPropsOrArray{
			Schema: &apiextensionsv1.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"key":  {Type: "string"},
					"path": {Type: "string"},
				},
			},
		},
	}
}

func configSpecProperties() map[string]apiextensionsv1.JSONSchemaProps {
	minLen := int64(1)
	maxLen63 := int64(63)
	resourceList := apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"cpu":               {Type: "string"},
			"memory":            {Type: "string"},
			"ephemeral-storage": {Type: "string"},
		},
	}
	return map[string]apiextensionsv1.JSONSchemaProps{
		"assignee":  {Type: "string", MinLength: &minLen},
		"image":     {Type: "string", MinLength: &minLen},
		"heartbeat": {Type: "string", Pattern: "^[0-9]+(s|m|h)$"},
		"type":      agentTypeSchema(),
		"taskType": {
			Type:      "string",
			Pattern:   `^[a-z0-9-]+$`,
			MaxLength: &maxLen63,
		},
		"taskTypes": {
			Type: "array",
			Items: &apiextensionsv1.JSONSchemaPropsOrArray{
				Schema: &apiextensionsv1.JSONSchemaProps{
					Type:      "string",
					Pattern:   `^[a-z0-9-]+$`,
					MaxLength: &maxLen63,
				},
			},
		},
		"resources": {
			Type: "object",
			Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"requests": resourceList,
				"limits":   resourceList,
			},
		},
		"env": {
			Type: "object",
			AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
				Schema: &apiextensionsv1.JSONSchemaProps{Type: "string"},
			},
		},
		"secretName":         {Type: "string"},
		"volumeClaim":        {Type: "string"},
		"volumeMountPath":    {Type: "string"},
		"configMapName":      {Type: "string"},
		"configMapMountPath": {Type: "string"},
		"configMapItems":     configMapItemsSchema(),
		"priorityClassName": {Type: "string", Pattern: crdDNS1123Label},
		"jobNamespace":    {Type: "string", Pattern: crdDNS1123Label},
		"imagePullSecret": {Type: "string"},
		// 0 = unlimited (see AgentConfiguration.MaxConcurrentJobs).
		"maxConcurrentJobs": {Type: "integer", Minimum: &crdMinZero},
		// Optional; unset means true. false parks a taken-over task at human_review.
		"rerunOnLostClaim": {Type: "boolean"},
		// Minimums mirror the Helm chart's config-crd.yaml (10 / 30) and the
		// admission guards in k8s/apis/agent.benjamin-borbe.de/v1/types.go.
		"zombieSweeperIntervalSeconds": {
			Type:    "integer",
			Minimum: &crdMinZombieSweeperInterval,
		},
		"zombieJobTimeoutSeconds": {Type: "integer", Minimum: &crdMinZombieJobTimeout},
		"trigger": {
			Type: "object",
			Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"phases": {
					Type: "array",
					Items: &apiextensionsv1.JSONSchemaPropsOrArray{
						Schema: &apiextensionsv1.JSONSchemaProps{Type: "string"},
					},
				},
				"statuses": {
					Type: "array",
					Items: &apiextensionsv1.JSONSchemaPropsOrArray{
						Schema: &apiextensionsv1.JSONSchemaProps{Type: "string"},
					},
				},
			},
		},
	}
}
