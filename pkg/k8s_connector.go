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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	versioned "github.com/bborbe/agent/task/executor/k8s/client/clientset/versioned"
	agentinformers "github.com/bborbe/agent/task/executor/k8s/client/informers/externalversions"
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

// SetupCustomResourceDefinition installs or updates the Config CRD on the cluster.
func (c *k8sConnector) SetupCustomResourceDefinition(ctx context.Context) error {
	clientset, err := c.crdBuilder(c.config)
	if err != nil {
		return errors.Wrapf(ctx, err, "build apiextensions clientset")
	}
	crds := clientset.ApiextensionsV1().CustomResourceDefinitions()
	existing, err := crds.Get(ctx, "configs.agent.benjamin-borbe.de", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		crd := &apiextensionsv1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: "configs.agent.benjamin-borbe.de"},
			Spec:       desiredCRDSpec(),
		}
		if _, err := crds.Create(ctx, crd, metav1.CreateOptions{}); err != nil {
			return errors.Wrapf(ctx, err, "create CRD")
		}
		return nil
	}
	if err != nil {
		return errors.Wrapf(ctx, err, "get CRD")
	}
	existing.Spec = desiredCRDSpec()
	if _, err := crds.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return errors.Wrapf(ctx, err, "update CRD")
	}
	return nil
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

func configSpecSchema() apiextensionsv1.JSONSchemaProps {
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
	return apiextensionsv1.JSONSchemaProps{
		Type:     "object",
		Required: []string{"assignee", "image", "heartbeat"},
		XValidations: apiextensionsv1.ValidationRules{
			{
				Rule:    "(has(self.taskType) && size(self.taskType) > 0) || (has(self.taskTypes) && size(self.taskTypes) > 0)",
				Message: "at least one of spec.taskType or spec.taskTypes must be non-empty",
			},
		},
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"assignee":  {Type: "string", MinLength: &minLen},
			"image":     {Type: "string", MinLength: &minLen},
			"heartbeat": {Type: "string", Pattern: "^[0-9]+(s|m|h)$"},
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
			"secretName":      {Type: "string"},
			"volumeClaim":     {Type: "string"},
			"volumeMountPath": {Type: "string"},
			"priorityClassName": {
				Type:    "string",
				Pattern: "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
			},
			"imagePullSecret": {Type: "string"},
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
		},
	}
}
