// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6@v6.12.2 -generate

package pkg_test

import (
	"context"
	stderrors "errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

func fakeBuilder(cs apiextensionsclient.Interface) pkg.CRDClientBuilder {
	return func(_ *rest.Config) (apiextensionsclient.Interface, error) {
		return cs, nil
	}
}

func getCRDFromCreateAction(actions []k8stesting.Action) *apiextensionsv1.CustomResourceDefinition {
	createAction, ok := actions[1].(k8stesting.CreateAction)
	Expect(ok).To(BeTrue(), "expected action[1] to be a CreateAction")
	crd, ok := createAction.GetObject().(*apiextensionsv1.CustomResourceDefinition)
	Expect(ok).To(BeTrue(), "expected object to be *CustomResourceDefinition")
	return crd
}

var _ = Describe("K8sConnector", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("SetupCustomResourceDefinition", func() {
		It("creates CRD when none exists", func() {
			cs := fake.NewSimpleClientset()
			connector := pkg.NewK8sConnector(&rest.Config{}, fakeBuilder(cs))

			err := connector.SetupCustomResourceDefinition(ctx)
			Expect(err).To(BeNil())

			actions := cs.Actions()
			// Expect a Get (not found) then a Create
			Expect(actions).To(HaveLen(2))
			Expect(actions[0].GetVerb()).To(Equal("get"))
			Expect(actions[0].GetResource().Resource).To(Equal("customresourcedefinitions"))
			Expect(actions[1].GetVerb()).To(Equal("create"))
			crd := getCRDFromCreateAction(actions)
			Expect(crd.Name).To(Equal("configs.agent.benjamin-borbe.de"))
			Expect(crd.Spec.Scope).To(Equal(apiextensionsv1.NamespaceScoped))
		})

		It("updates CRD when one already exists", func() {
			existingCRD := &apiextensionsv1.CustomResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{
					Name: "configs.agent.benjamin-borbe.de",
				},
				Spec: apiextensionsv1.CustomResourceDefinitionSpec{
					Group: "agent.benjamin-borbe.de",
					Names: apiextensionsv1.CustomResourceDefinitionNames{
						Kind:   "Config",
						Plural: "configs",
					},
					Scope: apiextensionsv1.NamespaceScoped,
					Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true},
					},
				},
			}
			cs := fake.NewSimpleClientset(existingCRD)
			connector := pkg.NewK8sConnector(&rest.Config{}, fakeBuilder(cs))

			err := connector.SetupCustomResourceDefinition(ctx)
			Expect(err).To(BeNil())

			actions := cs.Actions()
			// Expect Get (found) then Update
			Expect(actions).To(HaveLen(2))
			Expect(actions[0].GetVerb()).To(Equal("get"))
			Expect(actions[1].GetVerb()).To(Equal("update"))
		})

		It("returns a wrapped error when Get fails with a non-NotFound error", func() {
			cs := fake.NewSimpleClientset()
			cs.PrependReactor(
				"get",
				"customresourcedefinitions",
				func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewInternalError(
						stderrors.New("simulated internal error"),
					)
				},
			)
			connector := pkg.NewK8sConnector(&rest.Config{}, fakeBuilder(cs))

			err := connector.SetupCustomResourceDefinition(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("get CRD")))
		})
	})
})

var _ = Describe("desiredCRDSpec (via SetupCustomResourceDefinition)", func() {
	var ctx context.Context
	var cs *fake.Clientset
	var connector pkg.K8sConnector

	BeforeEach(func() {
		ctx = context.Background()
		cs = fake.NewSimpleClientset()
		connector = pkg.NewK8sConnector(&rest.Config{}, fakeBuilder(cs))
		err := connector.SetupCustomResourceDefinition(ctx)
		Expect(err).To(BeNil())
	})

	It("sets Scope to NamespaceScoped", func() {
		crd := getCRDFromCreateAction(cs.Actions())
		Expect(crd.Spec.Scope).To(Equal(apiextensionsv1.NamespaceScoped))
	})

	It("sets required spec fields to assignee, image, heartbeat", func() {
		crd := getCRDFromCreateAction(cs.Actions())
		schema := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
		specProps := schema.Properties["spec"]
		Expect(specProps.Required).To(ConsistOf("assignee", "image", "heartbeat"))
	})

	It("sets heartbeat pattern", func() {
		crd := getCRDFromCreateAction(cs.Actions())
		schema := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
		specProps := schema.Properties["spec"]
		Expect(specProps.Properties["heartbeat"].Pattern).To(Equal("^[0-9]+(s|m|h)$"))
	})

	It("sets Names correctly", func() {
		crd := getCRDFromCreateAction(cs.Actions())
		Expect(crd.Spec.Names.Plural).To(Equal("configs"))
		Expect(crd.Spec.Names.Singular).To(Equal("config"))
		Expect(crd.Spec.Names.Kind).To(Equal("Config"))
		Expect(crd.Spec.Names.ListKind).To(Equal("ConfigList"))
	})

	It("sets version v1 as served and storage", func() {
		crd := getCRDFromCreateAction(cs.Actions())
		Expect(crd.Spec.Versions).To(HaveLen(1))
		Expect(crd.Spec.Versions[0].Name).To(Equal("v1"))
		Expect(crd.Spec.Versions[0].Served).To(BeTrue())
		Expect(crd.Spec.Versions[0].Storage).To(BeTrue())
	})
})
