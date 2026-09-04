// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"

	libk8s "github.com/bborbe/k8s"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/bborbe/agent-task-executor/pkg"
)

var _ = Describe("ReadGitRestGatewaySecret", func() {
	var (
		ctx         context.Context
		kubeClient  *fake.Clientset
		secretName  string
		secretValue string
		err         error
	)

	BeforeEach(func() {
		ctx = context.Background()
	})

	JustBeforeEach(func() {
		secretValue, err = pkg.ReadGitRestGatewaySecret(
			ctx,
			kubeClient,
			libk8s.Namespace("test-ns"),
			secretName,
		)
	})

	Context("empty secret name (gateway auth disabled)", func() {
		BeforeEach(func() {
			kubeClient = fake.NewSimpleClientset()
			secretName = ""
		})

		It("returns empty value and nil error", func() {
			Expect(err).NotTo(HaveOccurred())
			Expect(secretValue).To(Equal(""))
		})
	})

	Context("secret present with gateway-secret data key", func() {
		BeforeEach(func() {
			kubeClient = fake.NewSimpleClientset(&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "gw-secret", Namespace: "test-ns"},
				Data:       map[string][]byte{"gateway-secret": []byte("s3cret")},
			})
			secretName = "gw-secret"
		})

		It("returns the secret value", func() {
			Expect(err).NotTo(HaveOccurred())
			Expect(secretValue).To(Equal("s3cret"))
		})
	})

	Context("secret missing", func() {
		BeforeEach(func() {
			kubeClient = fake.NewSimpleClientset()
			secretName = "gw-secret"
		})

		It("returns an error mentioning the secret name", func() {
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("gw-secret"))
			Expect(secretValue).To(Equal(""))
		})
	})

	Context("secret present without gateway-secret data key", func() {
		BeforeEach(func() {
			kubeClient = fake.NewSimpleClientset(&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "gw-secret", Namespace: "test-ns"},
				Data:       map[string][]byte{"other": []byte("x")},
			})
			secretName = "gw-secret"
		})

		It("returns an error mentioning the data key", func() {
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("gateway-secret"))
			Expect(secretValue).To(Equal(""))
		})
	})
})
