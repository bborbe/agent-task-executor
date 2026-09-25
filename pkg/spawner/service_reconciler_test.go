// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner_test

import (
	"context"

	libk8s "github.com/bborbe/k8s"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/spawner"
)

var _ = Describe("ServiceReconciler", func() {
	const namespace = "test-ns"

	var (
		ctx         context.Context
		fakeClient  *fake.Clientset
		reconciler  spawner.ServiceReconciler
		serviceConf agentv1.Config
		serviceCfg  pkg.AgentConfiguration
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeClient = fake.NewClientset()
		reconciler = spawner.NewServiceReconciler(
			libk8s.NewStatefulSetDeployer(fakeClient),
			fakeClient.AppsV1().StatefulSets(namespace),
			namespace,
			"standard",
		)
		serviceConf = agentv1.Config{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "identity",
				Namespace: namespace,
				UID:       "11111111-2222-3333-4444-555555555555",
			},
			Spec: agentv1.ConfigSpec{
				Assignee: "identity",
				Image:    "docker.io/bborbe/agent-pi:v0.1.7",
				Type:     agentv1.AgentTypeService,
			},
		}
		serviceCfg = pkg.AgentConfiguration{
			Assignee: "identity",
			Type:     agentv1.AgentTypeService,
			Image:    "docker.io/bborbe/agent-pi:v0.1.7",
			Env:      map[string]string{"ALLOWED_TOOLS": "Read,Grep"},
		}
	})

	getStatefulSet := func(name string) *appsv1.StatefulSet {
		sts, err := fakeClient.AppsV1().
			StatefulSets(namespace).
			Get(ctx, name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		return sts
	}

	Describe("ReconcileService", func() {
		It("creates one StatefulSet named after the Config, at one replica", func() {
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			sts := getStatefulSet("identity")
			Expect(sts.Spec.Replicas).NotTo(BeNil())
			Expect(*sts.Spec.Replicas).To(Equal(int32(1)))
		})

		It(
			"makes the Config the controller owner, so deleting the CR collects the workload",
			func() {
				Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

				sts := getStatefulSet("identity")
				Expect(sts.OwnerReferences).To(HaveLen(1))
				Expect(sts.OwnerReferences[0].Kind).To(Equal("Config"))
				Expect(sts.OwnerReferences[0].Name).To(Equal("identity"))
				Expect(sts.OwnerReferences[0].UID).To(Equal(serviceConf.UID))
				Expect(sts.OwnerReferences[0].Controller).NotTo(BeNil())
				Expect(*sts.OwnerReferences[0].Controller).To(BeTrue())
			},
		)

		It("provisions its own volume rather than reusing a named claim", func() {
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			sts := getStatefulSet("identity")
			Expect(sts.Spec.VolumeClaimTemplates).To(HaveLen(1))
			Expect(sts.Spec.VolumeClaimTemplates[0].Name).To(Equal("datadir"))
			// No standalone claim is referenced: the workload owns its storage.
			Expect(sts.Spec.Template.Spec.Volumes).To(BeEmpty())
		})

		It("deletes the volume when the StatefulSet is deleted", func() {
			// Kubernetes retains volumeClaimTemplates claims by default, which would
			// leave the session PVC behind after a CR delete.
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			sts := getStatefulSet("identity")
			Expect(sts.Spec.PersistentVolumeClaimRetentionPolicy).NotTo(BeNil())
			Expect(
				sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted,
			).To(Equal(appsv1.DeletePersistentVolumeClaimRetentionPolicyType))
		})

		It("mounts the session volume at pi's agent dir by default", func() {
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			sts := getStatefulSet("identity")
			Expect(sts.Spec.Template.Spec.Containers).To(HaveLen(1))
			mounts := sts.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(1))
			Expect(mounts[0].Name).To(Equal("datadir"))
			Expect(mounts[0].MountPath).To(Equal("/home/pi/.pi"))
		})

		It("honours a Config that names its own mount path", func() {
			serviceCfg.VolumeMountPath = "/home/pi/.pi-custom"
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			sts := getStatefulSet("identity")
			mounts := sts.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts[0].MountPath).To(Equal("/home/pi/.pi-custom"))
		})

		It(
			"stamps the agent type into the container env so a runner can tell the shapes apart",
			func() {
				Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

				sts := getStatefulSet("identity")
				env := sts.Spec.Template.Spec.Containers[0].Env
				Expect(env).To(ContainElement(corev1.EnvVar{Name: "AGENT_TYPE", Value: "service"}))
				Expect(
					env,
				).To(ContainElement(corev1.EnvVar{Name: "ALLOWED_TOOLS", Value: "Read,Grep"}))
			},
		)

		It("leaves a job Config alone, so the Job path stays untouched", func() {
			jobCfg := serviceCfg
			jobCfg.Type = agentv1.AgentTypeJob

			Expect(reconciler.ReconcileService(ctx, serviceConf, jobCfg)).To(Succeed())

			list, err := fakeClient.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(BeEmpty())
		})

		It("treats an unset type as job, matching the CRD contract", func() {
			unsetCfg := serviceCfg
			unsetCfg.Type = ""

			Expect(reconciler.ReconcileService(ctx, serviceConf, unsetCfg)).To(Succeed())

			list, err := fakeClient.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(BeEmpty())
		})

		It("is idempotent — a second reconcile with the same config does not error", func() {
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			sts := getStatefulSet("identity")
			Expect(sts.Spec.Template.Spec.Containers[0].Image).To(Equal(serviceCfg.Image))
		})

		It("rolls the pod when the image changes", func() {
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			updated := serviceCfg
			updated.Image = "docker.io/bborbe/agent-pi:v0.1.8"
			Expect(reconciler.ReconcileService(ctx, serviceConf, updated)).To(Succeed())

			sts := getStatefulSet("identity")
			Expect(sts.Spec.Template.Spec.Containers[0].Image).To(Equal(updated.Image))
		})

		It("mounts the Config's Secret as envFrom", func() {
			withSecret := serviceCfg
			withSecret.SecretName = "identity-secret"
			Expect(reconciler.ReconcileService(ctx, serviceConf, withSecret)).To(Succeed())

			sts := getStatefulSet("identity")
			envFrom := sts.Spec.Template.Spec.Containers[0].EnvFrom
			Expect(envFrom).To(HaveLen(1))
			Expect(envFrom[0].SecretRef.Name).To(Equal("identity-secret"))
		})
	})

	Describe("session volume storage class", func() {
		It("omits it when none is configured, so the cluster default applies", func() {
			// An empty StorageClassName is NOT "use the default". Kubernetes reads
			// an explicit "" as "bind to a PV that has no storage class", which
			// never binds when the cluster's default is something else; only a nil
			// pointer selects the default. The builder always emits the pointer
			// (hardcoding "standard"), so the reconciler has to unset it.
			defaulted := spawner.NewServiceReconciler(
				libk8s.NewStatefulSetDeployer(fakeClient),
				fakeClient.AppsV1().StatefulSets(namespace),
				namespace,
				"",
			)
			Expect(defaulted.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			sts := getStatefulSet("identity")
			Expect(sts.Spec.VolumeClaimTemplates).To(HaveLen(1))
			Expect(sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName).To(BeNil())
		})

		It("uses the configured class when one is set", func() {
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			sts := getStatefulSet("identity")
			Expect(sts.Spec.VolumeClaimTemplates).To(HaveLen(1))
			Expect(sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName).NotTo(BeNil())
			Expect(*sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName).To(Equal("standard"))
		})
	})

	Describe("UndeployService", func() {
		It("removes a StatefulSet this executor owns", func() {
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())
			Expect(reconciler.UndeployService(ctx, "identity")).To(Succeed())

			list, err := fakeClient.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(BeEmpty())
		})

		It("leaves a StatefulSet another owner created under the same name", func() {
			// The loop calls UndeployService for every Config that is not a service,
			// and a StatefulSet's name is just the Config's name — so a job Config
			// must never tear down a workload that merely shares that name.
			foreign := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claude-agent",
					Namespace: namespace,
					OwnerReferences: []metav1.OwnerReference{
						{APIVersion: "apps/v1", Kind: "Deployment", Name: "someone-else"},
					},
				},
			}
			_, err := fakeClient.AppsV1().
				StatefulSets(namespace).
				Create(ctx, foreign, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Expect(reconciler.UndeployService(ctx, "claude-agent")).To(Succeed())

			list, err := fakeClient.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(1), "an unowned StatefulSet must survive")
		})

		It("is a silent no-op for a name that has no StatefulSet", func() {
			Expect(reconciler.UndeployService(ctx, "never-deployed")).To(Succeed())
		})
	})
})
