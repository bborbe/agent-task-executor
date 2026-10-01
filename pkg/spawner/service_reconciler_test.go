// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner_test

import (
	"context"
	"fmt"
	"strings"

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

	envNames := func(env []corev1.EnvVar) []string {
		names := make([]string, 0, len(env))
		for _, e := range env {
			names = append(names, e.Name)
		}
		return names
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

		It("renders the container env in exactly one order across 50 reconciles", func() {
			multiEnv := serviceCfg
			multiEnv.Env = map[string]string{
				"MODEL":           "claude-sonnet-4-5",
				"PUSHGATEWAY_URL": "http://pushgateway:9091",
				"ALLOWED_TOOLS":   "Read,Grep,Bash",
				"LOG_LEVEL":       "debug",
			}

			orderings := map[string]struct{}{}
			for i := 0; i < 50; i++ {
				Expect(reconciler.ReconcileService(ctx, serviceConf, multiEnv)).To(Succeed())
				env := getStatefulSet("identity").Spec.Template.Spec.Containers[0].Env
				orderings[strings.Join(envNames(env), ",")] = struct{}{}
			}

			Expect(orderings).To(HaveLen(1),
				"an unchanged Config must render one env ordering, not a distribution over several")

			// The order must be the SORTED one, not merely *a* stable one: a deterministic
			// but wrong order satisfies the count above while still being a defect, and
			// this exact assertion is also what makes the ordering reproducible across
			// process restarts rather than merely within one.
			Expect(envNames(getStatefulSet("identity").Spec.Template.Spec.Containers[0].Env)).
				To(Equal([]string{"AGENT_TYPE", "ALLOWED_TOOLS", "LOG_LEVEL", "MODEL", "PUSHGATEWAY_URL"}),
					"AGENT_TYPE first, then the Config-declared keys in ascending order")
		})

		It("renders one env order for a map larger than one Go map bucket", func() {
			bigEnv := serviceCfg
			bigEnv.Env = map[string]string{}
			for i := 0; i < 20; i++ {
				bigEnv.Env[fmt.Sprintf("KEY_%02d", i)] = fmt.Sprintf("value-%d", i)
			}

			orderings := map[string]struct{}{}
			for i := 0; i < 50; i++ {
				Expect(reconciler.ReconcileService(ctx, serviceConf, bigEnv)).To(Succeed())
				env := getStatefulSet("identity").Spec.Template.Spec.Containers[0].Env
				orderings[strings.Join(envNames(env), ",")] = struct{}{}
			}

			Expect(orderings).To(HaveLen(1),
				"ordering must not depend on the single-bucket case")
		})

		It("produces a byte-identical pod template across two consecutive reconciles", func() {
			multiEnv := serviceCfg
			multiEnv.Env = map[string]string{
				"MODEL":           "claude-sonnet-4-5",
				"PUSHGATEWAY_URL": "http://pushgateway:9091",
				"ALLOWED_TOOLS":   "Read,Grep,Bash",
				"LOG_LEVEL":       "debug",
			}

			Expect(reconciler.ReconcileService(ctx, serviceConf, multiEnv)).To(Succeed())
			first := getStatefulSet("identity").Spec.Template

			Expect(reconciler.ReconcileService(ctx, serviceConf, multiEnv)).To(Succeed())
			second := getStatefulSet("identity").Spec.Template

			// Element-for-element, same index order — NOT set-equal. ConsistOf/ContainElements
			// would pass even while the order churns, which is exactly the bug this pins.
			Expect(second.Spec.Containers[0].Env).To(Equal(first.Spec.Containers[0].Env))
			Expect(second).To(Equal(first))
		})

		It("still rolls the pod when the image changes, leaving the env order stable", func() {
			multiEnv := serviceCfg
			multiEnv.Env = map[string]string{
				"MODEL":           "claude-sonnet-4-5",
				"PUSHGATEWAY_URL": "http://pushgateway:9091",
				"ALLOWED_TOOLS":   "Read,Grep,Bash",
				"LOG_LEVEL":       "debug",
			}

			Expect(reconciler.ReconcileService(ctx, serviceConf, multiEnv)).To(Succeed())
			before := getStatefulSet("identity").Spec.Template.Spec.Containers[0]

			updated := multiEnv
			updated.Image = "docker.io/bborbe/agent-pi:v0.1.8"
			Expect(reconciler.ReconcileService(ctx, serviceConf, updated)).To(Succeed())
			after := getStatefulSet("identity").Spec.Template.Spec.Containers[0]

			Expect(after.Image).To(Equal("docker.io/bborbe/agent-pi:v0.1.8"))
			Expect(after.Image).NotTo(Equal(before.Image))
			Expect(after.Env).To(Equal(before.Env))
		})

		It("still rolls the pod when an env value changes, leaving the key order stable", func() {
			multiEnv := serviceCfg
			multiEnv.Env = map[string]string{
				"MODEL":           "claude-sonnet-4-5",
				"PUSHGATEWAY_URL": "http://pushgateway:9091",
				"ALLOWED_TOOLS":   "Read,Grep,Bash",
				"LOG_LEVEL":       "debug",
			}

			Expect(reconciler.ReconcileService(ctx, serviceConf, multiEnv)).To(Succeed())
			before := getStatefulSet("identity").Spec.Template.Spec.Containers[0].Env

			changed := multiEnv
			changed.Env = map[string]string{
				"MODEL":           "claude-opus-4-5",
				"PUSHGATEWAY_URL": "http://pushgateway:9091",
				"ALLOWED_TOOLS":   "Read,Grep,Bash",
				"LOG_LEVEL":       "debug",
			}
			Expect(reconciler.ReconcileService(ctx, serviceConf, changed)).To(Succeed())
			after := getStatefulSet("identity").Spec.Template.Spec.Containers[0].Env

			Expect(envNames(after)).To(Equal(envNames(before)))
			Expect(after).NotTo(Equal(before))
			Expect(after).To(ContainElement(corev1.EnvVar{Name: "MODEL", Value: "claude-opus-4-5"}))
		})

		It("renders two Configs with the same key set independently and deterministically", func() {
			cfgA := serviceCfg
			cfgA.Env = map[string]string{"MODEL": "claude-sonnet-4-5", "LOG_LEVEL": "debug"}
			confA := serviceConf
			confA.Name = "identity-a"

			cfgB := serviceCfg
			cfgB.Env = map[string]string{"MODEL": "claude-opus-4-1", "LOG_LEVEL": "info"}
			confB := serviceConf
			confB.Name = "identity-b"

			for i := 0; i < 50; i++ {
				Expect(reconciler.ReconcileService(ctx, confA, cfgA)).To(Succeed())
				Expect(reconciler.ReconcileService(ctx, confB, cfgB)).To(Succeed())
			}

			Expect(envNames(getStatefulSet("identity-a").Spec.Template.Spec.Containers[0].Env)).
				To(Equal([]string{"AGENT_TYPE", "LOG_LEVEL", "MODEL"}))
			Expect(envNames(getStatefulSet("identity-b").Spec.Template.Spec.Containers[0].Env)).
				To(Equal([]string{"AGENT_TYPE", "LOG_LEVEL", "MODEL"}))
			Expect(getStatefulSet("identity-a").Spec.Template).
				NotTo(Equal(getStatefulSet("identity-b").Spec.Template),
					"a differing env value must still change the rendered template")
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

	Describe("readiness probe", func() {
		// Before this existed the service container declared no probe at all, so
		// `Ready` meant only "the container started": the endpoint was served and
		// nothing ever called it. That made SC5 unobservable rather than false,
		// which is the failure mode a probe that is never wired always has.
		It("probes the service agent's own readiness endpoint", func() {
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			probe := getStatefulSet("identity").Spec.Template.Spec.Containers[0].ReadinessProbe
			Expect(probe).NotTo(BeNil())
			Expect(probe.HTTPGet).NotTo(BeNil())
			Expect(probe.HTTPGet.Path).To(Equal("/readiness"))
			Expect(probe.HTTPGet.Port.StrVal).To(Equal("http"))
		})

		It("declares the container port the probe names", func() {
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			ports := getStatefulSet("identity").Spec.Template.Spec.Containers[0].Ports
			Expect(ports).To(HaveLen(1))
			Expect(ports[0].Name).To(Equal("http"))
			Expect(ports[0].ContainerPort).To(Equal(int32(9090)))
		})

		It("goes NotReady inside SC5's 30s budget, worst case", func() {
			// The endpoint dials the provider with a 5s timeout before it can answer,
			// so the kubelet's cycle is the period *plus* that cost. Asserting the
			// worst case rather than the tidy one is the point: the first cut of this
			// probe used failureThreshold 3 at an 8s period, which is 44s by this
			// arithmetic and would have missed the budget it was written for.
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			probe := getStatefulSet("identity").Spec.Template.Spec.Containers[0].ReadinessProbe
			Expect(probe).NotTo(BeNil())
			worstCase := int(probe.InitialDelaySeconds) +
				int(probe.FailureThreshold)*(int(probe.PeriodSeconds)+int(probe.TimeoutSeconds))
			Expect(worstCase).To(BeNumerically("<", 30))
		})

		It("needs more than one failure, so a single transient dial does not take it out", func() {
			Expect(reconciler.ReconcileService(ctx, serviceConf, serviceCfg)).To(Succeed())

			probe := getStatefulSet("identity").Spec.Template.Spec.Containers[0].ReadinessProbe
			Expect(probe).NotTo(BeNil())
			Expect(probe.FailureThreshold).To(BeNumerically(">", 1))
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
