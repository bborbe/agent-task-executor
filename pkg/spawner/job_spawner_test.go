// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner_test

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6@v6.12.2 -generate

import (
	"context"
	"fmt"
	"testing"

	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	lib "github.com/bborbe/agent/lib"
	agentv1 "github.com/bborbe/agent/task/executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent/task/executor/pkg"
	"github.com/bborbe/agent/task/executor/pkg/spawner"
)

func TestSpawner(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Spawner Suite")
}

var _ = Describe("JobSpawner", func() {
	var (
		ctx             context.Context
		fakeClient      *fake.Clientset
		jobSpawner      spawner.JobSpawner
		currentDateTime libtime.CurrentDateTime
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeClient = fake.NewClientset()
		currentDateTime = libtime.NewCurrentDateTime()
		currentDateTime.SetNow(libtimetest.ParseDateTime("2026-04-03T17:35:00Z"))
		jobSpawner = spawner.NewJobSpawner(
			fakeClient,
			"test-ns",
			"kafka:9092",
			"develop",
			currentDateTime,
			1800,
		)
	})

	Describe("SpawnJob", func() {
		It("creates a job with correct name and env vars", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc12345-rest-ignored"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
					"phase":    "planning",
				},
				Content: lib.TaskContent("do the work"),
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude",
				Image:    "my-image:latest",
				Env:      map[string]string{"GEMINI_API_KEY": "test-gemini-key"},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			job := jobs.Items[0]
			Expect(job.Name).To(Equal("claude-abc12345-20260403173500"))
			Expect(job.Namespace).To(Equal("test-ns"))
			Expect(*job.Spec.BackoffLimit).To(Equal(int32(0)))
			Expect(job.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))

			Expect(job.Spec.TTLSecondsAfterFinished).NotTo(BeNil())
			Expect(*job.Spec.TTLSecondsAfterFinished).To(Equal(int32(1800)))

			Expect(job.Spec.Template.Labels).To(HaveKeyWithValue("app", "agent"))
			Expect(job.Spec.Template.Labels).To(HaveKey("component"))

			Expect(job.Spec.Template.Spec.ImagePullSecrets).To(HaveLen(1))
			Expect(job.Spec.Template.Spec.ImagePullSecrets[0].Name).To(Equal("docker"))

			container := job.Spec.Template.Spec.Containers[0]
			Expect(container.Image).To(Equal("my-image:latest"))

			envMap := make(map[string]string)
			for _, e := range container.Env {
				envMap[e.Name] = e.Value
			}
			Expect(envMap["TASK_CONTENT"]).To(HavePrefix("---\n"))
			Expect(envMap["TASK_CONTENT"]).To(ContainSubstring("\n---\n"))
			Expect(envMap["TASK_CONTENT"]).To(ContainSubstring("assignee: claude"))
			Expect(envMap["TASK_CONTENT"]).To(ContainSubstring("phase: planning"))
			Expect(envMap["TASK_CONTENT"]).To(HaveSuffix("\n---\ndo the work"))
			Expect(envMap["TASK_ID"]).To(Equal("abc12345-rest-ignored"))
			Expect(envMap["KAFKA_BROKERS"]).To(Equal("kafka:9092"))
			Expect(envMap["BRANCH"]).To(Equal("develop"))
			Expect(envMap["GEMINI_API_KEY"]).To(Equal("test-gemini-key"))
			Expect(envMap["PHASE"]).To(Equal("planning"))
			Expect(envMap["TASK_TYPE"]).To(Equal(""))
		})

		It("propagates a non-default ttlSecondsAfterFinished to the Job spec", func() {
			customTTL := int32(60)
			jobSpawner = spawner.NewJobSpawner(
				fakeClient,
				"test-ns",
				"kafka:9092",
				"develop",
				currentDateTime,
				customTTL,
			)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("ttl-custom"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
					"phase":    "planning",
				},
				Content: lib.TaskContent("do the work"),
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude",
				Image:    "my-image:latest",
				Env:      map[string]string{"GEMINI_API_KEY": "test-gemini-key"},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))
			Expect(jobs.Items[0].Spec.TTLSecondsAfterFinished).NotTo(BeNil())
			Expect(*jobs.Items[0].Spec.TTLSecondsAfterFinished).To(Equal(customTTL))
		})

		It(
			"includes frontmatter delimiters and keys in TASK_CONTENT so spawned agents can parse fields like clone_url",
			func() {
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("pr-task-uuid"),
					Frontmatter: lib.TaskFrontmatter{
						"assignee":  "pr-reviewer",
						"phase":     "in_progress",
						"clone_url": "https://github.com/bborbe/code-reviewer.git",
						"ref":       "f82244d6abcdef",
						"base_ref":  "master",
					},
					Content: lib.TaskContent("# PR Review:\n\nbody here"),
				}
				config := pkg.AgentConfiguration{
					Assignee: "pr-reviewer",
					Image:    "pr-reviewer-agent:latest",
					Env:      map[string]string{},
				}
				_, err := jobSpawner.SpawnJob(ctx, task, config)
				Expect(err).To(BeNil())

				jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
				Expect(err).To(BeNil())
				Expect(jobs.Items).To(HaveLen(1))
				container := jobs.Items[0].Spec.Template.Spec.Containers[0]
				envMap := make(map[string]string)
				for _, e := range container.Env {
					envMap[e.Name] = e.Value
				}
				got := envMap["TASK_CONTENT"]
				// wrapper present
				Expect(got).To(HavePrefix("---\n"))
				Expect(got).To(ContainSubstring("\n---\n# PR Review:"))
				// frontmatter keys propagated — these are exactly the fields the
				// pr-reviewer execution step reads
				Expect(
					got,
				).To(ContainSubstring("clone_url: https://github.com/bborbe/code-reviewer.git"))
				Expect(got).To(ContainSubstring("ref: f82244d6abcdef"))
				Expect(got).To(ContainSubstring("base_ref: master"))
				Expect(got).To(ContainSubstring("assignee: pr-reviewer"))
				// body preserved
				Expect(got).To(ContainSubstring("# PR Review:\n\nbody here"))
			},
		)

		It(
			"emits TASK_CONTENT that lib.ParseMarkdown round-trips back to the original frontmatter",
			func() {
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("roundtrip-task"),
					Frontmatter: lib.TaskFrontmatter{
						"assignee":  "pr-reviewer",
						"clone_url": "https://github.com/bborbe/code-reviewer.git",
						"ref":       "abc123",
						"base_ref":  "master",
					},
					Content: lib.TaskContent("# Body\n"),
				}
				config := pkg.AgentConfiguration{Assignee: "pr-reviewer", Image: "x:y"}
				_, err := jobSpawner.SpawnJob(ctx, task, config)
				Expect(err).To(BeNil())

				jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
				Expect(err).To(BeNil())
				container := jobs.Items[0].Spec.Template.Spec.Containers[0]
				var taskContent string
				for _, e := range container.Env {
					if e.Name == "TASK_CONTENT" {
						taskContent = e.Value
					}
				}
				Expect(taskContent).NotTo(BeEmpty())

				parsed, parseErr := lib.ParseMarkdown(ctx, taskContent)
				Expect(parseErr).NotTo(HaveOccurred())
				Expect(
					parsed.Frontmatter,
				).To(HaveKeyWithValue("clone_url", "https://github.com/bborbe/code-reviewer.git"))
				Expect(parsed.Frontmatter).To(HaveKeyWithValue("ref", "abc123"))
				Expect(parsed.Frontmatter).To(HaveKeyWithValue("base_ref", "master"))
				Expect(parsed.Frontmatter).To(HaveKeyWithValue("assignee", "pr-reviewer"))
			},
		)

		It("emits raw body without wrapper when frontmatter is empty", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("no-fm-task"),
				Frontmatter:    lib.TaskFrontmatter{}, // explicitly empty
				Content:        lib.TaskContent("just a body"),
			}
			config := pkg.AgentConfiguration{Assignee: "claude", Image: "x:y"}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			container := jobs.Items[0].Spec.Template.Spec.Containers[0]
			envMap := make(map[string]string)
			for _, e := range container.Env {
				envMap[e.Name] = e.Value
			}
			Expect(envMap["TASK_CONTENT"]).To(Equal("just a body"))
			Expect(envMap["TASK_CONTENT"]).NotTo(ContainSubstring("---"))
			Expect(envMap["TASK_CONTENT"]).NotTo(ContainSubstring("{}"))
		})

		It("injects empty PHASE when task frontmatter has no phase", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("no-phase-task"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
				},
				Content: lib.TaskContent("do the work"),
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude",
				Image:    "my-image:latest",
				Env:      map[string]string{},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			envMap := make(map[string]string)
			for _, e := range jobs.Items[0].Spec.Template.Spec.Containers[0].Env {
				envMap[e.Name] = e.Value
			}
			Expect(envMap).To(HaveKey("PHASE"))
			Expect(envMap["PHASE"]).To(Equal(""))
		})

		It("sets agent.benjamin-borbe.de/task-id label on spawned job", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("task-uuid-label-test"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude",
				Image:    "my-image:latest",
				Env:      map[string]string{},
			}
			jobName, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobName).NotTo(BeEmpty())

			jobs, listErr := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(listErr).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))
			Expect(
				jobs.Items[0].Labels,
			).To(HaveKeyWithValue("agent.benjamin-borbe.de/task-id", string(task.TaskIdentifier)))
			Expect(
				jobs.Items[0].Spec.Template.Labels,
			).To(HaveKeyWithValue("agent.benjamin-borbe.de/task-id", string(task.TaskIdentifier)))
		})

		It("includes all per-agent env vars from config", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc-multi-env"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "trade-analysis-agent",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee: "trade-analysis-agent",
				Image:    "registry/agent-trade-analysis:dev",
				Env: map[string]string{
					"ANTHROPIC_API_KEY": "test-anthropic-key",
					"EXTRA_VAR":         "extra-value",
				},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			container := jobs.Items[0].Spec.Template.Spec.Containers[0]
			envMap := make(map[string]string)
			for _, e := range container.Env {
				envMap[e.Name] = e.Value
			}
			Expect(envMap["ANTHROPIC_API_KEY"]).To(Equal("test-anthropic-key"))
			Expect(envMap["EXTRA_VAR"]).To(Equal("extra-value"))
		})

		It("uses assignee from frontmatter in job name", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abcdefghijklmnop"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "backtest-agent",
				},
			}
			_, err := jobSpawner.SpawnJob(
				ctx,
				task,
				pkg.AgentConfiguration{Image: "img:latest", Env: map[string]string{}},
			)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items[0].Name).To(Equal("backtest-agent-abcdefgh-20260403173500"))
		})

		It("falls back to 'agent' prefix when assignee is empty", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc"),
				Frontmatter:    lib.TaskFrontmatter{},
			}
			_, err := jobSpawner.SpawnJob(
				ctx,
				task,
				pkg.AgentConfiguration{Image: "img:latest", Env: map[string]string{}},
			)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items[0].Name).To(HavePrefix("agent-"))
			Expect(jobs.Items[0].Name).To(Equal("agent-abc-20260403173500"))
		})

		It("returns job name when job already exists (AlreadyExists)", func() {
			existingJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claude-abc12345-20260403173500",
					Namespace: "test-ns",
				},
			}
			fakeClient = fake.NewClientset(existingJob)
			jobSpawner = spawner.NewJobSpawner(
				fakeClient,
				"test-ns",
				"kafka:9092",
				"develop",
				currentDateTime,
				1800,
			)

			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc12345-rest-ignored"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
				},
			}
			jobName, err := jobSpawner.SpawnJob(
				ctx,
				task,
				pkg.AgentConfiguration{Image: "img:latest", Env: map[string]string{}},
			)
			Expect(err).To(BeNil())
			Expect(jobName).To(Equal("claude-abc12345-20260403173500"))
		})

		It("mounts PVC when VolumeClaim is set", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc-pvc"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee:        "claude",
				Image:           "my-image:latest",
				Env:             map[string]string{},
				VolumeClaim:     "agent-claude-pvc",
				VolumeMountPath: "/data",
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			job := jobs.Items[0]
			Expect(job.Spec.Template.Spec.Volumes).To(HaveLen(1))
			Expect(job.Spec.Template.Spec.Volumes[0].Name).To(Equal("agent-data"))
			Expect(job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim).NotTo(BeNil())
			Expect(
				job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName,
			).To(Equal("agent-claude-pvc"))

			container := job.Spec.Template.Spec.Containers[0]
			Expect(container.VolumeMounts).To(HaveLen(1))
			Expect(container.VolumeMounts[0].Name).To(Equal("agent-data"))
			Expect(container.VolumeMounts[0].MountPath).To(Equal("/data"))
		})

		It("has no volumes when VolumeClaim is empty", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc-no-pvc"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude",
				Image:    "my-image:latest",
				Env:      map[string]string{},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			job := jobs.Items[0]
			Expect(job.Spec.Template.Spec.Volumes).To(BeEmpty())
			Expect(job.Spec.Template.Spec.Containers[0].VolumeMounts).To(BeEmpty())
		})

		It("returns error when VolumeClaim is set but VolumeMountPath is empty", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc-bad-pvc"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee:    "claude",
				Image:       "my-image:latest",
				Env:         map[string]string{},
				VolumeClaim: "agent-claude-pvc",
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).NotTo(BeNil())
			Expect(
				err.Error(),
			).To(ContainSubstring("VolumeMountPath required when VolumeClaim is set"))
		})

		It("mounts secret as envFrom when SecretName is set", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc-secret"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "backtest-agent",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee:   "backtest-agent",
				Image:      "my-image:latest",
				Env:        map[string]string{},
				SecretName: "agent-backtest",
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			container := jobs.Items[0].Spec.Template.Spec.Containers[0]
			Expect(container.EnvFrom).To(HaveLen(1))
			Expect(container.EnvFrom[0].SecretRef).NotTo(BeNil())
			Expect(container.EnvFrom[0].SecretRef.Name).To(Equal("agent-backtest"))
		})

		It("uses custom ImagePullSecret when set", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc-custom-pull-secret"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee:        "claude",
				Image:           "my-image:latest",
				Env:             map[string]string{},
				ImagePullSecret: "my-custom-secret",
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			Expect(jobs.Items[0].Spec.Template.Spec.ImagePullSecrets).To(HaveLen(1))
			Expect(
				jobs.Items[0].Spec.Template.Spec.ImagePullSecrets[0].Name,
			).To(Equal("my-custom-secret"))
		})

		It("defaults to 'docker' when ImagePullSecret is empty", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc-default-pull-secret"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee:        "claude",
				Image:           "my-image:latest",
				Env:             map[string]string{},
				ImagePullSecret: "",
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			Expect(jobs.Items[0].Spec.Template.Spec.ImagePullSecrets).To(HaveLen(1))
			Expect(jobs.Items[0].Spec.Template.Spec.ImagePullSecrets[0].Name).To(Equal("docker"))
		})

		It("has no envFrom when SecretName is empty", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc-no-secret"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude",
				Image:    "my-image:latest",
				Env:      map[string]string{},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			container := jobs.Items[0].Spec.Template.Spec.Containers[0]
			Expect(container.EnvFrom).To(BeEmpty())
		})

		It("applies resource requests and limits from config.Resources", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc-resources"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude",
				Image:    "my-image:latest",
				Env:      map[string]string{},
				Resources: &agentv1.AgentResources{
					Requests: agentv1.AgentResourceList{
						CPU:              "500m",
						Memory:           "1Gi",
						EphemeralStorage: "2Gi",
					},
					Limits: agentv1.AgentResourceList{
						CPU:              "1",
						Memory:           "2Gi",
						EphemeralStorage: "4Gi",
					},
				},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			container := jobs.Items[0].Spec.Template.Spec.Containers[0]
			Expect(container.Resources.Requests.Cpu().String()).To(Equal("500m"))
			Expect(container.Resources.Limits.Cpu().String()).To(Equal("1"))
			Expect(container.Resources.Requests.Memory().String()).To(Equal("1Gi"))
			Expect(container.Resources.Limits.Memory().String()).To(Equal("2Gi"))
			Expect(container.Resources.Requests[corev1.ResourceEphemeralStorage]).
				To(Equal(resource.MustParse("2Gi")))
			Expect(container.Resources.Limits[corev1.ResourceEphemeralStorage]).
				To(Equal(resource.MustParse("4Gi")))
		})

		It("uses k8s builder defaults when Resources is nil", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc-nil-resources"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee:  "claude",
				Image:     "my-image:latest",
				Env:       map[string]string{},
				Resources: nil,
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			container := jobs.Items[0].Spec.Template.Spec.Containers[0]
			Expect(container.Resources.Limits.Cpu().String()).To(Equal("50m"))
			Expect(container.Resources.Requests.Cpu().String()).To(Equal("20m"))
			Expect(container.Resources.Limits.Memory().String()).To(Equal("50Mi"))
			Expect(container.Resources.Requests.Memory().String()).To(Equal("20Mi"))
			_, hasEphReq := container.Resources.Requests[corev1.ResourceEphemeralStorage]
			Expect(hasEphReq).To(BeFalse())
			_, hasEphLim := container.Resources.Limits[corev1.ResourceEphemeralStorage]
			Expect(hasEphLim).To(BeFalse())
		})

		It("leaves CPU limit at builder default when only Requests.CPU is set", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc-one-sided"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude",
				Image:    "my-image:latest",
				Env:      map[string]string{},
				Resources: &agentv1.AgentResources{
					Requests: agentv1.AgentResourceList{CPU: "500m"},
				},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			container := jobs.Items[0].Spec.Template.Spec.Containers[0]
			Expect(container.Resources.Requests.Cpu().String()).To(Equal("500m"))
			Expect(container.Resources.Limits.Cpu().String()).To(Equal("50m"))
		})

		It("stamps priorityClassName on the spawned Job when config has it set", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-uuid-1234"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude-agent",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee:          "claude-agent",
				Image:             "example/image:latest",
				PriorityClassName: "agent-claude",
			}
			jobName, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())
			Expect(jobName).NotTo(BeEmpty())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))
			Expect(jobs.Items[0].Spec.Template.Spec.PriorityClassName).To(Equal("agent-claude"))
		})

		It("omits priorityClassName from the spawned Job when config has none", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-uuid-5678"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude-agent",
				},
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude-agent",
				Image:    "example/image:latest",
			}
			jobName, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())
			Expect(jobName).NotTo(BeEmpty())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))
			Expect(jobs.Items[0].Spec.Template.Spec.PriorityClassName).To(BeEmpty())
		})

		It("includes TASK_TYPE env var matching the frontmatter task_type value", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("healthcheck-task-id"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee":  "claude",
					"phase":     "planning",
					"task_type": "healthcheck",
				},
				Content: lib.TaskContent("run health probe"),
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude",
				Image:    "claude-agent:latest",
				Env:      map[string]string{},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			envMap := make(map[string]string)
			for _, e := range jobs.Items[0].Spec.Template.Spec.Containers[0].Env {
				envMap[e.Name] = e.Value
			}
			Expect(envMap["TASK_TYPE"]).To(Equal("healthcheck"))
		})

		It("sets TASK_TYPE to empty string when task_type is absent from frontmatter", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("no-type-task-id"),
				Frontmatter: lib.TaskFrontmatter{
					"assignee": "claude",
					"phase":    "planning",
				},
				Content: lib.TaskContent("work without task type"),
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude",
				Image:    "claude-agent:latest",
				Env:      map[string]string{},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(ctx, metav1.ListOptions{})
			Expect(err).To(BeNil())
			Expect(jobs.Items).To(HaveLen(1))

			envMap := make(map[string]string)
			for _, e := range jobs.Items[0].Spec.Template.Spec.Containers[0].Env {
				envMap[e.Name] = e.Value
			}
			Expect(envMap["TASK_TYPE"]).To(Equal(""))
		})

		It("returns error on unexpected K8s error", func() {
			fakeClient.PrependReactor(
				"create",
				"jobs",
				func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, k8serrors.NewInternalError(fmt.Errorf("server error"))
				},
			)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("abc12345"),
			}
			_, err := jobSpawner.SpawnJob(
				ctx,
				task,
				pkg.AgentConfiguration{Image: "img:latest", Env: map[string]string{}},
			)
			Expect(err).NotTo(BeNil())
		})
	})

	Describe("IsJobActive", func() {
		It("returns false when no jobs exist", func() {
			active, err := jobSpawner.IsJobActive(ctx, lib.TaskIdentifier("tid-1"))
			Expect(err).To(BeNil())
			Expect(active).To(BeFalse())
		})

		It("returns true when active job exists (status.active > 0)", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claude-20260403173500",
					Namespace: "test-ns",
					Labels:    map[string]string{"agent.benjamin-borbe.de/task-id": "tid-2"},
				},
				Status: batchv1.JobStatus{
					Active: 1,
				},
			}
			fakeClient = fake.NewClientset(job)
			jobSpawner = spawner.NewJobSpawner(
				fakeClient,
				"test-ns",
				"kafka:9092",
				"develop",
				currentDateTime,
				1800,
			)

			active, err := jobSpawner.IsJobActive(ctx, lib.TaskIdentifier("tid-2"))
			Expect(err).To(BeNil())
			Expect(active).To(BeTrue())
		})

		It("returns false when completed job exists (status.succeeded > 0)", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claude-20260403173500",
					Namespace: "test-ns",
					Labels:    map[string]string{"agent.benjamin-borbe.de/task-id": "tid-3"},
				},
				Status: batchv1.JobStatus{
					Succeeded: 1,
				},
			}
			fakeClient = fake.NewClientset(job)
			jobSpawner = spawner.NewJobSpawner(
				fakeClient,
				"test-ns",
				"kafka:9092",
				"develop",
				currentDateTime,
				1800,
			)

			active, err := jobSpawner.IsJobActive(ctx, lib.TaskIdentifier("tid-3"))
			Expect(err).To(BeNil())
			Expect(active).To(BeFalse())
		})

		It("returns false when failed job exists (status.failed > 0, active == 0)", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claude-20260403173500",
					Namespace: "test-ns",
					Labels:    map[string]string{"agent.benjamin-borbe.de/task-id": "tid-4"},
				},
				Status: batchv1.JobStatus{
					Failed: 1,
					Active: 0,
				},
			}
			fakeClient = fake.NewClientset(job)
			jobSpawner = spawner.NewJobSpawner(
				fakeClient,
				"test-ns",
				"kafka:9092",
				"develop",
				currentDateTime,
				1800,
			)

			active, err := jobSpawner.IsJobActive(ctx, lib.TaskIdentifier("tid-4"))
			Expect(err).To(BeNil())
			Expect(active).To(BeFalse())
		})

		It("returns true for newly created job (no status set yet)", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claude-20260403173500",
					Namespace: "test-ns",
					Labels:    map[string]string{"agent.benjamin-borbe.de/task-id": "tid-5"},
				},
				Status: batchv1.JobStatus{},
			}
			fakeClient = fake.NewClientset(job)
			jobSpawner = spawner.NewJobSpawner(
				fakeClient,
				"test-ns",
				"kafka:9092",
				"develop",
				currentDateTime,
				1800,
			)

			active, err := jobSpawner.IsJobActive(ctx, lib.TaskIdentifier("tid-5"))
			Expect(err).To(BeNil())
			Expect(active).To(BeTrue())
		})

		It("returns error when K8s List call fails", func() {
			fakeClient.PrependReactor(
				"list",
				"jobs",
				func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, k8serrors.NewInternalError(fmt.Errorf("server error"))
				},
			)
			jobSpawner = spawner.NewJobSpawner(
				fakeClient,
				"test-ns",
				"kafka:9092",
				"develop",
				currentDateTime,
				1800,
			)
			active, err := jobSpawner.IsJobActive(ctx, lib.TaskIdentifier("tid-list-err"))
			Expect(err).NotTo(BeNil())
			Expect(active).To(BeFalse())
		})
	})

	Describe("applyTaskIDLabel", func() {
		It("creates new Labels map when job.Labels is nil (via Create reactor)", func() {
			// Use a Create reactor to capture the job at the moment of creation.
			// applyTaskIDLabel is called BEFORE the K8s Create call, so the
			// captured job will have the labels already set.
			var capturedJob *batchv1.Job
			fakeClient.PrependReactor(
				"create",
				"jobs",
				func(action k8stesting.Action) (bool, runtime.Object, error) {
					createAction, ok := action.(k8stesting.CreateAction)
					if !ok {
						return false, nil, nil
					}
					capturedJob, ok = createAction.GetObject().(*batchv1.Job)
					if !ok {
						return false, nil, nil
					}
					return false, nil, nil // pass through to fake client
				},
			)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("nil-labels-task"),
				Frontmatter:    lib.TaskFrontmatter{"assignee": "claude"},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, pkg.AgentConfiguration{
				Assignee: "claude", Image: "my-image:latest",
			})
			Expect(err).To(BeNil())
			Expect(capturedJob).NotTo(BeNil())
			Expect(capturedJob.Labels).NotTo(BeNil())
			Expect(
				capturedJob.Labels["agent.benjamin-borbe.de/task-id"],
			).To(Equal("nil-labels-task"))
			Expect(capturedJob.Spec.Template.Labels).NotTo(BeNil())
			Expect(
				capturedJob.Spec.Template.Labels["agent.benjamin-borbe.de/task-id"],
			).To(Equal("nil-labels-task"))
		})
	})

	Describe("ActiveDeadlineSeconds", func() {
		ptrInt32 := func(v int32) *int32 { return &v }

		It("stamps ActiveDeadlineSeconds from config", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("deadline-task"),
				Frontmatter:    lib.TaskFrontmatter{"assignee": "claude"},
				Content:        lib.TaskContent("do the work"),
			}
			config := pkg.AgentConfiguration{
				Assignee:                "claude",
				Image:                   "my-image:latest",
				ZombieJobTimeoutSeconds: ptrInt32(900),
			}
			jobName, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())
			Expect(jobName).NotTo(BeEmpty())

			job, err := fakeClient.BatchV1().Jobs("test-ns").Get(ctx, jobName, metav1.GetOptions{})
			Expect(err).To(BeNil())
			Expect(job.Spec.ActiveDeadlineSeconds).NotTo(BeNil())
			Expect(*job.Spec.ActiveDeadlineSeconds).To(Equal(int64(900)))
		})

		It("uses default ActiveDeadlineSeconds when config is unset", func() {
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("default-deadline-task"),
				Frontmatter:    lib.TaskFrontmatter{"assignee": "claude"},
				Content:        lib.TaskContent("do the work"),
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude",
				Image:    "my-image:latest",
			}
			jobName, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())
			Expect(jobName).NotTo(BeEmpty())

			job, err := fakeClient.BatchV1().Jobs("test-ns").Get(ctx, jobName, metav1.GetOptions{})
			Expect(err).To(BeNil())
			Expect(job.Spec.ActiveDeadlineSeconds).NotTo(BeNil())
			Expect(*job.Spec.ActiveDeadlineSeconds).To(Equal(int64(1800)))
		})
	})

	// Regression guard: SpawnJob and IsJobActive must agree on the label key used
	// to identify a Job. A mismatch causes the executor to treat a freshly-spawned
	// Job as "no active job" and respawn another every poll cycle.
	Describe("SpawnJob + IsJobActive label contract", func() {
		It("IsJobActive returns true for a Job just spawned via SpawnJob (same taskID)", func() {
			taskID := lib.TaskIdentifier("e2e-tid-contract-1")
			task := lib.Task{
				TaskIdentifier: taskID,
				Frontmatter:    lib.TaskFrontmatter{"assignee": "claude"},
				Content:        lib.TaskContent("do the work"),
			}
			config := pkg.AgentConfiguration{
				Assignee: "claude",
				Image:    "my-image:latest",
			}

			_, err := jobSpawner.SpawnJob(ctx, task, config)
			Expect(err).To(BeNil())

			active, err := jobSpawner.IsJobActive(ctx, taskID)
			Expect(err).To(BeNil())
			Expect(active).
				To(BeTrue(), "IsJobActive must recognise the Job that SpawnJob just created")
		})

		It("IsJobActive returns false for a different taskID than the one spawned", func() {
			spawned := lib.TaskIdentifier("e2e-tid-contract-2")
			other := lib.TaskIdentifier("e2e-tid-contract-other")
			task := lib.Task{
				TaskIdentifier: spawned,
				Frontmatter:    lib.TaskFrontmatter{"assignee": "claude"},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, pkg.AgentConfiguration{
				Assignee: "claude", Image: "my-image:latest",
			})
			Expect(err).To(BeNil())

			active, err := jobSpawner.IsJobActive(ctx, other)
			Expect(err).To(BeNil())
			Expect(active).
				To(BeFalse(), "IsJobActive must not match a different task's spawned Job")
		})
	})

	Describe("applyTaskIDLabel", func() {
		It("creates new Labels map when job.Labels is nil (via Create reactor)", func() {
			// Use a Create reactor to capture the job at the moment of creation.
			// applyTaskIDLabel is called BEFORE the K8s Create call, so the
			// captured job will have the labels already set.
			var capturedJob *batchv1.Job
			fakeClient.PrependReactor(
				"create",
				"jobs",
				func(action k8stesting.Action) (bool, runtime.Object, error) {
					createAction, ok := action.(k8stesting.CreateAction)
					if !ok {
						return false, nil, nil
					}
					capturedJob, ok = createAction.GetObject().(*batchv1.Job)
					if !ok {
						return false, nil, nil
					}
					return false, nil, nil // pass through to fake client
				},
			)
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("nil-labels-task"),
				Frontmatter:    lib.TaskFrontmatter{"assignee": "claude"},
			}
			_, err := jobSpawner.SpawnJob(ctx, task, pkg.AgentConfiguration{
				Assignee: "claude", Image: "my-image:latest",
			})
			Expect(err).To(BeNil())
			Expect(capturedJob).NotTo(BeNil())
			Expect(capturedJob.Labels).NotTo(BeNil())
			Expect(
				capturedJob.Labels["agent.benjamin-borbe.de/task-id"],
			).To(Equal("nil-labels-task"))
			Expect(capturedJob.Spec.Template.Labels).NotTo(BeNil())
			Expect(
				capturedJob.Spec.Template.Labels["agent.benjamin-borbe.de/task-id"],
			).To(Equal("nil-labels-task"))
		})
	})
})
