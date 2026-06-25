// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner

import (
	"context"

	"github.com/bborbe/errors"
	k8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
	"gopkg.in/yaml.v3"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	lib "github.com/bborbe/agent/lib"
	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// taskIDLabelKey labels a spawned Job with the task UUID so IsJobActive
// can look it up. Must match the selector used in IsJobActive.
const taskIDLabelKey = "agent.benjamin-borbe.de/task-id"

//counterfeiter:generate -o ../../mocks/job_spawner.go --fake-name FakeJobSpawner . JobSpawner

// JobSpawner creates a K8s Job for a task.
type JobSpawner interface {
	// SpawnJob creates a K8s Job for the task and returns the job name.
	// Returns the job name even when the job already exists (idempotent).
	SpawnJob(ctx context.Context, task lib.Task, config pkg.AgentConfiguration) (string, error)
	// IsJobActive returns true if an active (not completed/failed) K8s Job exists
	// for the given task identifier. Uses the agent.benjamin-borbe.de/task-id
	// label set by SpawnJob.
	IsJobActive(ctx context.Context, taskIdentifier lib.TaskIdentifier) (bool, error)
}

// NewJobSpawner creates a new JobSpawner backed by the K8s batch/v1 API.
// jobTTLSecondsAfterFinished controls how long completed Job pods survive
// before Kubernetes' TTL controller garbage-collects them.
func NewJobSpawner(
	kubeClient kubernetes.Interface,
	namespace k8s.Namespace,
	kafkaBrokers string,
	branch string,
	currentDateTimeGetter libtime.CurrentDateTimeGetter,
	jobTTLSecondsAfterFinished int32,
) JobSpawner {
	return &jobSpawner{
		kubeClient:                 kubeClient,
		namespace:                  namespace,
		kafkaBrokers:               kafkaBrokers,
		branch:                     branch,
		currentDateTimeGetter:      currentDateTimeGetter,
		jobTTLSecondsAfterFinished: jobTTLSecondsAfterFinished,
	}
}

// jobSpawner implements JobSpawner by creating batch/v1 Jobs via the K8s client.
type jobSpawner struct {
	kubeClient                 kubernetes.Interface
	namespace                  k8s.Namespace
	kafkaBrokers               string
	branch                     string
	currentDateTimeGetter      libtime.CurrentDateTimeGetter
	jobTTLSecondsAfterFinished int32
}

func (s *jobSpawner) SpawnJob(
	ctx context.Context,
	task lib.Task,
	config pkg.AgentConfiguration,
) (string, error) {
	assignee := task.Frontmatter.Assignee().String()
	now := s.currentDateTimeGetter.Now()
	jobName := jobNameFromTask(assignee, task.TaskIdentifier, now)

	envBuilder, err := s.buildJobEnvBuilder(ctx, task, config)
	if err != nil {
		return "", err
	}

	containerBuilder := k8s.NewContainerBuilder()
	containerBuilder.SetName(k8s.Name("agent"))
	containerBuilder.SetImage(config.Image)
	containerBuilder.SetEnvBuilder(envBuilder)
	applyCPUMemoryResources(config, containerBuilder)

	podSpecBuilder := k8s.NewPodSpecBuilder()

	if err := applyVolumeMount(ctx, config, containerBuilder, podSpecBuilder); err != nil {
		return "", err
	}

	containersBuilder := k8s.NewContainersBuilder()
	containersBuilder.SetContainerBuilders([]k8s.HasBuildContainer{containerBuilder})

	podSpecBuilder.SetContainersBuilder(containersBuilder)
	podSpecBuilder.SetRestartPolicy(corev1.RestartPolicyNever)
	podSpecBuilder.SetImagePullSecrets([]string{imagePullSecretName(config)})

	objectMetaBuilder := k8s.NewObjectMetaBuilder()
	objectMetaBuilder.SetName(k8s.Name(jobName))
	objectMetaBuilder.SetNamespace(s.namespace)

	jobBuilder := k8s.NewJobBuilder()
	jobBuilder.SetObjectMetaBuild(objectMetaBuilder)
	jobBuilder.SetPodSpecBuilder(podSpecBuilder)
	jobBuilder.SetBackoffLimit(0)
	jobBuilder.SetTTLSecondsAfterFinished(s.jobTTLSecondsAfterFinished)
	jobBuilder.SetApp("agent")
	jobBuilder.SetComponent(string(task.TaskIdentifier))

	job, err := jobBuilder.Build(ctx)
	if err != nil {
		return "", errors.Wrapf(ctx, err, "build job for task %s", task.TaskIdentifier)
	}

	applyTaskIDLabel(task.TaskIdentifier, job)
	applySecretEnvFrom(config, job)
	applyEphemeralStorage(config, job)
	if config.PriorityClassName != "" {
		job.Spec.Template.Spec.PriorityClassName = config.PriorityClassName
	}
	applyActiveDeadlineSeconds(config, jobName, task.TaskIdentifier, job)

	_, err = s.kubeClient.BatchV1().
		Jobs(s.namespace.String()).
		Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			glog.V(2).
				Infof("job %s already exists for task %s, treating as success", jobName, task.TaskIdentifier)
			return jobName, nil
		}
		return "", errors.Wrapf(
			ctx,
			err,
			"create job %s for task %s failed",
			jobName,
			task.TaskIdentifier,
		)
	}
	glog.V(2).
		Infof("created job %s for task %s with image %s", jobName, task.TaskIdentifier, config.Image)
	return jobName, nil
}

func (s *jobSpawner) IsJobActive(
	ctx context.Context,
	taskIdentifier lib.TaskIdentifier,
) (bool, error) {
	labelSelector := taskIDLabelKey + "=" + string(taskIdentifier)
	jobs, err := s.kubeClient.BatchV1().Jobs(s.namespace.String()).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return false, errors.Wrapf(ctx, err, "list jobs for task %s", taskIdentifier)
	}
	for _, job := range jobs.Items {
		if job.Status.Succeeded > 0 {
			continue
		}
		if job.Status.Failed > 0 && job.Status.Active == 0 {
			continue
		}
		return true, nil
	}
	return false, nil
}

// applyVolumeMount configures a PVC volume mount on the container and pod spec builders
// when config.VolumeClaim is non-empty. Returns an error if VolumeMountPath is missing.
func applyVolumeMount(
	ctx context.Context,
	config pkg.AgentConfiguration,
	containerBuilder k8s.ContainerBuilder,
	podSpecBuilder k8s.PodSpecBuilder,
) error {
	if config.VolumeClaim == "" {
		return nil
	}
	if config.VolumeMountPath == "" {
		return errors.Errorf(ctx, "VolumeMountPath required when VolumeClaim is set")
	}
	containerBuilder.AddVolumeMounts(corev1.VolumeMount{
		Name:      "agent-data",
		MountPath: config.VolumeMountPath,
	})
	podSpecBuilder.SetVolumes([]corev1.Volume{
		{
			Name: "agent-data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: config.VolumeClaim,
				},
			},
		},
	})
	return nil
}

// imagePullSecretName returns the image pull secret name from the config,
// falling back to "docker" when not set.
func imagePullSecretName(config pkg.AgentConfiguration) string {
	if config.ImagePullSecret != "" {
		return config.ImagePullSecret
	}
	return "docker"
}

// applyCPUMemoryResources sets CPU and memory requests/limits on the container builder
// when the corresponding config values are non-empty. Empty values leave builder defaults untouched.
func applyCPUMemoryResources(config pkg.AgentConfiguration, containerBuilder k8s.ContainerBuilder) {
	if config.Resources == nil {
		return
	}
	if v := config.Resources.Requests.CPU; v != "" {
		containerBuilder.SetCpuRequest(v)
	}
	if v := config.Resources.Limits.CPU; v != "" {
		containerBuilder.SetCpuLimit(v)
	}
	if v := config.Resources.Requests.Memory; v != "" {
		containerBuilder.SetMemoryRequest(v)
	}
	if v := config.Resources.Limits.Memory; v != "" {
		containerBuilder.SetMemoryLimit(v)
	}
}

// applySecretEnvFrom appends an envFrom secretRef to the first container of the job
// when config.SecretName is non-empty.
func applySecretEnvFrom(config pkg.AgentConfiguration, job *batchv1.Job) {
	if config.SecretName == "" {
		return
	}
	job.Spec.Template.Spec.Containers[0].EnvFrom = append(
		job.Spec.Template.Spec.Containers[0].EnvFrom,
		corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: config.SecretName,
				},
			},
		},
	)
}

// applyEphemeralStorage sets ephemeral-storage as Requests and/or Limits on the
// first container of the job based on config.Resources.
// Each value is applied independently — empty means "leave unset".
// The bborbe/k8s container builder does not expose setters for ephemeral-storage,
// so we patch the built job directly.
func applyEphemeralStorage(config pkg.AgentConfiguration, job *batchv1.Job) {
	if config.Resources == nil {
		return
	}
	c := &job.Spec.Template.Spec.Containers[0]
	if v := config.Resources.Requests.EphemeralStorage; v != "" {
		if c.Resources.Requests == nil {
			c.Resources.Requests = corev1.ResourceList{}
		}
		c.Resources.Requests[corev1.ResourceEphemeralStorage] = resource.MustParse(v)
	}
	if v := config.Resources.Limits.EphemeralStorage; v != "" {
		if c.Resources.Limits == nil {
			c.Resources.Limits = corev1.ResourceList{}
		}
		c.Resources.Limits[corev1.ResourceEphemeralStorage] = resource.MustParse(v)
	}
}

// applyTaskIDLabel sets the agent.benjamin-borbe.de/task-id label on the Job and its pod template
// so the job informer can look up the owning task by label selector.
func applyTaskIDLabel(taskID lib.TaskIdentifier, job *batchv1.Job) {
	if job.Labels == nil {
		job.Labels = map[string]string{}
	}
	job.Labels[taskIDLabelKey] = string(taskID)
	if job.Spec.Template.Labels == nil {
		job.Spec.Template.Labels = map[string]string{}
	}
	job.Spec.Template.Labels[taskIDLabelKey] = string(taskID)
}

// applyActiveDeadlineSeconds stamps Job.Spec.ActiveDeadlineSeconds from the config's
// effective zombie job timeout so Kubernetes enforces a hard deadline on every spawned Job.
func applyActiveDeadlineSeconds(
	config pkg.AgentConfiguration,
	jobName string,
	taskID lib.TaskIdentifier,
	job *batchv1.Job,
) {
	deadline := int64(config.EffectiveZombieJobTimeoutSeconds())
	job.Spec.ActiveDeadlineSeconds = &deadline
	glog.V(2).Infof("set activeDeadlineSeconds=%d on job %s for task %s", deadline, jobName, taskID)
}

// taskPhaseString returns the string value of the task's phase, or "" when absent.
func taskPhaseString(f lib.TaskFrontmatter) string {
	if p := f.Phase(); p != nil {
		return string(*p)
	}
	return ""
}

// taskTypeString returns the string value of the task's task_type, or "" when absent.
func taskTypeString(f lib.TaskFrontmatter) string {
	return f.TaskType().String()
}

// jobNameFromTask returns the K8s Job name for a task: "{assignee}-{taskID8}-{YYYYMMDDHHMMSS}".
// taskID8 is the first 8 chars of the task UUID, included to prevent name collisions
// between concurrent spawns of different tasks sharing the same assignee and second.
// If assignee is empty, "agent" is used as the default prefix.
// Job names are DNS-compliant (<=63 chars, [a-z0-9]([-a-z0-9]*[a-z0-9])?).
// Assignees should be short lowercase strings (e.g. "claude", "backtest-agent").
func jobNameFromTask(assignee string, taskID lib.TaskIdentifier, now libtime.DateTime) string {
	if assignee == "" {
		assignee = "agent"
	}
	id := string(taskID)
	if len(id) > 8 {
		id = id[:8]
	}
	return assignee + "-" + id + "-" + now.UTC().Format("20060102150405")
}

// buildJobEnvBuilder constructs the env var set for the spawned Job's container.
// It renders the full markdown for TASK_CONTENT so agents can parse frontmatter
// fields like clone_url, ref, and base_ref.
func (s *jobSpawner) buildJobEnvBuilder(
	ctx context.Context,
	task lib.Task,
	config pkg.AgentConfiguration,
) (k8s.EnvBuilder, error) {
	taskContent, err := renderTaskContent(ctx, task)
	if err != nil {
		return nil, err
	}
	envBuilder := k8s.NewEnvBuilder()
	envBuilder.Add("TASK_CONTENT", taskContent)
	envBuilder.Add("TASK_ID", string(task.TaskIdentifier))
	envBuilder.Add("KAFKA_BROKERS", s.kafkaBrokers)
	envBuilder.Add("BRANCH", s.branch)
	envBuilder.Add("PHASE", taskPhaseString(task.Frontmatter))
	envBuilder.Add("TASK_TYPE", taskTypeString(task.Frontmatter))
	for key, value := range config.Env {
		envBuilder.Add(key, value)
	}
	return envBuilder, nil
}

// renderTaskContent serializes task into the markdown form an agent expects:
// "---\n<yaml-frontmatter>---\n<body>". When task.Frontmatter is empty, the
// body is returned unchanged (no empty "{}" wrapper).
//
// The agent side (lib.ParseMarkdown) reads frontmatter fields like
// clone_url / ref / base_ref directly from this string — keep the wrapper
// shape byte-compatible with controller/pkg/result.WriteResult.
func renderTaskContent(ctx context.Context, task lib.Task) (string, error) {
	if len(task.Frontmatter) == 0 {
		return string(task.Content), nil
	}
	fmBytes, err := yaml.Marshal(map[string]any(task.Frontmatter))
	if err != nil {
		return "", errors.Wrapf(ctx, err, "marshal frontmatter for task %s", task.TaskIdentifier)
	}
	return "---\n" + string(fmBytes) + "---\n" + string(task.Content), nil
}
