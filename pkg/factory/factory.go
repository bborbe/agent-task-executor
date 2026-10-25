// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package factory

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	cdb "github.com/bborbe/cqrs/cdb"
	libcron "github.com/bborbe/cron"
	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/log"
	"github.com/bborbe/run"
	libtime "github.com/bborbe/time"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/probe"
	"github.com/bborbe/agent-task-executor/pkg/spawner"
)

// CreateServiceReconciler returns a ServiceReconciler that deploys the StatefulSet
// for a service Config through the given clientset.
func CreateServiceReconciler(
	kubeClient kubernetes.Interface,
	namespace libk8s.Namespace,
	storageClass string,
) spawner.ServiceReconciler {
	return spawner.NewServiceReconciler(
		libk8s.NewStatefulSetDeployer(kubeClient),
		kubeClient.AppsV1().StatefulSets(namespace.String()),
		namespace,
		storageClass,
	)
}

// CreateServiceReconcileLoop returns the loop that keeps one StatefulSet per
// service Config in sync. An interval of 0 uses the loop's own default.
func CreateServiceReconcileLoop(
	provider libk8s.Provider[agentv1.Config],
	resolver pkg.ConfigResolver,
	reconciler spawner.ServiceReconciler,
	interval time.Duration,
) spawner.ServiceReconcileLoop {
	return spawner.NewServiceReconcileLoop(provider, resolver, reconciler, interval)
}

// CreateJobWatcher creates a JobWatcher that reacts to terminal batch/v1 Job
// states in the executor's namespace and in every namespace a Config names.
func CreateJobWatcher(
	kubeClient kubernetes.Interface,
	namespace libk8s.Namespace,
	configProvider libk8s.Provider[agentv1.Config],
	taskStore *pkg.TaskStore,
	publisher pkg.ResultPublisher,
	claimReleaser pkg.ClaimReleaser,
) pkg.JobWatcher {
	return pkg.NewJobWatcher(
		kubeClient,
		namespace,
		configProvider,
		taskStore,
		publisher,
		claimReleaser,
	)
}

// CreateClaimLifecycleRunFuncs returns the run funcs that keep the lease
// manager's claims alive: startup recovery re-adopts the claims this cluster
// held before a restart, then the loop renews a held claim while its Job runs,
// fences a claim whose renewals fail, and retries a release an end point could
// not deliver. Whether a manager exists at all is decided at boot in main, so
// this factory only wires the manager it is given.
//
// Every func returned here MUST block until ctx is done. service.Run wraps them
// in run.CancelOnFirstFinishWait, which cancels the whole service as soon as any
// one of them returns — so a one-shot step that returns once its work is done
// takes the lease loop, the HTTP server and the cron down with it. v0.30.2
// shipped exactly that and crash-looped all three clusters with a clean exit 0.
func CreateClaimLifecycleRunFuncs(
	manager claim.LeaseManager,
	viewReady func() bool,
) []run.Func {
	return []run.Func{CreateClaimRecoverFunc(manager, viewReady), manager.Run}
}

// CreateClaimRecoverFunc returns the startup step that re-adopts the claims this
// cluster held before a restart. Without it a replaced pod starts with an empty
// held set: nothing renews and nothing fences the Jobs it was running, so their
// leases lapse, another cluster takes the work over, and the original Job keeps
// running to its natural end — the duplicate this whole mechanism exists to
// prevent. Every rollout replaces a pod, so this is not a rare path.
//
// The retry policy, the never-fail-the-boot rule and the wait that keeps the run
// group alive all live in claim.RunClaimRecovery; this factory only wires it.
func CreateClaimRecoverFunc(
	manager claim.LeaseManager,
	viewReady func() bool,
) run.Func {
	return func(ctx context.Context) error {
		return claim.RunClaimRecovery(ctx, manager, viewReady)
	}
}

// CreateZombieSweeper creates a deadline sweeper that classifies stuck tasks as
// zombies and emits failure events via the publisher. Interval and per-task
// deadline are sourced from the AgentConfig CRD knobs (see ConfigSpec). The
// sweeper receives the JobWatcher (not its lister) because the lister is
// populated only after JobWatcher.Run completes its informer cache sync; passing
// the watcher lets the sweeper resolve the lister lazily on each tick and skip
// the tick if cache sync has not yet happened (avoids a nil-deref panic at the
// first tick when service.Run starts all components concurrently).
func CreateZombieSweeper(
	jobWatcher pkg.JobWatcher,
	namespace libk8s.Namespace,
	taskStore *pkg.TaskStore,
	publisher pkg.ResultPublisher,
	configProvider pkg.EventHandlerConfig,
	currentDateTime libtime.CurrentDateTimeGetter,
) pkg.ZombieSweeper {
	return pkg.NewZombieSweeper(
		jobWatcher,
		namespace,
		taskStore,
		publisher,
		configProvider,
		currentDateTime,
	)
}

// CreateK8sConnector returns a K8sConnector wired to the given rest.Config.
func CreateK8sConnector(config *rest.Config) pkg.K8sConnector {
	return pkg.NewK8sConnector(config, pkg.DefaultCRDClientBuilder)
}

// CreateEventHandlerConfig returns an empty in-memory event handler for Config resources.
func CreateEventHandlerConfig() pkg.EventHandlerConfig {
	return pkg.NewEventHandlerConfig()
}

// CreateResourceEventHandlerConfig adapts an EventHandlerConfig to cache.ResourceEventHandler.
func CreateResourceEventHandlerConfig(
	ctx context.Context,
	handler pkg.EventHandlerConfig,
) cache.ResourceEventHandler {
	return pkg.NewResourceEventHandlerConfig(ctx, handler)
}

// CreateConfigResolver returns a ConfigResolver backed by the given store,
// scoped to the given vault so Config CRs resolve by {assignee}-{vaultName}.
func CreateConfigResolver(
	handler pkg.EventHandlerConfig,
	branch base.Branch,
	vaultName string,
) pkg.ConfigResolver {
	return pkg.NewConfigResolver(handler, branch, vaultName)
}

// CreateClaimRefClientFactory returns the lazily-invoked constructor for the
// git-rest claim ref client. The initiator is the executor's own identity, so
// git-rest's auth-failure logs name this component.
func CreateClaimRefClientFactory(
	baseURL, gatewaySecret string,
) pkg.ClaimRefClientFactory {
	return func() claim.ClaimRefClient {
		return claim.NewClaimRefClient(baseURL, gatewaySecret, "agent-task-executor")
	}
}

// CreateConsumer wires together all components and returns a Kafka Consumer that
// reads task events and spawns K8s Jobs for qualifying tasks, along with the
// TaskEventHandler so callers can wire RunDeferredRespawnLoop.
func CreateConsumer(
	saramaClient sarama.Client,
	branch base.Branch,
	cluster string,
	stageFilter bool,
	runUntagged bool,
	consumerGroup string,
	topicPrefix base.TopicPrefix,
	kubeClient kubernetes.Interface,
	namespace libk8s.Namespace,
	kafkaBrokers string,
	resolver pkg.ConfigResolver,
	logSamplerFactory log.SamplerFactory,
	currentDateTimeGetter libtime.CurrentDateTimeGetter,
	resultPublisher pkg.ResultPublisher,
	taskStore *pkg.TaskStore,
	jobTTLSecondsAfterFinished int32,
	jobKafkaClientCertSecret string,
	jobKafkaCaCertSecret string,
	jobFsGroup int64,
	view handler.TaskView,
	taskVault string,
	vaultAliases []string,
	reconcilePassTimeout time.Duration,
	reconcileInterval time.Duration,
	claimMode pkg.ClaimMode,
	claimRefClientFactory pkg.ClaimRefClientFactory,
) (libkafka.Consumer, handler.ClaimLifecycleEventHandler) {
	jobSpawner := spawner.NewJobSpawner(
		kubeClient,
		namespace,
		kafkaBrokers,
		string(branch),
		string(topicPrefix),
		currentDateTimeGetter,
		jobTTLSecondsAfterFinished,
		jobKafkaClientCertSecret,
		jobKafkaCaCertSecret,
		jobFsGroup,
	)
	taskEventHandler := handler.NewTaskEventHandler(
		jobSpawner,
		namespace,
		branch,
		cluster,
		stageFilter,
		runUntagged,
		resolver,
		resultPublisher,
		taskStore,
		currentDateTimeGetter,
		view,
		taskVault,
		reconcilePassTimeout,
		reconcileInterval,
		claimMode,
		claimRefClientFactory,
		handler.WithVaultAliases(vaultAliases),
		handler.WithPodQuotaLimiter(
			spawner.NewPodQuotaLimiter(kubeClient, currentDateTimeGetter),
		),
		handler.WithClaimJobControl(
			handler.NewKubeJobControl(kubeClient, namespace, resolver, view),
		),
	)
	topic := lib.TaskV1SchemaID.EventTopic(topicPrefix)
	offsetManager := libkafka.NewSaramaOffsetManager(
		saramaClient,
		libkafka.Group(consumerGroup),
		libkafka.OffsetOldest,
		libkafka.OffsetOldest,
	)
	return libkafka.NewOffsetConsumerHighwaterMarks(
		saramaClient,
		topic,
		offsetManager,
		taskEventHandler,
		run.NewTrigger(),
		logSamplerFactory,
	), taskEventHandler
}

// CreateTaskView returns the in-memory task view and the run.Func that captures
// the topic's high-water marks and then replays the compacted task event topic
// from the oldest offset on every partition. The reader is a non-group simple
// consumer: it never joins CONSUMER_GROUP and never commits offsets, so it cannot
// steal partitions from, or move the offsets of, the live consumer.
func CreateTaskView(
	saramaClient sarama.Client,
	topicPrefix base.TopicPrefix,
	currentDateTimeGetter libtime.CurrentDateTimeGetter,
) (handler.TaskView, run.Func) {
	topic := lib.TaskV1SchemaID.EventTopic(topicPrefix)
	view := handler.NewTaskView(
		func(ctx context.Context, topic libkafka.Topic) (libkafka.PartitionOffsets, error) {
			return libkafka.HighWaterMarks(ctx, saramaClient, topic)
		},
		topic,
		currentDateTimeGetter,
	)
	consumer := libkafka.NewSimpleConsumer(
		saramaClient,
		topic,
		libkafka.OffsetOldest,
		view,
		log.DefaultSamplerFactory,
	)
	return view, func(ctx context.Context) error {
		if err := view.CaptureHighWaterMarks(ctx); err != nil {
			return errors.Wrapf(ctx, err, "capture task view high-water marks")
		}
		return consumer.Consume(ctx)
	}
}

// CreateHealthcheckRunner creates the healthcheck runner shared between the cron path and the
// HTTP trigger path. Callers must pass the same instance to both CreateHealthcheckCron and
// the HTTP handler so probe behavior is identical regardless of invocation path.
func CreateHealthcheckRunner(
	configProvider pkg.EventHandlerConfig,
	syncProducer libkafka.SyncProducer,
	topicPrefix base.TopicPrefix,
	branch base.Branch,
) probe.HealthcheckRunner {
	sender := cdb.NewCommandObjectSender(syncProducer, topicPrefix, log.DefaultSamplerFactory)
	publisher := probe.NewCommandPublisher(sender)
	return probe.NewHealthcheckRunner(configProvider, publisher, branch)
}

// CreateHealthcheckCron wraps the given runner in a cron scheduler. Pass the runner returned by
// CreateHealthcheckRunner so the cron and the HTTP handler share the same instance.
func CreateHealthcheckCron(
	expression libcron.Expression,
	runner probe.HealthcheckRunner,
) pkg.CronScheduler {
	return libcron.NewExpressionCron(expression, runner)
}
