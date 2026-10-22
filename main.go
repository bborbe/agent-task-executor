// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"regexp"
	"time"

	"github.com/bborbe/cqrs/base"
	libcron "github.com/bborbe/cron"
	"github.com/bborbe/errors"
	libhttp "github.com/bborbe/http"
	libk8s "github.com/bborbe/k8s"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/log"
	libmetrics "github.com/bborbe/metrics"
	"github.com/bborbe/run"
	libsentry "github.com/bborbe/sentry"
	"github.com/bborbe/service"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/factory"
	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/probe"
)

func main() {
	app := &application{}
	os.Exit(service.Main(context.Background(), app, &app.SentryDSN, &app.SentryProxy))
}

type application struct {
	SentryDSN                  string            `required:"true"  arg:"sentry-dsn"                     env:"SENTRY_DSN"                     usage:"SentryDSN"                                                                                                                                                                                                                                                         display:"length"`
	SentryProxy                string            `required:"false" arg:"sentry-proxy"                   env:"SENTRY_PROXY"                   usage:"Sentry Proxy"`
	Listen                     string            `required:"true"  arg:"listen"                         env:"LISTEN"                         usage:"address to listen to"`
	KafkaBrokers               string            `required:"true"  arg:"kafka-brokers"                  env:"KAFKA_BROKERS"                  usage:"comma-separated Kafka broker addresses"`
	Branch                     base.Branch       `required:"true"  arg:"branch"                         env:"BRANCH"                         usage:"Kafka topic prefix branch (develop/live)"`
	Cluster                    string            `required:"false" arg:"cluster"                        env:"CLUSTER"                        usage:"Name of the cluster this executor runs in (e.g. nukedev, nukeprod, sun); tasks whose cluster frontmatter names another cluster are skipped. Empty runs only tasks without a cluster field"`
	StageFilter                bool              `required:"false" arg:"stage-filter"                   env:"STAGE_FILTER"                   usage:"Skip tasks whose stage frontmatter (default prod) differs from BRANCH; set false on a cluster whose controller already scopes tasks by cluster (sun), so its tasks need no stage field"                                                                                             default:"true"`
	RunUntagged                bool              `required:"false" arg:"run-untagged"                   env:"RUN_UNTAGGED"                   usage:"Run tasks without a cluster frontmatter field; set false on a cluster whose controller publishes no untagged tasks (sun, PUBLISH_UNTAGGED=false), so an untagged task never runs there"                                                                                             default:"true"`
	ConsumerGroup              string            `required:"false" arg:"consumer-group"                 env:"CONSUMER_GROUP"                 usage:"Kafka consumer group for the task event topic; give each cluster its own group so every cluster sees every event"                                                                                                                                                                   default:"agent-task-executor"`
	TopicPrefix                base.TopicPrefix  `required:"false" arg:"topic-prefix"                   env:"TOPIC_PREFIX"                   usage:"Explicit Kafka topic prefix; empty means unprefixed topics"`
	Namespace                  libk8s.Namespace  `required:"true"  arg:"namespace"                      env:"NAMESPACE"                      usage:"K8s namespace to spawn Jobs in"`
	VaultName                  string            `required:"true"  arg:"vault-name"                     env:"VAULT_NAME"                     usage:"Obsidian vault name this executor instance serves; Config CRs are resolved by the composed {assignee}-{vaultName} assignee"`
	TaskVault                  string            `required:"false" arg:"task-vault"                     env:"TASK_VAULT"                     usage:"Vault slug this executor owns tasks for; empty means VAULT_NAME. A task is owned when its stamped vault equals this value"`
	GitRestURL                 string            `required:"false" arg:"gitrest-url"                    env:"GITREST_URL"                    usage:"git-rest HTTP API base URL the claim ref client writes leased claim refs through"`
	GitRestGatewaySecret       string            `required:"false" arg:"gitrest-gateway-secret"         env:"GITREST_GATEWAY_SECRET"         usage:"NAME of the K8s Secret carrying the git-rest gateway secret (data key gateway-secret) — never the secret value; empty disables gateway auth"`
	BuildGitVersion            string            `required:"false" arg:"build-git-version"              env:"BUILD_GIT_VERSION"              usage:"Build Git version (git describe --tags --always --dirty)"                                                                                                                                                                                                                           default:"dev"`
	BuildGitCommit             string            `required:"false" arg:"build-git-commit"               env:"BUILD_GIT_COMMIT"               usage:"Build Git commit hash"                                                                                                                                                                                                                                                              default:"none"`
	BuildDate                  *libtime.DateTime `required:"false" arg:"build-date"                     env:"BUILD_DATE"                     usage:"Build timestamp (RFC3339)"`
	HealthcheckCronExpression  string            `required:"true"  arg:"healthcheck-cron-expression"    env:"HEALTHCHECK_CRON_EXPRESSION"    usage:"Cron expression for agent liveness health checks"                                                                                                                                                                                                                                   default:"0 0 8 * * 1"`
	JobTTLSecondsAfterFinished int32             `required:"false" arg:"job-ttl-seconds-after-finished" env:"JOB_TTL_SECONDS_AFTER_FINISHED" usage:"K8s Job TTL after completion (seconds) — completed Job pods are GCed after this delay"                                                                                                                                                                                              default:"1800"`
	ServiceStorageClass        string            `required:"false" arg:"service-storage-class"          env:"SERVICE_STORAGE_CLASS"          usage:"StorageClass for a service agent's session volume; empty uses the cluster default"`
	// JobKafkaClientCertSecret holds the NAME of the K8s Secret carrying the
	// Kafka client cert/key — not the cert material itself. The value is a
	// resource reference (already public in the Deployment manifest), so no
	// display:"length" guard is needed.
	JobKafkaClientCertSecret string `required:"false" arg:"job-kafka-client-cert-secret"   env:"JOB_KAFKA_CLIENT_CERT_SECRET"   usage:"Name of the existing K8s secret holding the Kafka client cert/key (keys user.crt/user.key) to mount into spawned Jobs; empty disables cert mounting. Must be a valid K8s Secret name (RFC 1123: lowercase alphanumeric or '-', <=253 chars) or Job creation fails"`
	// JobKafkaCaCertSecret holds the NAME of the K8s Secret carrying the Kafka
	// CA cert — not the cert material itself. Same reasoning as
	// JobKafkaClientCertSecret: a resource reference, not secret-shaped.
	JobKafkaCaCertSecret string `required:"false" arg:"job-kafka-ca-cert-secret"       env:"JOB_KAFKA_CA_CERT_SECRET"       usage:"Name of the existing K8s secret holding the Kafka CA cert (key ca.crt) to mount into spawned Jobs; empty disables cert mounting. Must be a valid K8s Secret name (RFC 1123: lowercase alphanumeric or '-', <=253 chars) or Job creation fails"`
	// JobFsGroup is the pod-level fsGroup stamped onto spawned Jobs when Kafka
	// cert mounting is active (both cert secrets set). Cert files are projected
	// with defaultMode 0440 (owner+group read, root-owned), so a non-root agent
	// image can only read them if the pod fsGroup is added to its supplementary
	// groups. 65534 (nobody) is the fleet convention (watcher-github-pr StatefulSet).
	// Ignored when cert mounting is disabled — the produced pod spec stays
	// byte-identical to pre-fsGroup behavior for plaintext (quant) deployments.
	JobFsGroup int64 `required:"false" arg:"job-fs-group"                   env:"JOB_FS_GROUP"                   usage:"Pod fsGroup stamped onto spawned Jobs when Kafka cert mounting is active (0 disables); 65534 is the fleet convention"                                                                                                                                                               default:"65534"`
	// ReconcilePassTimeout and ReconcileInterval bound and pace the reconcile
	// loop. A non-positive value is rejected at startup.
	ReconcilePassTimeout time.Duration `required:"false" arg:"reconcile-pass-timeout"         env:"RECONCILE_PASS_TIMEOUT"         usage:"Deadline for one reconcile pass over the in-memory task view"                                                                                                                                                                                                                       default:"45s"`
	ReconcileInterval    time.Duration `required:"false" arg:"reconcile-interval"             env:"RECONCILE_INTERVAL"             usage:"Interval between reconcile passes"                                                                                                                                                                                                                                                  default:"60s"`
}

// vaultSlugRegexp mirrors the controller's VAULT_NAME validation (pkg/routing):
// a lowercase letter, then lowercase letters, digits, or hyphens. The composed
// {assignee}-{vaultName} lookup key depends on a well-formed vault slug — a
// malformed value would silently never match a Config CR.
var vaultSlugRegexp = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// claimModeEnv is the environment variable carrying the executor's claim mode.
const claimModeEnv = "CLAIM_MODE"

// parseClaimModeFromEnv reads CLAIM_MODE, distinguishing an absent variable
// (off) from an explicitly empty one (invalid).
func parseClaimModeFromEnv(ctx context.Context) (pkg.ClaimMode, error) {
	value, present := os.LookupEnv(claimModeEnv)
	return pkg.ParseClaimMode(ctx, value, present)
}

// effectiveTaskVault returns TASK_VAULT, defaulting to VAULT_NAME when unset.
// VAULT_NAME keeps its meaning for Config CR resolution; this setting only
// decides which tasks this executor owns.
func (a *application) effectiveTaskVault() string {
	if a.TaskVault != "" {
		return a.TaskVault
	}
	return a.VaultName
}

// validate checks the startup configuration before any client is constructed.
func (a *application) validate(ctx context.Context) error {
	if a.JobTTLSecondsAfterFinished < 0 {
		return errors.Errorf(
			ctx,
			"job-ttl-seconds-after-finished must be >= 0 (0 deletes immediately), got %d",
			a.JobTTLSecondsAfterFinished,
		)
	}
	if !vaultSlugRegexp.MatchString(a.VaultName) {
		return errors.Errorf(
			ctx,
			"vault-name must match ^[a-z][a-z0-9-]*$ (lowercase letter, then lowercase/digits/hyphens), got %q",
			a.VaultName,
		)
	}
	if !vaultSlugRegexp.MatchString(a.effectiveTaskVault()) {
		return errors.Errorf(
			ctx,
			"task-vault (TASK_VAULT) must match ^[a-z][a-z0-9-]*$ (lowercase letter, then lowercase/digits/hyphens), got %q",
			a.effectiveTaskVault(),
		)
	}
	if a.ReconcilePassTimeout <= 0 {
		return errors.Errorf(
			ctx,
			"reconcile-pass-timeout (RECONCILE_PASS_TIMEOUT) must be > 0, got %s",
			a.ReconcilePassTimeout,
		)
	}
	if a.ReconcileInterval <= 0 {
		return errors.Errorf(
			ctx,
			"reconcile-interval (RECONCILE_INTERVAL) must be > 0, got %s",
			a.ReconcileInterval,
		)
	}
	return nil
}

//nolint:funlen // Initialization sequence; wiring is linear with no branching.
func (a *application) Run(ctx context.Context, sentryClient libsentry.Client) error {
	if err := a.validate(ctx); err != nil {
		return err
	}
	libmetrics.NewBuildInfoMetrics().SetBuildInfo(a.BuildGitVersion, a.BuildGitCommit, a.BuildDate)
	glog.V(1).
		Infof("agent-task-executor started version=%s commit=%s", a.BuildGitVersion, a.BuildGitCommit)

	claimMode, err := parseClaimModeFromEnv(ctx)
	if err != nil {
		return errors.Wrapf(ctx, err, "parse claim mode")
	}

	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return errors.Wrapf(ctx, err, "get in-cluster k8s config")
	}
	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return errors.Wrapf(ctx, err, "create k8s client")
	}

	connector := factory.CreateK8sConnector(kubeConfig)
	if err := connector.SetupCustomResourceDefinition(ctx); err != nil {
		return errors.Wrapf(ctx, err, "setup Config CRD")
	}
	eventHandlerConfig := factory.CreateEventHandlerConfig()
	resourceEventHandler := factory.CreateResourceEventHandlerConfig(
		ctx,
		eventHandlerConfig,
	)
	resolver := factory.CreateConfigResolver(eventHandlerConfig, a.Branch, a.VaultName)

	// Service (identity) agents are reconciled as StatefulSets, alongside the
	// per-task Jobs the rest of this binary spawns. Both read the same Config CR;
	// spec.type picks the shape.
	serviceReconcileLoop := factory.CreateServiceReconcileLoop(
		eventHandlerConfig,
		resolver,
		factory.CreateServiceReconciler(kubeClient, a.Namespace, a.ServiceStorageClass),
		0,
	)

	saramaClient, err := libkafka.CreateSaramaClient(
		ctx,
		libkafka.ParseBrokersFromString(a.KafkaBrokers),
	)
	if err != nil {
		return errors.Wrapf(ctx, err, "create sarama client")
	}
	defer saramaClient.Close()
	currentDateTimeGetter := libtime.NewCurrentDateTime()
	syncProducer, err := libkafka.NewSyncProducerFromSaramaClient(ctx, saramaClient)
	if err != nil {
		return errors.Wrapf(ctx, err, "create kafka sync producer")
	}
	defer syncProducer.Close()

	resultPublisher := pkg.NewResultPublisher(syncProducer, a.TopicPrefix, currentDateTimeGetter)
	taskStore := pkg.NewTaskStore()
	jobWatcher := factory.CreateJobWatcher(kubeClient, a.Namespace, taskStore, resultPublisher)

	zombieSweeper := factory.CreateZombieSweeper(
		jobWatcher,
		a.Namespace,
		taskStore,
		resultPublisher,
		eventHandlerConfig,
		currentDateTimeGetter,
	)

	// The git-rest gateway secret is read by NAME from K8s (never logged, never
	// in env) so the claim ref client can authenticate to git-rest without secret
	// material entering the Deployment manifest (spec 005 Security).
	gitRestGatewaySecretValue, err := pkg.ReadGitRestGatewaySecret(
		ctx,
		kubeClient,
		a.Namespace,
		a.GitRestGatewaySecret,
	)
	if err != nil {
		return errors.Wrapf(ctx, err, "read git-rest gateway secret")
	}
	claimRefClientFactory := factory.CreateClaimRefClientFactory(
		a.GitRestURL,
		gitRestGatewaySecretValue,
	)

	healthcheckRunner := factory.CreateHealthcheckRunner(
		eventHandlerConfig,
		syncProducer,
		a.TopicPrefix,
		a.Branch,
	)
	healthcheckCron := factory.CreateHealthcheckCron(
		libcron.Expression(a.HealthcheckCronExpression),
		healthcheckRunner,
	)

	taskView, runTaskView := factory.CreateTaskView(
		saramaClient,
		a.TopicPrefix,
		currentDateTimeGetter,
	)

	consumer, taskEventHandler := factory.CreateConsumer(
		saramaClient,
		a.Branch,
		a.Cluster,
		a.StageFilter,
		a.RunUntagged,
		a.ConsumerGroup,
		a.TopicPrefix,
		kubeClient,
		a.Namespace,
		a.KafkaBrokers,
		resolver,
		log.DefaultSamplerFactory,
		currentDateTimeGetter,
		resultPublisher,
		taskStore,
		a.JobTTLSecondsAfterFinished,
		a.JobKafkaClientCertSecret,
		a.JobKafkaCaCertSecret,
		a.JobFsGroup,
		taskView,
		a.effectiveTaskVault(),
		a.ReconcilePassTimeout,
		a.ReconcileInterval,
		claimMode,
		claimRefClientFactory,
	)

	return service.Run(
		ctx,
		func(ctx context.Context) error {
			return connector.Listen(ctx, a.Namespace, resourceEventHandler)
		},
		consumer.Consume,
		runTaskView,
		taskEventHandler.RunDeferredRespawnLoop,
		taskEventHandler.RunReconcileLoop,
		serviceReconcileLoop.Run,
		jobWatcher.Run,
		zombieSweeper.Run,
		a.createHTTPServer(eventHandlerConfig, healthcheckRunner),
		healthcheckCron.Run,
	)
}

func (a *application) createHTTPServer(
	configProvider pkg.EventHandlerConfig,
	runner probe.HealthcheckRunner,
) run.Func {
	return func(ctx context.Context) error {
		router := mux.NewRouter()
		router.Path("/healthz").Handler(libhttp.NewPrintHandler("OK"))
		router.Path("/readiness").Handler(libhttp.NewPrintHandler("OK"))
		router.Path("/metrics").Handler(promhttp.Handler())
		router.Path("/setloglevel/{level}").
			Handler(log.NewSetLoglevelHandler(ctx, log.NewLogLevelSetter(2, 5*time.Minute)))
		router.Path("/gc").Handler(libhttp.NewGarbageCollectorHandler())

		router.Path("/agents").
			Handler(handler.NewAgentsHandler(configProvider, os.Getenv("AGENTS_AUTH_SECRET")))
		router.Path("/healthcheck-trigger").Handler(
			handler.NewHealthcheckTriggerHandler(runner),
		)

		glog.V(2).Infof("starting http server listen on %s", a.Listen)
		// Using libhttp defaults: ReadTimeout=30s, WriteTimeout=30s, IdleTimeout=60s,
		// ReadHeaderTimeout=10s, MaxHeaderBytes=1MB — appropriate for task-executor API.
		return libhttp.NewServer(
			a.Listen,
			router,
		).Run(ctx)
	}
}
