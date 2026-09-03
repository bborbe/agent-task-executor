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
	TopicPrefix                base.TopicPrefix  `required:"false" arg:"topic-prefix"                   env:"TOPIC_PREFIX"                   usage:"Explicit Kafka topic prefix; empty means unprefixed topics"`
	Namespace                  libk8s.Namespace  `required:"true"  arg:"namespace"                      env:"NAMESPACE"                      usage:"K8s namespace to spawn Jobs in"`
	VaultName                  string            `required:"true"  arg:"vault-name"                     env:"VAULT_NAME"                     usage:"Obsidian vault name this executor instance serves; Config CRs are resolved by the composed {assignee}-{vaultName} assignee"`
	BuildGitVersion            string            `required:"false" arg:"build-git-version"              env:"BUILD_GIT_VERSION"              usage:"Build Git version (git describe --tags --always --dirty)"                                                                                                                                                                                                                           default:"dev"`
	BuildGitCommit             string            `required:"false" arg:"build-git-commit"               env:"BUILD_GIT_COMMIT"               usage:"Build Git commit hash"                                                                                                                                                                                                                                                              default:"none"`
	BuildDate                  *libtime.DateTime `required:"false" arg:"build-date"                     env:"BUILD_DATE"                     usage:"Build timestamp (RFC3339)"`
	HealthcheckCronExpression  string            `required:"true"  arg:"healthcheck-cron-expression"    env:"HEALTHCHECK_CRON_EXPRESSION"    usage:"Cron expression for agent liveness health checks"                                                                                                                                                                                                                                   default:"0 0 8 * * 1"`
	JobTTLSecondsAfterFinished int32             `required:"false" arg:"job-ttl-seconds-after-finished" env:"JOB_TTL_SECONDS_AFTER_FINISHED" usage:"K8s Job TTL after completion (seconds) — completed Job pods are GCed after this delay"                                                                                                                                                                                              default:"1800"`
	// JobKafkaClientCertSecret holds the NAME of the K8s Secret carrying the
	// Kafka client cert/key — not the cert material itself. The value is a
	// resource reference (already public in the Deployment manifest), so no
	// display:"length" guard is needed.
	JobKafkaClientCertSecret string `required:"false" arg:"job-kafka-client-cert-secret"   env:"JOB_KAFKA_CLIENT_CERT_SECRET"   usage:"Name of the existing K8s secret holding the Kafka client cert/key (keys user.crt/user.key) to mount into spawned Jobs; empty disables cert mounting. Must be a valid K8s Secret name (RFC 1123: lowercase alphanumeric or '-', <=253 chars) or Job creation fails"`
	// JobKafkaCaCertSecret holds the NAME of the K8s Secret carrying the Kafka
	// CA cert — not the cert material itself. Same reasoning as
	// JobKafkaClientCertSecret: a resource reference, not secret-shaped.
	JobKafkaCaCertSecret string `required:"false" arg:"job-kafka-ca-cert-secret"       env:"JOB_KAFKA_CA_CERT_SECRET"       usage:"Name of the existing K8s secret holding the Kafka CA cert (key ca.crt) to mount into spawned Jobs; empty disables cert mounting. Must be a valid K8s Secret name (RFC 1123: lowercase alphanumeric or '-', <=253 chars) or Job creation fails"`
	// GitRestURL is the git-rest HTTP API base URL the reconcile loop reads the
	// vault through. Matches the controller's default git-rest service address.
	// Consumed by the reconcile loop in the follow-up prompt.
	GitRestURL string `required:"false" arg:"git-rest-url"                   env:"GITREST_URL"                    usage:"git-rest HTTP API base URL the reconcile loop reads vault task files through"                                                                                                                                                                                                       default:"http://vault-obsidian-openclaw:9090"`
	// GitRestGatewaySecret holds the NAME of the K8s Secret carrying the
	// git-rest gateway secret (data key `gateway-secret`) — not the secret
	// value itself. Same resource-reference pattern as JobKafkaClientCertSecret:
	// a name, not secret material, so nothing secret-shaped enters the image or
	// the Deployment manifest. Consumed by the reconcile loop in the follow-up
	// prompt.
	GitRestGatewaySecret string `required:"false" arg:"git-rest-gateway-secret"        env:"GITREST_GATEWAY_SECRET"         usage:"Name of the existing K8s secret holding the git-rest gateway secret (data key gateway-secret); empty disables gateway auth"`
	// TaskGlob is the git-rest single-level glob selecting the vault task files
	// the reconcile loop evaluates, relative to the repo root. Consumed by the
	// reconcile loop in the follow-up prompt.
	TaskGlob string `required:"false" arg:"task-glob"                      env:"TASK_GLOB"                      usage:"git-rest glob selecting vault task files to reconcile"                                                                                                                                                                                                                              default:"24 Tasks/*.md"`
}

// vaultSlugRegexp mirrors the controller's VAULT_NAME validation (pkg/routing):
// a lowercase letter, then lowercase letters, digits, or hyphens. The composed
// {assignee}-{vaultName} lookup key depends on a well-formed vault slug — a
// malformed value would silently never match a Config CR.
var vaultSlugRegexp = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

//nolint:funlen // Initialization sequence; wiring is linear with no branching.
func (a *application) Run(ctx context.Context, sentryClient libsentry.Client) error {
	libmetrics.NewBuildInfoMetrics().SetBuildInfo(a.BuildGitVersion, a.BuildGitCommit, a.BuildDate)
	glog.V(1).
		Infof("agent-task-executor started version=%s commit=%s", a.BuildGitVersion, a.BuildGitCommit)

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

	consumer, taskEventHandler := factory.CreateConsumer(
		saramaClient,
		a.Branch,
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
	)

	return service.Run(
		ctx,
		func(ctx context.Context) error {
			return connector.Listen(ctx, a.Namespace, resourceEventHandler)
		},
		consumer.Consume,
		taskEventHandler.RunDeferredRespawnLoop,
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
