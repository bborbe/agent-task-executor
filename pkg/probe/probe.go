// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package probe

import (
	"context"

	lib "github.com/bborbe/agent"
	taskcmd "github.com/bborbe/agent/command/task"
	"github.com/bborbe/cqrs/base"
	cdb "github.com/bborbe/cqrs/cdb"
	cqrsiam "github.com/bborbe/cqrs/iam"
	"github.com/bborbe/errors"
	"github.com/google/uuid"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

//counterfeiter:generate -o mocks/fake_config_provider.go --fake-name FakeConfigProvider . ConfigProvider

// ConfigProvider lists all known Config CRs.
type ConfigProvider interface {
	Get(ctx context.Context) ([]agentv1.Config, error)
}

//counterfeiter:generate -o mocks/fake_command_publisher.go --fake-name FakeCommandPublisher . CommandPublisher

// CommandPublisher publishes a single serialised command to the request topic.
type CommandPublisher interface {
	Publish(ctx context.Context, operation string, payload interface{}) error
}

type commandPublisher struct {
	commandObjectSender cdb.CommandObjectSender
}

// NewCommandPublisher creates a CommandPublisher backed by the given sender.
func NewCommandPublisher(sender cdb.CommandObjectSender) CommandPublisher {
	return &commandPublisher{commandObjectSender: sender}
}

func (p *commandPublisher) Publish(
	ctx context.Context,
	operation string,
	payload interface{},
) error {
	event, err := base.ParseEvent(ctx, payload)
	if err != nil {
		return errors.Wrapf(ctx, err, "parse event for operation %s", operation)
	}
	requestIDCh := make(chan base.RequestID, 1)
	requestIDCh <- base.NewRequestID()
	commandCreator := base.NewCommandCreator(requestIDCh)
	commandObject := cdb.CommandObject{
		Command: commandCreator.NewCommand(
			base.CommandOperation(operation),
			cqrsiam.Initiator("executor"),
			"",
			event,
		),
		SchemaID: lib.TaskV1SchemaID,
	}
	if err := p.commandObjectSender.SendCommandObject(ctx, commandObject); err != nil {
		return errors.Wrapf(ctx, err, "send command for operation %s", operation)
	}
	return nil
}

//counterfeiter:generate -o mocks/fake_healthcheck_runner.go --fake-name FakeHealthcheckRunner . HealthcheckRunner

// HealthcheckRunner executes one liveness check tick: publishes create-task + update-frontmatter per Config CR.
type HealthcheckRunner interface {
	Run(ctx context.Context) error
}

type healthcheckRunner struct {
	configProvider ConfigProvider
	publisher      CommandPublisher
	branch         base.Branch
}

// NewHealthcheckRunner creates a HealthcheckRunner.
func NewHealthcheckRunner(
	configProvider ConfigProvider,
	publisher CommandPublisher,
	branch base.Branch,
) HealthcheckRunner {
	return &healthcheckRunner{
		configProvider: configProvider,
		publisher:      publisher,
		branch:         branch,
	}
}

// probeNamespace is the UUIDv5 namespace for OAuth probe task identifiers.
// Stable per spec 024 follow-up — do NOT change without a migration plan.
var probeNamespace = uuid.MustParse("00000000-0000-0000-0000-000000000024")

// probeTaskID returns the deterministic UUIDv5 for a probe task targeting agentName and stage.
// Same (agentName, stage) always yields the same UUID, both within a single executor
// process and across restarts.
func probeTaskID(agentName, stage string) lib.TaskIdentifier {
	return lib.TaskIdentifier(uuid.NewSHA1(probeNamespace, []byte(agentName+"-"+stage)).String())
}

// Run lists all Config CRs and publishes two commands per agent on each cron tick.
func (r *healthcheckRunner) Run(ctx context.Context) error {
	configs, err := r.configProvider.Get(ctx)
	if err != nil {
		return errors.Wrap(ctx, err, "list configs")
	}
	for _, config := range configs {
		agentName := config.Spec.Assignee
		taskID := probeTaskID(agentName, string(r.branch))

		createCmd := taskcmd.CreateCommand{
			TaskIdentifier: taskID,
			Title:          "probe-" + agentName + "-" + string(r.branch),
			Frontmatter: lib.TaskFrontmatter{
				"task_type": lib.TaskTypeHealthcheck.String(),
				"status":    "in_progress",
				"phase":     "in_progress",
				"assignee":  agentName,
				"stage":     string(r.branch),
			},
			Body: "reply 'ok'",
		}
		if err := r.publisher.Publish(ctx, string(taskcmd.CreateCommandOperation), createCmd); err != nil {
			return errors.Wrapf(ctx, err, "publish create-task for %s", agentName)
		}

		updateCmd := taskcmd.UpdateFrontmatterCommand{
			TaskIdentifier: taskID,
			Updates: lib.TaskFrontmatter{
				"status":        "in_progress",
				"phase":         "in_progress",
				"trigger_count": 0,
				"retry_count":   0,
			},
		}
		if err := r.publisher.Publish(ctx, string(taskcmd.UpdateFrontmatterCommandOperation), updateCmd); err != nil {
			return errors.Wrapf(ctx, err, "publish update-frontmatter for %s", agentName)
		}
	}
	return nil
}
