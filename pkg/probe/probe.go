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
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"
	"github.com/google/uuid"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
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
	targetVault    string
}

// NewHealthcheckRunner creates a HealthcheckRunner.
//
// targetVault is the vault this executor owns (TASK_VAULT, defaulting to
// VAULT_NAME). It is stamped onto every published create-task command: the
// controller substitutes routing.LegacyDefaultVault ("openclaw") when
// TargetVault is empty and skips the create as a cross-vault write, so an
// unstamped probe is silently dropped on every vault but openclaw.
func NewHealthcheckRunner(
	configProvider ConfigProvider,
	publisher CommandPublisher,
	branch base.Branch,
	targetVault string,
) HealthcheckRunner {
	return &healthcheckRunner{
		configProvider: configProvider,
		publisher:      publisher,
		branch:         branch,
		targetVault:    targetVault,
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

// probePhase picks the phase a probe task for one Config is written at: the
// first phase in the Config's effective trigger list that the executor actually
// runs, preferring planning. It returns false when the list holds no such phase
// (only todo, human_review or done), so the Config is not probed at all: a probe
// at todo would land in the operator approval inbox, and a probe at a terminal
// phase would sit in the operator review inbox forever.
func probePhase(trigger *agentv1.Trigger) (domain.TaskPhase, bool) {
	phases := pkg.EffectiveTriggerPhases(trigger)
	if phases.Contains(domain.TaskPhasePlanning) {
		return domain.TaskPhasePlanning, true
	}
	for _, phase := range phases {
		if pkg.DefaultTriggerPhases.Contains(phase) {
			return phase, true
		}
	}
	return "", false
}

// Run lists all Config CRs and publishes two commands per agent on each cron tick.
func (r *healthcheckRunner) Run(ctx context.Context) error {
	configs, err := r.configProvider.Get(ctx)
	if err != nil {
		return errors.Wrap(ctx, err, "list configs")
	}
	for _, config := range configs {
		agentName := config.Spec.Assignee
		phase, ok := probePhase(config.Spec.Trigger)
		if !ok {
			glog.V(2).Infof(
				"event=probe_skipped assignee=%s reason=no_spawnable_trigger_phase",
				agentName,
			)
			continue
		}
		taskID := probeTaskID(agentName, string(r.branch))

		createCmd := taskcmd.CreateCommand{
			TaskIdentifier: taskID,
			Title:          "probe-" + agentName + "-" + string(r.branch),
			Frontmatter: lib.TaskFrontmatter{
				"task_type": lib.TaskTypeHealthcheck.String(),
				"status":    "in_progress",
				"phase":     string(phase),
				"assignee":  agentName,
				"stage":     string(r.branch),
			},
			Body:        "reply 'ok'",
			TargetVault: r.targetVault,
		}
		if err := r.publisher.Publish(ctx, string(taskcmd.CreateCommandOperation), createCmd); err != nil {
			return errors.Wrapf(ctx, err, "publish create-task for %s", agentName)
		}

		updateCmd := taskcmd.UpdateFrontmatterCommand{
			TaskIdentifier: taskID,
			// Stamped for the same reason the create is, and it is load-bearing on
			// the controller's side: a command addressed to a vault must find its
			// task there, so a miss is a FAILURE. Unstamped, both controllers
			// attempt the write and the non-owner's miss has to stay a skip — which
			// would swallow a genuine miss on the owner too.
			TargetVault: r.targetVault,
			Updates: lib.TaskFrontmatter{
				"status":        "in_progress",
				"phase":         string(phase),
				"trigger_count": 0,
				// Clear the executor-owned run-state from the PRIOR run. Probe files are
				// reused per (agent, stage), so without this the re-trigger reopens the
				// task while current_job / job_started_at still point at last week's job.
				// The executor then reads a stale (hours/days old) current_job whose grace
				// window has long elapsed and respawns 2-3 jobs for a single probe, and the
				// zombie sweeper fires a phantom deadline_exceeded on the ancient job ref.
				// Resetting these alongside trigger_count makes every re-trigger start clean.
				"current_job":        "",
				"job_started_at":     "",
				"spawn_notification": false,
			},
		}
		if err := r.publisher.Publish(ctx, string(taskcmd.UpdateFrontmatterCommandOperation), updateCmd); err != nil {
			return errors.Wrapf(ctx, err, "publish update-frontmatter for %s", agentName)
		}
	}
	return nil
}
