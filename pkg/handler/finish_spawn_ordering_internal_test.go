// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// Plain Go tests, not Ginkgo: specs declared in an internal test package of
// this repository are not discovered by the package-external suite.

import (
	"context"
	"errors"
	"testing"

	lib "github.com/bborbe/agent"
	libtime "github.com/bborbe/time"

	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/claim/claimtest"
)

var errInjectedTriggerWrite = errors.New("injected trigger write failure")

// triggerFailPublisher fails the trigger-counter write and records whether the
// spawn notification was published. The notification is what writes
// current_job, so a failed counter write must not prevent it: an empty
// current_job beside a running Job reads as unclaimed, and nothing retries it.
type triggerFailPublisher struct {
	stubResultPublisher
	spawnNotifications int
	triggerCalls       int
}

func (p *triggerFailPublisher) PublishSpawnNotification(
	context.Context, lib.Task, string, string,
) error {
	p.spawnNotifications++
	return nil
}

func (p *triggerFailPublisher) PublishIncrementTriggerCount(context.Context, lib.Task) error {
	p.triggerCalls++
	return errInjectedTriggerWrite
}

func (p *triggerFailPublisher) PublishSetTriggerScope(
	context.Context, lib.Task, string, int,
) error {
	p.triggerCalls++
	return errInjectedTriggerWrite
}

// TestFinishSpawnPublishesNotificationBeforeFailingTriggerWrite pins the
// ordering: when recordTriggerSpawn fails, the spawn notification must already
// have been attempted, and the trigger error must still be surfaced rather than
// swallowed. On the pre-fix ordering the notification is never attempted, so
// the record keeps an empty current_job beside a live Job.
func TestFinishSpawnPublishesNotificationBeforeFailingTriggerWrite(t *testing.T) {
	ctx := context.Background()

	publisher := &triggerFailPublisher{}
	handler := NewTaskEventHandler(
		&stubJobSpawner{},
		"dev",
		"prod",
		"self",
		true,
		true,
		nil,
		publisher,
		pkg.NewTaskStore(),
		libtime.NewCurrentDateTime(),
		nil,
		"v",
		DefaultReconcilePassTimeout,
		DefaultReconcileInterval,
		pkg.ClaimModeShadow,
		func() claim.ClaimRefClient { return &countingClaimRefClient{} },
		WithClaimClientFactory(func() claim.ClaimClient {
			return claimtest.NewClient(&countingClaimRefClient{}, libtime.NewCurrentDateTime().Now)
		}),
		WithClaimJobControl(noopJobControl{}),
	)
	h, ok := handler.(*taskEventHandler)
	if !ok {
		t.Fatalf("expected *taskEventHandler, got %T", handler)
	}

	task := lib.Task{
		TaskIdentifier: lib.TaskIdentifier("task-1"),
		Vault:          "v",
		Frontmatter: lib.TaskFrontmatter{
			"status":   "in_progress",
			"phase":    "execution",
			"assignee": "claude",
		},
	}

	err := h.finishSpawn(
		ctx,
		task,
		&pkg.AgentConfiguration{Assignee: "claude", Image: "img"},
		"job-1",
		"sess-1",
	)

	if publisher.triggerCalls == 0 {
		t.Fatalf("expected the trigger write to be attempted")
	}
	if publisher.spawnNotifications != 1 {
		t.Fatalf(
			"expected the spawn notification to be published despite the trigger error, got %d",
			publisher.spawnNotifications,
		)
	}
	if err == nil {
		t.Fatalf("expected the trigger error to be surfaced, got nil")
	}
}
