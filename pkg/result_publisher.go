// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"fmt"
	"sync"
	"time"

	lib "github.com/bborbe/agent"
	taskcmd "github.com/bborbe/agent/command/task"
	"github.com/bborbe/cqrs/base"
	cdb "github.com/bborbe/cqrs/cdb"
	cqrsiam "github.com/bborbe/cqrs/iam"
	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/log"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
)

const (
	dedupeCapacity = 1024
	dedupeTTL      = 3600 * time.Second
)

//counterfeiter:generate -o ../mocks/result_publisher.go --fake-name FakeResultPublisher . ResultPublisher

// ResultPublisher publishes agent-task-v1-request commands to Kafka so the
// controller writes them to the vault task file.
type ResultPublisher interface {
	// PublishSpawnNotification publishes current_job, job_started_at, and
	// spawn_notification without touching any other frontmatter keys.
	PublishSpawnNotification(ctx context.Context, task lib.Task, jobName string) error
	// PublishFailure publishes a zombie failure: clears current_job and atomically
	// bumps trigger_count by 1 via a paired IncrementFrontmatterCommand. Leaves
	// phase, status, and assignee untouched so the existing trigger_count retry
	// cap (applyTriggerCap in task/controller/pkg/result/result_writer.go) handles
	// eventual operator-inbox escalation. Idempotent per current_job via a TTL'd
	// LRU; concurrent classifications for the same job emit one event.
	PublishFailure(ctx context.Context, task lib.Task, jobName string, reason string) error
	// PublishClearCurrentJob clears current_job in the vault task frontmatter
	// (writes "" only — no body section, no trigger_count bump, no status/phase
	// touch). Used on the job-success path, where the agent already published its
	// real result and the task is about to be evicted from the TaskStore: without
	// this clear, a succeeded job leaves current_job/job_started_at set forever
	// (the zombie sweeper is blind to it because the task left Snapshot()). The
	// write is idempotent (empty → empty is a no-op visually) and deduped per
	// jobName by the same TTL'd LRU as PublishFailure, so the informer's repeated
	// terminal re-deliveries do not spam Kafka.
	PublishClearCurrentJob(ctx context.Context, task lib.Task, jobName string) error
	// PublishIncrementTriggerCount sends an IncrementFrontmatterCommand that atomically
	// increments trigger_count by 1. Must complete before SpawnJob is called.
	PublishIncrementTriggerCount(ctx context.Context, task lib.Task) error
	// PublishSetTriggerScope sends an UpdateFrontmatterCommand that records a
	// trigger_scope and sets trigger_count to count, in one atomic write. Called
	// instead of PublishIncrementTriggerCount in the two cases where the scope
	// itself has to be written:
	//
	//   - the scope CHANGED (count 1): a re-dispatch against a new phase or a new
	//     ref starts from a fresh budget, counting the spawn about to happen.
	//   - the scope was ABSENT (count = existing trigger_count + 1): the task
	//     predates the field and adopts the current scope WITHOUT a budget reset.
	//     Absent must not be treated as changed — every task in flight today has no
	//     trigger_scope, so resetting on absence would hand each of them a free
	//     budget on the first event after deploy, including one already at cap.
	//
	// This is an UpdateFrontmatterCommand rather than a result publish on purpose.
	// trigger_count is controller-owned: the result writer's ownership guard
	// (MergeFrontmatter in task/controller) discards any incoming counter value, so
	// a reset routed through a result payload would be silently dropped. The atomic
	// frontmatter commands take a different path (buildUpdateModifyFn), which applies
	// Updates directly to the on-disk frontmatter — an explicit, intentional write
	// rather than a stale spawn-time snapshot.
	PublishSetTriggerScope(ctx context.Context, task lib.Task, scope string, count int) error
	// PublishTypeMismatchFailure publishes a synthetic failure when the task's task_type
	// is not in the agent's effective type set. Clears assignee and current_job so the
	// task surfaces in the operator inbox via assignee=="" filter. Does not bump
	// trigger_count or retry_count.
	PublishTypeMismatchFailure(ctx context.Context, task lib.Task, reason string) error
	// PublishRaw publishes a raw payload for testing error paths.
	PublishRaw(ctx context.Context, operation base.CommandOperation, payload interface{}) error
}

// NewResultPublisher creates a ResultPublisher.
func NewResultPublisher(
	syncProducer libkafka.SyncProducer,
	topicPrefix base.TopicPrefix,
	currentDateTime libtime.CurrentDateTimeGetter,
) ResultPublisher {
	return &resultPublisher{
		commandObjectSender: cdb.NewCommandObjectSender(
			syncProducer,
			topicPrefix,
			log.DefaultSamplerFactory,
		),
		currentDateTime: currentDateTime,
		dedupe:          newDedupe(dedupeCapacity, currentDateTime),
	}
}

// ttlDedupe implements a minimal TTL'd LRU with RWMutex for publish-layer dedupe.
// The eviction order is tracked via a separate []string so map lookups never
// hold stale slice indices after the oldest entry is shifted out.
type ttlDedupe struct {
	mu       sync.RWMutex
	capacity int
	ttl      time.Duration
	order    []string             // insertion order; index 0 is oldest
	seen     map[string]time.Time // jobName -> insertion ts; existence = "in dedupe window"
	now      libtime.CurrentDateTimeGetter
}

func newDedupe(capacity int, now libtime.CurrentDateTimeGetter) *ttlDedupe {
	return &ttlDedupe{
		capacity: capacity,
		ttl:      dedupeTTL,
		order:    make([]string, 0, capacity),
		seen:     make(map[string]time.Time, capacity),
		now:      now,
	}
}

// checkDedupe returns true if a non-expired entry exists for jobName.
// No mutation occurs.
func (d *ttlDedupe) checkDedupe(jobName string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	ts, ok := d.seen[jobName]
	if !ok {
		return false
	}
	return d.now.Now().Time().Sub(ts) < d.ttl
}

// recordDedupe inserts or refreshes the entry for jobName with the current timestamp.
// Evicts the oldest entry if at capacity.
func (d *ttlDedupe) recordDedupe(jobName string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now.Now().Time()
	if _, ok := d.seen[jobName]; ok {
		d.seen[jobName] = now // refresh ts
		return
	}
	if len(d.order) >= d.capacity {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.seen, oldest)
	}
	d.order = append(d.order, jobName)
	d.seen[jobName] = now
}

// resultPublisher implements ResultPublisher by sending CQRS command objects to Kafka.
type resultPublisher struct {
	commandObjectSender cdb.CommandObjectSender
	currentDateTime     libtime.CurrentDateTimeGetter
	dedupe              *ttlDedupe
}

// targetVaultFromTask reads the task's frontmatter "target_vault" key and
// returns it as a string, returning "" when the key is absent or holds a
// non-string value. TaskFrontmatter.String returns (string, bool) — the
// discarded second value is the found/type-assert flag, not an error; "" on
// false is exactly the documented fallback. The executor serves all vaults
// ("shared") and must never inject its own vault name, so the routing stamp
// always comes from the task.
func targetVaultFromTask(task lib.Task) string {
	vault, _ := task.Frontmatter.String("target_vault")
	return vault
}

func (p *resultPublisher) PublishSpawnNotification(
	ctx context.Context,
	task lib.Task,
	jobName string,
) error {
	cmd := taskcmd.UpdateFrontmatterCommand{
		TaskIdentifier: task.TaskIdentifier,
		TargetVault:    targetVaultFromTask(task),
		Updates: lib.TaskFrontmatter{
			"spawn_notification": true,
			"current_job":        jobName,
			"job_started_at":     p.currentDateTime.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
		},
	}
	return p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd)
}

func (p *resultPublisher) PublishFailure(
	ctx context.Context,
	task lib.Task,
	jobName string,
	reason string,
) error {
	if p.dedupe.checkDedupe(jobName) {
		glog.V(2).Infof("event=zombie_dedupe job=%s task=%s", jobName, task.TaskIdentifier)
		return nil
	}

	now := p.currentDateTime.Now().UTC().Format(time.RFC3339)
	section := fmt.Sprintf(
		"## Failure\n\n- **Timestamp:** %s\n- **Job:** %s\n- **Reason:** %s\n",
		now,
		jobName,
		reason,
	)
	updateCmd := taskcmd.UpdateFrontmatterCommand{
		TaskIdentifier: task.TaskIdentifier,
		TargetVault:    targetVaultFromTask(task),
		Updates: lib.TaskFrontmatter{
			"current_job": "",
		},
		Body: &taskcmd.BodySection{
			Heading: "## Failure",
			Section: section,
		},
	}
	if err := p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, updateCmd); err != nil {
		return errors.Wrapf(
			ctx,
			err,
			"publish zombie failure update for task %s",
			task.TaskIdentifier,
		)
	}

	incrementCmd := taskcmd.IncrementFrontmatterCommand{
		TaskIdentifier: task.TaskIdentifier,
		TargetVault:    targetVaultFromTask(task),
		Field:          "trigger_count",
		Delta:          1,
	}
	if err := p.publishRaw(ctx, taskcmd.IncrementFrontmatterCommandOperation, incrementCmd); err != nil {
		return errors.Wrapf(
			ctx,
			err,
			"publish zombie failure trigger_count increment for task %s",
			task.TaskIdentifier,
		)
	}

	// Record dedupe only after BOTH publishes succeed. If the increment fails,
	// the next cycle re-sends both messages — the duplicate current_job=""
	// write is idempotent (writing empty to already-empty is a no-op visually),
	// and the retry allows trigger_count to eventually bump so the retry cap
	// (applyTriggerCap in result_writer.go) fires.
	p.dedupe.recordDedupe(jobName)

	return nil
}

func (p *resultPublisher) PublishClearCurrentJob(
	ctx context.Context,
	task lib.Task,
	jobName string,
) error {
	if p.dedupe.checkDedupe(jobName) {
		glog.V(2).Infof("event=clear_dedupe job=%s task=%s", jobName, task.TaskIdentifier)
		return nil
	}
	updateCmd := taskcmd.UpdateFrontmatterCommand{
		TaskIdentifier: task.TaskIdentifier,
		TargetVault:    targetVaultFromTask(task),
		Updates: lib.TaskFrontmatter{
			"current_job": "",
		},
	}
	if err := p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, updateCmd); err != nil {
		return errors.Wrapf(ctx, err, "publish current_job clear for task %s", task.TaskIdentifier)
	}
	p.dedupe.recordDedupe(jobName)
	return nil
}

func (p *resultPublisher) PublishIncrementTriggerCount(ctx context.Context, task lib.Task) error {
	cmd := taskcmd.IncrementFrontmatterCommand{
		TaskIdentifier: task.TaskIdentifier,
		TargetVault:    targetVaultFromTask(task),
		Field:          "trigger_count",
		Delta:          1,
	}
	return p.publishRaw(ctx, taskcmd.IncrementFrontmatterCommandOperation, cmd)
}

func (p *resultPublisher) PublishSetTriggerScope(
	ctx context.Context,
	task lib.Task,
	scope string,
	count int,
) error {
	cmd := taskcmd.UpdateFrontmatterCommand{
		TaskIdentifier: task.TaskIdentifier,
		TargetVault:    targetVaultFromTask(task),
		Updates: lib.TaskFrontmatter{
			"trigger_scope": scope,
			// Never 0: this cycle is about to spawn, and that spawn must be counted
			// in the same write. Writing 0 and relying on a follow-up increment
			// would need two publishes for one spawn, which can interleave.
			"trigger_count": count,
		},
	}
	if err := p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd); err != nil {
		return errors.Wrapf(
			ctx,
			err,
			"publish set trigger scope for task %s",
			task.TaskIdentifier,
		)
	}
	return nil
}

func (p *resultPublisher) PublishTypeMismatchFailure(
	ctx context.Context,
	task lib.Task,
	reason string,
) error {
	now := p.currentDateTime.Now().UTC().Format(time.RFC3339)
	priorAssignee := string(task.Frontmatter.Assignee())
	section := fmt.Sprintf(
		"## Failure\n\n- **Timestamp:** %s\n- **Assignee:** %s\n- **Reason:** %s\n",
		now,
		priorAssignee,
		reason,
	)

	updates := lib.TaskFrontmatter{
		"assignee":    "",
		"current_job": "",
	}
	if priorAssignee != "" {
		updates["previous_assignee"] = priorAssignee
	}

	cmd := taskcmd.UpdateFrontmatterCommand{
		TaskIdentifier: task.TaskIdentifier,
		TargetVault:    targetVaultFromTask(task),
		Updates:        updates,
		Body: &taskcmd.BodySection{
			Heading: "## Failure",
			Section: section,
		},
	}
	if err := p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd); err != nil {
		return errors.Wrapf(
			ctx,
			err,
			"publish type mismatch failure for task %s",
			task.TaskIdentifier,
		)
	}
	return nil
}

func (p *resultPublisher) publishRaw(
	ctx context.Context,
	operation base.CommandOperation,
	payload interface{},
) error {
	// Payload validation is the publisher's test-suite contract plus the
	// consumer's job (the controller validates each command before persisting);
	// publishRaw intentionally does not re-validate.
	event, err := base.ParseEvent(ctx, payload)
	if err != nil {
		return errors.Wrapf(ctx, err, "parse event for operation %s", operation)
	}

	requestIDCh := make(chan base.RequestID, 1)
	requestIDCh <- base.NewRequestID()
	commandCreator := base.NewCommandCreator(requestIDCh)
	commandObject := cdb.CommandObject{
		Command: commandCreator.NewCommand(
			operation,
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

// PublishRaw exposes publishRaw for testing error path coverage.
func (p *resultPublisher) PublishRaw(
	ctx context.Context,
	operation base.CommandOperation,
	payload interface{},
) error {
	return p.publishRaw(ctx, operation, payload)
}
