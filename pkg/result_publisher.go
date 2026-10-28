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
	"github.com/bborbe/vault-cli/pkg/domain"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

const (
	dedupeCapacity = 1024
	dedupeTTL      = 3600 * time.Second

	// defaultMaxTriggers is the cap armed on a pod_crash_no_stdout failure. It
	// mirrors the fallback lib.TaskFrontmatter.MaxTriggers() returns when the key
	// is absent, so the cap written here equals the cap the dispatch budget
	// already assumes. Writing the literal is required, not redundant: the
	// controller's assignee-clear is opt-in on the key being PRESENT, while the
	// dispatch budget defaults the cap ON for any task carrying a ref — the two
	// disagree exactly when the key is missing, which is the stuck state this
	// write removes.
	defaultMaxTriggers = 3
)

// The two reasons a record may be closed out with. The vocabulary is closed:
// nothing else is ever written to aborted_reason by the re-drive path, which is
// why executor_redrive_aborted_total can be pre-initialised for both values.
const (
	// AbortReasonBrokenSignature means the most recent attempts all failed with
	// the same broken signature — the work is not going to succeed by retrying.
	AbortReasonBrokenSignature = metrics.RedriveAbortReasonBrokenSignature
	// AbortReasonTransientExhausted means the failures were transient, but the
	// bounded extension is spent.
	AbortReasonTransientExhausted = metrics.RedriveAbortReasonTransientExhausted
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
	// phase, status, and assignee untouched — the escalation is the controller's
	// increment executor, which clears assignee once trigger_count reaches an
	// explicitly-set max_triggers, and an empty assignee is what surfaces the
	// record in the operator inbox.
	//
	// That cap is opt-in on the key being present: an absent max_triggers means
	// no cap, so a record without the key accrues trigger_count forever and its
	// assignee is never cleared. For the pod_crash_no_stdout class this method
	// therefore arms the cap by writing max_triggers=defaultMaxTriggers when the
	// key is absent — the class is otherwise unreachable, because a crash before
	// the first stdout write leaves nothing for the deliverer to act on. The
	// other seven classes keep their own escalation semantics and are untouched.
	//
	// applyTriggerCap (task/controller/pkg/result/result_writer.go) does NOT cover
	// this path, and reading it as though it did is what left the hole open: it
	// runs on the result-merge path only, while these writes take the atomic
	// frontmatter-command path (buildUpdateModifyFn), which never reaches it.
	//
	// Idempotent per current_job via a TTL'd LRU; concurrent classifications for
	// the same job emit one event.
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
	// PublishRestoreAssignee writes the resolved configuration's assignee back
	// onto a task whose routing value was cleared, so the controller applies the
	// write and the resulting hash change re-publishes TaskUpdated — the event
	// the normal spawn path already consumes. Writes the "assignee" key and
	// nothing else.
	PublishRestoreAssignee(ctx context.Context, task lib.Task, assignee string) error
	// PublishParkLostClaim writes phase "human_review" onto a task whose claim
	// another executor took over and whose Config says not to re-run it. Writes
	// the "phase" key and nothing else: status, assignee and the counters stay
	// as they are. human_review is already in the executor's terminal-phase set,
	// so a parked task is never re-spawned and needs no second guard.
	PublishParkLostClaim(ctx context.Context, task lib.Task) error
	// PublishAbort closes a record the consecutive-re-drive cap has decided to
	// stop driving. It writes status aborted plus the close-out pair vault-cli
	// requires (aborted_reason and gate_successor) and clears current_job. It
	// writes those four keys and no others. Idempotent per task: an already
	// aborted task is left alone, and a publish is deduped through the same
	// TTL'd LRU as PublishFailure. streak is the cap's count at the time of the
	// decision, carried for the log line only — the reason store's ref count is
	// deliberately not the streak count.
	PublishAbort(ctx context.Context, task lib.Task, reason string, streak int) error
	// PublishQueueEntry records when a task first started waiting behind the
	// concurrency cap. It publishes queued_at and touches no other key.
	PublishQueueEntry(ctx context.Context, task lib.Task, queuedAt string) error
	// PublishRaw publishes a raw payload for testing error paths.
	PublishRaw(ctx context.Context, operation base.CommandOperation, payload interface{}) error
}

// NewResultPublisher creates a ResultPublisher. claimRefClientFactory may be
// nil — claim mode off, or a mode that never built a client — in which case no
// failure reason is recorded and every publish behaves exactly as before.
func NewResultPublisher(
	syncProducer libkafka.SyncProducer,
	topicPrefix base.TopicPrefix,
	currentDateTime libtime.CurrentDateTimeGetter,
	claimRefClientFactory ClaimRefClientFactory,
) ResultPublisher {
	return &resultPublisher{
		commandObjectSender: cdb.NewCommandObjectSender(
			syncProducer,
			topicPrefix,
			log.DefaultSamplerFactory,
		),
		currentDateTime:       currentDateTime,
		claimRefClientFactory: claimRefClientFactory,
		dedupe:                newDedupe(dedupeCapacity, currentDateTime),
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
	// claimRefClientFactory builds the client the reason store records through.
	// Nil under claim mode off.
	claimRefClientFactory ClaimRefClientFactory
	// reasonStoreOnce guards the lazy construction below. PublishFailure is
	// called from more than one goroutine (the Job and Pod informers), and the
	// precommit gate runs with -race=false, so an unguarded nil-check would ship
	// as a silent data race.
	reasonStoreOnce sync.Once
	reasonStore     claim.RedriveReasonStore
	// streakStoreOnce guards the lazily-built streak store. The publisher owns
	// the streak because the streak counts PUBLISHED FAILURES: the write happens
	// here, beside the reason write, so the count and the evidence it is
	// classified from can never diverge.
	streakStoreOnce sync.Once
	streakStore     claim.RedriveStreakStore
}

// redriveReasonStore returns the lazily-built reason store, or nil when no
// claim ref client factory was injected. A nil store means "no evidence": the
// failure publish still succeeds and nothing is recorded.
func (p *resultPublisher) redriveReasonStore() claim.RedriveReasonStore {
	if p.claimRefClientFactory == nil {
		return nil
	}
	p.reasonStoreOnce.Do(func() {
		refClient := p.claimRefClientFactory()
		if refClient == nil {
			return
		}
		p.reasonStore = claim.NewRedriveReasonStore(refClient, p.currentDateTime)
	})
	return p.reasonStore
}

// redriveStreakStore returns the lazily-built streak store, or nil when no claim
// ref client factory was injected. A nil store means the streak is not tracked
// here at all, and the handler falls back to its in-memory floor.
func (p *resultPublisher) redriveStreakStore() claim.RedriveStreakStore {
	if p.claimRefClientFactory == nil {
		return nil
	}
	p.streakStoreOnce.Do(func() {
		refClient := p.claimRefClientFactory()
		if refClient == nil {
			return
		}
		p.streakStore = claim.NewRedriveStreakStore(refClient, p.currentDateTime)
	})
	return p.streakStore
}

// recordRedriveFailure advances the task's re-drive streak by one, at its
// current phase. It is the ONLY writer of the streak, and it runs only from
// PublishFailure.
//
// That placement is the whole fix. The streak used to advance on every
// successful spawn, while a reason was recorded only here — so a spawn that
// published no failure (an exit-0 human park, a successful run, a reconcile
// re-drive whose Job vanished) advanced the streak with no evidence behind it.
// At the cap the classifier then had no reasons to read, returned unknown, and
// refused the record forever with no abort and no release: the permanent
// class=unknown strand. Counting failures instead of spawns makes the count and
// the evidence the same KIND of thing, and lets a task that keeps succeeding or
// parks for a human leave the budget entirely rather than consume it.
//
// It does not make them the same WRITE. The streak and the reason are two
// separate git-rest puts, so a reason write that fails after a successful streak
// write leaves count > len(reasons). That is bounded, not silent: partial
// evidence classifies as transient and terminates at
// maxConsecutiveRedrives+transientRedriveBudget, and a count with no reasons
// behind it degrades to class=unknown until RedriveStreakLeaseDuration (6 h)
// releases it. The same non-atomicity already applied to the reason path.
//
// The dedupe that keeps one failed Job from counting twice is ttlDedupe, an
// in-process map, so two executor replicas that both observe the same Job's
// terminal failure can each Record it. That tightens the cap (a premature abort)
// rather than bypassing it, and the duplicate publish already applied to the
// reason write before this change; it is noted rather than fixed here.
//
// When the streak is still at 0 this failure starts a new streak, so the
// task's reason refs are cleared first: a reason set left by an earlier streak
// would otherwise satisfy the classifier's window on partial evidence and could
// close the record as broken. The clear must precede the Record, and the caller
// records the new reason after this call, so the new streak's evidence starts
// empty and gains exactly the reason being published.
//
// A store failure is logged and swallowed, exactly as recordRedriveReason
// swallows one: recording evidence must never fail a failure publish.
func (p *resultPublisher) recordRedriveFailure(ctx context.Context, task lib.Task) {
	store := p.redriveStreakStore()
	if store == nil {
		return
	}
	phase := taskPhase(task)
	count, countErr := store.Count(ctx, string(task.TaskIdentifier), phase)
	switch {
	case countErr != nil:
		glog.Warningf(
			"event=redrive_streak_read_failed task=%s phase=%q error=%v",
			task.TaskIdentifier,
			phase,
			countErr,
		)
	case count == 0:
		p.clearReasonsForNewStreak(ctx, task, phase)
	}
	if err := store.Record(ctx, string(task.TaskIdentifier), phase); err != nil {
		glog.Warningf(
			"event=redrive_streak_write_failed task=%s phase=%q error=%v",
			task.TaskIdentifier,
			phase,
			err,
		)
	}
}

// clearReasonsForNewStreak clears the task's reason refs when the streak is
// still at 0, i.e. the failure about to be recorded starts a new streak. A
// reason set left by an earlier streak would otherwise satisfy the classifier's
// window on partial evidence and could close the record as broken. Both the
// store lookup and the clear are logged and swallowed.
func (p *resultPublisher) clearReasonsForNewStreak(
	ctx context.Context,
	task lib.Task,
	phase string,
) {
	reasonStore := p.redriveReasonStore()
	if reasonStore == nil {
		return
	}
	if err := reasonStore.Clear(ctx, string(task.TaskIdentifier)); err != nil {
		glog.Warningf(
			"event=redrive_reason_write_failed task=%s phase=%q error=%v",
			task.TaskIdentifier,
			phase,
			err,
		)
	}
}

// recordRedriveReason persists the failure reason of the task's current phase.
// A recognised ZombieReason is stored verbatim; anything else, including the
// formatted "pod_oom_killed (OOMKilled/137)" form, is normalised to "unknown" or
// its parsed value — the formatted string's spaces and parentheses are not
// valid claim key segments. A store failure is logged and swallowed: recording
// evidence must never fail a failure publish.
func (p *resultPublisher) recordRedriveReason(
	ctx context.Context,
	task lib.Task,
	reason string,
) {
	store := p.redriveReasonStore()
	if store == nil {
		return
	}
	normalized := claim.RedriveReasonUnknown
	if parsed, ok := ParseZombieReason(reason); ok {
		normalized = parsed.String()
	}
	phase := taskPhase(task)
	if err := store.Record(ctx, string(task.TaskIdentifier), phase, normalized); err != nil {
		glog.Warningf(
			"event=redrive_reason_write_failed task=%s phase=%q error=%v",
			task.TaskIdentifier,
			phase,
			err,
		)
	}
}

// taskPhase returns the task's phase as a string, or "" when it carries none.
// The reason ref normalizes "" to the frozen "none" component.
func taskPhase(task lib.Task) string {
	if phase := task.Frontmatter.Phase(); phase != nil {
		return string(*phase)
	}
	return ""
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

// crashFailureUpdates builds the frontmatter Updates for a zombie failure. Every
// class clears current_job. The pod_crash_no_stdout class additionally arms the
// trigger cap, because it is the one class whose counter nothing acts on: the
// deliverer never sees the record (the pod died before writing stdout), so no
// outcome is ever published for it, and without a cap the controller's
// assignee-clear — the only operator-inbox signal this path has — never fires.
//
// The gate is ParseZombieReason, not string equality. PublishFailure takes reason
// as a raw string and is shared by all eight ZombieReason values, several of
// which already carry their own escalation semantics; matching on the parsed
// vocabulary keeps a suffixed form such as "pod_oom_killed (OOMKilled/137)"
// correctly classified as its own class rather than falling through to a literal
// comparison that cannot match it.
//
// An already-present max_triggers is left alone. The controller's write path is a
// blind Updates apply, so writing here would clobber an operator's deliberate
// budget — the store holds records at 5 and 6, and halving one of those is a
// regression, not a repair.
func crashFailureUpdates(task lib.Task, reason string) lib.TaskFrontmatter {
	updates := lib.TaskFrontmatter{"current_job": ""}
	parsed, ok := ParseZombieReason(reason)
	if !ok || parsed != ZombieReasonPodCrashNoStdout {
		return updates
	}
	if _, present := task.Frontmatter["max_triggers"]; present {
		return updates
	}
	updates["max_triggers"] = defaultMaxTriggers
	return updates
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
		Updates:        crashFailureUpdates(task, reason),
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
	// and the retry allows trigger_count to eventually bump so the controller's
	// increment executor clears assignee at the cap.
	//
	// Order between the update and the increment is NOT guaranteed: they are two
	// independent commands and nothing keys them to one partition. When the
	// increment lands first it sees no max_triggers and clears nothing, and the
	// cap takes effect from the next failure cycle instead. That costs one cycle,
	// never the escalation: the update is idempotent, so every later cycle finds
	// the key present and the assignee-clear fires there.
	p.dedupe.recordDedupe(jobName)

	// Recorded here, after the dedupe key, so a repeated informer delivery of
	// the same job cannot double-record. The record is swallowed on failure:
	// evidence gathering must never turn a delivered failure into an error.
	//
	// The streak advances BEFORE the reason, and only on this path: the streak
	// counts published failures, so a spawn that publishes none — an exit-0
	// human park, a successful run, a suppressed spawn — never consumes the
	// re-drive budget. See recordRedriveFailure.
	p.recordRedriveFailure(ctx, task)
	p.recordRedriveReason(ctx, task, reason)

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

// PublishRestoreAssignee writes a task's routing value back onto its frontmatter
// after an agent cleared it and parked the task. The value is the resolved
// configuration's assignee — never the task's recorded prior assignee, which is
// a history of what the task carried before it was parked rather than a routing
// decision.
//
// This is an UpdateFrontmatterCommand rather than a result publish for the same
// reason PublishSetTriggerScope is: the write must land on the on-disk
// frontmatter directly (buildUpdateModifyFn), not be merged through the result
// writer's ownership guard. PublishTypeMismatchFailure is the precedent that the
// controller's update path accepts an "assignee" key — it writes that key on the
// failure path.
//
// Updates carries exactly one key, the routing value, and nothing else: this
// restores routing and touches no counter, no scope, no current_job, no status
// and no phase. The write is idempotent — every pass over a task the controller
// has not yet updated re-publishes the same resolved value, and rewriting the
// same assignee is a no-op on the file.
func (p *resultPublisher) PublishRestoreAssignee(
	ctx context.Context,
	task lib.Task,
	assignee string,
) error {
	cmd := taskcmd.UpdateFrontmatterCommand{
		TaskIdentifier: task.TaskIdentifier,
		TargetVault:    targetVaultFromTask(task),
		Updates: lib.TaskFrontmatter{
			"assignee": assignee,
		},
	}
	return p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd)
}

// PublishParkLostClaim writes phase "human_review" and nothing else.
func (p *resultPublisher) PublishParkLostClaim(
	ctx context.Context,
	task lib.Task,
) error {
	cmd := taskcmd.UpdateFrontmatterCommand{
		TaskIdentifier: task.TaskIdentifier,
		TargetVault:    targetVaultFromTask(task),
		Updates: lib.TaskFrontmatter{
			"phase": string(domain.TaskPhaseHumanReview),
		},
	}
	return p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd)
}

// PublishAbort closes a record the consecutive-re-drive cap has stopped
// driving. It writes status, aborted_reason, gate_successor and current_job and
// touches no other key.
//
// gate_successor is not optional: vault-cli freezes the close-out pair as
// aborted_reason + gate_successor, and its SetStatus rejects aborted when
// either is missing, so a task written without it would sit in a state no
// vault-cli write could then reproduce.
//
// This is an UpdateFrontmatterCommand rather than a result publish for the same
// reason PublishSetTriggerScope is: the write must land on the on-disk
// frontmatter directly (buildUpdateModifyFn), not be merged through the result
// writer's ownership guard.
//
// Idempotent twice. A task already carrying status aborted is left untouched,
// and a successful publish is deduped per task through the same TTL'd LRU as
// PublishFailure — the cap check runs on every event and every pass, and a
// write the controller has not yet applied must not be re-published each time.
// The dedupe key is recorded only after publishRaw succeeds, so a failed send
// is retried on the next event instead of being suppressed for the TTL.
func (p *resultPublisher) PublishAbort(
	ctx context.Context,
	task lib.Task,
	reason string,
	streak int,
) error {
	if task.Frontmatter.Status() == domain.TaskStatusAborted {
		return nil
	}
	dedupeKey := "abort:" + string(task.TaskIdentifier)
	if p.dedupe.checkDedupe(dedupeKey) {
		glog.V(2).Infof(
			"event=redrive_abort_dedupe task=%s reason=%s",
			task.TaskIdentifier,
			reason,
		)
		return nil
	}
	cmd := taskcmd.UpdateFrontmatterCommand{
		TaskIdentifier: task.TaskIdentifier,
		TargetVault:    targetVaultFromTask(task),
		Updates: lib.TaskFrontmatter{
			"status":         string(domain.TaskStatusAborted),
			"aborted_reason": reason,
			"gate_successor": "none",
			"current_job":    "",
		},
	}
	if err := p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd); err != nil {
		return errors.Wrapf(
			ctx,
			err,
			"publish abort for task %s",
			task.TaskIdentifier,
		)
	}
	metrics.RedriveAbortedTotal.WithLabelValues(reason).Inc()
	// V(0): a record leaving the re-drive loop is not a steady state, so it must
	// be visible without raising a verbosity flag.
	glog.Infof(
		"event=redrive_aborted task=%s task_type=%s reason=%s streak=%d phase=%q",
		task.TaskIdentifier,
		task.Frontmatter.TaskType().String(),
		reason,
		streak,
		taskPhase(task),
	)
	p.dedupe.recordDedupe(dedupeKey)
	return nil
}

// PublishQueueEntry writes only the queued_at key: the moment the task first
// began waiting behind the concurrency cap. Every other frontmatter key is left
// untouched, so a consumer reading the task file can compute a queue age for the
// agent-execution stage without this write disturbing the spawn state.
func (p *resultPublisher) PublishQueueEntry(
	ctx context.Context,
	task lib.Task,
	queuedAt string,
) error {
	cmd := taskcmd.UpdateFrontmatterCommand{
		TaskIdentifier: task.TaskIdentifier,
		TargetVault:    targetVaultFromTask(task),
		Updates: lib.TaskFrontmatter{
			"queued_at": queuedAt,
		},
	}
	return p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd)
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
