// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"sync"
	"time"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

var errEmptyTaskEvent = stderrors.New("empty task event value")

var errEmptyTaskIdentifier = stderrors.New("task event has empty taskIdentifier")

// decodeTaskEvent decodes one task event value. It is the single decode path
// shared by the live consumer and the task view.
func decodeTaskEvent(ctx context.Context, value []byte) (lib.Task, error) {
	if len(value) == 0 {
		return lib.Task{}, errEmptyTaskEvent
	}
	var task lib.Task
	if err := json.Unmarshal(value, &task); err != nil {
		return lib.Task{}, errors.Wrapf(ctx, err, "unmarshal task event")
	}
	if task.TaskIdentifier == "" {
		return lib.Task{}, errEmptyTaskIdentifier
	}
	return task, nil
}

// HighwaterMarksFunc returns the high-water mark of every partition of topic.
type HighwaterMarksFunc func(ctx context.Context, topic libkafka.Topic) (libkafka.PartitionOffsets, error)

//counterfeiter:generate -o ../../mocks/task_view.go --fake-name FakeTaskView . TaskView

// TaskView is an in-memory, latest-wins view of the compacted task event topic.
type TaskView interface {
	// CaptureHighWaterMarks records the topic's per-partition high-water marks at
	// start. It must be called once, before the replay consumer starts.
	CaptureHighWaterMarks(ctx context.Context) error
	// ConsumeMessage applies one task event. It is the MessageHandler the replay
	// consumer is built with.
	ConsumeMessage(ctx context.Context, msg *sarama.ConsumerMessage) error
	// Ready reports whether every partition reached the mark captured at start.
	Ready() bool
	// Snapshot returns a point-in-time copy of every task in the view.
	Snapshot() []lib.Task
	// Get returns the task for id, or false when the view has none.
	Get(id lib.TaskIdentifier) (lib.Task, bool)
}

// NewTaskView creates an empty view for topic.
func NewTaskView(
	highwaterMarks HighwaterMarksFunc,
	topic libkafka.Topic,
	currentDateTimeGetter libtime.CurrentDateTimeGetter,
) TaskView {
	return &taskView{
		tasks:                 make(map[lib.TaskIdentifier]lib.Task),
		consumed:              make(map[libkafka.Partition]libkafka.Offset),
		highwaterMarks:        highwaterMarks,
		topic:                 topic,
		currentDateTimeGetter: currentDateTimeGetter,
	}
}

type taskView struct {
	mu                    sync.RWMutex
	tasks                 map[lib.TaskIdentifier]lib.Task
	marks                 libkafka.PartitionOffsets
	consumed              map[libkafka.Partition]libkafka.Offset
	marksCaptured         bool
	ready                 bool
	startedAt             libtime.DateTime
	highwaterMarks        HighwaterMarksFunc
	topic                 libkafka.Topic
	currentDateTimeGetter libtime.CurrentDateTimeGetter
}

// CaptureHighWaterMarks implements TaskView.
func (v *taskView) CaptureHighWaterMarks(ctx context.Context) error {
	v.mu.Lock()
	v.startedAt = v.currentDateTimeGetter.Now()
	v.mu.Unlock()

	marks, err := v.highwaterMarks(ctx, v.topic)
	if err != nil {
		return errors.Wrapf(ctx, err, "capture high-water marks for topic %s", v.topic)
	}

	v.mu.Lock()
	v.marks = marks
	v.marksCaptured = true
	becameReady := v.evaluateReadinessLocked()
	v.mu.Unlock()

	if becameReady {
		v.logReady()
	}
	return nil
}

// ConsumeMessage implements TaskView.
func (v *taskView) ConsumeMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	task, err := decodeTaskEvent(ctx, msg.Value)

	v.mu.Lock()
	switch {
	case err == nil:
		// A task at a terminal phase is never spawned again (see IsTerminal), and
		// the view retains a body only so a spawn can render the task file. So the
		// body of a terminal task is dead weight: the skip decisions read
		// frontmatter alone. Dropping it here is what keeps the view inside its
		// container limit — the body is ~88% of a task's bytes, and at 15k tasks
		// retaining them all puts the live heap at ~105MiB, which GOGC then doubles
		// past a 256Mi limit and OOM-kills every cold start. A reopened task
		// publishes a new event, which carries the body back.
		if phase := task.Frontmatter.Phase(); phase != nil && IsTerminal(*phase) {
			task.Content = ""
		}
		v.tasks[task.TaskIdentifier] = task
	case stderrors.Is(err, errEmptyTaskEvent):
		// Tombstone: the key names the task to remove.
		delete(v.tasks, lib.TaskIdentifier(msg.Key))
	default:
		// A decode failure drops any stale entry for the key and never blocks
		// readiness — one bad record must not wedge the view.
		delete(v.tasks, lib.TaskIdentifier(msg.Key))
	}
	partition := libkafka.Partition(msg.Partition)
	if next := libkafka.Offset(msg.Offset + 1); next > v.consumed[partition] {
		v.consumed[partition] = next
	}
	becameReady := v.evaluateReadinessLocked()
	v.mu.Unlock()

	switch {
	case err == nil, stderrors.Is(err, errEmptyTaskEvent):
		// Nothing to report.
	default:
		metrics.TaskViewSkippedTotal.Inc()
		glog.Warningf(
			"event=task_view_skip partition=%d offset=%d err=%v",
			msg.Partition, msg.Offset, err,
		)
	}
	if becameReady {
		v.logReady()
	}
	return nil
}

// evaluateReadinessLocked reports whether this evaluation made the view ready.
// The caller must hold the write lock; logging happens after the lock is
// released.
func (v *taskView) evaluateReadinessLocked() bool {
	if v.ready || !v.marksCaptured {
		return false
	}
	for partition, mark := range v.marks {
		if v.consumed[partition] < mark {
			return false
		}
	}
	v.ready = true
	return true
}

// logReady emits the one-time readiness line. Called without the lock held.
func (v *taskView) logReady() {
	v.mu.RLock()
	tasks := len(v.tasks)
	// retained_bodies is the memory driver: the view's live heap is dominated by
	// the bodies it holds, so this number next to tasks= is what tells an operator
	// whether the retention rule is doing its job on a real vault. A value near
	// tasks= means bodies are being kept for tasks that can never spawn.
	retainedBodies := 0
	for _, task := range v.tasks {
		if task.Content != "" {
			retainedBodies++
		}
	}
	partitions := len(v.marks)
	elapsed := time.Time(v.currentDateTimeGetter.Now()).Sub(time.Time(v.startedAt))
	v.mu.RUnlock()
	glog.Infof(
		"event=task_view_ready tasks=%d retained_bodies=%d partitions=%d elapsed=%s",
		tasks, retainedBodies, partitions, elapsed,
	)
}

// Ready implements TaskView.
func (v *taskView) Ready() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.ready
}

// Snapshot implements TaskView.
func (v *taskView) Snapshot() []lib.Task {
	v.mu.RLock()
	defer v.mu.RUnlock()
	result := make([]lib.Task, 0, len(v.tasks))
	for _, task := range v.tasks {
		result = append(result, task)
	}
	return result
}

// Get implements TaskView.
func (v *taskView) Get(id lib.TaskIdentifier) (lib.Task, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	task, ok := v.tasks[id]
	return task, ok
}
