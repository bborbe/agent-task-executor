// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
	"strings"

	"github.com/IBM/sarama"
	lib "github.com/bborbe/agent"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bborbe/agent-task-executor/pkg/handler"
	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// captureStderr runs fn with glog's stderr output redirected into a pipe and
// returns everything written. Specs in this suite run serially, so the global
// os.Stderr swap cannot race another spec's logging.
func captureStderr(fn func()) string {
	Expect(flag.Set("logtostderr", "true")).To(Succeed())
	defer func() {
		Expect(flag.Set("logtostderr", "false")).To(Succeed())
	}()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	Expect(err).ToNot(HaveOccurred())
	os.Stderr = writer

	fn()
	glog.Flush()

	os.Stderr = original
	Expect(writer.Close()).To(Succeed())
	out, err := io.ReadAll(reader)
	Expect(err).ToNot(HaveOccurred())
	Expect(reader.Close()).To(Succeed())
	return string(out)
}

// taskEventMessage builds a Kafka message carrying the JSON encoding of task.
func taskEventMessage(
	partition int32,
	offset int64,
	key string,
	task lib.Task,
) *sarama.ConsumerMessage {
	value, err := json.Marshal(task)
	Expect(err).ToNot(HaveOccurred())
	return &sarama.ConsumerMessage{
		Partition: partition,
		Offset:    offset,
		Key:       []byte(key),
		Value:     value,
	}
}

// tombstoneMessage builds a tombstone message whose key names the task to delete.
func tombstoneMessage(partition int32, offset int64, key string) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Partition: partition,
		Offset:    offset,
		Key:       []byte(key),
		Value:     nil,
	}
}

// newTestTaskView builds a view whose high-water marks are fixed at marks.
func newTestTaskView(marks libkafka.PartitionOffsets) handler.TaskView {
	return handler.NewTaskView(
		func(_ context.Context, _ libkafka.Topic) (libkafka.PartitionOffsets, error) {
			return marks, nil
		},
		libkafka.Topic("test-topic"),
		libtime.NewCurrentDateTime(),
	)
}

var _ = Describe("TaskView", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It(
		"replays every partition, keeps the latest version, honours tombstones and reports readiness",
		func() {
			view := newTestTaskView(libkafka.PartitionOffsets{
				0: 3,
				1: 1,
			})
			Expect(view.CaptureHighWaterMarks(ctx)).To(Succeed())

			tidA := lib.Task{
				TaskIdentifier: "tid-a",
				Frontmatter:    lib.TaskFrontmatter{"version": 1},
			}
			tidAUpdated := lib.Task{
				TaskIdentifier: "tid-a",
				Frontmatter:    lib.TaskFrontmatter{"version": 2},
			}
			tidB := lib.Task{
				TaskIdentifier: "tid-b",
				Frontmatter:    lib.TaskFrontmatter{"version": 1},
			}

			Expect(view.ConsumeMessage(ctx, taskEventMessage(0, 0, "tid-a", tidA))).To(Succeed())
			Expect(
				view.ConsumeMessage(ctx, taskEventMessage(0, 1, "tid-a", tidAUpdated)),
			).To(Succeed())
			Expect(view.ConsumeMessage(ctx, taskEventMessage(0, 2, "tid-b", tidB))).To(Succeed())

			// Partition 0 has reached its mark, but partition 1's tombstone has not
			// been applied yet.
			Expect(view.Ready()).To(BeFalse())

			Expect(view.ConsumeMessage(ctx, tombstoneMessage(1, 0, "tid-b"))).To(Succeed())
			Expect(view.Ready()).To(BeTrue())

			snapshot := view.Snapshot()
			Expect(snapshot).To(HaveLen(1))
			Expect(snapshot[0].TaskIdentifier).To(Equal(lib.TaskIdentifier("tid-a")))
			Expect(snapshot[0].Frontmatter["version"]).To(Equal(float64(2)))

			_, ok := view.Get("tid-b")
			Expect(ok).To(BeFalse())
		},
	)

	It("does not let an empty partition hold readiness back", func() {
		view := newTestTaskView(libkafka.PartitionOffsets{
			0: 1,
			1: 0,
		})
		Expect(view.CaptureHighWaterMarks(ctx)).To(Succeed())
		Expect(view.Ready()).To(BeFalse())

		Expect(view.ConsumeMessage(ctx, taskEventMessage(0, 0, "tid-a", lib.Task{
			TaskIdentifier: "tid-a",
		}))).To(Succeed())

		Expect(view.Ready()).To(BeTrue())
	})

	It("keeps applying every partition after ready", func() {
		view := newTestTaskView(libkafka.PartitionOffsets{
			0: 1,
			1: 0,
		})
		Expect(view.CaptureHighWaterMarks(ctx)).To(Succeed())
		Expect(view.ConsumeMessage(ctx, taskEventMessage(0, 0, "tid-a", lib.Task{
			TaskIdentifier: "tid-a",
		}))).To(Succeed())
		Expect(view.Ready()).To(BeTrue())

		Expect(view.ConsumeMessage(ctx, taskEventMessage(0, 1, "tid-b", lib.Task{
			TaskIdentifier: "tid-b",
		}))).To(Succeed())
		Expect(view.ConsumeMessage(ctx, taskEventMessage(1, 0, "tid-c", lib.Task{
			TaskIdentifier: "tid-c",
		}))).To(Succeed())

		snapshot := view.Snapshot()
		ids := make([]lib.TaskIdentifier, 0, len(snapshot))
		for _, task := range snapshot {
			ids = append(ids, task.TaskIdentifier)
		}
		Expect(ids).To(ConsistOf(
			lib.TaskIdentifier("tid-a"),
			lib.TaskIdentifier("tid-b"),
			lib.TaskIdentifier("tid-c"),
		))
	})

	It("removes the entry and counts a decode failure", func() {
		view := newTestTaskView(libkafka.PartitionOffsets{0: 0})
		Expect(view.CaptureHighWaterMarks(ctx)).To(Succeed())
		Expect(view.Ready()).To(BeTrue())

		Expect(view.ConsumeMessage(ctx, taskEventMessage(0, 0, "tid-a", lib.Task{
			TaskIdentifier: "tid-a",
		}))).To(Succeed())
		_, ok := view.Get("tid-a")
		Expect(ok).To(BeTrue())

		before := testutil.ToFloat64(metrics.TaskViewSkippedTotal)
		Expect(view.ConsumeMessage(ctx, &sarama.ConsumerMessage{
			Partition: 0,
			Offset:    1,
			Key:       []byte("tid-a"),
			Value:     []byte("not-json"),
		})).To(Succeed())

		_, ok = view.Get("tid-a")
		Expect(ok).To(BeFalse())
		Expect(testutil.ToFloat64(metrics.TaskViewSkippedTotal)).To(Equal(before + 1))
	})

	It("logs readiness exactly once", func() {
		view := newTestTaskView(libkafka.PartitionOffsets{0: 0})

		out := captureStderr(func() {
			Expect(view.CaptureHighWaterMarks(ctx)).To(Succeed())
			Expect(view.ConsumeMessage(ctx, taskEventMessage(0, 0, "tid-a", lib.Task{
				TaskIdentifier: "tid-a",
			}))).To(Succeed())
		})

		Expect(strings.Count(out, "event=task_view_ready")).To(Equal(1))
		Expect(out).To(ContainSubstring("tasks=0"))
		Expect(out).To(ContainSubstring("partitions=1"))
	})

	It("returns a high-water-mark capture error and stays unready", func() {
		view := handler.NewTaskView(
			func(_ context.Context, _ libkafka.Topic) (libkafka.PartitionOffsets, error) {
				return nil, os.ErrDeadlineExceeded
			},
			libkafka.Topic("test-topic"),
			libtime.NewCurrentDateTime(),
		)

		Expect(view.CaptureHighWaterMarks(ctx)).ToNot(Succeed())
		Expect(view.Ready()).To(BeFalse())
	})
})
