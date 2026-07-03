// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	kafkamocks "github.com/bborbe/kafka/mocks"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg"
)

// Golden test: proves the published Kafka topic name for the
// agent-task-v1-request topic under an explicit TopicPrefix, and under an
// empty (unprefixed) TopicPrefix. See github.com/bborbe/cqrs base.TopicPrefix
// and cdb.SchemaID.CommandTopic / cdb.BuildTopic for the construction rule:
// non-empty prefix -> "<prefix>-<group>-<kind>-<version>-<suffix>",
// empty prefix -> "<group>-<kind>-<version>-<suffix>" (no leading dash).
var _ = Describe("ResultPublisher topic naming", func() {
	var (
		ctx              context.Context
		currentDateTime  libtime.CurrentDateTime
		fakeSyncProducer *kafkamocks.KafkaSyncProducer
	)

	BeforeEach(func() {
		ctx = context.Background()
		currentDateTime = libtime.NewCurrentDateTime()
		currentDateTime.SetNow(libtimetest.ParseDateTime("2026-04-18T12:00:00Z"))
		fakeSyncProducer = &kafkamocks.KafkaSyncProducer{}
	})

	publish := func(topicPrefix base.TopicPrefix) string {
		publisher := pkg.NewResultPublisher(fakeSyncProducer, topicPrefix, currentDateTime)
		task := lib.Task{
			TaskIdentifier: lib.TaskIdentifier("topic-test-task"),
			Frontmatter: lib.TaskFrontmatter{
				"status":        "in_progress",
				"phase":         "ai_review",
				"assignee":      "claude",
				"trigger_count": 1,
			},
			Content: lib.TaskContent("do the work"),
		}
		Expect(
			publisher.PublishSpawnNotification(ctx, task, "claude-20260418120000"),
		).NotTo(HaveOccurred())

		Expect(fakeSyncProducer.SendMessageCallCount()).To(Equal(1))
		_, msg := fakeSyncProducer.SendMessageArgsForCall(0)
		return msg.Topic
	}

	It("prefixes the topic with the branch when TopicPrefix is set", func() {
		Expect(publish(base.TopicPrefix("develop"))).To(Equal("develop-agent-task-v1-request"))
	})

	It("publishes an unprefixed topic when TopicPrefix is empty", func() {
		Expect(publish(base.TopicPrefix(""))).To(Equal("agent-task-v1-request"))
	})
})
