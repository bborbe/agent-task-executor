// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafkatest_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/log"
	"github.com/bborbe/run"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// recordingHandler records the payloads one consumer group received.
type recordingHandler struct {
	mu   sync.Mutex
	seen []string
}

func (r *recordingHandler) ConsumeMessage(
	_ context.Context,
	msg *sarama.ConsumerMessage,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, string(msg.Value))
	return nil
}

// Seen returns a copy so the assertion never races the consumer goroutine.
func (r *recordingHandler) Seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

var _ libkafka.MessageHandler = &recordingHandler{}

// This is a live-broker test: it needs a reachable Kafka (KAFKA_BROKERS) and is
// skipped without one, the same way pkg/envtest skips without KUBEBUILDER_ASSETS.
//
// Prerequisites beyond reachability: the credentials need CreateTopics and
// DeleteTopics ACLs, because the spec creates a uniquely-named topic with
// ReplicationFactor 1 and deletes it on the way out. A crash between the two
// leaves an agent-task-executor-group-isolation-* topic behind on the broker.
//
// It encodes the failure that motivated per-deployment consumer groups: the task
// event topic has ONE partition, so a single shared group hands the whole topic to
// ONE member. That member filters by assignee, skips what it cannot run, and
// commits the offsets — the other clusters never see those events. Distinct groups
// are independent offset domains, so every group receives every event.
var _ = Describe("two consumer groups on one topic", func() {
	It("both receive every event", func() {
		brokers := os.Getenv("KAFKA_BROKERS")
		if brokers == "" {
			Skip("KAFKA_BROKERS not set: live-broker test, run it against the dev cluster")
		}

		config := sarama.NewConfig()
		config.Version = sarama.V2_1_0_0
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
		// NewSyncProducerFromClient validates this: sarama leaves
		// Producer.Return.Successes disabled, and a SyncProducer built from a
		// client that disables it fails with a ConfigurationError. Without this
		// line the spec can never pass on a real broker — and because it Skips
		// when KAFKA_BROKERS is unset, CI would never have shown it.
		config.Producer.Return.Successes = true

		client, err := sarama.NewClient(strings.Split(brokers, ","), config)
		Expect(err).To(BeNil())
		defer func() { _ = client.Close() }()

		admin, err := sarama.NewClusterAdminFromClient(client)
		Expect(err).To(BeNil())
		defer func() { _ = admin.Close() }()

		topic := libkafka.Topic(fmt.Sprintf(
			"agent-task-executor-group-isolation-%d", time.Now().UnixNano(),
		))
		Expect(admin.CreateTopic(string(topic), &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)).To(Succeed())
		defer func() { _ = admin.DeleteTopic(string(topic)) }()

		producer, err := sarama.NewSyncProducerFromClient(client)
		Expect(err).To(BeNil())
		defer func() { _ = producer.Close() }()

		const events = 5
		for i := range events {
			_, _, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: string(topic),
				Value: sarama.StringEncoder(fmt.Sprintf("event-%d", i)),
			})
			Expect(err).To(BeNil())
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		groupA := &recordingHandler{}
		groupB := &recordingHandler{}
		// Join both consumers before the deferred DeleteTopic runs, so cleanup is
		// deterministic and a failing Eventually cannot leave a goroutine reading
		// a topic that is being deleted underneath it.
		var wg sync.WaitGroup
		start := func(group string, handler *recordingHandler) {
			consumer := libkafka.NewOffsetConsumerHighwaterMarks(
				client,
				topic,
				libkafka.NewSaramaOffsetManager(
					client,
					libkafka.Group(group),
					libkafka.OffsetOldest,
					libkafka.OffsetOldest,
				),
				handler,
				run.NewTrigger(),
				log.DefaultSamplerFactory,
			)
			wg.Go(func() {
				_ = consumer.Consume(ctx)
			})
		}
		start(string(topic)+"-a", groupA)
		start(string(topic)+"-b", groupB)

		Eventually(groupA.Seen, "30s", "200ms").Should(HaveLen(events))
		Eventually(groupB.Seen, "30s", "200ms").Should(HaveLen(events))
		Expect(groupA.Seen()).To(ConsistOf(
			"event-0", "event-1", "event-2", "event-3", "event-4",
		))
		Expect(groupB.Seen()).To(ConsistOf(
			"event-0", "event-1", "event-2", "event-3", "event-4",
		))

		cancel()
		wg.Wait()
	})
})
