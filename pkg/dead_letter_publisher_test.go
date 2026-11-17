// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	stderrors "errors"

	"github.com/IBM/sarama"
	libkafka "github.com/bborbe/kafka"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	pkg "github.com/bborbe/agent-task-executor/pkg"
)

var _ = Describe("KafkaDeadLetterPublisher", func() {
	It("writes the original message to the dead-letter topic with source headers", func() {
		producer := &capturingSyncProducer{}
		publisher := pkg.NewKafkaDeadLetterPublisher(
			producer,
			libkafka.Topic("develop-agent-task-v1-dead-letter"),
		)

		msg := &sarama.ConsumerMessage{
			Topic:     "develop-agent-task-v1-event",
			Partition: 7,
			Offset:    123,
			Key:       []byte("task-key"),
			Value:     []byte("poison-payload"),
		}
		cause := stderrors.New("metadata.name: Invalid value")

		Expect(publisher.Publish(context.Background(), msg, cause)).To(Succeed())

		Expect(producer.messages).To(HaveLen(1))
		out := producer.messages[0]
		Expect(out.Topic).To(Equal("develop-agent-task-v1-dead-letter"))

		key, err := out.Key.Encode()
		Expect(err).NotTo(HaveOccurred())
		Expect(key).To(Equal([]byte("task-key")))

		value, err := out.Value.Encode()
		Expect(err).NotTo(HaveOccurred())
		Expect(value).To(Equal([]byte("poison-payload")))

		headers := map[string]string{}
		for _, h := range out.Headers {
			headers[string(h.Key)] = string(h.Value)
		}
		Expect(headers).To(HaveKeyWithValue("source-topic", "develop-agent-task-v1-event"))
		Expect(headers).To(HaveKeyWithValue("source-partition", "7"))
		Expect(headers).To(HaveKeyWithValue("source-offset", "123"))
		Expect(headers).To(HaveKeyWithValue("error", cause.Error()))
	})

	It("returns the producer error so the caller does not advance the offset", func() {
		producer := &failingSyncProducer{err: stderrors.New("kafka unavailable")}
		publisher := pkg.NewKafkaDeadLetterPublisher(
			producer,
			libkafka.Topic("develop-agent-task-v1-dead-letter"),
		)

		err := publisher.Publish(
			context.Background(),
			&sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 0},
			stderrors.New("boom"),
		)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("kafka unavailable"))
	})
})
