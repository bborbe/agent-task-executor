// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	"github.com/golang/glog"
)

//counterfeiter:generate -o ../mocks/dead_letter_publisher.go --fake-name FakeDeadLetterPublisher . DeadLetterPublisher

// DeadLetterPublisher records a task event the consumer can never process, so
// skipping it does not also erase the evidence of what was skipped.
//
// It is the durable half of the permanent-skip path. The consumer returns nil
// after a successful Publish, which advances the offset past the message; a
// record that lives only in a sampled log line is not recoverable after the fact,
// which is exactly the gap SC2 closes.
type DeadLetterPublisher interface {
	// Publish records msg, the message that failed, together with cause, the
	// error that proved it unprocessable.
	Publish(ctx context.Context, msg *sarama.ConsumerMessage, cause error) error
}

// NewKafkaDeadLetterPublisher returns a DeadLetterPublisher that writes the
// original message to a dead-letter topic. The message key is preserved so
// partition affinity is kept, and the source coordinates plus the error travel
// as record headers.
func NewKafkaDeadLetterPublisher(
	producer libkafka.SyncProducer,
	topic libkafka.Topic,
) DeadLetterPublisher {
	return &kafkaDeadLetterPublisher{producer: producer, topic: topic}
}

type kafkaDeadLetterPublisher struct {
	producer libkafka.SyncProducer
	topic    libkafka.Topic
}

func (p *kafkaDeadLetterPublisher) Publish(
	ctx context.Context,
	msg *sarama.ConsumerMessage,
	cause error,
) error {
	out := &sarama.ProducerMessage{
		Topic: string(p.topic),
		Key:   sarama.ByteEncoder(msg.Key),
		Value: sarama.ByteEncoder(msg.Value),
		Headers: []sarama.RecordHeader{
			{Key: []byte("source-topic"), Value: []byte(msg.Topic)},
			{Key: []byte("source-partition"), Value: []byte(fmt.Sprintf("%d", msg.Partition))},
			{Key: []byte("source-offset"), Value: []byte(fmt.Sprintf("%d", msg.Offset))},
			{Key: []byte("error"), Value: []byte(cause.Error())},
		},
	}
	partition, offset, err := p.producer.SendMessage(ctx, out)
	if err != nil {
		return errors.Wrapf(ctx, err, "publish dead letter to topic %s", p.topic)
	}
	glog.Warningf(
		"event=event_dead_letter source_topic=%s source_partition=%d source_offset=%d dead_letter_topic=%s dead_letter_partition=%d dead_letter_offset=%d err=%v",
		msg.Topic,
		msg.Partition,
		msg.Offset,
		p.topic,
		partition,
		offset,
		cause,
	)
	return nil
}
