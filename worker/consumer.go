package worker

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer interface {
	Assign([]kafka.TopicPartition) error
	Unassign() error
	Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	Poll(int) kafka.Event
	Events() chan kafka.Event
	CommitMessage(*kafka.Message) ([]kafka.TopicPartition, error)
	Close() error
}

type kafkaConsumer struct {
	inner *kafka.Consumer
}

func newConsumer(kafkaConfig *kafka.ConfigMap) (Consumer, error) {
	consumer, err := kafka.NewConsumer(kafkaConfig)
	return &kafkaConsumer{
		inner: consumer,
	}, err
}

func (consumer *kafkaConsumer) Assign(partitions []kafka.TopicPartition) error {
	return consumer.inner.Assign(partitions)
}

func (consumer *kafkaConsumer) Unassign() error {
	return consumer.inner.Unassign()
}

func (consumer *kafkaConsumer) Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error {
	return consumer.inner.Subscribe(topic, rebalanceCb)
}

func (consumer *kafkaConsumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error {
	return consumer.inner.SubscribeTopics(topics, rebalanceCb)
}

func (consumer *kafkaConsumer) Poll(ms int) kafka.Event {
	return consumer.inner.Poll(ms)
}

func (consumer *kafkaConsumer) Events() chan kafka.Event {
	return consumer.inner.Events()
}

func (consumer *kafkaConsumer) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	return consumer.inner.CommitMessage(msg)
}

func (consumer *kafkaConsumer) Close() error {
	return consumer.inner.Close()
}
