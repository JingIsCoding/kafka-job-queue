package worker

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer interface {
	Assign([]kafka.TopicPartition) error
	Assignment() ([]kafka.TopicPartition, error)
	Unassign() error
	Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	Poll(int) kafka.Event
	Events() chan kafka.Event
	Commit() ([]kafka.TopicPartition, error)
	CommitMessage(*kafka.Message) ([]kafka.TopicPartition, error)
	GetConsumerGroupMetadata() (*kafka.ConsumerGroupMetadata, error)
	GetMetadata(topic *string, allTopics bool, timeout int) (*kafka.Metadata, error)
	GetWatermarkOffsets(topic string, partition int32) (int64, int64, error)
	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (int64, int64, error)
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

func (consumer *kafkaConsumer) Assignment() ([]kafka.TopicPartition, error) {
	return consumer.inner.Assignment()
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

func (consumer *kafkaConsumer) Commit() ([]kafka.TopicPartition, error) {
	return consumer.inner.Commit()
}

func (consumer *kafkaConsumer) GetConsumerGroupMetadata() (*kafka.ConsumerGroupMetadata, error) {
	return consumer.inner.GetConsumerGroupMetadata()
}

func (consumer *kafkaConsumer) GetMetadata(topic *string, allTopics bool, timeout int) (*kafka.Metadata, error) {
	return consumer.inner.GetMetadata(topic, allTopics, timeout)
}

func (consumer *kafkaConsumer) GetWatermarkOffsets(topic string, partition int32) (int64, int64, error) {
	return consumer.inner.GetWatermarkOffsets(topic, partition)
}

func (consumer *kafkaConsumer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (int64, int64, error) {
	return consumer.inner.QueryWatermarkOffsets(topic, partition, timeoutMs)
}

func (consumer *kafkaConsumer) Close() error {
	return consumer.inner.Close()
}
