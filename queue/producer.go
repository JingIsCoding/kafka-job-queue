package queue

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Producer interface {
	Produce(*kafka.Message, chan kafka.Event) error
	Events() chan kafka.Event
	Close()
}

type kafkaProducer struct {
	inner *kafka.Producer
}

func newKafkaProducer(configMap *kafka.ConfigMap) (Producer, error) {
	producer, err := kafka.NewProducer(configMap)
	return &kafkaProducer{inner: producer}, err
}

func (producer *kafkaProducer) Produce(msg *kafka.Message, delivery chan kafka.Event) error {
	return producer.inner.Produce(msg, delivery)
}

func (producer *kafkaProducer) Events() chan kafka.Event {
	return producer.inner.Events()
}

func (producer *kafkaProducer) Close() {
	producer.inner.Close()
}
