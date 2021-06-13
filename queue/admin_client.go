package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type AdminClient interface {
	DescribeConfigs(ctx context.Context, resources []kafka.ConfigResource, options ...kafka.DescribeConfigsAdminOption) (result []kafka.ConfigResourceResult, err error)
	CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) (result []kafka.TopicResult, err error)
	Close()

	createTopicsIfNotExist(partition int, factor int, topics ...string) error
}

type kafkaAdmin struct {
	inner *kafka.AdminClient
}

func newAdminClient(conf *kafka.ConfigMap) (AdminClient, error) {
	client, err := kafka.NewAdminClient(conf)
	return &kafkaAdmin{inner: client}, err
}

func (admin *kafkaAdmin) DescribeConfigs(ctx context.Context, resources []kafka.ConfigResource, options ...kafka.DescribeConfigsAdminOption) (result []kafka.ConfigResourceResult, err error) {
	return admin.inner.DescribeConfigs(ctx, resources, options...)
}

func (admin *kafkaAdmin) CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) (result []kafka.TopicResult, err error) {
	return admin.inner.CreateTopics(ctx, topics, options...)
}

func (admin *kafkaAdmin) Close() {
	admin.inner.Close()
}

func (admin *kafkaAdmin) createTopicsIfNotExist(partition int, factor int, topics ...string) error {
	missingTopics := []string{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}
	describeConfigs := []kafka.ConfigResource{}
	for _, topic := range topics {
		describeConfigs = append(describeConfigs, kafka.ConfigResource{
			Type: kafka.ResourceTopic,
			Name: topic,
		})
	}

	results, err := admin.DescribeConfigs(ctx, describeConfigs, kafka.SetAdminRequestTimeout(maxDur))
	if err != nil {
		return fmt.Errorf("Failed to describe configs: %v\n", err)
	}
	for _, result := range results {
		if result.Error.Code() == kafka.ErrUnknownTopicOrPart || result.Error.Code() == kafka.ErrUnknownTopic {
			missingTopics = append(missingTopics, result.Name)
		}
	}

	specifications := []kafka.TopicSpecification{}
	for _, topic := range missingTopics {
		specifications = append(specifications, kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     partition,
			ReplicationFactor: factor,
		})
	}
	if len(missingTopics) == 0 {
		return nil
	}
	createTopicResults, err := admin.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		specifications,
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		return fmt.Errorf("Failed to create topic: %v\n", err)
	}
	for _, r := range createTopicResults {
		if r.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("Failed to create topics %v\n", r.Error)
		}
	}
	return nil
}
