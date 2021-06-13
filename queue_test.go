package queue

import (
	"context"
	"log"
	"os"
	"testing"
)

func TestShouldCreateKafkaQueue(t *testing.T) {
	_, err := NewKafkaQueueWithConfigFile("/Users/jingguo/.confluent/librdkafka.config")
	if err != nil {
		t.Errorf("Failed %w", err)
	}
}

func TestShouldPublishJobs(t *testing.T) {
	kafkaQueue, err := NewKafkaQueueWithConfigFile("/Users/jingguo/.confluent/librdkafka.config")
	if err != nil {
		log.Println("failed ", err)
		os.Exit(1)
	}

	OnSuccess := JobDefinition{
		Topic:   "test-event",
		JobName: "OnAddSuccess",
		Retries: 1,
		Perform: func(ctx context.Context, job Job) (interface{}, error) {
			result := job.Args[0].(float64)
			return result, nil
		},
	}

	definition := JobDefinition{
		Topic:   "test-event",
		JobName: "Add",
		Retries: 1,
		Perform: func(ctx context.Context, job Job) (interface{}, error) {
			x := job.Args[0].(float64)
			y := job.Args[1].(float64)
			return x + y, nil
		},
		OnSuccess: &OnSuccess,
	}

	kafkaQueue.Register(definition)
	kafkaQueue.Start()
	kafkaQueue.Enqueue(NewJob("Add", []JobArgument{3, 2}))
	kafkaQueue.Enqueue(NewJob("Add", []JobArgument{4, 8}))
}
