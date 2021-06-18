package stat

import (
	"fmt"
	"log"
	"os"

	"github.com/JingIsCoding/kafka_job_queue/queue"
	jobQueue "github.com/JingIsCoding/kafka_job_queue/queue"
	"github.com/JingIsCoding/kafka_job_queue/worker"
)

type QueueStat interface {
	NumberOfProcessedTasks() (int64, error)
	NumberOfDeplayedTasks() (int64, error)
	NumberOfDeadTasks() (int64, error)
}

type kafkaQueueStat struct {
	config    queue.Config
	queue     queue.Queue
	admin     queue.AdminClient
	producer  queue.Producer
	consumers worker.ConsumerGroup
}

func NewKafkaQueueStat(queue jobQueue.Queue) QueueStat {
	admin, err := jobQueue.NewAdminClient(queue.GetConfig().KafkaConfig)
	if err != nil {
		queue.GetLogger().Errorf("Failed to create queue stat", err)
		os.Exit(1)
	}
	producer, err := jobQueue.NewKafkaProducer(queue.GetConfig().KafkaConfig)
	if err != nil {
		queue.GetLogger().Errorf("Failed to create queue stat", err)
		os.Exit(1)
	}
	consumers, err := worker.NewConsumerGroupFromQueue(queue)
	if err != nil {
		queue.GetLogger().Errorf("Failed to create queue stat", err)
		os.Exit(1)
	}
	return &kafkaQueueStat{
		config:    queue.GetConfig(),
		queue:     queue,
		admin:     admin,
		producer:  producer,
		consumers: consumers,
	}
}

func (stat *kafkaQueueStat) NumberOfProcessedTasks() (int64, error) {
	partitions, err := stat.consumers.GetDefaultQueueConsumer().Commit()
	if err != nil {
		return 0, fmt.Errorf("Failed to commit %w", err)
	}
	for i, partition := range partitions {
		log.Println("partition ", i, " is ", partition)
	}
	return 0, nil
}

func (stat *kafkaQueueStat) NumberOfDeplayedTasks() (int64, error) {
	partitions, err := stat.consumers.GetDelayedQueueConsumer().Commit()
	if err != nil {
		return 0, fmt.Errorf("Failed to commit %w", err)
	}
	for i, partition := range partitions {
		log.Println("partition ", i, " is ", partition)
	}
	return 0, nil
}

func (stat *kafkaQueueStat) NumberOfDeadTasks() (int64, error) {
	partitions, err := stat.consumers.GetDeadQueueConsumer().Commit()
	if err != nil {
		return 0, fmt.Errorf("Failed to commit %w", err)
	}
	for i, partition := range partitions {
		log.Println("partition ", i, " is ", partition)
	}
	return 0, nil
}
