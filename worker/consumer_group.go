package worker

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/JingIsCoding/kafka_job_queue/queue"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConsumerGroup interface {
	Start() error
	Close() error
	GetQueue() queue.Queue

	GetDefaultQueueConsumer() Consumer
	GetDelayedQueueConsumer() Consumer
	GetDeadQueueConsumer() Consumer

	GetDefaultJobChan() chan kafka.Message
	GetDelayedJobChan() chan kafka.Message
	GetDeadJobChan() chan kafka.Message
}

type kafkaConsumerGroup struct {
	config queue.Config
	queue  queue.Queue

	defaultQueueConsumer Consumer
	delayedQueueConsumer Consumer
	deadQueueConsumer    Consumer

	defaultJobChan  chan kafka.Message
	delayedJobChan  chan kafka.Message
	deadJobChan     chan kafka.Message
	waitWorkerGroup *sync.WaitGroup
}

func NewConsumerGroupFromQueue(queue queue.Queue) (ConsumerGroup, error) {
	config := queue.GetConfig()
	kafkaConfig := config.KafkaConfig
	defaultQueueConsumer, err := newConsumer(defaultConsumerConfig(config.ConsumerGroupID, kafkaConfig))
	if err != nil {
		return nil, fmt.Errorf("Failed to create consumer %w", err)
	}
	delayedQueueConsumer, err := newConsumer(defaultConsumerConfig(config.ConsumerGroupID, kafkaConfig))
	if err != nil {
		return nil, fmt.Errorf("Failed to create consumer %w", err)
	}
	deadQueueConsumer, err := newConsumer(defaultConsumerConfig(config.ConsumerGroupID, kafkaConfig))
	if err != nil {
		return nil, fmt.Errorf("Failed to create consumer %w", err)
	}
	return &kafkaConsumerGroup{
		queue:                queue,
		config:               config,
		defaultQueueConsumer: defaultQueueConsumer,
		delayedQueueConsumer: delayedQueueConsumer,
		deadQueueConsumer:    deadQueueConsumer,

		defaultJobChan: make(chan kafka.Message, config.Concurrency),
		delayedJobChan: make(chan kafka.Message, config.Concurrency),
		deadJobChan:    make(chan kafka.Message, config.Concurrency),

		waitWorkerGroup: &sync.WaitGroup{},
	}, nil
}

func (group *kafkaConsumerGroup) Start() error {
	config := group.queue.GetConfig()
	if err := group.defaultQueueConsumer.Subscribe(config.DefaultQueueTopic, nil); err != nil {
		return fmt.Errorf("Failed to subscribe to %s %w", config.DefaultQueueTopic, err)
	}
	if err := group.delayedQueueConsumer.Subscribe(config.DeplayedQueueTopic, nil); err != nil {
		return fmt.Errorf("Failed to subscribe to %s %w", config.DefaultQueueTopic, err)
	}
	if err := group.deadQueueConsumer.Subscribe(config.DeadQueueTopic, nil); err != nil {
		return fmt.Errorf("Failed to subscribe to %s %w", config.DefaultQueueTopic, err)
	}
	dispatchJob(group.queue, group.defaultQueueConsumer, func(msg kafka.Message) {
		group.defaultJobChan <- msg
	})
	dispatchJob(group.queue, group.delayedQueueConsumer, func(msg kafka.Message) {
		group.delayedJobChan <- msg
	})
	dispatchJob(group.queue, group.deadQueueConsumer, func(msg kafka.Message) {
		group.deadJobChan <- msg
	})

	// Start workers
	for i := 1; i <= group.config.Concurrency; i++ {
		worker := newWorker(fmt.Sprintf("%d", i), group)
		group.waitWorkerGroup.Add(1)
		// delay initial fetch randomly to prevent thundering herd.
		// this will pause between 0 and 2B nanoseconds, i.e. 0-2 seconds
		time.Sleep(time.Duration(rand.Int31()))
		go func() {
			defer group.waitWorkerGroup.Done()
			worker.Run()
		}()
	}
	group.queue.GetLogger().Debugf("Consumer group is ready")
	group.waitWorkerGroup.Wait()
	return nil
}

func (group *kafkaConsumerGroup) GetQueue() queue.Queue {
	return group.queue
}

func (group *kafkaConsumerGroup) GetDefaultJobChan() chan kafka.Message {
	return group.defaultJobChan
}

func (group *kafkaConsumerGroup) GetDelayedJobChan() chan kafka.Message {
	return group.delayedJobChan
}

func (group *kafkaConsumerGroup) GetDeadJobChan() chan kafka.Message {
	return group.deadJobChan
}

func (group *kafkaConsumerGroup) GetDefaultQueueConsumer() Consumer {
	return group.defaultQueueConsumer
}

func (group *kafkaConsumerGroup) GetDelayedQueueConsumer() Consumer {
	return group.delayedQueueConsumer
}

func (group *kafkaConsumerGroup) GetDeadQueueConsumer() Consumer {
	return group.deadQueueConsumer
}

func (group *kafkaConsumerGroup) Close() error {
	if err := group.defaultQueueConsumer.Close(); err != nil {
		return err
	}
	if err := group.delayedQueueConsumer.Close(); err != nil {
		return err
	}
	if err := group.deadQueueConsumer.Close(); err != nil {
		return err
	}
	return nil
}

func dispatchJob(queue queue.Queue, consumer Consumer, dispatch func(kafka.Message)) {
	go func() {
		for {
			select {
			case <-queue.GetDoneChan():
				return
			case ev := <-consumer.Events():
				switch e := ev.(type) {
				case kafka.AssignedPartitions:
					consumer.Assign(e.Partitions)
					continue
				case kafka.RevokedPartitions:
					consumer.Unassign()
					continue
				case *kafka.Message:
					if e.TopicPartition.Error != nil {
						queue.GetLogger().Errorf("Failed to get message %w \n", e.TopicPartition.Error)
						// Do not know what else to do than continue
						consumer.CommitMessage(e)
						continue
					}
					dispatch(*e)
				case kafka.PartitionEOF:
					continue
				case kafka.Error:
					// Errors should generally be considered as informational, the client will try to automatically recover
					if e.IsFatal() {
						queue.GetLogger().Errorf("Can not recover from %v \n", e)
						return
					}
					queue.GetLogger().Errorf("%% Error: %v\n", e)
				}
			}
		}
	}()
}

func defaultConsumerConfig(groupId string, config *kafka.ConfigMap) *kafka.ConfigMap {
	newConfig := kafka.ConfigMap{}
	for k, v := range *config {
		newConfig[k] = v
	}
	newConfig.SetKey("group.id", groupId)
	//newConfig.SetKey("session.timeout.ms", 6000)
	newConfig.SetKey("go.events.channel.enable", true)
	newConfig.SetKey("go.application.rebalance.enable", true)
	newConfig.SetKey("enable.partition.eof", true)
	newConfig.SetKey("auto.offset.reset", "earliest")
	newConfig.SetKey("enable.auto.commit", false)
	return &newConfig
}
