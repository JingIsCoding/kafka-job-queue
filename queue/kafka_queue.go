package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/JingIsCoding/kafka_job_queue/job"
	queueJob "github.com/JingIsCoding/kafka_job_queue/job"
	"github.com/JingIsCoding/kafka_job_queue/log"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaQueue struct {
	config          *Config
	logger          log.Logger
	taskDefinitions *sync.Map
	admin           AdminClient
	producer        Producer
	doneChan        chan bool
	concurrency     int
}

func newKafkaQueue(configMap *kafka.ConfigMap, queueConfig *Config) (Queue, error) {
	// Create admin instance
	adminClient, err := newAdminClient(configMap)
	if err != nil {
		return nil, fmt.Errorf("Failed to create admin client %w", err)
	}

	// Create Producer instance
	producer, err := newKafkaProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("Failed to create producer %w", err)
	}

	queue := &kafkaQueue{
		config:          queueConfig,
		taskDefinitions: new(sync.Map),
		admin:           adminClient,
		producer:        producer,
		doneChan:        make(chan bool),
		concurrency:     queueConfig.Concurrency,
	}
	queue.SetLogger(log.NewStdLogger())
	go func() {
		sigchan := hookSignals()
		for {
			sig := <-sigchan
			queue.handleEvent(signalMap[sig])
		}
	}()
	return queue, nil
}

func NewQueue(config *Config) (Queue, error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	queue, err := newKafkaQueue(config.KafkaConfig, config)
	return queue, err
}

func (queue *kafkaQueue) Init() error {
	err := queue.admin.createTopicsIfNotExist(queue.config.Partitions, queue.config.ReplicationFactor, queue.config.DefaultQueueTopic, queue.config.DeplayedQueueTopic, queue.config.DeadQueueTopic)
	if err != nil {
		return fmt.Errorf("Failed to initialize queue %w", err)
	}
	return nil
}

func (queue *kafkaQueue) Register(def queueJob.JobDefinition) error {
	if def.Perform == nil {
		return errors.New("Need to define a job perform function")
	}
	queue.taskDefinitions.Store(def.JobName, def)
	return nil
}

func (queue *kafkaQueue) Enqueue(job queueJob.Job) error {
	return queue.EnqueueTo(job, queue.config.DefaultQueueTopic)
}

func (queue *kafkaQueue) EnqueueTo(job queueJob.Job, queueName string) error {
	def, ok := queue.taskDefinitions.Load(job.Name)
	if !ok {
		return JobDefinitionNotExists
	}
	definition := def.(queueJob.JobDefinition)
	jobJson, err := json.Marshal(job)
	if err != nil {
		return err
	}
	key := job.ID.String()
	if definition.FIFO {
		key = job.Name
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &queueName, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          jobJson,
	}
	return queue.producer.Produce(msg, nil)
}

func (queue *kafkaQueue) Start() {
	queue.startProducer()
	<-queue.doneChan
}

func (queue *kafkaQueue) SetLogger(logger log.Logger) {
	queue.logger = logger
}

func (queue *kafkaQueue) Stop() {
	close(queue.doneChan)
	queue.producer.Close()
	queue.admin.Close()
}

func (queue *kafkaQueue) GetConfig() Config {
	return *queue.config
}

func (queue *kafkaQueue) GetJobDefinition(jobName string) (*job.JobDefinition, error) {
	defRaw, ok := queue.taskDefinitions.Load(jobName)
	if !ok {
		return nil, JobDefinitionNotExists
	}
	def := defRaw.(job.JobDefinition)
	return &def, nil
}

func (queue *kafkaQueue) GetLogger() log.Logger {
	return queue.logger
}

func (queue *kafkaQueue) startProducer() {
	go func() {
		for {
			select {
			case <-queue.doneChan:
				{
					return
				}
			case e := <-queue.producer.Events():
				{
					switch ev := e.(type) {
					case *kafka.Message:
						m := ev
						if m.TopicPartition.Error != nil {
							queue.logger.Errorf("Delivery failed: %v\n", m.TopicPartition.Error)
						} else {
							queue.logger.Infof("Delivered message to topic %s %s\n",
								*m.TopicPartition.Topic, string(m.Value))
						}
						continue
					default:
					}
				}
			}
		}
	}()
}

func (queue *kafkaQueue) GetDoneChan() chan bool {
	return queue.doneChan
}

func (queue *kafkaQueue) handleEvent(event string) {
	switch event {
	case "terminate":
		{
			queue.Stop()
		}
	}
}
