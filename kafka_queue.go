package queue

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaQueue struct {
	logger          Logger
	taskDefinitions *sync.Map
	producer        *kafka.Producer
	doneChan        chan bool
	waitWorkerGroup *sync.WaitGroup
	concurrency     int
}

func NewKafkaQueue(config kafka.ConfigMap) (Queue, error) {
	// Create Producer instance
	producer, err := kafka.NewProducer(&config)
	if err != nil {
		return nil, fmt.Errorf("Failed to create producer %w", err)
	}
	queue := &kafkaQueue{
		taskDefinitions: new(sync.Map),
		producer:        producer,
		doneChan:        make(chan bool),
		waitWorkerGroup: &sync.WaitGroup{},
		concurrency:     20,
	}
	queue.SetLogger(NewStdLogger())
	return queue, nil
}

func NewKafkaQueueWithConfigFile(configFilePath string) (Queue, error) {
	conf := readCCloudConfig(configFilePath)
	// Create Producer instance
	configMap := kafka.ConfigMap{}
	for k, v := range conf {
		configMap[k] = v
	}
	queue, err := NewKafkaQueue(configMap)
	defer func() {
		sigchan := hookSignals()
		for {
			sig := <-sigchan
			queue.handleEvent(signalMap[sig])
		}
	}()
	return queue, err
}

func (queue *kafkaQueue) Register(def JobDefinition) error {
	if def.Perform == nil {
		return errors.New("Need to define a job perform function")
	}
	queue.taskDefinitions.Store(def.JobName, def)
	return nil
}

func (queue *kafkaQueue) Enqueue(job Job) error {
	def, ok := queue.taskDefinitions.Load(job.Name)
	if !ok {
		return JobDefinitionNotExists
	}
	definition := def.(JobDefinition)
	topic := definition.Topic
	jobJson, err := json.Marshal(job)
	if err != nil {
		return err
	}

	key := job.ID.String()
	if definition.FIFO {
		key = job.Name
	}

	return queue.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          []byte(jobJson),
	}, nil)
}

func (queue *kafkaQueue) Start() {
	for i := 0; i <= queue.concurrency; i++ {
		worker := &kafkaWorker{
			name:  fmt.Sprintf("%d", i),
			queue: queue,
		}
		go func() {
			queue.waitWorkerGroup.Add(1)
			defer queue.waitWorkerGroup.Done()
			worker.Start()
		}()
	}
}

func (queue *kafkaQueue) SetLogger(logger Logger) Queue {
	queue.logger = logger
	return queue
}

func (queue *kafkaQueue) Stop() {
	queue.producer.Close()
	log.Println("before close..")
	close(queue.doneChan)
	log.Println("after close..")
	queue.waitWorkerGroup.Wait()
}

func (queue *kafkaQueue) getJobDefinition(jobName string) (*JobDefinition, error) {
	defRaw, ok := queue.taskDefinitions.Load(jobName)
	if !ok {
		return nil, JobDefinitionNotExists
	}
	def := defRaw.(JobDefinition)
	return &def, nil
}

func (queue *kafkaQueue) getQueueChan() chan bool {
	return queue.doneChan
}

func (queue *kafkaQueue) getEventChan() chan kafka.Event {
	return queue.producer.Events()
}

func (queue *kafkaQueue) handleEvent(event string) {
	switch event {
	case "terminate":
		{
			queue.Stop()
		}
	}
}

func readCCloudConfig(configFile string) map[string]string {
	m := make(map[string]string)
	file, err := os.Open(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
		os.Exit(1)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "#") && len(line) != 0 {
			kv := strings.Split(line, "=")
			parameter := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			m[parameter] = value
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Failed to read file: %s", err)
		os.Exit(1)
	}
	return m
}
