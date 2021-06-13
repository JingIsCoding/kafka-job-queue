package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Worker interface {
	Start()
	Stop() error
}

type kafkaWorker struct {
	name               string
	queue              Queue
	queueLifecycleChan chan string
}

func (worker *kafkaWorker) Start() {
	for {
		select {
		case event := <-worker.queue.getEventChan():
			{
				switch ev := event.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						fmt.Errorf("Failed to get message %w", ev.TopicPartition.Error)
					} else {
						log.Println(worker.name + "receives a job ")
						go func() {
							job := Job{}
							err := json.Unmarshal(ev.Value, &job)
							if err != nil {
								fmt.Errorf("Failed to parse json %w", err)
								return
							}
							def, err := worker.queue.getJobDefinition(job.Name)
							if err != nil {
								fmt.Errorf("Failed to get job definition%w", err)
								return
							}
							result := processJob(def.Perform, job)
							if !result.Ok() {
								if err = <-retryJob(worker.queue, def, &job); err != nil {
									if def.OnFailure != nil {
										job := NewJob(def.OnSuccess.JobName, []JobArgument{err})
										worker.queue.Enqueue(job)
									}
								}
							} else {
								if def.OnSuccess != nil {
									job := NewJob(def.OnSuccess.JobName, []JobArgument{result.data})
									worker.queue.Enqueue(job)
								}
							}
						}()

					}

				case *kafka.Error:
				}
			}
		case <-worker.queue.getQueueChan():
			{
				log.Println(worker.name, "stops")
				worker.Stop()
				return
			}
		}
	}
}

func processJob(perform Perform, job Job) JobResult {
	data, err := perform(context.Background(), job)
	return NewJobResult(err == nil, data, err)
}

func retryJob(queue Queue, def *JobDefinition, job *Job) chan error {
	result := make(chan error)
	if job.Retries >= def.Retries {
		result <- errors.New("Too many retries")
	}
	newJob := job.Clone()
	newJob.Retries += 1
	timer := time.NewTimer(time.Duration(newJob.Retries) * 10 * time.Second)
	go func() {
		<-timer.C
		result <- queue.Enqueue(newJob)
	}()
	return result
}

func (worker *kafkaWorker) Stop() error {
	return nil
}
