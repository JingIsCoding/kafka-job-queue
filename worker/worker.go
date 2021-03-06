package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/JingIsCoding/kafka_job_queue/job"
	queueJob "github.com/JingIsCoding/kafka_job_queue/job"
	"github.com/JingIsCoding/kafka_job_queue/queue"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var counter uint64

type Worker interface {
	Run()
	Stop() error
}

type kafkaWorker struct {
	name          string
	queue         queue.Queue
	consumerGroup ConsumerGroup
}

func newWorker(name string, consumerGroup ConsumerGroup) Worker {
	return &kafkaWorker{
		name:          name,
		queue:         consumerGroup.GetQueue(),
		consumerGroup: consumerGroup,
	}
}

func (worker *kafkaWorker) Run() {
	queue := worker.queue
	consumerGroup := worker.consumerGroup

	doneChan := queue.GetDoneChan()

	defaultJobChan := consumerGroup.GetDefaultJobChan()
	delayedJobChan := consumerGroup.GetDelayedJobChan()
	deadJobChan := consumerGroup.GetDeadJobChan()

	defaultQueueConsumer := consumerGroup.GetDefaultQueueConsumer()
	delayedQueueConsumer := consumerGroup.GetDelayedQueueConsumer()
	deadQueueConsumer := consumerGroup.GetDeadQueueConsumer()

	for {
		select {
		case <-doneChan:
			{
				worker.Stop()
				return
			}
		case msg := <-defaultJobChan:
			{
				job, err := parseJob(msg)
				if err != nil {
					worker.queue.GetLogger().Errorf("Failed to parse job%v", err)
					defaultQueueConsumer.CommitMessage(&msg)
					continue
				}
				def, err := worker.queue.GetJobDefinition(job.Name)
				if err != nil {
					worker.queue.GetLogger().Errorf("Failed to get job definition%v", err)
					continue
				}
				result := processJob(def.Perform, job)
				job.SetResult(result)
				if result.Ok() {
					if def.OnSuccess != nil {
						job := queueJob.NewJob(def.OnSuccess.JobName, result.Value())
						worker.queue.Enqueue(job)
					}
				} else {
					if err = retryJob(worker.queue, def, job); err != nil {
						// insert into dead job queue
						worker.queue.EnqueueTo(job, worker.queue.GetConfig().DeadQueueTopic)
						if def.OnFailure != nil {
							job := queueJob.NewJob(def.OnFailure.JobName, err)
							worker.queue.Enqueue(job)
						}
					}
				}
				defaultQueueConsumer.CommitMessage(&msg)
			}
		case msg := <-delayedJobChan:
			{
				job, err := parseJob(msg)
				if err != nil {
					worker.queue.GetLogger().Errorf("Failed to parse job%v", err)
					continue
				}
				schedule(queue, delayedQueueConsumer, msg, job)
				continue
			}
		case msg := <-deadJobChan:
			{
				job, err := parseJob(msg)
				if err != nil {
					worker.queue.GetLogger().Errorf("Failed to parse job%v", err)
					continue
				}
				deadQueueConsumer.CommitMessage(&msg)
				queue.GetLogger().Errorf("Job has been dead %v\n", job)
				continue
			}
		}
	}
}

func (worker *kafkaWorker) Stop() error {
	return nil
}

func processJob(perform queueJob.Perform, job queueJob.Job) queueJob.JobResult {
	data, err := perform(context.Background(), job)
	return queueJob.NewJobResult(err == nil, data, err)
}

func retryJob(q queue.Queue, def *queueJob.JobDefinition, job queueJob.Job) error {
	if job.Retries >= def.Retries {
		return queue.TooManyRetries
	}
	delayedJob := queueJob.NewDelayedJob(job.Name, 15*time.Second, job.Args)
	return q.EnqueueTo(delayedJob, q.GetConfig().DeplayedQueueTopic)
}

func schedule(q queue.Queue, consumer Consumer, msg kafka.Message, job queueJob.Job) {
	now := time.Now()
	if job.ReadyTime.After(now) {
		tilJobReady := job.ReadyTime.Sub(now)
		timer := time.NewTimer(tilJobReady)
		go func() {
			<-timer.C
			if err := q.Enqueue(job); err == nil {
				consumer.CommitMessage(&msg)
			}
		}()
	} else {
		if err := q.Enqueue(job); err == nil {
			consumer.CommitMessage(&msg)
		}
	}
}

func parseJob(msg kafka.Message) (job.Job, error) {
	job := job.Job{}
	err := json.Unmarshal(msg.Value, &job)
	if err != nil {
		return job, fmt.Errorf("Failed to parse to a job %w", err)
	}
	return job, nil
}
