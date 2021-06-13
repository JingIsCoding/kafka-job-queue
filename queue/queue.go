package queue

import (
	"github.com/JingIsCoding/kafka_job_queue/job"
	"github.com/JingIsCoding/kafka_job_queue/log"
)

type Queue interface {
	Init() error
	Start()
	Stop()
	Register(job.JobDefinition) error
	Enqueue(job.Job) error
	EnqueueTo(job.Job, string) error
	SetLogger(log.Logger)
	GetLogger() log.Logger
	GetConfig() Config
	GetDoneChan() chan bool
	GetJobDefinition(jobName string) (*job.JobDefinition, error)
}
