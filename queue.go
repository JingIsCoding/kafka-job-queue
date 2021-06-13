package queue

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Queue interface {
	Register(JobDefinition) error
	Enqueue(Job) error
	Start()
	Stop()
	SetLogger(Logger) Queue

	getQueueChan() chan bool
	getEventChan() chan kafka.Event
	getJobDefinition(jobName string) (*JobDefinition, error)
	handleEvent(event string)
}
