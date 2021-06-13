package queue

import "time"

type JobDefinition struct {
	Topic     string
	JobName   string
	FIFO      bool
	Retries   int
	Deplayed  *time.Duration
	Perform   Perform
	OnFailure *JobDefinition
	OnSuccess *JobDefinition
}

func NewJobDefinition(topic string, jobName string, retries int, deplayed *time.Duration, perform Perform) JobDefinition {
	return JobDefinition{
		Topic:    topic,
		JobName:  jobName,
		Retries:  retries,
		Deplayed: deplayed,
		Perform:  perform,
		FIFO:     false,
	}
}

func (definition *JobDefinition) SetFIFO(fifo bool) {
	definition.FIFO = fifo
}
