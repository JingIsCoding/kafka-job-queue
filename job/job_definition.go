package job

import "time"

type JobDefinition struct {
	JobName   string
	FIFO      bool
	Retries   int
	Deplayed  *time.Duration
	Perform   Perform
	OnFailure *JobDefinition
	OnSuccess *JobDefinition
}

func NewJobDefinition(jobName string, retries int, perform Perform) JobDefinition {
	return JobDefinition{
		JobName:  jobName,
		Retries:  retries,
		Deplayed: nil,
		Perform:  perform,
		FIFO:     false,
	}
}

func NewDelayedJobDefinition(jobName string, retries int, deplayed *time.Duration, perform Perform) JobDefinition {
	return JobDefinition{
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
