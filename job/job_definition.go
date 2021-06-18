package job

type JobDefinition struct {
	JobName   string
	FIFO      bool
	Retries   int
	Perform   Perform
	OnFailure *JobDefinition
	OnSuccess *JobDefinition
}

func NewJobDefinition(jobName string, retries int, perform Perform) JobDefinition {
	return JobDefinition{
		JobName: jobName,
		Retries: retries,
		Perform: perform,
		FIFO:    false,
	}
}

func (definition *JobDefinition) SetFIFO(fifo bool) {
	definition.FIFO = fifo
}
