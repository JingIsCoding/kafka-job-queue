package queue

import (
	"context"
	"log"

	"github.com/gofrs/uuid"
)

// Perform actually executes the job.
// It must be thread-safe.
type Perform func(ctx context.Context, job Job) (interface{}, error)

type JobResult struct {
	ok   bool
	data interface{}
	err  error
}

func (result JobResult) Ok() bool {
	return result.ok
}

func (result JobResult) Data() interface{} {
	return result.data
}

func (result JobResult) Error() error {
	return result.err
}

func NewJobResult(ok bool, data interface{}, err error) JobResult {
	return JobResult{
		ok:   ok,
		data: data,
		err:  err,
	}
}

type JobArgument interface{}

type Job struct {
	ID      uuid.UUID
	Name    string
	Args    []JobArgument
	Retries int
	result  JobResult
}

func NewJob(name string, args []JobArgument) Job {
	uuid, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("Can not generate uuid %v", err)
	}
	return Job{
		ID:   uuid,
		Name: name,
		Args: args,
	}
}

func (job *Job) Clone() Job {
	uuid, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("Can not generate uuid %v", err)
	}
	return Job{
		ID:      uuid,
		Name:    job.Name,
		Args:    job.Args,
		Retries: job.Retries,
	}
}

func (job *Job) SetResult(result JobResult) {
	job.result = result
}

func (job *Job) GetResult() JobResult {
	return job.result
}
