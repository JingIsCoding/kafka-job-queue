package job

import (
	"context"
	"log"
	"time"

	"github.com/gofrs/uuid"
)

// Argument to pass to the perform function
type Argument interface{}

// Value to store for the result of the perform function
type Value interface{}

// Perform actually executes the job.
// It must be thread-safe.
type Perform func(ctx context.Context, job Job) (Value, error)

type Job struct {
	ID        uuid.UUID
	Name      string
	Args      []Argument
	Retries   int
	ReadyTime *time.Time
	result    JobResult
}

func NewJob(name string, args ...Argument) Job {
	uuid, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("Can not generate uuid %v\n", err)
	}
	return Job{
		ID:      uuid,
		Name:    name,
		Args:    args,
		Retries: 0,
	}
}

func NewDelayedJob(name string, delayed time.Duration, args ...Argument) Job {
	readyTime := time.Now().Add(delayed)
	job := NewJob(name, args)
	job.ReadyTime = &readyTime
	return job
}

func NewSchedulJob(name string, future time.Time, args ...Argument) Job {
	delay := future.Sub(time.Now())
	return NewDelayedJob(name, delay, args)
}

func (job *Job) SetResult(result JobResult) {
	job.result = result
}

func (job *Job) GetResult() JobResult {
	return job.result
}

type JobResult struct {
	ok  bool
	val interface{}
	err error
}

func (result JobResult) Ok() bool {
	return result.ok
}

func (result JobResult) Value() interface{} {
	return result.val
}

func (result JobResult) Error() error {
	return result.err
}

func NewJobResult(ok bool, val interface{}, err error) JobResult {
	return JobResult{
		ok:  ok,
		val: val,
		err: err,
	}
}
