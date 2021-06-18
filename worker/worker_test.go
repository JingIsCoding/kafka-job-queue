package worker

import (
	"testing"

	"github.com/JingIsCoding/kafka_job_queue/job"
	"github.com/JingIsCoding/kafka_job_queue/test"
	"github.com/stretchr/testify/suite"
)

type unitTest struct {
	name string
	run  func()
}

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing context
type WorkerTestSuite struct {
	suite.Suite
	mockQueue         *test.Queue
	mockConsumerGroup *test.ConsumerGroup

	defaultQueueConsumer *test.Consumer
	delayedQueueConsumer *test.Consumer
	deadQueueConsumer    *test.Consumer

	doneChan       chan bool
	defaultJobChan chan job.Job
	delayedJobChan chan job.Job
	deadJobChan    chan job.Job
}

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *WorkerTestSuite) SetupTest() {
	suite.mockQueue = new(test.Queue)
	suite.mockConsumerGroup = new(test.ConsumerGroup)

	suite.defaultQueueConsumer = new(test.Consumer)
	suite.delayedQueueConsumer = new(test.Consumer)
	suite.deadQueueConsumer = new(test.Consumer)

	suite.doneChan = make(chan bool)
	suite.defaultJobChan = make(chan job.Job)
	suite.delayedJobChan = make(chan job.Job)
	suite.deadJobChan = make(chan job.Job)

	suite.mockConsumerGroup.On("GetDefaultQueueConsumer").Return(suite.defaultQueueConsumer)
	suite.mockConsumerGroup.On("GetDelayedQueueConsumer").Return(suite.delayedQueueConsumer)
	suite.mockConsumerGroup.On("GetDeadQueueConsumer").Return(suite.deadQueueConsumer)
}

// All methods that begin with "Test" are run as tests within a
// suite.
func (suite *WorkerTestSuite) TestRun() {
	tests := []unitTest{
		unitTest{
			name: "should call stop on done chan",
			run:  func() {},
		},
		unitTest{
			name: "should commit message if parse job failed",
			run:  func() {},
		},
		unitTest{
			name: "should call perform on job definition",
			run:  func() {},
		},
		unitTest{
			name: "should enqueue on success callback on job success",
			run:  func() {},
		},
		unitTest{
			name: "should return error if retries exceed limit",
			run:  func() {},
		},
		unitTest{
			name: "should enqueue delayed job on retry",
			run:  func() {},
		},
	}
	for _, test := range tests {
		suite.Run(test.name, test.run)
	}
}

func newMockWoker(suite *WorkerTestSuite) Worker {
	return newWorker("1", suite.mockConsumerGroup)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestWorkerTestSuite(t *testing.T) {
	suite.Run(t, new(WorkerTestSuite))
}
