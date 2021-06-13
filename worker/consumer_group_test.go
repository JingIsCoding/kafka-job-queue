package worker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/JingIsCoding/kafka_job_queue/job"
	"github.com/JingIsCoding/kafka_job_queue/test"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type unitTest struct {
	name string
	run  func()
}

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing context
type ConsumerGroupTestSuite struct {
	suite.Suite
	mockQueue    *test.MockQueue
	mockConsumer *test.MockConsumer
}

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *ConsumerGroupTestSuite) SetupTest() {
	suite.mockQueue = new(test.MockQueue)
	suite.mockConsumer = new(test.MockConsumer)
}

// All methods that begin with "Test" are run as tests within a
// suite.
func (suite *ConsumerGroupTestSuite) TestHandleDelayedMessage() {
	tests := []unitTest{
		unitTest{
			name: "should get error if partition error exists",
			run: func() {
				value, _ := json.Marshal(job.Job{})
				msg := &kafka.Message{
					Value: value,
					TopicPartition: kafka.TopicPartition{
						Error: &kafka.Error{},
					},
				}
				_, err := handleDelayedMessage(msg, suite.mockQueue, suite.mockConsumer)
				suite.NotEmpty(err, "should return err")
			},
		},
		unitTest{
			name: "should enqueue if it has reached next retry",
			run: func() {
				ago := time.Now().Add(-1 * time.Second)
				nextRetry := &ago
				retryJob := job.Job{
					NextRetry: nextRetry,
				}
				value, _ := json.Marshal(retryJob)
				msg := &kafka.Message{
					Value: value,
					TopicPartition: kafka.TopicPartition{
						Error: nil,
					},
				}
				suite.mockQueue.On("Enqueue", mock.Anything).Return(nil)
				suite.mockConsumer.On("CommitMessage", msg).Return(nil, nil)
				duration, err := handleDelayedMessage(msg, suite.mockQueue, suite.mockConsumer)

				suite.Empty(err, "should not return err")
				suite.Equal(duration, 1*time.Millisecond, "should enqueue after 1 mill second")
			},
		},
		unitTest{
			name: "should get correct duration til next retry",
			run: func() {
				now := time.Now()
				future := now.Add(10 * time.Second)
				nextRetry := &future
				retryJob := job.Job{
					NextRetry: nextRetry,
				}
				value, _ := json.Marshal(retryJob)
				msg := &kafka.Message{
					Value: value,
					TopicPartition: kafka.TopicPartition{
						Error: nil,
					},
				}
				suite.mockQueue.On("Enqueue", mock.Anything).Return(nil)
				suite.mockConsumer.On("CommitMessage", msg).Return(nil, nil)
				duration, err := handleDelayedMessage(msg, suite.mockQueue, suite.mockConsumer)

				suite.Empty(err, "should not return err")
				suite.Greater(duration, 990*time.Millisecond, "should give some time to reenqueu")
			},
		},
	}
	for _, test := range tests {
		suite.Run(test.name, test.run)
	}
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(ConsumerGroupTestSuite))
}
