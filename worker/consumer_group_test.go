package worker

import (
	"testing"

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
type ConsumerGroupTestSuite struct {
	suite.Suite
	mockQueue    *test.Queue
	mockConsumer *test.Consumer
}

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *ConsumerGroupTestSuite) SetupTest() {
	suite.mockQueue = new(test.Queue)
	suite.mockConsumer = new(test.Consumer)
}

// All methods that begin with "Test" are run as tests within a
// suite.
func (suite *ConsumerGroupTestSuite) TestHandleDelayedMessage() {
	tests := []unitTest{}
	for _, test := range tests {
		suite.Run(test.name, test.run)
	}
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestConsumerGroupTestSuite(t *testing.T) {
	suite.Run(t, new(ConsumerGroupTestSuite))
}
