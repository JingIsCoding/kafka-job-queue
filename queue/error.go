package queue

import "errors"

var (
	JobDefinitionNotExists = errors.New("JobDefinitionNotExists")
	TooManyRetries         = errors.New("TooManyRetries")
)
