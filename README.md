## Job Queue on Kafka

**kafka_job_queue** is an asynchronous job queue build on top of confluent kafka, it is inspired by [Machinery](https://github.com/RichardKnop/machinery) 
and [faktory](https://github.com/contribsys/faktory). 


## Dependencies
This project relies on [confluent kafka](https://github.com/confluentinc/confluent-kafka-go) to communicate with kafka cluster.

## Get started
#### Install the library by
```
go get -u github.com/JingIsCoding/kafka_job_queue
```

#### Examples
Create a queue to push tasks to
```golang
func main() {
	// Define config
	config := queue.NewConfig(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
	})

	// Create a job queue
	jobQueue, err := queue.NewQueue(config)
	if err != nil {
		log.Println("failed ", err)
		os.Exit(1)
	}

	// Init a queue by creating all the necessary topics
	err = jobQueue.Init()
	if err != nil {
		log.Println("failed to initiate queue ", err)
		os.Exit(1)
	}

	// Create a thread to push tasks
	go func() {
		jobQueue.Enqueue(job.NewJob("Add", []job.JobArgument{3, 5}))
	}()

	// This function will not return
	jobQueue.Start()
}
```

Create a consumer group to consume those tasks
```golang
func main() {
	config := queue.NewConfig(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
	})

	// Create a job queue
	jobQueue, err := queue.NewQueue(config)
	if err != nil {
		log.Println("failed ", err)
		os.Exit(1)
	}

	// Define a job to add two numbers
	definition := job.JobDefinition{
		JobName: "Add",
		Retries: 3,
		// this is the function you should define to process the task,
		// and return error if it fails to trigger retry
		Perform: func(ctx context.Context, job job.Job) (interface{}, error) {
			x := job.Args[0].(float64)
			y := job.Args[1].(float64)
			return x + y, nil
		},
	}

	jobQueue.Register(definition)

	// Create a consumerGroup to listen to the incoming tasks
	consumerGroup, err := worker.NewConsumerGroupFromQueue(jobQueue)
	if err != nil {
		os.Exit(1)
	}
	consumerGroup.Start()
}
```
