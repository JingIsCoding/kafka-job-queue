## Job Queue on Kafka

**kafka-job-queue** is an asynchronous job queue build on top of confluent kafka, it is inspired by [Machinery](https://github.com/RichardKnop/machinery) 
and [faktory](https://github.com/contribsys/faktory). 


## Dependencies
This project relies on [confluent kafka](https://github.com/confluentinc/confluent-kafka-go) to communicate with kafka cluster.

## Get started
#### Install the library by
```
go get -u github.com/JingIsCoding/kafka-job-queue
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


## Configs
A quick way to setup a real connection to remote kafka server is to define a config file ~/.confluent/librdkafka.config
```
# Kafka
bootstrap.servers=YOUR_BROKER_URL
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=YOUR_USERNAME
sasl.password=YOUR_PASSWORD
```
And you could build your queue config like
```golang
config := queue.NewConfigWithConfigFilePath("/Users/you/.confluent.confluent/librdkafka.config")
// Create a job queue
jobQueue, err := queue.NewQueue(config)
```

## Topics
The job system current use three queues to store tasks, default-job-queue is where all the active jobs are stored, default-delayed-queue are all the retries jobs are stored, and default-dead-queue is where the dead jobs are stored. you could choose to over ride the topics by changing the config
```golang
config.DefaultQueueTopic = "some-other-name-queue"
```
Currently there is not priority on jobs, will probably introduce more queues later to implement that.

## Order and partitions
By default all tasks are distributed evenly across all parititions by hash the uuid of the job, so there is no guaranteed orders on tasks, if you absolutely need to keep tasks in order, you will have to change the JobDefinition to be FIFO, however, that will make your kafka topic unevenly distributed on partitions
