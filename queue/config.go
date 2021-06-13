package queue

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Config struct {
	KafkaConfig *kafka.ConfigMap

	DefaultQueueTopic  string
	DeplayedQueueTopic string
	DeadQueueTopic     string

	ConsumerGroupID   string
	Concurrency       int
	Rebalance         bool
	Partitions        int
	ReplicationFactor int
}

func NewConfig(configMap *kafka.ConfigMap) *Config {
	return &Config{
		KafkaConfig:        configMap,
		DefaultQueueTopic:  "default-job-queue",
		DeplayedQueueTopic: "default-delayed-job-queue",
		DeadQueueTopic:     "default-dead-job-queue",
		ConsumerGroupID:    "default-consumer-group",
		Concurrency:        20,
		Rebalance:          false,
		Partitions:         5,
		ReplicationFactor:  3,
	}
}

func NewConfigWithConfigFilePath(configFilePath string) *Config {
	cloudConf := readCCloudConfig(configFilePath)
	// Create Producer instance
	kafkaConf := kafka.ConfigMap{}
	for k, v := range cloudConf {
		kafkaConf[k] = v
	}
	return NewConfig(&kafkaConf)
}

func (config *Config) Validate() error {
	if config.DefaultQueueTopic == "" {
		return fmt.Errorf("Default queue topic can not be empty")
	}
	if config.DeplayedQueueTopic == "" {
		return fmt.Errorf("Deplayed queue topic can not be empty")
	}
	if config.DeadQueueTopic == "" {
		return fmt.Errorf("Dead topic can not be empty")
	}
	if config.ConsumerGroupID == "" {
		return fmt.Errorf("Consumer group id can not be empty")
	}
	if config.Concurrency <= 0 {
		return fmt.Errorf("Concurrency has to be greater than 0")
	}
	if config.Partitions <= 0 {
		return fmt.Errorf("Partitions number has to be greater than 0")
	}
	if config.ReplicationFactor <= 0 {
		return fmt.Errorf("ReplicationFactor number has to be greater than 0")
	}
	return nil
}

func readCCloudConfig(configFile string) map[string]string {
	m := make(map[string]string)
	file, err := os.Open(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
		os.Exit(1)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "#") && len(line) != 0 {
			kv := strings.Split(line, "=")
			parameter := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			m[parameter] = value
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Failed to read file: %s", err)
		os.Exit(1)
	}
	return m
}
