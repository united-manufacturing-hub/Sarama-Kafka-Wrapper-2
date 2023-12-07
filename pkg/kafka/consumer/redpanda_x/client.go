package redpanda_x

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper-2/pkg/kafka/shared"
	"go.uber.org/zap"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type Consumer struct {
	subscribeRegexes   []*regexp.Regexp
	topics             []string
	topicsMutex        sync.RWMutex
	consumerGroup      *sarama.ConsumerGroup
	client             *sarama.Client
	groupId            string
	incomingMessages   chan *shared.KafkaMessage
	messagesToMarkChan chan *shared.KafkaMessage
	read               atomic.Uint64
	marked             atomic.Uint64
	isReady            atomic.Bool
}

func NewConsumer(kafkaBrokers, subscribeRegexes []string, groupId, instanceId string) (*Consumer, error) {
	zap.S().Infof("Connecting to brokers: %v", kafkaBrokers)
	zap.S().Infof("Creating new consumer with Group ID: %s, Instance ID: %s", groupId, instanceId)
	zap.S().Infof("Subscribing to topics: %v", subscribeRegexes)

	sarama.Logger = zap.NewStdLog(zap.L())

	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	config.Consumer.Group.InstanceId = instanceId
	config.Version = sarama.V2_3_0_0
	config.Metadata.RefreshFrequency = 1 * time.Minute

	c := Consumer{}
	c.subscribeRegexes = make([]*regexp.Regexp, len(subscribeRegexes))
	for i, regex := range subscribeRegexes {
		re, err := regexp.Compile(regex)
		if err != nil {
			zap.S().Errorf("Failed to compile regex: %v", err)
			return nil, err
		}
		c.subscribeRegexes[i] = re
	}

	zap.S().Debugf("Setting up channels")
	c.incomingMessages = make(chan *shared.KafkaMessage, 100_000)
	c.messagesToMarkChan = make(chan *shared.KafkaMessage, 100_000)

	zap.S().Debugf("Setting up client")
	newClient, err := sarama.NewClient(kafkaBrokers, config)
	if err != nil {
		zap.S().Errorf("Failed to create new client: %v", err)
		return nil, err
	}
	c.client = &newClient

	zap.S().Debugf("Setting up consumer group")
	consumerGroup, err := sarama.NewConsumerGroupFromClient(groupId, newClient)
	if err != nil {
		zap.S().Errorf("Failed to create consumer group: %v", err)
		return nil, err
	}
	c.consumerGroup = &consumerGroup
	c.groupId = groupId

	zap.S().Debugf("Retrieving topics")
	var topics []string
	for {
		err = newClient.RefreshMetadata()
		if err != nil {
			zap.S().Errorf("Failed to refresh metadata: %v", err)
			return nil, err
		}

		topics, err = newClient.Topics()
		if err != nil {
			zap.S().Errorf("Failed to retrieve topics: %v", err)
			return nil, err
		}
		zap.S().Debugf("Filtering topics")
		topics = filter(topics, c.subscribeRegexes)
		if len(topics) > 0 {
			c.topicsMutex.Lock()
			c.topics = topics
			c.topicsMutex.Unlock()
			break
		}
		zap.S().Infof("No topics found. Waiting for 1 second")
		time.Sleep(1 * time.Second)
	}

	readyChan := make(chan bool, 1)
	c.topicsMutex.RLock()
	zap.S().Debugf("Starting consumer for %v", c.topics)
	c.topicsMutex.RUnlock()
	go c.start(readyChan)
	zap.S().Debugf("Waiting for consumer to start")
	<-readyChan
	c.isReady.Store(true)
	zap.S().Debugf("Consumer loop started")
	go c.refreshTopics()

	zap.S().Infof("Finished setting up consumer")
	return &c, nil
}

func (c *Consumer) start(ready chan bool) {
	zap.S().Infof("Starting consumer loop")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		var topics []string
		c.topicsMutex.RLock()
		topics = make([]string, len(c.topics))
		copy(topics, c.topics)
		c.topicsMutex.RUnlock()
		if len(topics) == 0 {
			zap.S().Infof("No topics to consume. Waiting for 1 second")
			time.Sleep(1 * time.Second)
			continue
		}
		zap.S().Debugf("Consuming topics: %v", topics)
		consumer := ConsumerGroupHandler{
			ready:              ready,
			incomingMessages:   c.incomingMessages,
			messagesToMarkChan: c.messagesToMarkChan,
			read:               &c.read,
			marked:             &c.marked,
		}
		err := (*c.consumerGroup).Consume(ctx, topics, &consumer)
		if errors.Is(err, sarama.ErrClosedConsumerGroup) {
			zap.S().Infof("Consumer group closed")
			return
		} else if err != nil {
			zap.S().Errorf("Error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			zap.S().Infof("Context err from consumer: %v", ctx.Err())
			return
		}
		ready = make(chan bool, 1)
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *Consumer) refreshTopics() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		<-ticker.C
		zap.S().Debugf("Refreshing metadata")

		err := (*c.client).RefreshMetadata()
		if err != nil {
			zap.S().Errorf("Error refreshing metadata: %v", err)
			continue
		}

		topics, err := (*c.client).Topics()
		if err != nil {
			zap.S().Errorf("Error getting topics: %v", err)
			continue
		}

		topics = filter(topics, c.subscribeRegexes)
		c.topicsMutex.RLock()
		compare := slices.Compare(c.topics, topics)
		c.topicsMutex.RUnlock()
		if compare == 0 {
			zap.S().Infof("No change in topics")
			continue
		}
		c.topicsMutex.RLock()
		zap.S().Infof("Detected topic change. Old topics: %v, New topics: %v", c.topics, topics)
		c.topicsMutex.RUnlock()
		c.isReady.Store(false)
		err = (*c.consumerGroup).Close()
		if err != nil {
			zap.S().Errorf("Failed to close consumer group: %v", err)
			continue
		}

		// This is for debugging purposes only
		zap.S().Debugf("Getting partitions for %v", topics)
		for _, topic := range topics {
			partitions, err := (*c.client).WritablePartitions(topic)
			if err != nil {
				zap.S().Errorf("Failed to get partitions for topic %s: %v", topic, err)
				continue
			}
			zap.S().Debugf("Partitions for topic %s: %v", topic, partitions)
		}

		// Give the old goroutine a chance to exit
		time.Sleep(1 * time.Second)
		zap.S().Debugf("Creating new consumer group")
		newConsumerGroup, err := sarama.NewConsumerGroupFromClient(c.groupId, *c.client)
		if err != nil {
			zap.S().Errorf("Error creating consumer group: %v", err)
			continue
		}
		c.consumerGroup = &newConsumerGroup
		c.topicsMutex.Lock()
		c.topics = topics
		c.topicsMutex.Unlock()
		readyChan := make(chan bool, 1)
		go c.start(readyChan)
		timeout := time.NewTimer(10 * time.Second)
		select {
		case <-timeout.C:
			zap.S().Errorf("Timeout waiting for consumer to start")
			continue
		case <-readyChan:
		}
		c.isReady.Store(true)
		// Reset the ticker to avoid spamming the API
		ticker = time.NewTicker(5 * time.Second)
	}
}

// GetStats returns consumed message counts.
func (c *Consumer) GetStats() (uint64, uint64) {
	return c.marked.Load(), c.read.Load()
}

// GetTopics returns the topics that the consumer is subscribed to.
func (c *Consumer) GetTopics() []string {
	c.topicsMutex.RLock()
	topics := make([]string, len(c.topics))
	copy(topics, c.topics)
	c.topicsMutex.RUnlock()
	return topics
}

// GetMessage returns the next message from the consumer.
func (c *Consumer) GetMessage() *shared.KafkaMessage {
	return <-c.incomingMessages
}

// GetMessages returns the channel of messages from the consumer.
func (c *Consumer) GetMessages() <-chan *shared.KafkaMessage {
	return c.incomingMessages
}

// MarkMessage marks a message as processed.
func (c *Consumer) MarkMessage(message *shared.KafkaMessage) {
	c.messagesToMarkChan <- message
}

// MarkMessages marks a slice of messages as processed.
func (c *Consumer) MarkMessages(messages []*shared.KafkaMessage) {
	for _, message := range messages {
		c.messagesToMarkChan <- message
	}
}

// IsReady returns whether the consumer is ready to consume messages.
func (c *Consumer) IsReady() bool {
	return c.isReady.Load()
}

func filter(topics []string, regexes []*regexp.Regexp) []string {
	filtered := make(map[string]bool)
	for _, topic := range topics {
		for _, re := range regexes {
			if re.MatchString(topic) {
				filtered[topic] = true
				break
			}
		}
	}
	result := make([]string, 0, len(filtered))
	for topic := range filtered {
		result = append(result, topic)
	}
	zap.S().Debugf("Filtered topics: %v to %v", topics, result)
	slices.Sort(result)
	return result
}