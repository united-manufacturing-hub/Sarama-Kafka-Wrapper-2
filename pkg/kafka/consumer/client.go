package consumer

import (
	"Sarama-Kafka-Wrapper-2/pkg/kafka/shared"
	"context"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"sync/atomic"
)

// Consumer is a wrapper around the sarama consumer group
// Messages must be marked as consumed, otherwise they will be re-delivered
type Consumer struct {
	brokers               []string
	topic                 string
	consumerGroup         *sarama.ConsumerGroup
	running               atomic.Bool
	incomingMessages      chan *shared.KafkaMessage
	consumerContextCancel context.CancelFunc
	messagesToMark        chan *shared.KafkaMessage

	markedMessages   atomic.Uint64
	consumedMessages atomic.Uint64
}

// NewConsumer creates a new consumer, given a list of brokers, a topic and a group name
func NewConsumer(brokers []string, topic string, groupName string) (*Consumer, error) {

	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupName, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		brokers:          brokers,
		topic:            topic,
		consumerGroup:    &consumerGroup,
		running:          atomic.Bool{},
		incomingMessages: make(chan *shared.KafkaMessage, 100_000),
		messagesToMark:   make(chan *shared.KafkaMessage, 100_000),
	}, nil
}

func (c *Consumer) Start(ctx context.Context) error {
	// Return if already running
	if c.running.Swap(true) {
		return nil
	}
	internalCtx, cancel := context.WithCancel(ctx)
	c.consumerContextCancel = cancel

	go func(running *atomic.Bool, consumerGroup *sarama.ConsumerGroup, topic string, incomingMessages chan *shared.KafkaMessage, messagesToMark chan *shared.KafkaMessage) {
		for running.Load() {

			err := (*consumerGroup).Consume(internalCtx, []string{topic}, &GroupHandler{
				incomingMessages: incomingMessages,
				messagesToMark:   messagesToMark,
				running:          running,
				markedMessages:   &c.markedMessages,
				consumedMessages: &c.consumedMessages,
			})
			if err != nil {
				running.Store(false)
				zap.S().Error(err)
			}
		}

	}(&c.running, c.consumerGroup, c.topic, c.incomingMessages, c.messagesToMark)

	return nil
}

func (c *Consumer) Close() error {
	// Return if already stopped / not running
	if !c.running.Swap(false) {
		return nil
	}
	c.running.Store(false)
	c.consumerContextCancel()
	(*c.consumerGroup).PauseAll()
	return (*c.consumerGroup).Close()
}

func (c *Consumer) IsRunning() bool {
	return c.running.Load()
}

func (c *Consumer) GetMessage() *shared.KafkaMessage {
	select {
	case message := <-c.incomingMessages:
		return message
	default:
		return nil
	}
}

func (c *Consumer) GetMessages() chan *shared.KafkaMessage {
	return c.incomingMessages
}

func (c *Consumer) MarkMessage(message *shared.KafkaMessage) {
	c.messagesToMark <- message
}

func (c *Consumer) MarkMessages(messages []*shared.KafkaMessage) {
	for _, message := range messages {
		c.messagesToMark <- message
	}
}

// GetStats returns the number of messages marked and consumed
func (c *Consumer) GetStats() (uint64, uint64) {
	return c.markedMessages.Load(), c.consumedMessages.Load()
}
