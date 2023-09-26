package shared

import (
	"github.com/IBM/sarama"
	"time"
)

const CycleTime = 100 * time.Millisecond

type KafkaMessage struct {
	Headers   map[string]string
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
}

func FromConsumerMessage(message *sarama.ConsumerMessage) *KafkaMessage {
	if message == nil {
		return nil
	}
	m := &KafkaMessage{
		Headers:   make(map[string]string),
		Key:       message.Key,
		Value:     message.Value,
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
	}
	for _, header := range message.Headers {
		m.Headers[string(header.Key)] = string(header.Value)
	}
	return m
}

func ToConsumerMessage(message *KafkaMessage) *sarama.ConsumerMessage {
	if message == nil {
		return nil
	}
	m := &sarama.ConsumerMessage{
		Key:       message.Key,
		Value:     message.Value,
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
	}
	for k, v := range message.Headers {
		m.Headers = append(m.Headers, &sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	return m
}

// ToProducerMessage converts a KafkaMessage to a sarama.ProducerMessage
// It will ignore the Partition and Offset fields
// It also sets trace headers
func ToProducerMessage(message *KafkaMessage) *sarama.ProducerMessage {
	if message == nil {
		return nil
	}
	m := &sarama.ProducerMessage{
		Topic: message.Topic,
	}
	if message.Key != nil {
		m.Key = sarama.StringEncoder(message.Key)
	}
	if message.Value != nil {
		m.Value = sarama.StringEncoder(message.Value)
	}
	for k, v := range message.Headers {
		m.Headers = append(m.Headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	// Add x-origin headers
	if err := AddXOriginIfMissing(&m.Headers); err != nil {
		return nil
	}

	// Add x-trace headers
	if err := AddXTrace(&m.Headers); err != nil {

	}

	return m
}
