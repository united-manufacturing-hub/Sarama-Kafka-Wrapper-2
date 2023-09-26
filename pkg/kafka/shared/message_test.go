package shared

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConsumerMessage(t *testing.T) {
	c := &sarama.ConsumerMessage{
		Key:       []byte("key"),
		Value:     []byte("value"),
		Topic:     "topic",
		Partition: 1,
		Offset:    2,
		Headers: []*sarama.RecordHeader{
			{
				Key:   []byte("header1"),
				Value: []byte("value1"),
			},
		},
	}

	m := FromConsumerMessage(c)
	cX := ToConsumerMessage(m)

	assert.Equal(t, *c, *cX)
}
