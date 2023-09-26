package producer

import (
	"Sarama-Kafka-Wrapper-2/pkg/kafka/shared"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"sync/atomic"
	"time"
)

type Producer struct {
	brokers []string

	producedMessages atomic.Uint64
	erroredMessages  atomic.Uint64
	producer         *sarama.AsyncProducer

	running atomic.Bool
}

func NewProducer(brokers []string) (*Producer, error) {
	config := sarama.NewConfig()

	// We don't care about successful deliveries
	config.Producer.Return.Successes = false
	// But we do care about errors
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	p := &Producer{
		brokers:  brokers,
		producer: &producer,
	}
	p.running.Store(true)
	p.handleErrors()

	return p, nil
}

func (p *Producer) handleErrors() {
	go func(running *atomic.Bool, producer *sarama.AsyncProducer) {
		timeout := time.NewTimer(shared.CycleTime)
		for running.Load() {
			select {
			case <-timeout.C:
				timeout.Reset(shared.CycleTime)
			case err := <-(*producer).Errors():
				if err != nil {
					p.erroredMessages.Add(1)
					zap.S().Debugf("Error while producing message: %s", err.Error())
				}
			}
		}
	}(&p.running, p.producer)
}

func (p *Producer) SendMessage(message *shared.KafkaMessage) {
	if message == nil {
		return
	}
	(*p.producer).Input() <- shared.ToProducerMessage(message)
	p.producedMessages.Add(1)
}

func (p *Producer) Close() error {
	p.running.Store(false)
	return (*p.producer).Close()
}

// GetProducedMessages returns the number of messages produced and the number of errored messages
func (p *Producer) GetProducedMessages() (uint64, uint64) {
	return p.producedMessages.Load(), p.erroredMessages.Load()
}
