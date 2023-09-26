package producer

import (
	"Sarama-Kafka-Wrapper-2/pkg/kafka/shared"
	"encoding/json"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestConnectAndProduce(t *testing.T) {
	t.Log("Starting TestConnectAndProduce test")
	brokers := []string{
		"10.99.112.33:31092",
		"10.99.112.34:31092",
		"10.99.112.35:31092",
	}

	t.Logf("Connecting to brokers: %v", brokers)
	testProducer, err := NewProducer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Connected")

	runtime := time.NewTimer(1 * time.Minute)

	t.Log("Starting producer")

	now := time.Now()
breakOuter:
	for {
		select {
		case <-runtime.C:
			break breakOuter
		default:
			testProducer.SendMessage(genMessage(t))
			produced := testProducer.producedMessages.Load()
			if produced%100000 == 0 {
				msgPerSec := float64(produced) / time.Since(now).Seconds()
				t.Logf("Produced messages: %d (%f msg/s)", testProducer.producedMessages.Load(), msgPerSec)
			}
		}
	}
	elapsed := time.Since(now)

	t.Log("Stopping producer")
	err = testProducer.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Stats

	messages, errors := testProducer.GetProducedMessages()
	msgPerSec := float64(messages) / elapsed.Seconds()
	t.Logf("Produced messages: %d (%f msg/s)", messages, msgPerSec)
	errorRate := float64(errors) / float64(messages)
	t.Logf("Errored messages: %d (%f%%)", errors, errorRate*100)
}

func genMessage(t *testing.T) *shared.KafkaMessage {
	msg := shared.KafkaMessage{
		Topic: "umh.v1.producer.test",
	}

	randKey := randSeq(rand.Intn(60))
	randValMap := make(map[string]string, 10)
	randValMap[randSeq(rand.Intn(10))] = randSeq(rand.Intn(60))
	randValMap["timestamp_ms"] = strconv.FormatInt(time.Now().UnixMilli(), 10)
	randVal, err := json.Marshal(randValMap)
	if err != nil {
		t.Logf("Error while marshalling random value map: %s", err.Error())
		return nil
	}
	msg.Value = randVal

	msg.Key = []byte(randKey)

	return &msg
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
