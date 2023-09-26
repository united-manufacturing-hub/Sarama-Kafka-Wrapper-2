package shared

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/IBM/sarama"
	"github.com/united-manufacturing-hub/umh-utils/env"
	"go.uber.org/zap"
	"time"
)

// serialNumber's error is ignored because it will fall back to the default value
// microserviceName's error is ignored because it will fall back to the default value
var (
	serialNumber, _     = env.GetAsString("SERIAL_NUMBER", false, "")     //nolint:errcheck
	microserviceName, _ = env.GetAsString("MICROSERVICE_NAME", false, "") //nolint:errcheck
)

// TraceValue holds trace information.
type TraceValue struct {
	Traces map[int64]string `json:"trace"`
}

// addXOrigin adds x-origin to Kafka headers.
func addXOrigin(headers *[]sarama.RecordHeader, origin string) error {
	return addHeaderTrace(headers, "x-origin", origin)
}

// addXOriginIfMissing conditionally adds x-origin to Kafka headers.
func addXOriginIfMissing(headers *[]sarama.RecordHeader) error {
	if getTrace(headers, "x-origin") == nil {
		return addXOrigin(headers, serialNumber)
	}
	return nil
}

// addXTrace adds x-trace to Kafka headers.
func addXTrace(headers *[]sarama.RecordHeader) error {
	identifier := microserviceName + "-" + serialNumber
	if err := addHeaderTrace(headers, "x-trace", identifier); err != nil {
		return err
	}
	return addXOriginIfMissing(headers)
}

// addHeaderTrace is a helper for adding new traces to Kafka headers.
func addHeaderTrace(headers *[]sarama.RecordHeader, key, value string) error {
	if len(*headers) == 0 {
		*headers = make([]sarama.RecordHeader, 0)
	}

	for i, header := range *headers {
		if bytes.EqualFold(header.Key, []byte(key)) {
			var trace TraceValue
			if err := json.Unmarshal(header.Value, &trace); err != nil {
				return err
			}
			t := time.Now().UnixNano()
			if _, exists := trace.Traces[t]; exists {
				return errors.New("trace already exists")
			}
			trace.Traces[t] = value
			jsonBytes, err := json.Marshal(trace)
			if err != nil {
				return err
			}
			header.Value = jsonBytes
			(*headers)[i] = header
			return nil
		}
	}

	trace := TraceValue{Traces: map[int64]string{time.Now().UnixNano(): value}}
	jsonBytes, err := json.Marshal(trace)
	if err != nil {
		return err
	}

	*headers = append(*headers, sarama.RecordHeader{Key: []byte(key), Value: jsonBytes})
	return nil
}

// getTrace retrieves trace information from Kafka headers.
func getTrace(message *[]sarama.RecordHeader, key string) *TraceValue {
	for _, header := range *message {
		if bytes.EqualFold(header.Key, []byte(key)) {
			var trace TraceValue
			if err := json.Unmarshal(header.Value, &trace); err != nil {
				zap.S().Errorf("Failed to unmarshal trace header: %s (%s)", err, key)
				return nil
			}
			return &trace
		}
	}
	return nil
}
