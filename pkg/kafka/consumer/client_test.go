package consumer

import (
	"Sarama-Kafka-Wrapper-2/pkg/kafka/shared"
	"context"
	"fmt"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
	"os"
	"testing"
	"time"
)

func TestConnectAndReceive(t *testing.T) {
	t.Log("Starting TestConnectAndReceive test")
	brokers := []string{
		"10.99.112.33:31092",
		"10.99.112.34:31092",
		"10.99.112.35:31092",
	}

	topic := "umh.v1.chernobylnuclearpowerplant"

	groupName := "sarama-kafka-wrapper-test-tcar-2"

	t.Logf("Connecting to brokers: %v", brokers)
	testConsumer, err := NewConsumer(brokers, topic, groupName)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Connected")

	runtime := 1 * time.Minute

	t.Log("Starting consumer")

	ctx, cncl := context.WithTimeout(context.Background(), runtime+(5*time.Second))
	defer cncl()

	err = testConsumer.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Started")

	timeout := time.After(runtime)

	received := 0
	k := 100_000
	messages := make([]*shared.KafkaMessage, k)

	timeMsgPerSecMap := make(map[int64]int, 720)

	now := time.Now()
	statTimer := time.NewTicker(60 * time.Second)
breakOuter:
	for {
		select {
		case <-timeout:
			break breakOuter
		case msg := <-testConsumer.GetMessages():
			messages[received%k] = msg
			if received%k == 0 {
				nowX := time.Now()
				testConsumer.MarkMessages(messages)
				elapsedMark := time.Since(nowX)
				msgPerSecond := float64(received) / time.Since(now).Seconds()
				t.Logf("[RECV] received %d messages (%d/s) [Marking took %s]", received, int(msgPerSecond), elapsedMark)
			}
			received++
		case <-statTimer.C:
			msgPerSecond := float64(received) / time.Since(now).Seconds()
			timeMsgPerSecMap[time.Now().UnixNano()] = int(msgPerSecond)
			statTimer.Reset(60 * time.Second)
			t.Logf("[STAT] received %d messages (%d/s)", received, int(msgPerSecond))
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
	t.Logf("received %d messages", received)
	testConsumer.MarkMessages(messages)
	elapsed := time.Since(now)

	t.Log("Closing consumer")

	err = testConsumer.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Closed")

	t.Logf("received %d (%d/s) messages in %s", received, int(float64(received)/elapsed.Seconds()), elapsed)

	// Write map to file
	f, err := os.Create("msgPerSecMap.txt")
	if err != nil {
		t.Log(err)
	} else {
		defer f.Close()
		for k, v := range timeMsgPerSecMap {
			if _, err := f.WriteString(fmt.Sprintf("[%d] %d\n", k, v)); err != nil {
				t.Log(err)
			}
		}
	}

	time.Sleep(10 * time.Second)
	t.Log("Goodbye")
}

func TestGenPlot(t *testing.T) {
	// read msgPerSecMap.txt
	f, err := os.Open("msgPerSecMap.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ptsMsgSec := make(plotter.XYs, 0, 720)
	var startTs int64

	// For each line
	for {
		var x, y int64
		_, err := fmt.Fscanf(f, "[%d] %d\n", &x, &y)
		if err != nil {
			break
		}
		if startTs == 0 {
			startTs = x
		}
		timeDiffToStart := x - startTs
		// convert x (nanoseconds) to minutes
		timeDiffToStart = timeDiffToStart / 1_000_000_000 / 60
		ptsMsgSec = append(ptsMsgSec, plotter.XY{X: float64(timeDiffToStart), Y: float64(y)})
	}

	// Plot graph

	p := plot.New()

	p.Title.Text = "Messages per second"
	p.X.Label.Text = "Time (minutes)"
	p.Y.Label.Text = "Messages/s"

	err = plotutil.AddScatters(p, "Messages/s", ptsMsgSec)
	if err != nil {
		t.Log(err)
	} else {
		if err := p.Save(20*vg.Inch, 8*vg.Inch, "msgPerSecMap.svg"); err != nil {
			t.Log(err)
		}
	}
}
