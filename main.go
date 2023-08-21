package main

import (
	"context"
	"fmt"
)

const (
	brokerAddress = "localhost:9092"
	topic         = "keshav"
)

func main() {
	ctx := context.Background()
	kafkaHelper := NewKafkaHelper(ctx, "kenway", []string{brokerAddress}, "default-group")
	eventChannel := make(chan string)
	defer close(eventChannel)
	streamingHelper := NewStreamingHelper("https://stream.wikimedia.org/v2/stream/recentchange", eventChannel)
	go streamingHelper.GetStreamingDataWithCustomParse()
	go func() {
		for event := range eventChannel {
			kafkaHelper.Produce(nil, []byte(fmt.Sprint(event)))
		}
	}()
	for {
		message := kafkaHelper.Consume()
		if message == nil {
			continue
		}
		fmt.Println(string(message.Value))
	}

	// kafkaHelper.CloseProducer()
	// kafkaHelper.CloseConsumer()

}
