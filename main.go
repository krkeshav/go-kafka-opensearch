package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
)

const (
	brokerAddress       = "localhost:9092"
	openSearchAddress   = "http://localhost:9200"
	topic               = "kenway"
	openSearchIndexName = "wikistream"
)

type StreamStruct struct {
	Meta Meta `json:"meta"`
}

type Meta struct {
	Id string `json:"id"`
}

func main() {
	ctx := context.Background()
	kafkaHelper := NewKafkaHelper(ctx, topic, []string{brokerAddress}, "default-group")
	openSearchHelper := NewOpenSearchHelper(ctx, []string{openSearchAddress}, true)
	created := openSearchHelper.CreateIndex(openSearchIndexName, `{"settings":{"index":{"number_of_shards":2}}}`)
	if !created {
		log.Fatal("opensearch index creation failed")
	}
	eventChannel := make(chan string)
	defer close(eventChannel)
	streamingHelper := NewStreamingHelper("https://stream.wikimedia.org/v2/stream/recentchange", eventChannel)
	go streamingHelper.GetStreamingDataWithCustomParse()
	go func() {
		for event := range eventChannel {
			if event == "" {
				continue
			}
			kafkaHelper.Produce(nil, []byte(fmt.Sprint(event)))
		}
	}()
	notifyChan := make(chan os.Signal, 1)
	signal.Notify(notifyChan, os.Interrupt)
	go func() {
		<-notifyChan
		fmt.Println("Interrupt detected, closing producer and consumer")
		streamingHelper.StopStreaming()
		kafkaHelper.CloseProducer()
		kafkaHelper.CloseConsumer()
		close(eventChannel)
		os.Exit(1)
	}()
	for {
		message := kafkaHelper.Consume()
		if message == nil {
			continue
		}
		var streamData StreamStruct
		err := json.Unmarshal(message.Value, &streamData)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println(streamData.Meta.Id)
		openSearchHelper.IndexDocument(openSearchIndexName, streamData.Meta.Id, string(message.Value))
	}

}
