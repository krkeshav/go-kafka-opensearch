package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type kafkaHelper struct {
	brokerAddress   []string
	topic           string
	ctx             context.Context
	kafkaWriter     *kafka.Writer
	kafkaReader     *kafka.Reader
	consumerGroupId string
}

func NewKafkaHelper(ctx context.Context, topic string, brokerAddress []string, consumerGroupId string) *kafkaHelper {
	return &kafkaHelper{
		ctx:           ctx,
		brokerAddress: brokerAddress,
		topic:         topic,
		kafkaWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokerAddress...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
		consumerGroupId: consumerGroupId,
		kafkaReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokerAddress,
			GroupID: consumerGroupId,
			Topic:   topic,
		}),
	}
}

func (k *kafkaHelper) Produce(key, value []byte) {
	err := k.kafkaWriter.WriteMessages(k.ctx,
		kafka.Message{
			Key:   key,
			Value: value,
		})
	if err != nil {
		log.Println("failed to write the message")
	}
}

func (k *kafkaHelper) CloseProducer() {
	k.kafkaWriter.Close()
}

func (k *kafkaHelper) SetConsumerGroup(consumerGroupId string) {
	k.consumerGroupId = consumerGroupId
	k.kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   k.topic,
		GroupID: k.consumerGroupId,
	})
}

func (k *kafkaHelper) Consume() *kafka.Message {
	message, err := k.kafkaReader.ReadMessage(k.ctx)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	return &message
}

func (k *kafkaHelper) ConsumeWithCtxTimeOut(duration time.Duration) *kafka.Message {
	childCtx, cancelCtx := context.WithTimeout(k.ctx, duration)
	message, err := k.kafkaReader.ReadMessage(childCtx)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	cancelCtx()
	return &message
}

func (k *kafkaHelper) CloseConsumer() {
	k.kafkaReader.Close()
}
