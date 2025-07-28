package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go-mini-server/internal/metrics"
	"strings"

	"log"
)

type Producer struct {
	p     *kafka.Producer
	topic string
}

func NewProducer(c Config) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(c.Brokers, ","),
	})
	if err != nil {
		return nil, err
	}

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v", ev.TopicPartition)
				} else {
					log.Printf("Delivered message to %v", ev.TopicPartition)
				}
			}
		}
	}()

	return &Producer{p: p, topic: c.Topic}, nil
}

func (kp *Producer) Produce(key string, value string) error {
	return kp.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kp.topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          []byte(value),
	}, nil)
}

func (kp *Producer) Send(key, value string) error {
	err := kp.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kp.topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          []byte(value),
	}, nil)

	if err != nil {
		log.Println("Kafka send error:", err)
	}

	metrics.KafkaMessagesProduced.Inc()
	return err
}

func (kp *Producer) Close() {
	kp.p.Close()
}
