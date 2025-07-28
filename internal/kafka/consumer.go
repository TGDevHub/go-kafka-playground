package kafka

import (
	"context"
	"fmt"
	"go-mini-server/internal/metrics"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func CreateTopic(cfg Config) error {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": cfg.Brokers[0]})
	if err != nil {
		return fmt.Errorf("failed to create AdminClient: %w", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, err := admin.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             cfg.Topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}},
	)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			return fmt.Errorf("topic creation failed: %v", result.Error.String())
		}
		fmt.Printf("Topic %s created or already exists\n", result.Topic)
	}

	return nil
}

// Солло-консьюмер
func StartConsumer(ctx context.Context, cfg Config) error {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Brokers[0],
		"group.id":          cfg.GroupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return err
	}

	err = c.Subscribe(cfg.Topic, nil)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("Kafka consumer shutting down...")
				_ = c.Close()
				return

			default:
				msg, err := c.ReadMessage(-1)
				if err == nil {
					log.Printf("Kafka received: %s = %s", string(msg.Key), string(msg.Value))
				} else {
					log.Println("Kafka consumer error:", err)
				}
			}
		}
	}()

	return nil
}

// Мульти-консьюмер
func StartConsumerGroup(ctx context.Context, cfg Config, workers int) error {
	for i := 0; i < workers; i++ {
		go func(id int) {
			c, err := kafka.NewConsumer(&kafka.ConfigMap{
				"bootstrap.servers": cfg.Brokers[0],
				"group.id":          cfg.GroupID,
				"auto.offset.reset": "earliest",
			})
			if err != nil {
				log.Printf("[Worker %d] Error creating consumer: %v", id, err)
				return
			}

			defer c.Close()

			if err := c.Subscribe(cfg.Topic, nil); err != nil {
				log.Printf("[Worker %d] Subscribe error: %v", id, err)
				return
			}

			for {
				select {
				case <-ctx.Done():
					log.Printf("[Worker %d] Shutdown", id)
					return
				default:
					metrics.KafkaMessagesConsumed.Inc()
					msg, err := c.ReadMessage(-1)
					if err == nil {
						log.Printf("[Worker %d] Received: %s = %s", id, msg.Key, msg.Value)
					} else {
						log.Printf("[Worker %d] Read error: %v", id, err)
					}
				}
			}
		}(i)
	}
	return nil
}
