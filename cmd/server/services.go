package main

import (
	"context"
	"go-mini-server/core/db/pool"
	"go-mini-server/internal/kafka"
	"go-mini-server/internal/metrics"
	"go-mini-server/internal/storage"
	"go-mini-server/internal/user"
	"log"
)

var (
	userService user.Service
)

func initServices(c Config) {
	mainPool := pool.NewService(c.DB)

	userRepository := storage.NewUserRepository(mainPool)
	userService = user.NewService(userRepository)

	metrics.Init()

	startKafka(c)
}

func startKafka(c Config) {
	ctx := context.Background()
	if err := kafka.CreateTopic(c.Kafka); err != nil {
		log.Fatalf("Kafka create topic error: %v", err)
	}

	if err := kafka.StartConsumerGroup(ctx, c.Kafka, 3); err != nil {
		log.Fatalf("Kafka consumer error: %v", err)
	}
}
