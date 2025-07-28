package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	KafkaMessagesProduced = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_messages_produced_total",
		Help: "Total number of messages produced to Kafka",
	})

	KafkaMessagesConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_messages_consumed_total",
		Help: "Total number of messages consumed from Kafka",
	})
)

func Init() {
	prometheus.MustRegister(KafkaMessagesProduced)
	prometheus.MustRegister(KafkaMessagesConsumed)
}
