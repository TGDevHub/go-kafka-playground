#!/bin/sh

echo "Waiting for Kafka to be ready..."

while ! nc -z localhost 9092; do
  sleep 1
done

echo "Creating topic user-event"

/opt/bitnami/kafka/bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic user-event

echo "Topic user-event created"