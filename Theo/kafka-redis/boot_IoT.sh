#!/bin/bash

redis-server --daemonize yes
sudo docker compose up -d
confluent local services start

echo "$(tput setaf 6)Creating dailyAggr topic...$(tput setaf 2)"

~/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic dailyAggr --partitions 10 --replication-factor 1

echo "$(tput setaf 6)Creating lateRej topic...$(tput setaf 2)"

~/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic lateRej --partitions 1 --replication-factor 1

confluent local services connect connector load kafka-connect-redis --config redis-sink.properties
confluent local services connect connector status kafka-connect-redis
