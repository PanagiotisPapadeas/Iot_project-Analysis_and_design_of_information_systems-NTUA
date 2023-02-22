#!/bin/bash

sudo docker compose up -d

echo "$(tput setaf 6)Creating dailyAggr topic...$(tput setaf 2)"

~/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic dailyAggr --partitions 10 --replication-factor 1

echo "$(tput setaf 6)Creating lateRej topic...$(tput setaf 2)"

~/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic lateRej --partitions 1 --replication-factor 1
