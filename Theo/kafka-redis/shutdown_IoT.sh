#!/bin/bash

~/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic dailyAggr
~/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic lateRej
confluent local services stop
sudo docker compose down
redis-cli shutdown

