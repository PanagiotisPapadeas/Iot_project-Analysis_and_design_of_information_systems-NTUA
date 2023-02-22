#!/bin/bash

/usr/local/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic TutorialTopic

/usr/local/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic asynchronous

./../../apache/flink/flink-1.16.1/bin/start-cluster.sh
