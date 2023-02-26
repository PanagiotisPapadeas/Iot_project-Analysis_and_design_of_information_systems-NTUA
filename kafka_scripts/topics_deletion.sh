#!/bin/bash

/usr/local/kafka/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic dailyAggr

/usr/local/kafka/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic lateRej

/usr/local/kafka/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic flinkAggr

