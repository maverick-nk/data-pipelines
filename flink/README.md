## POC for Apache Flink

A proof of concept with Apache Flink to read from kafka messages and write to postgreSQL DB.

Kafka Topic/s
1. Clicks


# Setup

```bash
./deploy.sh
# OR
docker-compose up -d
```

Creating Kafka Topics
```bash
docker exec -it kafka-broker bash
cd opt/bitnami/kafka/bin

# Creating 'clicks' topic
kafka-topics.sh --create --bootstrap-server 127.0.0.1:9094 --replication-factor 1 --partitions 1 --topic clicks


# OR single command
docker exec -it kafka-broker "opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server 127.0.0.1:9094 --replication-factor 1 --partitions 1 --topic clicks"
```

Check Kafka broker
```bash
# Produce in one window
kafka-console-producer.sh --bootstrap-server 127.0.0.1:9094 --producer.config /opt/bitnami/kafka/config/producer.properties --topic clicks

# Consume in another window
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9094 --consumer.config /opt/bitnami/kafka/config/consumer.properties --topic clicks --from-beginning
```


## References:

1. Kafka Setup: https://andres-plazas.medium.com/a-python-kafka-producer-edafa7de879c 