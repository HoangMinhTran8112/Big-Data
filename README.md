# How to Run

1. Start Kafka + Kafka Connect
```bash
docker compose up -d kafka kafka-connect
```


2. Create Topics
Run once to bootstrap the topics your producers and sink use:
```bash
docker compose exec kafka bash -lc '
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic weather --partitions 1 --replication-factor 1
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic air_quality --partitions 1 --replication-factor 1
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic bike_summary --partitions 1 --replication-factor 1
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic road_disruption --partitions 1 --replication-factor 1
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic weather.raw --partitions 1 --replication-factor 1
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
'
```


3. Wait for Kafka Connect & Register Mongo Sink
This will wait for Kafka Connect to come up, normalize CRLF, then POST the sink config from create-sink-mongodb.sh.
```bash
docker compose exec kafka-connect sh -lc "
  sed -i 's/\r$//' /workspace/connect/*.sh && \
  chmod +x /workspace/connect/*.sh && \
  /workspace/connect/start-and-wait.sh
"
```
This creates the connector mongo-sink pointing to your MongoDB Atlas:
Database: bdg
Collection: events
Topics: weather, air_quality, bike_summary, road_disruption, weather.raw
Document ID: {city, provider_ts} (so you don’t get duplicates per city per timestamp)


4. Build and Start Producers
Build and run all producer services:
```bash
docker compose --profile apps build
docker compose --profile apps up -d owm-producer airquality-producer bike-summary-producer roaddisrupt-producer
```


5. View logs
```bash
docker compose logs -f owm-producer
docker compose logs -f airquality-producer
docker compose logs -f bike-summary-producer
docker compose logs -f roaddisrupt-producer
```



---
Adding More Producers or Consumers

To add new producers/consumers, drop them under producer/ or consumer/ and add a service block in docker-compose.yml.

Example:

services:
  my-extra-producer:
    build: ./producer/my-extra-producer
    container_name: my-extra-producer
    depends_on:
      - kafka
    environment:
      KAFKA_BROKER_URL: kafka:9092
      TOPIC_NAME: my_topic
    networks: [pipeline]
    profiles: ["apps"]

  my-extra-consumer:
    build: ./consumer/my-extra-consumer
    container_name: my-extra-consumer
    depends_on:
      - kafka
    environment:
      KAFKA_BROKER_URL: kafka:9092
      TOPIC_NAME: my_topic
      GROUP_ID: my-consumer-group
    networks: [pipeline]
    profiles: ["apps"]


Start them the same way:

docker compose --profile apps up -d my-extra-producer my-extra-consumer


Or scale an existing consumer:

docker compose --profile apps up -d --scale airquality-consumer=3