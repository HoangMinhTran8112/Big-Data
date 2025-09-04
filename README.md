# How to Run

### Start Kafka + Kafka Connect
```bash
docker compose up -d kafka kafka-connect
```

### Wait for Kafka Connect & register Mongo sink
```bash
docker compose exec kafka-connect sh -lc "/workspace/connect/start-and-wait.sh"
```
This creates the connector **mongo-sink** pointing to your MongoDB.

### Build and start Producer + Consumer
```bash
docker compose --profile apps build producer consumer
docker compose --profile apps up -d producer consumer
```

### View logs
```bash
docker compose logs -f producer
docker compose logs -f consumer
```

- **Producer** shows weather data being published to Kafka.  
- **Consumer** prints messages received from Kafka.  
- **MongoDB Atlas** will contain documents in database `weather`, collection `events`.

---

## Adding More Producers or Consumers

You don’t need a new Dockerfile for each one — just reuse the same image and override the command or environment.

Example in `docker-compose.yml`:

```yaml
services:
  producer_a:
    build: ./producer
    command: ["python", "app/producer_a.py"]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      TOPIC: weather.raw

  producer_b:
    image: ${COMPOSE_PROJECT_NAME}_producer:latest  # reuse built image
    command: ["python", "app/producer_b.py"]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      TOPIC: weather.raw

  consumer_extra:
    image: ${COMPOSE_PROJECT_NAME}_consumer:latest  # reuse consumer image
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      TOPIC: weather.raw
      GROUP_ID: weather-extra
```

Start them the same way:
```bash
docker compose --profile apps up -d producer_a producer_b consumer_extra
```

Or scale an existing service:
```bash
docker compose --profile apps up -d --scale consumer=3
```
