#!/bin/sh
set -eu

CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="mongo-sink"

SINK_TOPICS="weather,air_quality,weather.raw,bike_summary,road_disruption"

MONGO_URI="mongodb+srv://s3979239:whatsup@cluster0.zalzedb.mongodb.net/"
MONGO_DB="bdg"
MONGO_COLLECTION="events"

create_payload() {
cat <<'EOF'
{
  "name": "mongo-sink",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "weather,air_quality,weather.raw,bike_summary,road_disruption",

    "connection.uri": "mongodb+srv://s3979239:whatsup@cluster0.zalzedb.mongodb.net/",
    "database": "bdg",
    "collection": "events",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "mongo-sink_dlq",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.topic.partitions": "1",
    "max.num.retries": "3",
    "retries.defer.timeout": "5000",

    "transforms": "AddTopic,KeyWeather,KeyAir,KeyWeatherRaw,KeyBike,KeyRoad",
    "transforms.AddTopic.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.AddTopic.topic.field": "topic",

    "transforms.KeyWeather.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.KeyWeather.fields": "topic,city,provider_ts",
    "transforms.KeyWeather.predicate": "IsWeather",

    "transforms.KeyAir.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.KeyAir.fields": "topic,city,provider_ts",
    "transforms.KeyAir.predicate": "IsAir",

    "transforms.KeyWeatherRaw.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.KeyWeatherRaw.fields": "topic,city,provider_ts",
    "transforms.KeyWeatherRaw.predicate": "IsWeatherRaw",

    "transforms.KeyBike.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.KeyBike.fields": "topic,timestamp",
    "transforms.KeyBike.predicate": "IsBike",

    "transforms.KeyRoad.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.KeyRoad.fields": "topic,id",
    "transforms.KeyRoad.predicate": "IsRoad",

    "predicates": "IsWeather,IsAir,IsWeatherRaw,IsBike,IsRoad",
    "predicates.IsWeather.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.IsWeather.pattern": "^weather$",
    "predicates.IsAir.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.IsAir.pattern": "^air_quality$",
    "predicates.IsWeatherRaw.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.IsWeatherRaw.pattern": "^weather\\.raw$",
    "predicates.IsBike.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.IsBike.pattern": "^bike_summary$",
    "predicates.IsRoad.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.IsRoad.pattern": "^road_disruption$",

    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy"
  }
}
EOF
}

update_payload() {
cat <<'EOF'
{
  "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
  "tasks.max": "1",
  "topics": "weather,air_quality,weather.raw,bike_summary,road_disruption",

  "connection.uri": "mongodb+srv://s3979239:whatsup@cluster0.zalzedb.mongodb.net/",
  "database": "bdg",
  "collection": "events",

  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",

  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "mongo-sink_dlq",
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.deadletterqueue.topic.replication.factor": "1",
  "errors.deadletterqueue.topic.partitions": "1",
  "max.num.retries": "3",
  "retries.defer.timeout": "5000",

  "transforms": "AddTopic,KeyWeather,KeyAir,KeyWeatherRaw,KeyBike,KeyRoad",
  "transforms.AddTopic.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.AddTopic.topic.field": "topic",

  "transforms.KeyWeather.type": "org.apache.kafka.connect.transforms.ValueToKey",
  "transforms.KeyWeather.fields": "topic,city,provider_ts",
  "transforms.KeyWeather.predicate": "IsWeather",

  "transforms.KeyAir.type": "org.apache.kafka.connect.transforms.ValueToKey",
  "transforms.KeyAir.fields": "topic,city,provider_ts",
  "transforms.KeyAir.predicate": "IsAir",

  "transforms.KeyWeatherRaw.type": "org.apache.kafka.connect.transforms.ValueToKey",
  "transforms.KeyWeatherRaw.fields": "topic,city,provider_ts",
  "transforms.KeyWeatherRaw.predicate": "IsWeatherRaw",

  "transforms.KeyBike.type": "org.apache.kafka.connect.transforms.ValueToKey",
  "transforms.KeyBike.fields": "topic,timestamp",
  "transforms.KeyBike.predicate": "IsBike",

  "transforms.KeyRoad.type": "org.apache.kafka.connect.transforms.ValueToKey",
  "transforms.KeyRoad.fields": "topic,id",
  "transforms.KeyRoad.predicate": "IsRoad",

  "predicates": "IsWeather,IsAir,IsWeatherRaw,IsBike,IsRoad",
  "predicates.IsWeather.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
  "predicates.IsWeather.pattern": "^weather$",
  "predicates.IsAir.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
  "predicates.IsAir.pattern": "^air_quality$",
  "predicates.IsWeatherRaw.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
  "predicates.IsWeatherRaw.pattern": "^weather\\.raw$",
  "predicates.IsBike.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
  "predicates.IsBike.pattern": "^bike_summary$",
  "predicates.IsRoad.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
  "predicates.IsRoad.pattern": "^road_disruption$",

  "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy",

  "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy"
}
EOF
}

EXISTS_CODE="$(curl -s -o /dev/null -w '%{http_code}' "${CONNECT_URL}/connectors/${CONNECTOR_NAME}")"

if [ "$EXISTS_CODE" = "200" ]; then
  echo "[mongo-sink] updating ${CONNECTOR_NAME}..."
  update_payload > /tmp/mongo-sink.update.json
  HTTP_CODE="$(curl -s -o /tmp/resp.txt -w '%{http_code}' -X PUT \
    -H 'Content-Type: application/json' \
    --data @/tmp/mongo-sink.update.json \
    "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config")"
else
  echo "[mongo-sink] creating ${CONNECTOR_NAME}..."
  create_payload > /tmp/mongo-sink.create.json
  HTTP_CODE="$(curl -s -o /tmp/resp.txt -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' \
    --data @/tmp/mongo-sink.create.json \
    "${CONNECT_URL}/connectors")"
fi

echo "[mongo-sink] HTTP ${HTTP_CODE}"
cat /tmp/resp.txt || true
echo
case "$HTTP_CODE" in 2*) : ;; *) echo "[mongo-sink] ERROR"; exit 1 ;; esac
curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" || true
echo
echo "[mongo-sink] ready."
