#!/bin/sh
set -eu

# ---- Hard-coded values  ----
CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="mongo-sink"
TOPIC="weather.raw"

MONGO_URI="mongodb+srv://s3979239:whatsup@cluster0.zalzedb.mongodb.net/"
MONGO_DB="weather"
MONGO_COLLECTION="events"


# Build JSON payload into /tmp to avoid CRLF issues
cat >/tmp/mongo-sink.json <<EOF
{
  "name": "${CONNECTOR_NAME}",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "weather.raw",
    "connection.uri": "mongodb+srv://s3979239:whatsup@cluster0.zalzedb.mongodb.net/",
    "database": "weather",
    "collection": "events",

    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",

    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy",
    "document.id.strategy.partial.value.projection.list": "id,_id,dt,dt_iso",
    "document.id.strategy.partial.value.projection.type": "AllowList",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "mongo-sink_dlq",
    "errors.deadletterqueue.context.headers.enable": "true",

    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.topic.partitions": "1",

    "max.num.retries": "3",
    "retries.defer.timeout": "5000"
  }
}
EOF

# Decide POST vs PUT based on existence
EXISTS_CODE="$(curl -s -o /dev/null -w '%{http_code}' "${CONNECT_URL}/connectors/${CONNECTOR_NAME}")"

if [ "$EXISTS_CODE" = "200" ]; then
  echo "[mongo-sink] updating ${CONNECTOR_NAME}..."
  HTTP_CODE="$(curl -s -o /tmp/resp.txt -w '%{http_code}' -X PUT \
    -H 'Content-Type: application/json' \
    --data @/tmp/mongo-sink.json \
    "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config")"
else
  echo "[mongo-sink] creating ${CONNECTOR_NAME}..."
  HTTP_CODE="$(curl -s -o /tmp/resp.txt -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' \
    --data @/tmp/mongo-sink.json \
    "${CONNECT_URL}/connectors")"
fi

echo "[mongo-sink] HTTP ${HTTP_CODE}"
cat /tmp/resp.txt || true
echo

# Treat non-2xx as failure so you notice
case "$HTTP_CODE" in
  2*) : ;;  # ok
  *) echo "[mongo-sink] ERROR: request failed (see response above)"; exit 1 ;;
esac

# Show status
curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" || true
echo
echo "[mongo-sink] ready."
