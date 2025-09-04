#!/usr/bin/env bash
set -e

# Start Kafka Connect in background
/etc/confluent/docker/run &

echo "[connect] Waiting for Connect to be ready..."
until curl -sSf http://localhost:8083/connectors >/dev/null 2>&1; do
  sleep 2
done

echo "[connect] Registering MongoDB sink connector..."
curl -s -X DELETE http://localhost:8083/connectors/mongo-sink || true
curl -s -X POST http://localhost:8083/connectors   -H 'Content-Type: application/json'   --data-binary @/mongo-sink.json

wait -n
