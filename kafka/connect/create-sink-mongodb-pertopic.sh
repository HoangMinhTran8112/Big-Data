#!/bin/sh
set -eu

# ---- Hardcoded base config ----
CONNECT_URL="http://localhost:8083"

TOPICS_CSV="weather,air_quality,weather.raw,bike_summary,road_disruption"

MONGO_URI="mongodb+srv://s3979239:whatsup@cluster0.zalzedb.mongodb.net/"
MONGO_DB="bdg"

ID_FIELDS="city,provider_ts"
TASKS_MAX="1"

trim() { echo "$1" | awk '{$1=$1;print}'; }
exists() { curl -fsS "${CONNECT_URL}/connectors/$1" >/dev/null 2>&1; }

mk_create_payload() {
  name="$1"; topic="$2"; coll="$3"; dlq="$4"
  cat <<EOF
{
  "name": "${name}",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "${TASKS_MAX}",

    "topics": "${topic}",

    "connection.uri": "${MONGO_URI}",
    "database": "${MONGO_DB}",
    "collection": "${coll}",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy",
    "document.id.strategy.partial.value.projection.list": "${ID_FIELDS}",
    "document.id.strategy.partial.value.projection.type": "AllowList",

    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "${dlq}_dlq",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.topic.partitions": "1",

    "max.num.retries": "3",
    "retries.defer.timeout": "5000"
  }
}
EOF
}

mk_update_payload() {
  topic="$1"; coll="$2"; dlq="$3"
  cat <<EOF
{
  "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
  "tasks.max": "${TASKS_MAX}",

  "topics": "${topic}",

  "connection.uri": "${MONGO_URI}",
  "database": "${MONGO_DB}",
  "collection": "${coll}",

  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",

  "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy",
  "document.id.strategy.partial.value.projection.list": "${ID_FIELDS}",
  "document.id.strategy.partial.value.projection.type": "AllowList",

  "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy",

  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "${dlq}_dlq",
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.deadletterqueue.topic.replication.factor": "1",
  "errors.deadletterqueue.topic.partitions": "1",

  "max.num.retries": "3",
  "retries.defer.timeout": "5000"
}
EOF
}

upsert_one() {
  topic="$1"
  coll="$(echo "$topic" | tr '.' '_' )"
  name="mongo-sink-${coll}"
  dlq="mongo-sink-${coll}"

  if exists "$name"; then
    echo "[${name}] updating..."
    mk_update_payload "$topic" "$coll" "$dlq" > /tmp/${name}.update.json
    HTTP_CODE="$(curl -s -o /tmp/${name}.resp.txt -w '%{http_code}' \
      -X PUT -H 'Content-Type: application/json' \
      --data @/tmp/${name}.update.json \
      "${CONNECT_URL}/connectors/${name}/config")"
  else
    echo "[${name}] creating..."
    mk_create_payload "$name" "$topic" "$coll" "$dlq" > /tmp/${name}.create.json
    HTTP_CODE="$(curl -s -o /tmp/${name}.resp.txt -w '%{http_code}' \
      -X POST -H 'Content-Type: application/json' \
      --data @/tmp/${name}.create.json \
      "${CONNECT_URL}/connectors")"
  fi

  echo "[${name}] HTTP ${HTTP_CODE}"
  cat /tmp/${name}.resp.txt || true
  echo
  case "$HTTP_CODE" in
    2*) : ;;
    *) echo "[${name}] ERROR: request failed"; return 1 ;;
  esac

  curl -s "${CONNECT_URL}/connectors/${name}/status" || true
  echo
}

OLD_IFS="$IFS"
IFS=','

for raw in $TOPICS_CSV; do
  topic="$(trim "$raw")"
  [ -z "$topic" ] && continue
  upsert_one "$topic"
done

IFS="$OLD_IFS"
echo "[mongo-sink-pertopic] done."
