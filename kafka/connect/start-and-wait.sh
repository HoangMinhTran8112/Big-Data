#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

echo "[start-and-wait] waiting for Kafka Connect at ${CONNECT_URL} ..."
# Wait for HTTP 200 on /connectors
until [ "$(curl -s -o /dev/null -w '%{http_code}' "${CONNECT_URL}/connectors")" = "200" ]; do
  echo "$(date)  waiting..."
  sleep 2
done
echo "[start-and-wait] Kafka Connect is up."

run_script() {
  name="$1"
  path="/workspace/connect/$name"
  if [ -f "$path" ]; then
    echo "[start-and-wait] running $name ..."
    # strip CRLF → copy to /tmp → execute
    tr -d '\r' < "$path" > "/tmp/$name"
    chmod +x "/tmp/$name"
    "/tmp/$name"
  else
    echo "[start-and-wait] WARNING: $path not found, skipping" >&2
  fi
}

# Run both scripts
run_script "create-sink-mongodb.sh"
run_script "create-sink-mongodb-pertopic.sh"

echo "[start-and-wait] done."
