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

# Normalize CRLF if this file was edited on Windows
if [ -f /workspace/connect/create-sink-mongodb.sh ]; then
  # copy to /tmp (bind mounts from Windows can be picky with perms)
  tr -d '\r' < /workspace/connect/create-sink-mongodb.sh > /tmp/create-sink-mongodb.sh
  chmod +x /tmp/create-sink-mongodb.sh
  /tmp/create-sink-mongodb.sh
else
  echo "[start-and-wait] ERROR: /workspace/connect/create-sink-mongodb.sh not found" >&2
  exit 1
fi

echo "[start-and-wait] done."
