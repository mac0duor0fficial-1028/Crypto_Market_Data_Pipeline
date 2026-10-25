#!/bin/bash
# One-shot init: creates topics and pins Schema Registry compatibility to BACKWARD.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
SR="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
TOPIC="${KAFKA_TOPIC_TRADES:-crypto.trades.v1}"
DLQ="${KAFKA_TOPIC_DLQ:-crypto.trades.dlq}"

echo "Creating topics on ${BOOTSTRAP}"
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "$TOPIC" --partitions 3 --replication-factor 1 \
  --config retention.ms=604800000
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "$DLQ" --partitions 1 --replication-factor 1 \
  --config retention.ms=2592000000

echo "Waiting for Schema Registry at ${SR}"
until curl -sf "${SR}/subjects" >/dev/null; do sleep 2; done

curl -sf -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"BACKWARD"}' "${SR}/config/${TOPIC}-value"
echo
kafka-topics --bootstrap-server "$BOOTSTRAP" --list
