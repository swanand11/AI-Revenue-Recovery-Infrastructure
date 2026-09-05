#!/usr/bin/env bash
set -euo pipefail

PROJECT="${COMPOSE_PROJECT_NAME:-ai-revenue-recovery-infrastructure}"
SPLUNK_INDEX="${SPLUNK_INDEX:-revtrace}"
SPLUNK_PASSWORD="${SPLUNK_PASSWORD:-Changeme123!}"

echo "[reset] stopping application containers"
docker compose down --remove-orphans

echo "[reset] removing application volumes"
docker volume rm -f \
  "${PROJECT}_kafka_data" \
  "${PROJECT}_wal_data" \
  "${PROJECT}_dashboard_data" \
  "${PROJECT}_splunk_etc" \
  "${PROJECT}_splunk_var" >/dev/null 2>&1 || true

echo "[reset] clearing local dashboard snapshots"
rm -f runner/data/ingestion_events.json runner/data/customer_intent.json

echo "[reset] application state cleared"
echo "[reset] next: docker compose up"
