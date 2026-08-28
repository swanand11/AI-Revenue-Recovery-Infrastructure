# Run Guide

This repository now includes:

- Kafka ingestion services
- the detection service
- Splunk Enterprise in Docker
- a live dashboard that queries Splunk directly

## Prerequisites

- Docker and Docker Compose
- A local Python 3.12 environment if you want to run repo tests outside Docker

## Start from scratch

1. Build and start the full stack:

```bash
docker compose up --build
```

2. Wait for these services to become healthy:

- `kafka`
- `kafka-init`
- `splunk`
- `detection-service`
- the ingestion services

3. Open Splunk Web:

```text
http://localhost:8000
```

Login:

- username: `admin`
- password: the value of `SPLUNK_PASSWORD` if set, otherwise `Changeme123!`

4. Open the dashboard:

```text
http://localhost:8080
```

## What the system does

1. The ingestion services publish events to:

- `checkout.events`
- `payment.events`
- `authorization.events`
- `capture.events`
- `settlement.events`

2. The detection consumer reads those topics, computes:

- failure signals
- degradation signals
- customer intent
- graph-based RCA

3. Detection emits to:

- Kafka topic `detection.events`
- Splunk HEC

4. The dashboard queries Splunk directly and shows:

- traceability for a `transaction_id`
- core details of an event
- smart search filters
- live summary stats

## Seed Splunk with real data

The repository includes a real loader that posts JSON events into Splunk HEC.

Run it manually if you want to reseed:

```bash
./.venv/bin/python scripts/mock_splunk_seed.py \
  --hec-url https://localhost:8088 \
  --hec-token revtrace-hec-token \
  --index revtrace \
  --dataset events_sent.json \
  --no-verify-cert
```

The detection service also forwards live detection events to Splunk when these environment variables are present:

- `SPLUNK_HEC_URL` (set to `https://localhost:8088` for host-side tooling, `https://splunk:8088` for in-container services)
- `SPLUNK_HEC_TOKEN`
- `SPLUNK_INDEX`
- `SPLUNK_HEC_VERIFY_CERT` (`true` for trusted CA certificates, `false` for the repo’s local self-signed Splunk dev cert)

## Generate historical data

```bash
./.venv/bin/python detection/scripts/generate_historical_data.py \
  --output /tmp/detection_history.json \
  --count 200
```

Precompute model metadata:

```bash
./.venv/bin/python detection/scripts/precompute_models.py \
  --input /tmp/detection_history.json \
  --output /tmp/detection_models.json
```

## Dashboard usage

### 1. Traceability view

Enter a `transaction_id` and click `Load Trace`.

The dashboard will query Splunk for all matching ingestion and detection events, sort them by time, and display:

- stage
- event type
- timestamp
- raw JSON payload

### 2. Smart query

Fill in any of these fields:

- `transaction_id`
- `customer_id`
- `stage`
- `status`
- `event_type`
- `failure_code`

Click `Search Splunk` to run a live query against the Splunk index.

### 3. Live updates

The dashboard refreshes summary stats every 5 seconds.

## Exact commands

### Start the full system

```bash
docker compose up --build
```

### Start only dashboard after the stack is up

```bash
docker compose up dashboard
```

### Run detection tests

```bash
./.venv/bin/python -m pytest -q detection/tests
```

### Run repo tests

```bash
./.venv/bin/python -m pytest -q
```

### Run ingestion checks

```bash
./.venv/bin/python smoke_test.py
./.venv/bin/python smoke_validation.py
```

## Smoke flow

1. Publish or wait for a mock ingestion event.
2. Kafka receives it on the appropriate stage topic.
3. Detection consumes it.
4. Detection computes signals and RCA.
5. Detection publishes to `detection.events`.
6. Detection emits the same event to Splunk.
7. The dashboard queries Splunk and displays both traceability and details.

