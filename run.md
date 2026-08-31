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

2. The payment lifecycle is the source of truth and must stay in this order:

- `checkout`
- `payment`
- `auth`
- `capture`
- `settlement`

3. The detection consumer reads those topics, computes:

- failure signals
- degradation signals
- customer intent
- graph-based RCA

4. Detection emits to:

- Kafka topic `detection.events`
- Splunk HEC

5. The dashboard shows:

- a live ingestion table from the Kafka bridge snapshot
- a live detection table from Splunk
- traceability for a `transaction_id`
- core details of an event
- smart search filters
- live summary stats
- a dedicated mock event flow page

## Recommended launch flow

The mock services already emit fresh events every 5 seconds, so the normal validation path is to start the stack and watch the bridge and Splunk reflect those live events.

```bash
./.venv/bin/python runner/e2e_runner.py --build
```

That command starts:

- the mock ingestion services
- Kafka and the topic bootstrap flow
- the ingestion bridge
- detection
- the Kafka-to-Splunk forwarder
- Splunk
- the dashboard

You should see terminal debug output confirming the launcher started the mock services and bridge, and then the dashboard should reflect new ingestion rows from the topic snapshot and detection rows from Splunk.

## Step-by-step debug flow

Use these commands to isolate where data stops moving:

1. Start only the mock ingestion services and watch their terminal logs:

```bash
./.venv/bin/python runner/start_mocks.py --build
```

2. Start Kafka and the detection consumer:

```bash
./.venv/bin/python runner/start_detection.py --build
```

3. Start Splunk and the Kafka-to-Splunk forwarder:

```bash
./.venv/bin/python runner/start_splunk_forwarder.py --build
```

4. Start the dashboard last:

```bash
./.venv/bin/python runner/start_dashboard.py --build
```

If the dashboard is still empty after step 4, the most likely leak points are:

- Kafka topic creation
- the ingestion bridge not consuming the topic snapshot
- detection not publishing to `detection.events`
- the forwarder not consuming `detection.events`
- Splunk HEC not accepting writes

## Reset and reseed Splunk

If you want to refresh the demo index, clear the live Splunk index and then replay the bundled mock data into Splunk HEC.

```bash
./.venv/bin/python runner/reseed_live_splunk.py \
  --index revtrace \
  --hec-url https://localhost:8088 \
  --hec-token revtrace-hec-token \
  --splunk-password Changeme123! \
  --no-verify-cert
```

To seed only one mock transaction flow:

```bash
./.venv/bin/python runner/simulate_case.py \
  --hec-url https://localhost:8088 \
  --hec-token revtrace-hec-token \
  --index revtrace \
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
6. Detection publishes the same event to `detection.events`.
7. The Splunk forwarder consumes `detection.events` and writes to Splunk HEC.
8. The dashboard queries Splunk and displays both traceability and details.
