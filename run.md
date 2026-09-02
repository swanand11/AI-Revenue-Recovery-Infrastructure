# Debug Run Guide

Start each layer in its own terminal. Keep the terminals open: every service prints the last event it produced, consumed, or forwarded.

## 0. Stop old containers

```bash
docker compose down
```

## 1. Start Kafka

```bash
docker compose up kafka kafka-init
```

Watch for a healthy broker and creation of these topics:

```text
checkout.events
payment.events
authorization.events
capture.events
settlement.events
detection.events
recovery.events
```

## 2. Start ingestion

In a second terminal:

```bash
./.venv/bin/python runner/start_mocks.py --build
```

This starts one authoritative `mock-pipeline`, not five independent transaction generators. Watch for `sent topic=... key=... event_id=... transaction_id=...`. If the transaction ID changes within one lifecycle, the leak is at the generator boundary.

With no scenario override, the demo emits a seeded random failure at a valid lifecycle stage with a contract-valid failure code, so Detection/RCA/Splunk output is visible immediately. Use `MOCK_SCENARIO=normal` when you need a healthy lifecycle with no failure output. Change `MOCK_SEED` to reproduce or vary the generated failure.

Choose a coherent scenario when needed:

```bash
MOCK_SCENARIO=normal ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=payment_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=authorization_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=capture_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=settlement_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=random_failure MOCK_SEED=42 ./.venv/bin/python runner/start_mocks.py --build
```

To reproduce an unknown outcome at a specific lifecycle boundary, use one of:

```bash
MOCK_SCENARIO=checkout_unknown ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=payment_unknown ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=authorization_unknown ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=capture_unknown ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=settlement_unknown ./.venv/bin/python runner/start_mocks.py --build
```

Every normal-flow line should show `status=success`. A `failure` or `unknown` line is terminal;
the producer must not print any later lifecycle event for that transaction. The producer also
prints `lifecycle_validation=passed` before each accepted event. Any invalid source change should
fail with `INVALID_LIFECYCLE_TRANSITION` before WAL/Kafka publication.

Run one named transaction for dashboard reconstruction:

```bash
MOCK_TRANSACTION_ID=txn_trace_final_001 MOCK_SCENARIO=normal ./.venv/bin/python runner/start_mocks.py --build
```

## 3. Start the ingestion bridge

In a third terminal:

```bash
docker compose up ingestion-bridge
```

Watch for:

```text
[ingestion-bridge] captured topic=... key=... event_id=... transaction_id=...
```

This Kafka-side feed powers the dashboard ingestion table. It replays available offsets so starting it after the mock pipeline does not hide the lifecycle.

## 4. Start Detection

In a fourth terminal:

```bash
./.venv/bin/python runner/start_detection.py --build
docker compose logs -f detection-service
```

Watch for this sequence for every event:

```text
consumed topic=... key=... event_id=... transaction_id=... transport_validation=passed
published detection event_id=... transaction_id=...
```

The publisher writes the same Detection Event to both `detection.events` and `recovery.events`.

The next line proves the Detection components ran:

```text
validation=passed ... failure_applied=True ... ewma_degradation_applied=True ml_applied=True ml_probability=... intent_applied=True rca_applied=True rca_component=... rca_confidence=...
```

If `validation=failed`, inspect `validation_error`. If `consumed` appears but `published` does not, inspect `reason`, `ml_probability`, and the RCA fields.

## 5. Start Splunk and its forwarder

In a fifth terminal:

```bash
./.venv/bin/python runner/start_splunk_forwarder.py --build
docker compose logs -f splunk-forwarder
```

Watch for:

```text
Splunk forwarder listening on recovery.events
FORWARDED topic=recovery.events key=... event_id=... transaction_id=...
```

Splunk is not on Detection’s Kafka hot path. Detection must continue if HEC is unavailable.

## 6. Start the dashboard

In a sixth terminal:

```bash
./.venv/bin/python runner/start_dashboard.py --build
```

Open `http://localhost:8080`.

Dashboard views:

- `/`: Kafka ingestion table, Recovery output table, search, and event JSON
- `/flow`: newest events arriving in Splunk
- `/lifecycle`: enter a `transaction_id` and check Kafka ingestion plus Recovery output across checkout → payment → auth → capture → settlement

Copy a transaction ID from the ingestion terminal or dashboard, open `/lifecycle`, and click `Load lifecycle`. The trace joins the Kafka bridge records with the Splunk Recovery record. Source ingestion and Detection output are shown separately even when they share the same `event_id`. A missing stage identifies the leak.

## Useful checks

```bash
docker compose ps
docker compose logs -f kafka
docker compose logs -f ingestion-bridge
docker compose logs -f detection-service
docker compose logs -f splunk-forwarder
```

Check Kafka delivery and partition affinity with a deterministic batch:

```bash
./.venv/bin/python scripts/generate_events.py --count 20 --seed 7 --bootstrap-server localhost:9092 --output /tmp/events_sent.json
./.venv/bin/python scripts/verify_events.py --sent-events /tmp/events_sent.json --bootstrap-server localhost:9092 --report /tmp/events_report.json
```

Run tests without Docker:

```bash
./.venv/bin/python -m pytest -q detection/tests
./.venv/bin/python -m pytest -q
```

Reset Splunk demo data if needed:

```bash
./.venv/bin/python runner/reseed_live_splunk.py --index revtrace --hec-url https://localhost:8088 --hec-token revtrace-hec-token --splunk-password Changeme123! --no-verify-cert
```

Stop everything:

```bash
docker compose down
```
