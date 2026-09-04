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
recovery.acknowledgements
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
MOCK_SCENARIO=full_success ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=normal ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=payment_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=authorization_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=capture_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=settlement_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=checkout_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=recovery_retry ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=random_failure MOCK_SEED=42 ./.venv/bin/python runner/start_mocks.py --build
```

Every normal-flow line should show `status=success`. A `failure` line is terminal;
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

## 5. Start Redis for Recovery state

In a fifth terminal:

```bash
docker compose up redis
```

Watch for:

```text
Ready to accept connections
```

## 6. Start the Intent Agent

In a sixth terminal:

```bash
docker compose up intent-agent
```

The Intent Agent is a standalone gRPC agent on port `50051`. It evaluates customer history and returns a signed `intent-agent` belief.

Watch for:

```text
Intent Agent running on port 50051
```

## 7. Start the Provider Agent

In a seventh terminal:

```bash
docker compose up provider-agent
```

The Provider Agent is a standalone gRPC agent on port `50052`. It predicts provider degradation from provider health signals. If `degradation_probability` is not already present, it precomputes it with the same logistic-regression model code used by Detection.

Watch for:

```text
Provider Agent running on port 50052
```

## 8. Start the Transaction Agent debug runner

In an eighth terminal:

```bash
docker compose up transaction-agent
```

This starts a standalone debug process for the Transaction Agent. The live recovery service still invokes the Transaction Agent in-process for candidate evaluation, while this runner proves startup and commits `recovery.agent_started` plus heartbeat records to the WAL.

Watch for:

```text
transaction-agent debug runner started
transaction-agent heartbeat
```

## 9. Start the Economics Agent debug runner

In a ninth terminal:

```bash
docker compose up economics-agent
```

This starts a standalone debug process for the Economics Agent and writes startup/heartbeat records to the recovery WAL.

Watch for:

```text
economics-agent debug runner started
economics-agent heartbeat
```

## 10. Start the Risk Agent debug runner

In a tenth terminal:

```bash
docker compose up risk-agent
```

This starts a standalone debug process for the Risk Agent and writes startup/heartbeat records to the recovery WAL.

Watch for:

```text
risk-agent debug runner started
risk-agent heartbeat
```

## 11. Start the Recovery API

In an eleventh terminal:

```bash
docker compose up recovery-api
```

Inspect state with:

```bash
curl http://localhost:8090/recovery/<transaction_id>
```

## 12. Start the Recovery Service

In a twelfth terminal:

```bash
docker compose up recovery-service
```

The Recovery service consumes failure-only `recovery.events`, calls the standalone `intent-agent` and `provider-agent` over gRPC, and runs the local assessment agents in-process:

```text
transaction-agent
economics-agent
risk-agent
```

For Phase 1 it collects five beliefs, commits recovery WAL records, evaluates consensus and policy guardrails, and persists the assessment state in Redis. It does not execute recovery actions, retry payments, switch providers, or mark money as recovered.

Watch for:

```text
[recovery-service] acknowledged transaction_id=... state_version=... duplicate=... beliefs_generated=...
```

Recovery WAL records should include:

```text
recovery.candidate_received
recovery.agents_evaluating
recovery.belief_recorded
recovery.beliefs_collected
recovery.recovery_assessed
```

## 13. Start Splunk and its forwarder

In a thirteenth terminal:

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

## 14. Start the dashboard

In a fourteenth terminal:

```bash
./.venv/bin/python runner/start_dashboard.py --build
```

Open `http://localhost:8080`.

Dashboard views:

- `/`: Kafka ingestion table, Recovery output table, search, and event JSON
- `/flow`: newest events arriving in Splunk
- `/lifecycle`: enter a `transaction_id` and check Kafka ingestion plus Recovery output across checkout → payment → auth → capture → settlement
- `/profile`: enter a `customer_id` to view Intent Profiler and analyze intent scoring, or enter a `transaction_id` in the Recovery Assessment to view independent agent beliefs.

Copy a transaction ID from the ingestion terminal or dashboard, open `/lifecycle`, and click `Load lifecycle`. The trace joins the Kafka bridge records with the Splunk Recovery record. Source ingestion and Detection output are shown separately even when they share the same `event_id`. A missing stage identifies the leak.

## 15. Visualize agent beliefs and BFT logic

In the dashboard, open:

```text
http://localhost:8080/profile
```

Enter a `transaction_id` in Recovery Assessment and click `Get Assessment`. The page shows:

- all agent beliefs and evidence
- consensus decision
- quorum support vs threshold
- valid agent count
- conflicts detected
- per-agent BFT status such as `PARTICIPATING`, `MINORITY`, `REJECTED`, or `QUARANTINED`
- Phase 1 policy/guardrail assessment

You can also inspect the raw state:

```bash
curl http://localhost:8090/recovery/<transaction_id>
```

The JSON response includes:

```text
agent_beliefs
consensus
policy
action
outcome
```

In Phase 1, `action` and `outcome` remain `null` because recovery execution is intentionally not implemented yet.

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
