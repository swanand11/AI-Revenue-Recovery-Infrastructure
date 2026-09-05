# Debug Run Guide

## One-command startup

The recommended local startup is now `run.sh`. It builds the Compose images once,
then opens one terminal window per active service so Kafka, Detection, Recovery,
Settlement, Splunk, and Dashboard logs remain independently visible.

```bash
chmod +x run.sh
./run.sh
```

Open the admin control room at <http://localhost:8080> or
<http://localhost:8080/admin>. The default mock scenario is a continuous
`live_mix` of successful captures, checkout abandonment, provider failures,
recovery follow-up attempts, and async settlement. Override it before starting
when you want a specific flow:

```bash
MOCK_SCENARIO=normal ./run.sh
MOCK_SCENARIO=payment_failure ./run.sh
SETTLEMENT_OUTCOME_PATTERN=success,failed,success SETTLEMENT_BATCH_INTERVAL_SECONDS=5 ./run.sh
```

Useful options:

```bash
SKIP_BUILD=1 ./run.sh       # reuse already-built images
./run.sh --down             # stop and remove the Compose stack
```

Clean application state before a verified run:

```bash
scripts/reset_demo_state.sh
docker compose up
PYTHONPATH=. ./.venv/bin/python scripts/verify_live_system.py
```

The reset script removes only this Compose application’s Kafka, WAL, Splunk,
and dashboard materialized-state volumes plus local dashboard snapshots.

`run.sh` detects `gnome-terminal`, `konsole`, `xfce4-terminal`, `kitty`, or
`xterm`. If no supported terminal emulator is installed, use the manual
per-terminal commands below.

The manual startup remains below for debugging individual services or running a
single layer in the foreground.

Start each layer in its own terminal. Keep the terminals open: every service prints the last event it produced, consumed, or forwarded.

## Admin UI routes

The admin UI is served by the existing dashboard service and reuses the same
backend state. Use these pages during the demo:

```text
/admin
/admin/ingestion
/admin/detection
/admin/providers
/admin/recovery/customer
/admin/recovery/provider
/admin/settlement
/admin/settlement/rca
/admin/money-trail
/admin/escalations
/admin/audit
/customer
/merchant
```

The admin portal observes the live materialized event store populated by
`ingestion-bridge`; it does not generate demo state. To run the live
integration observer after the stack is up, use:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/verify_live_system.py
```

Failed rows on `/admin/settlement` include a `Visualize money trail` link. Use
that link to inspect customer to merchant mapping, captured amount, settlement
status, and where the settlement rail stopped. Use `/customer` for payment-link
notification acceptance status and `/merchant` for merchant-facing settlement
messages.

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

With no scenario override, the demo emits a paced `live_mix`: normal successful
captures, checkout abandonment, payment failure, authorization failure, and
capture failure. Detection/RCA/Splunk output is visible from the valid terminal
failures, while successful captures continue accumulating toward exact
100-transaction settlement batches. Use `MOCK_SCENARIO=normal` when you need a
healthy lifecycle with no failure output. Change `MOCK_SEED` to reproduce or
vary the generated traffic.

Choose a coherent scenario when needed:

```bash
MOCK_SCENARIO=full_success ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=normal ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=payment_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=authorization_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=capture_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=checkout_failure ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=recovery_retry ./.venv/bin/python runner/start_mocks.py --build
MOCK_SCENARIO=random_failure MOCK_SEED=42 ./.venv/bin/python runner/start_mocks.py --build
```

For async settlement failure, keep the customer lifecycle successful through capture and configure the settlement batcher. The default Compose pattern is `success,failed,success`, so Batch 001 succeeds, Batch 002 fails with RCA/escalation, and Batch 003 succeeds unless overridden:

```bash
MOCK_SCENARIO=settlement_failure ./.venv/bin/python runner/start_mocks.py --build
SETTLEMENT_OUTCOME_PATTERN=success,failed,success SETTLEMENT_BATCH_INTERVAL_SECONDS=5 docker compose up settlement-service
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

- `/` or `/admin`: RevTrace Admin control room
- `/admin/settlement`: settlement batches with failed-batch money-trail links
- `/admin/money-trail?batch_id=<batch_id>`: customer to merchant settlement mapping
- `/admin/escalations`: highlighted bank and merchant escalation messages
- `/customer`: customer-side payment-link notification and acceptance view
- `/merchant`: merchant-side settlement status and messages

Copy a failed batch ID from `/admin/settlement`, click `Visualize money trail`,
and confirm that captured customer payments map to the affected merchant
settlement path.

## 15. Visualize agent beliefs and BFT logic

Use the Recovery API directly when you need transaction-level agent detail:

```bash
curl http://localhost:8090/recovery/<transaction_id>
```

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
