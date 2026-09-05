# UI Navigation Guide

This guide is for walking through the RevTrace live admin UI after running
`./run.sh`.

## Start Here

Open:

```text
http://localhost:8080
```

This opens the admin control room. There are no scenario buttons; payment,
recovery, and settlement activity appears as the backend processes Kafka
events.

## Admin Control Room

Open:

```text
http://localhost:8080/admin
```

The admin page has tabs for every major operator workflow:

- `Overview`: top-level transaction, recovery, money, provider, settlement, escalation, and event-rate metrics.
- `Ingestion`: recent Kafka ingestion events with source fields such as service, stage, event type, status, amount, payment ID, and trace ID.
- `Detection`: actual failure detections only, with intent score, current median, provider degradation, RCA, confidence, and recoverability.
- `Providers`: Gateway_A and Gateway_B health, success rate, failure rate, timeout rate, latency, degradation probability, and current state.
- `Customer Recovery`: high-intent payment-link flow from failure through notification, customer open, payment attempt, verified capture, conversion rate, and recovered amount.
- `Provider Recovery`: provider degradation flow showing Gateway_B to Gateway_A, retry result, authorization, capture, and recovered amount only when a recovery capture exists.
- `Settlement`: async settlement batch state, including exactly 100 transactions per completed batch, captured amount, settled amount, failed amount, at-risk revenue, and failure percentage.
- `Money Trail`: customer-to-merchant settlement visualizer. Failed settlement batches link here directly from the `Settlement` table.
- `Settlement RCA`: candidate cause ranking, confidence, evidence, affected count, and revenue at risk.
- `Escalations`: highlighted complaint workflow, current owner, next action, escalation chain, and clearer bank/merchant messages.
- `Audit`: traceable events for detections, recovery actions, settlement RCA, and escalations.

## Customer And Merchant Pages

Open:

```text
http://localhost:8080/customer
```

Use this as the customer-side view. It shows the notification, payment-link
status, customer acceptance state, and recovered amount for live customer
recovery links.

Open:

```text
http://localhost:8080/merchant
```

Use this as the merchant-side view. It shows settlement batches, captured
amounts, settled amounts, money at risk, merchant-facing messages, and a link
back to the money-trail visualizer for failed batches.

## Suggested Demo Path

1. Start the stack with `./run.sh` or `docker compose up`.
2. Open `/admin` and watch the money metrics update from live events.
3. Open `/admin/ingestion` to verify Kafka events are arriving without refreshing the page.
4. Open `/admin/detection` and show that detections attach to failures, not successes.
5. Open `/admin/recovery/customer` and confirm links are sent only for above-median intent cases; only a small percentage convert to captured recovery revenue.
6. Open `/admin/recovery/provider` when a provider recovery appears and verify the provider changes from `Gateway_B` to `Gateway_A`.
7. Open `/admin/settlement` and show settlement remains async after capture; every completed batch has exactly 100 unique captured transactions.
8. On `/admin/settlement`, click `Visualize money trail` on a failed batch to show customer-to-merchant mapping and where the settlement flow stopped.
9. Open `/admin/settlement/rca`, then `/admin/escalations`, to show highlighted owner, next action, bank message, and merchant message.
10. Open `/customer` and `/merchant` to show the simplified external views.
11. Finish on `/admin/audit` to show traceability across decisions and money impact.

## Useful Commands

Observe the live integration path through the Admin API:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/verify_live_system.py
```

Reset all application-owned demo state before a fresh run:

```bash
scripts/reset_demo_state.sh
```

Run the focused tests for the patched areas:

```bash
PYTHONPATH=. ./.venv/bin/pytest recovery/tests/test_recovery.py runner/tests/test_transaction_lifecycle_integrity.py runner/tests/test_transaction_state_machine.py runner/tests/test_mock_pipeline.py runner/tests/test_phase2_settlement_batching.py dashboard/tests/test_trace.py dashboard/tests/test_queries.py -q
```
