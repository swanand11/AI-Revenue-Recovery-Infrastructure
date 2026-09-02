# AI-Revenue-Recovery-Infrastructure

## Detection Layer

The detection layer consumes the existing ingestion topics, and the system follows a fixed payment-gateway lifecycle:

- `checkout`
- `payment`
- `auth`
- `capture`
- `settlement`

Internally, the code still uses the existing service/topic names, but every mock, detector, and dashboard view must stay faithful to that lifecycle order.

The detection layer consumes the existing ingestion topics:

- `checkout.events`
- `payment.events`
- `authorization.events`
- `capture.events`
- `settlement.events`

It preserves the original Kafka keying convention (`transaction_id`), calculates failure, degradation, intent, and graph-based RCA signals, then publishes detection output to `detection.events`.

### Services

- `detection-service` runs the Kafka consumer and publisher loop.
- Ingestion services publish directly to Kafka, and `detection-service` consumes those topics and forwards detection output to Splunk.
- `splunk-forwarder` consumes `detection.events` and writes the records to Splunk HEC.
- `detection.events` is created in Kafka alongside the ingestion topics.
- `splunk` runs locally in Docker with persisted `/opt/splunk/etc` and `/opt/splunk/var` volumes.
- `splunk-seed` loads the bundled synthetic dataset into Splunk through HEC.
- `ingestion-bridge` snapshots Kafka ingestion topics into a shared local store so the dashboard can render live ingestion tables without pretending the data already lives in Splunk.
- `mock-pipeline` is the active demo producer. It creates one transaction context, propagates it through the lifecycle, and stops a configured scenario at its failure point. The older per-service generators are available only under the `legacy` Compose profile.

### Historical data and model artifacts

Use the scripts under `detection/scripts/` to generate synthetic historical data and precompute lightweight model artifacts. Generated artifacts should live outside application code.

### Splunk seeding

Use `runner/reseed_live_splunk.py` to clear the live demo index and replay the bundled mock datasets into Splunk HEC.

Use `runner/simulate_case.py` for a single mock transaction flow seeded into Splunk HEC.

Use `scripts/splunk_bootstrap.py` for a small bootstrap event.

Local development uses the HTTPS HEC endpoint on `https://localhost:8088` and the repo’s local self-signed Splunk certificate. Set `SPLUNK_HEC_VERIFY_CERT=false` for the local dev default, or supply `--no-verify-cert` to the seed/bootstrap scripts when using the self-signed cert.
