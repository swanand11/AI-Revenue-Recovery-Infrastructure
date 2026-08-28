# AI-Revenue-Recovery-Infrastructure

## Detection Layer

The detection layer consumes the existing ingestion topics:

- `checkout.events`
- `payment.events`
- `authorization.events`
- `capture.events`
- `settlement.events`

It preserves the original Kafka keying convention (`transaction_id`), calculates failure, degradation, intent, and graph-based RCA signals, then publishes detection output to `detection.events`.

### Services

- `detection-service` runs the Kafka consumer and publisher loop.
- `detection.events` is created in Kafka alongside the ingestion topics.
- A Splunk adapter interface is provided so detection can emit to Splunk without coupling the core pipeline to a vendor transport.
- `splunk` runs locally in Docker with persisted `/opt/splunk/etc` and `/opt/splunk/var` volumes.
- `splunk-seed` loads the bundled synthetic dataset into Splunk through HEC.

### Historical data and model artifacts

Use the scripts under `detection/scripts/` to generate synthetic historical data and precompute lightweight model artifacts. Generated artifacts should live outside application code.

### Splunk seeding

Use `scripts/mock_splunk_seed.py` to push the bundled datasets into Splunk HEC, and `scripts/splunk_bootstrap.py` for a small bootstrap event.

Local development uses the HTTPS HEC endpoint on `https://localhost:8088` and the repo’s local self-signed Splunk certificate. Set `SPLUNK_HEC_VERIFY_CERT=false` for the local dev default, or supply `--no-verify-cert` to the seed/bootstrap scripts when using the self-signed cert.
