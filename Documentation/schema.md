demo ingestion event schema
{
  "event_id": "evt_01JXYZ...",
  "event_version": 1,

  "timestamp": "2026-08-26T14:31:02.481Z",

  "service": "authorization-service",
  "stage": "authorization",
  "event_type": "authorization_failed",

  "merchant_id": "merchant_001",
  "customer_id": "customer_9182",
  "order_id": "order_72819",
  "transaction_id": "txn_839201",
  "payment_id": "pay_293812",

  "trace_id": "trace_839201",
  "span_id": "span_7812",
  "parent_span_id": "span_7121",

  "amount": 5000,
  "currency": "INR",

  "status": "failure",
  "failure_code": "TIMEOUT",

  "metadata": {}
}
demo detection event schema 
{
  "detection_id": "det_01JXYZ...",
  "detection_version": 1,
  "timestamp": "2026-08-27T14:31:02.481Z",

  "event_id": "evt_01JXYZ...",
  "trace_id": "trace_839201",
  "transaction_id": "txn_839201",
  "payment_id": "pay_293812",
  "order_id": "order_72819",

  "merchant_id": "merchant_001",
  "customer_id": "customer_9182",

  "service": "authorization-service",
  "stage": "authorization",
  "event_type": "authorization_failed",

  "payment_method": "UPI",
  "payment_provider": "gateway_B",

  "failure_code": "ISSUER_TIMEOUT",

  "signals": {
    "customer_intent_score": 0.81,
    "current_median_intent": 0.57,
    "payment_degradation_score": 0.94
  },

  "root_cause": {
    "type": "gateway_degradation",
    "component": "gateway_B",
    "confidence": 0.93,
    "evidence": [
      "timeout_rate_above_baseline",
      "gateway_latency_spike"
    ]
  },

  "amount": 5000,
  "currency": "INR",

  "model_version": "detection_v1"
}

Settlement batch event schema. Completed batches contain exactly 100 captured
transactions. `status` stays in the source event vocabulary
`success | failure | unknown`; `batch_status` carries the settlement batch
state machine.
{
  "event_id": "evt_01JXYZ...",
  "event_version": 1,
  "timestamp": "2026-09-04T14:31:02.481Z",
  "service": "settlement-service",
  "stage": "settlement",
  "event_type": "settlement_batch_failed",
  "batch_id": "batch_001",
  "transaction_date": "2026-09-04",
  "transaction_count": 100,
  "gross_captured_amount": 50000,
  "settled_amount": 0,
  "failed_amount": 50000,
  "pending_amount": 0,
  "settlement_at_risk": 50000,
  "currency": "INR",
  "status": "failed",
  "batch_status": "FAILED",
  "failure_code": "CHECKSUM_MISMATCH",
  "root_cause": {
    "candidate_root_cause": "bank_file_corruption",
    "confidence": 0.91,
    "evidence": []
  },
  "complaint": {
    "complaint_id": "cmp_batch_001",
    "status": "ESCALATED_TO_BANK",
    "owner": "Bank Operations"
  }
}
syntehetic payment historical dataset demo record 
{
  "observation_id": "obs_001",

  "timestamp": "2026-07-14T14:00:00Z",

  "payment_method": "UPI",
  "payment_provider": "gateway_B",

  "merchant_id": "merchant_001",

  "attempts": 10000,
  "successful": 9730,
  "failed": 270,

  "failure_rate": 0.027,
  "timeout_rate": 0.011,

  "avg_latency_ms": 842,

  "failure_codes": {
    "ISSUER_TIMEOUT": 110,
    "GATEWAY_ERROR": 60
  }
}
customer -intent data 
{
  "observation_id": "cust_obs_001",
  "timestamp": "2026-07-14T14:05:00Z",

  "customer_id": "customer_9182",
  "session_id": "session_7721",
  "transaction_id": "txn_839201",

  "checkout_started": true,
  "payment_attempted": true,
  "payment_failed": true,
  "payment_retried": true,
  "checkout_abandoned": false,

  "previous_attempts": 3,
  "previous_successes": 2,
  "time_to_retry_seconds": 38,

  "outcome": {
    "completed_within_24h": true
  }
}
payment dataset model metadata
{
  "model_version": "detection_v1",
  "model_type": "root_cause_classifier",
  "trained_at": "2026-08-27T00:00:00Z",

  "training_window": {
    "start": "2026-07-01",
    "end": "2026-08-26"
  },

  "features": [
    "failure_rate",
    "timeout_rate",
    "latency",
    "payment_method",
    "provider"
  ]
}
