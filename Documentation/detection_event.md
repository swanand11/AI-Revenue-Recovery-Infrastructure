detection event schema
{
  "event_id": "evt_01JXYZ...",
  "event_version": 1,

  "timestamp": "2026-08-26T14:31:02.481Z",

  "service": "detection-service",
  "stage": "payment",
  "event_type": "payment_failed",

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
  "failure_code": "GATEWAY_ERROR",

  "metadata": {
    "source_event_type": "payment_failed",
    "source_status": "failure",
    "signals": {},
    "root_cause": {}
  }
}

Detection is an analytical annotation over a real failed source event. It is
not a lifecycle stage and it does not emit fake `*_detected` event types.
