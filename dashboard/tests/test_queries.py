from __future__ import annotations

from dashboard.splunk_client import SplunkClient, SplunkConfig, build_smart_query, build_trace_query


def test_trace_query_contains_transaction_id():
    query = build_trace_query("revtrace", "txn_123")
    assert 'search index=revtrace' in query
    assert 'transaction_id="txn_123"' in query


def test_smart_query_contains_filters():
    query = build_smart_query("revtrace", {"stage": "payment", "status": "failure"})
    assert 'search index=revtrace' in query
    assert 'stage="payment"' in query
    assert 'status="failure"' in query
