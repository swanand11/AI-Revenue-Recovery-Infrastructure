from __future__ import annotations

import datetime as dt
import ssl

from detection.publisher.splunk import (
    NullSplunkAdapter,
    build_hec_event_url,
    build_ssl_context,
    normalize_hec_time,
)


def test_detection_event_to_splunk_interface():
    adapter = NullSplunkAdapter()
    assert adapter.emit({"transaction_id": "txn_1"}) is None


def test_build_hec_event_url_uses_https_for_localhost():
    url = build_hec_event_url("https://localhost:8088")
    assert url == "https://localhost:8088/services/collector/event"


def test_build_ssl_context_supports_local_self_signed_cert():
    context = build_ssl_context(verify_cert=False)
    assert context.verify_mode == ssl.CERT_NONE
    assert context.check_hostname is False


def test_build_ssl_context_keeps_verification_by_default():
    context = build_ssl_context(verify_cert=True)
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True


def test_normalize_hec_time_converts_iso_string_to_epoch_seconds():
    iso_value = "2026-08-26T14:00:00.000Z"
    expected = dt.datetime.fromisoformat(iso_value.replace("Z", "+00:00")).timestamp()
    assert normalize_hec_time(iso_value) == expected

