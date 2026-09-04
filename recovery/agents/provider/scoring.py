import uuid
from recovery.api import provider_pb2
from recovery.agents.provider.health import precompute_provider_health
from recovery.models.contracts import utc_now

def evaluate_provider(context: provider_pb2.ProviderContext) -> dict:
    stage = context.current_failure.get("stage", "")
    failure_code = context.current_failure.get("failure_code", "")
    
    # Defaults
    payment_method = context.provider_metadata.get("payment_method", "UNKNOWN")
    current_provider = context.provider_metadata.get("provider", "UNKNOWN")
    
    # Try to extract signals if passed directly as stringified json or via Coordinator
    import json
    signals_str = context.provider_metadata.get("signals", "{}")
    try:
        signals = json.loads(signals_str)
    except Exception:
        signals = {}
        
    provider_failure_rate = float(signals.get("failure_rate", 0.0) or 0.0)
    provider_timeout_rate = float(signals.get("timeout_rate", 0.0) or 0.0)
    degradation_model = signals.get("degradation_model") or {}
    detection_degradation = signals.get("degradation") or {}
    provider_degradation_detected = bool(detection_degradation.get("anomaly"))
    explicit_degradation_probability = "degradation_probability" in signals
    if "degradation_probability" not in signals and "probability" in degradation_model:
        signals["degradation_probability"] = degradation_model["probability"]
    provider_health = precompute_provider_health(context, signals)
    provider_degradation_probability = provider_health["provider_degradation_probability"]
    
    alternative_provider = "Gateway_B" if current_provider == "Gateway_A" else "Gateway_A"
    if current_provider == "UNKNOWN":
        alternative_provider = "UNKNOWN"
        
    alternative_provider_available = alternative_provider != "UNKNOWN"
    
    provider_related_failures = {"TIMEOUT", "PROVIDER_ERROR", "GATEWAY_ERROR", "CONNECTION_ERROR", "ISSUER_TIMEOUT"}
    
    is_provider_fault = failure_code in provider_related_failures
    is_degraded = provider_degradation_detected or (
        explicit_degradation_probability and provider_degradation_probability > 0.5
    )
    
    if is_provider_fault and is_degraded:
        recommendation = "SWITCH_PROVIDER"
        confidence = max(0.8, provider_degradation_probability)
        reason_code = "PROVIDER_DEGRADED"
    elif is_provider_fault and not is_degraded:
        recommendation = "SWITCH_PROVIDER" # Wait, the prompt says: 'provider is not degraded. Expected: DO_NOTHING'
        recommendation = "DO_NOTHING"
        confidence = 0.8
        reason_code = "NO_PROVIDER_DEGRADATION"
    else:
        recommendation = "DO_NOTHING"
        confidence = 0.9
        reason_code = "CUSTOMER_SIDE_FAILURE" if not is_provider_fault else "NO_PROVIDER_DEGRADATION"
        
    if not alternative_provider_available and recommendation == "SWITCH_PROVIDER":
        recommendation = "DO_NOTHING"
        reason_code = "NO_ALTERNATIVE_PROVIDER"
        
    return {
        "recommendation": recommendation,
        "confidence": confidence,
        "reason_code": reason_code,
        "evidence": {
            "payment_method": payment_method,
            "current_provider": current_provider,
            "failure_code": failure_code,
            "provider_failure_rate": provider_failure_rate,
            "provider_timeout_rate": provider_timeout_rate,
            "provider_degradation_probability": provider_degradation_probability,
            "provider_degradation_detected": provider_degradation_detected,
            "provider_health_score": provider_health["provider_health_score"],
            "provider_health_source": provider_health["provider_health_source"],
            "provider_health_model": provider_health["provider_health_model"],
            "alternative_provider_available": alternative_provider_available,
            "alternative_provider": alternative_provider
        }
    }
