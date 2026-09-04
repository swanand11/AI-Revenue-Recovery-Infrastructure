import os

INTENT_SUCCESS_REWARD = float(os.getenv("INTENT_SUCCESS_REWARD", "3.0"))
INTENT_RETRY_REWARD = float(os.getenv("INTENT_RETRY_REWARD", "1.0"))

INTENT_CHECKOUT_FAILURE_PENALTY = float(os.getenv("INTENT_CHECKOUT_FAILURE_PENALTY", "-5.0"))
INTENT_PAYMENT_FAILURE_PENALTY = float(os.getenv("INTENT_PAYMENT_FAILURE_PENALTY", "-3.0"))
INTENT_AUTH_FAILURE_PENALTY = float(os.getenv("INTENT_AUTH_FAILURE_PENALTY", "-2.0"))
INTENT_CAPTURE_FAILURE_PENALTY = float(os.getenv("INTENT_CAPTURE_FAILURE_PENALTY", "-1.5"))
INTENT_SETTLEMENT_FAILURE_PENALTY = float(os.getenv("INTENT_SETTLEMENT_FAILURE_PENALTY", "-1.0"))

def get_intent_level(score: float) -> str:
    if score < -5.0:
        return "LOW"
    elif -5.0 <= score < 0.0:
        return "BELOW_NORMAL"
    elif score == 0.0:
        return "NEUTRAL"
    elif 0.0 < score < 5.0:
        return "POSITIVE"
    else:
        return "HIGH"
