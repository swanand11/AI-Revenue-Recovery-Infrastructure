from recovery.agents.intent.scoring import calculate_intent

def test_new_customer():
    events = []
    res = calculate_intent(events)
    assert res['intent_score'] == 0.0
    assert res['confidence'] == 0.2

def test_one_checkout_failure():
    events = [{'event_type': 'checkout_failed', 'stage': 'checkout', 'transaction_id': 't1'}]
    res = calculate_intent(events)
    assert res['intent_score'] == -5.0
    
def test_one_payment_failure():
    events = [{'event_type': 'payment_failed', 'stage': 'payment', 'transaction_id': 't1'}]
    res = calculate_intent(events)
    assert res['intent_score'] == -3.0
    
def test_authorization_failure():
    events = [{'event_type': 'authorization_failed', 'stage': 'authorization', 'transaction_id': 't1'}]
    res = calculate_intent(events)
    assert res['intent_score'] == -2.0

def test_capture_failure():
    events = [{'event_type': 'capture_failed', 'stage': 'capture', 'transaction_id': 't1'}]
    res = calculate_intent(events)
    assert res['intent_score'] == -1.5

def test_settlement_failure():
    events = [{'event_type': 'settlement_failed', 'stage': 'settlement', 'transaction_id': 't1'}]
    res = calculate_intent(events)
    assert res['intent_score'] == -1.0

def test_successful_transaction():
    events = [{'event_type': 'payment_succeeded', 'stage': 'payment', 'transaction_id': 't1'}]
    res = calculate_intent(events)
    assert res['intent_score'] == 3.0

def test_multiple_checkout_failures():
    events = [
        {'event_type': 'checkout_failed', 'stage': 'checkout', 'transaction_id': 't1', 'timestamp': '1'},
        {'event_type': 'checkout_failed', 'stage': 'checkout', 'transaction_id': 't2', 'timestamp': '2'}
    ]
    res = calculate_intent(events)
    assert res['intent_score'] == -10.0

def test_good_customer():
    events = [
        {'event_type': 'payment_succeeded', 'stage': 'payment', 'transaction_id': 't1', 'timestamp': '1'},
        {'event_type': 'payment_succeeded', 'stage': 'payment', 'transaction_id': 't2', 'timestamp': '2'},
    ]
    res = calculate_intent(events)
    assert res['intent_score'] == 6.0
    assert res['intent_level'] == 'HIGH'

def test_mixed_customer():
    events = [
        {'event_type': 'payment_started', 'stage': 'payment', 'transaction_id': 't1', 'timestamp': '1'},
        {'event_type': 'payment_failed', 'stage': 'payment', 'transaction_id': 't1', 'timestamp': '2'},
        {'event_type': 'payment_started', 'stage': 'payment', 'transaction_id': 't2', 'timestamp': '3'},
        {'event_type': 'payment_succeeded', 'stage': 'payment', 'transaction_id': 't2', 'timestamp': '4'}
    ]
    res = calculate_intent(events)
    assert res['intent_score'] == 1.0
    assert res['retries'] == 1

def test_no_double_counting():
    events = [
        {'event_type': 'payment_succeeded', 'stage': 'payment', 'transaction_id': 't1', 'timestamp': '1'},
        {'event_type': 'authorization_succeeded', 'stage': 'authorization', 'transaction_id': 't1', 'timestamp': '2'}
    ]
    res = calculate_intent(events)
    assert res['intent_score'] == 3.0
    assert res['successful_transactions'] == 1
