import os
import uuid
from concurrent import futures
import grpc

from recovery.api import intent_pb2, intent_pb2_grpc
from recovery.agents.intent.splunk_client import SplunkClient
from recovery.agents.intent.scoring import calculate_intent
from recovery.models.contracts import utc_now

class IntentAgentServicer(intent_pb2_grpc.IntentAgentServiceServicer):
    def __init__(self, splunk_client: SplunkClient):
        self.splunk_client = splunk_client
        self.agent_id = "intent-agent"
        self.agent_version = "intent-v1"

    def Evaluate(self, request: intent_pb2.IntentContext, context: grpc.ServicerContext) -> intent_pb2.BeliefResponse:
        try:
            history = self.splunk_client.get_customer_history(request.customer_id)
        except Exception as e:
            context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
            
        evidence = calculate_intent(history)
        score = evidence["intent_score"]
        
        if score >= 5.0:
            recommendation = "RECOVER"
            reason = "STRONG_CUSTOMER_INTENT"
        elif score > 0.0:
            recommendation = "RECOVER"
            reason = "POSITIVE_CUSTOMER_INTENT"
        elif score == 0.0:
            recommendation = "DO_NOTHING"
            reason = "NEUTRAL_OR_INSUFFICIENT_HISTORY"
        else:
            recommendation = "DO_NOTHING"
            reason = "POOR_CUSTOMER_INTENT"
            
        return intent_pb2.BeliefResponse(
            belief_id=f"belief_{uuid.uuid4().hex[:8]}",
            agent_id=self.agent_id,
            agent_version=self.agent_version,
            transaction_id=request.transaction_id,
            state_version=request.state_version,
            recommendation=recommendation,
            confidence=evidence["confidence"],
            reason_code=reason,
            timestamp=utc_now(),
            evidence=intent_pb2.BeliefEvidence(
                intent_score=evidence["intent_score"],
                intent_level=evidence["intent_level"],
                historical_transactions=evidence["historical_transactions"],
                successful_transactions=evidence["successful_transactions"],
                failed_transactions=evidence["failed_transactions"],
                checkout_failures=evidence["checkout_failures"],
                payment_failures=evidence["payment_failures"],
                authorization_failures=evidence["authorization_failures"],
                capture_failures=evidence["capture_failures"],
                settlement_failures=evidence["settlement_failures"],
                retries=evidence["retries"],
                history_status=evidence["history_status"],
                confidence=evidence["confidence"],
                raw_event_count=evidence["raw_event_count"],
                parsed_event_count=evidence["parsed_event_count"],
                parse_error_count=evidence["parse_error_count"],
            )
        )

def serve():
    splunk_url = os.environ.get("SPLUNK_REST_URL", "https://localhost:8089")
    splunk_user = os.environ.get("SPLUNK_USERNAME", "admin")
    splunk_password = os.environ.get("SPLUNK_PASSWORD", "Changeme123!")
    verify_tls = os.environ.get("SPLUNK_VERIFY_TLS", "false").lower() == "true"
    
    splunk_client = SplunkClient(
        base_url=splunk_url, 
        username=splunk_user, 
        password=splunk_password, 
        verify_tls=verify_tls
    )
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    intent_pb2_grpc.add_IntentAgentServiceServicer_to_server(IntentAgentServicer(splunk_client), server)
    
    port = os.environ.get("INTENT_AGENT_PORT", "50051")
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Intent Agent running on port {port}...", flush=True)
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
