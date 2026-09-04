import os
import uuid
import json
from concurrent import futures
import grpc

from recovery.api import provider_pb2, provider_pb2_grpc
from recovery.agents.provider.scoring import evaluate_provider
from recovery.models.contracts import utc_now

class ProviderAgentServicer(provider_pb2_grpc.ProviderAgentServiceServicer):
    def __init__(self):
        self.agent_id = "provider-agent"
        self.agent_version = "provider-v1"

    def Evaluate(self, request: provider_pb2.ProviderContext, context: grpc.ServicerContext) -> provider_pb2.ProviderBeliefResponse:
        evidence = evaluate_provider(request)
        
        return provider_pb2.ProviderBeliefResponse(
            belief_id=f"belief_{uuid.uuid4().hex[:8]}",
            agent_id=self.agent_id,
            agent_version=self.agent_version,
            transaction_id=request.transaction_id,
            state_version=request.state_version,
            recommendation=evidence["recommendation"],
            confidence=evidence["confidence"],
            reason_code=evidence["reason_code"],
            timestamp=utc_now(),
            evidence=provider_pb2.ProviderBeliefEvidence(
                payment_method=evidence["evidence"]["payment_method"],
                current_provider=evidence["evidence"]["current_provider"],
                failure_code=evidence["evidence"]["failure_code"],
                provider_failure_rate=evidence["evidence"]["provider_failure_rate"],
                provider_timeout_rate=evidence["evidence"]["provider_timeout_rate"],
                provider_degradation_probability=evidence["evidence"]["provider_degradation_probability"],
                alternative_provider_available=evidence["evidence"]["alternative_provider_available"],
                alternative_provider=evidence["evidence"]["alternative_provider"]
            )
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    provider_pb2_grpc.add_ProviderAgentServiceServicer_to_server(ProviderAgentServicer(), server)
    
    port = os.environ.get("PROVIDER_AGENT_PORT", "50052")
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Provider Agent running on port {port}...", flush=True)
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
