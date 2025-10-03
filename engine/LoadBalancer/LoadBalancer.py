from .models import Server
from typing import Dict
from .LeastConnections import LeastConnectionsLoadBalancing

class LoadBalancer():
    STRATEGIES = ["least_connections"]

    def __init__(self, strategy: str, timeout: int, servers: Dict[Server], healthy: float):
        assert isinstance(servers, list), "servers must be a list"
        assert len(servers) > 0, "must have at least 1 server"
        assert all(isinstance(server, Server) for key, server in servers.items()), "all items in servers must be Server objects"
        self.strategy = strategy
        self.timeout = timeout
        self.healthy = healthy
        self.load_balancer = self.__init_load_balancer(self.strategy)

    def __init_load_balancer(self, strategy: str):
        match strategy:
            case "least_connections":
                return LeastConnectionsLoadBalancing(servers=self.servers, timeout=self.timeout)

    async def handle_incoming_request(self, request):
        return self.load_balancer.handle_request(request)
