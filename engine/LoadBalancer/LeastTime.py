
from .LBStrategy import LoadBalancerStrategy
import time
import random
from .models import Server
from typing import Dict

class LeastTime(LoadBalancerStrategy):
    def __init__(self, servers: Dict[str, Server], timeout: int, healthy: float):
        super().__init__(servers, timeout, healthy)
        self.min_response_time = float("inf")
        self.min_response_time_key = None

    def __next__(self):
        # If we haven't tracked any response times yet, choose randomly from healthy servers
        if self.min_response_time_key is None:
            healthy_servers = [key for key, server in self.servers.items() if server.healthy]
            if not healthy_servers:
                # No healthy servers, fall back to any server
                healthy_servers = list(self.servers.keys())
            return random.choice(healthy_servers)

        # Return the server with minimum response time
        return self.min_response_time_key

    def finally_after_request(self, server: Server, elapsed: float):
        """Update minimum response time after each request"""
        # Check if this server has the best latency we've seen
        if server.metadata.latency < self.min_response_time and server.healthy:
            self.min_response_time = server.metadata.latency
            self.min_response_time_key = server.name
        
        return super().finally_after_request(server, elapsed)
