from .LBStrategy import LoadBalancerStrategy
from typing import Dict
from .models import Server
import random

class RoundRobin(LoadBalancerStrategy):
    def __init__(self, servers: Dict[str, Server], timeout: int, healthy: float):
        super().__init__(servers, timeout, healthy)
        
        self.ordered_servers = list(self.servers.keys())
        self.current_idx = random.randint(0, len(self.ordered_servers) - 1)
        self.n = len(self.servers)

    def __next__(self):
        """Select next server in round-robin fashion, skipping unhealthy ones"""
        iterations = 0
        
        while iterations < self.n:
            # Move to next server
            self.current_idx = (self.current_idx + 1) % self.n
            
            server_key = self.ordered_servers[self.current_idx]
            server = self.servers[server_key]
            
            if server.healthy and server.active_connections < server.max_connections:
                return server_key
            
            iterations += 1
        
        # No healthy servers found after full rotation
        raise RuntimeError("Load Balancer Failed. No healthy servers available with capacity.")

    def add_server(self, server: Server):
        assert isinstance(server, Server), "failed to add server, input is not of type Server"
        self.ordered_servers.append(server)
        self.n += 1
            
        
            