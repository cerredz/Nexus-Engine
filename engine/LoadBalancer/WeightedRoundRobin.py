from .LBStrategy import LoadBalancerStrategy
from typing import Dict, List, Tuple
from .models import Server, WeightedRoundRobinRequests
import random

class WeightedRoundRobin(LoadBalancerStrategy):
    def __init__(self, servers: Dict[str, Server], timeout: int, healthy: float):
        super().__init__(servers, timeout, healthy)
        self.ordered_servers: List[Tuple[str, WeightedRoundRobinRequests]] = self.__build_round_request_data()
        self.n = len(self.ordered_servers)
        self.current_idx = random.randint(0, self.n - 1)

    def __build_round_request_data(self):
        """Build weighted round robin data: each server gets proportional requests"""
        res = []
        
        # Get minimum weight from all servers
        min_weight = min(server.weight for server in self.servers.values())
        
        # For each server, calculate how many requests it should get per round
        for server_key, server in self.servers.items():
            requests_per_round = server.weight // min_weight
            round_request_data = WeightedRoundRobinRequests(
                requests=requests_per_round, 
                request_number=0
            )
            res.append((server_key, round_request_data))
        
        return res

    def __next__(self):
        """Select next server in weighted round-robin fashion"""
        iterations = 0
        
        while iterations < self.n:
            self.current_idx = (self.current_idx + 1) % self.n
            iterations += 1
            
            server_key, round_request = self.ordered_servers[self.current_idx]
            server = self.servers[server_key]
            
            # Skip unhealthy or at-capacity servers
            if not server.healthy or server.active_connections >= server.max_connections:
                continue

            # Check if this server can accept more requests in this round
            if round_request.request_number < round_request.requests:
                round_request.request_number += 1
                return server_key
            
            # Server is healthy but hit request limit for current round - reset it
            round_request.request_number = 0
        
        raise RuntimeError("Load Balancer Failed. No healthy servers available with capacity.")

    def add_server(self, server_key: str, server: Server):
        """Add a server dynamically"""
        assert isinstance(server, Server), "failed to add server, input is not of type Server"
        
        # Add to main servers dict
        self.servers[server_key] = server
        
        # Rebuild the weighted structure (simplest approach)
        self.ordered_servers = self.__build_round_request_data()
        self.n = len(self.ordered_servers)



            
        
            