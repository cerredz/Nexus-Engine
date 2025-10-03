from .models import Server
from typing import Dict
from .LeastConnections import LeastConnectionsLoadBalancing
from .RoundRobin import RoundRobin
from .WeightedRoundRobin import WeightedRoundRobin
from .LeastTime import LeastTime
import time
from .ServerBuilder import ServerBuilder

class LoadBalancer():

    STRATEGIES = ["least_connections", "round_robin", "weighted_round_robin", "least_time"]

    def __init__(self, strategy: str, timeout: int, servers: Dict[str, Server], healthy: float):
        assert isinstance(servers, dict), "servers must be a dict"
        assert len(servers) > 0, "must have at least 1 server"
        assert all(isinstance(server, Server) for server in servers.values()), "all items in servers must be Server objects"
        
        self.strategy = strategy
        self.timeout = timeout
        self.healthy = healthy
        self.servers = servers
        self.load_balancer = self.__init_load_balancer(self.strategy)
        self.start_time = time.time()  # Fixed: added ()
        self.requests = 0

    def __len__(self):
        return len(self.servers)

    def __repr__(self):
        server_names = ", ".join(server.name for server in self.servers.values())
        return f"LoadBalancer(strategy={self.strategy}, servers=[{server_names}], timeout={self.timeout}, healthy={self.healthy})"

    def __str__(self):
        return self.traffic_metrics() + "\n" + self.performance_metrics() + "\n" + self.health_metrics()

    def __init_load_balancer(self, strategy: str):
        match strategy:
            case "least_connections":
                return LeastConnectionsLoadBalancing(servers=self.servers, timeout=self.timeout, healthy=self.healthy)
            case "round_robin":
                return RoundRobin(servers=self.servers, timeout=self.timeout, healthy=self.healthy)
            case "weighted_round_robin":
                return WeightedRoundRobin(servers=self.servers, timeout=self.timeout, healthy=self.healthy)
            case "least_time":
                return LeastTime(servers=self.servers, timeout=self.timeout, healthy=self.healthy)
            case _:
                raise ValueError(f"Error initializing load balancer, strategy must be one of {LoadBalancer.STRATEGIES}, got '{strategy}'")


    async def handle_incoming_request(self, request):
        self.requests += 1
        return await self.load_balancer.handle_request(request)  # Fixed: added await

    def add_server(self, server_key: str, server: Server):
        """Add a server to the load balancer"""
        assert isinstance(server, Server), "failed to add server, input is not of type Server"
        assert isinstance(server_key, str), "server_key must be a string"
        
        self.servers[server_key] = server  # Fixed: dict assignment, not append
        # Note: Strategies don't support dynamic add_server yet - you'd need to rebuild

    def get_healthy_servers(self):
        return [server.name for server in self.load_balancer.servers.values() if server.healthy]

    def traffic_metrics(self):
        total_active_connections = sum(server.active_connections for server in self.load_balancer.servers.values())
        curr_time = time.time()  # Fixed: added ()
        elapsed_time = curr_time - self.start_time
        request_rate = self.requests / elapsed_time if elapsed_time > 0 else 0  # Fixed: float division
        return f"Request Rate: {request_rate:.2f} req/s\nTotal Active Connections: {total_active_connections}"

    def performance_metrics(self):
        res = ""
        for server in self.load_balancer.servers.values():
            total_requests = server.metadata.total_requests
            total_time = server.metadata.total_time
            latency = server.metadata.latency
            server_performance_str = (
                f"\nServer {server.name}\n"
                f"  Total Requests: {total_requests}\n"
                f"  Total Request Time: {total_time:.2f}s\n"
                f"  Average Response Time: {latency:.4f}s\n"
            )
            res += server_performance_str
        return res

    def health_metrics(self):
        res = ""
        healthy_server_list = [server for server in self.load_balancer.servers.values() if server.healthy]
        unhealthy_server_list = [server for server in self.load_balancer.servers.values() if not server.healthy]

        for server in healthy_server_list:  # Fixed: iterate over list, not integer
            server_str = f"\nServer {server.name} STATUS: HEALTHY | Failures: {server.metadata.failure}\n"
            res += server_str

        for server in unhealthy_server_list:  # Fixed: iterate over list, not integer
            server_str = f"\nServer {server.name} STATUS: UNHEALTHY | Failures: {server.metadata.failure}\n"
            res += server_str

        return res

    @classmethod
    def from_config(cls, config: dict):
        """Create load balancer from configuration dict"""
        servers = {}
        for server_config in config['servers']:
            server = ServerBuilder(
                server_config['name'],
                server_config['url']
            ).with_weight(server_config.get('weight', 1)).build()
            servers[server_config['name']] = server
        
        return cls(
            strategy=config['strategy'],
            timeout=config.get('timeout', 30),
            servers=servers,
            healthy=config.get('healthy_threshold', 0.5)
        )

