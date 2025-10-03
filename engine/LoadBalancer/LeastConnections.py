from .models import Server
from typing import Dict
import random
import aiohttp
import time

class LeastConnectionsLoadBalancing():
    def __init__(self, servers: Dict[str, Server], timeout: int):
        assert isinstance(servers, Dict), "failed to create load balancer, server must be a dict of type Servers"
        assert len(servers) > 0, "failed to create load balancer, must have at least 1 server for the load balancer"
        self.servers = servers
        self.session = aiohttp.ClientSession()

    def __next__(self):
        # returns the key of the server that is "next"
        # in other words, will return the server that the incoming next request is going to
        min_connection, res = float("inf"), ""
        for server_key, server in self.servers.items():
            if server.active_connections < min_connection:
                min_connection = server.active_connections
                res = server_key
        
        return res

    async def handle_request(self, request):
        server_key = next(self)
        next_server = self.servers[server_key]
        assert next_server.active_connections < next_server.max_connections, "no servers available, all exceed max_connections allowed"
        
        try:
            start = time.time()
            next_server.active_connections += 1
            response = await self.session.request(
                method=request.method,
                url=f"{next_server.url}{request.path}",
                headers=request.headers,
                params=request.query_params,
                data=request.body
            )
            
            end = time.time()
            next_server.metadata.sucess += 1
            return response

        except Exception as e:
            end = time.time()
            next_server.metadata.failure += 1
            raise

        finally:
            next_server.active_connections -= 1
            
            total = next_server.metadata.sucess + next_server.metadata.failure
            if total > 0:
                next_server.metadata.sucess_rate = round(
                    next_server.metadata.sucess / total, 5
                )
            
            next_server.metadata.total_requests += 1
            next_server.healthy = True if next_server.metadata.sucess_rate > 0.5 else False
            
            elapsed = end - start
            next_server.metadata.speeds.append(elapsed)
            next_server.metadata.total_time += elapsed
            next_server.metadata.latency = round(
                next_server.metadata.total_time / len(next_server.metadata.speeds), 5
            )

    async def close(self):
        await self.session.close()