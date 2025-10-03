from abc import ABC, abstractmethod
from .models import Server
from typing import Dict
import aiohttp
import time

class LoadBalancerStrategy(ABC):
    def __init__(self, servers: Dict[str, Server], timeout: int, healthy: float):
        assert isinstance(servers, Dict), "failed to create load balancer, server must be a dict of type Servers"
        assert len(servers) > 0, "failed to create load balancer, must have at least 1 server for the load balancer"
        assert all(isinstance(server, Server) for key, server in servers.items()), "all items in servers must be Server objects"
        assert timeout > 0, "timeout must be greater than 0"
        assert 0 < healthy < 1, "healthy must be between 0 and 1 (exclusive)"
        self.servers = servers
        self.timeout = timeout
        self.healthy = healthy
        self.session = aiohttp.ClientSession()

    @abstractmethod
    def __next__(self):
        """Strategy-specific logic for selecting the next server"""
        pass

    def finally_after_request(self, server: Server, elapsed: float, response):
        pass

    # Hook methods (optional overrides)
    def before_request(self, server: Server, request):
        """Called before making the request. Override to customize."""
        pass

    def after_success(self, server: Server, response, elapsed: float):
        """Called after successful request. Override to customize."""
        pass

    def after_failure(self, server: Server, error: Exception, elapsed: float):
        """Called after failed request. Override to customize."""
        pass


    async def handle_request(self, request):
        """Template method - defines the request handling skeleton"""
        server_key = next(self)
        next_server = self.servers[server_key]
        assert next_server.active_connections < next_server.max_connections, "no servers available, all exceed max_connections allowed"
        
        start = time.time()
        end = start
        
        try:
            next_server.active_connections += 1
            
            # Hook: before request
            self.before_request(next_server, request)
            
            response = await self.session.request(
                method=request.method,
                url=f"{next_server.url}{request.path}",
                headers=request.headers,
                params=getattr(request, 'query_params', None),
                data=request.body,
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )
            
            end = time.time()
            next_server.metadata.sucess += 1
            
            # Hook: after success
            self.after_success(next_server, response, end - start)
            
            return response

        except Exception as e:
            end = time.time()
            next_server.metadata.failure += 1
            
            # Hook: after failure
            self.after_failure(next_server, e, end - start)
            
            raise

        finally:
            next_server.active_connections -= 1
            
            # Update metadata
            total = next_server.metadata.sucess + next_server.metadata.failure
            if total > 0:
                next_server.metadata.sucess_rate = round(
                    next_server.metadata.sucess / total, 5
                )
            
            next_server.metadata.total_requests += 1
            next_server.healthy = True if next_server.metadata.sucess_rate > self.healthy else False
            
            elapsed = end - start
            next_server.metadata.speeds.append(elapsed)
            next_server.metadata.total_time += elapsed
            next_server.metadata.latency = round(
                next_server.metadata.total_time / len(next_server.metadata.speeds), 5
            )

            if next_server.metadata.sucess > 0:
                self.finally_after_request(next_server, end - start)

    async def close(self):
        """Close the HTTP client session"""
        await self.session.close()

        
