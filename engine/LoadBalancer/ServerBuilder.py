from .models import Server, Metadata

class ServerBuilder:
    """Fluent API for building servers"""
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.weight = 1
        self.max_connections = 100
        self.healthy = True
    
    def with_weight(self, weight: int):
        self.weight = weight
        return self
    
    def with_max_connections(self, max_conn: int):
        self.max_connections = max_conn
        return self
    
    def build(self) -> Server:
        return Server(
            name=self.name,
            url=self.url,
            active_connections=0,
            healthy=self.healthy,
            weight=self.weight,
            max_connections=self.max_connections,
            metadata=Metadata(
                total_requests=0,
                speeds=[],
                total_time=0.0,
                latency=0.0,
                sucess=0,
                failure=0,
                sucess_rate=1.0
            )
        )

