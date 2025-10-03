from dataclasses import dataclass
from typing import List, Deque

@dataclass 
class Metadata:
    total_requests: int
    speeds: List[float]
    total_time: float
    latency: float
    sucess: int
    failure: int
    sucess_rate: float

@dataclass
class Server():
    name: str
    url: str
    active_connections: int
    healthy: bool
    weight: int
    max_connections: int
    metadata: Metadata


