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

@dataclass
class WeightedRoundRobinRequests:
    requests: int # how many requests to send in "round" of round robin
    request_number: int # current request number we are on in current "round"

