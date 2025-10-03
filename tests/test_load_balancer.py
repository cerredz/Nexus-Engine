import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from collections import deque
from engine.LoadBalancer.LoadBalancer import LoadBalancer
from engine.LoadBalancer.LeastConnections import LeastConnectionsLoadBalancing
from engine.LoadBalancer.RoundRobin import RoundRobin
from engine.LoadBalancer.WeightedRoundRobin import WeightedRoundRobin
from engine.LoadBalancer.LeastTime import LeastTime
from engine.LoadBalancer.ServerBuilder import ServerBuilder
from engine.LoadBalancer.models import Server, Metadata, WeightedRoundRobinRequests


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def basic_metadata():
    return Metadata(
        total_requests=0,
        speeds=[],
        total_time=0.0,
        latency=0.0,
        sucess=0,
        failure=0,
        sucess_rate=1.0
    )


@pytest.fixture
def basic_server(basic_metadata):
    return Server(
        name="server-1",
        url="http://localhost:5001",
        active_connections=0,
        healthy=True,
        weight=1,
        max_connections=100,
        metadata=basic_metadata
    )


@pytest.fixture
def server_dict():
    servers = {}
    for i in range(3):
        servers[f"server-{i}"] = Server(
            name=f"server-{i}",
            url=f"http://localhost:500{i}",
            active_connections=0,
            healthy=True,
            weight=1,
            max_connections=100,
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
    return servers


@pytest.fixture
def weighted_server_dict():
    servers = {}
    weights = [5, 3, 2]
    for i, weight in enumerate(weights):
        servers[f"server-{i}"] = Server(
            name=f"server-{i}",
            url=f"http://localhost:500{i}",
            active_connections=0,
            healthy=True,
            weight=weight,
            max_connections=100,
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
    return servers


@pytest.fixture
def mock_request():
    request = Mock()
    request.method = "GET"
    request.path = "/test"
    request.headers = {}
    request.query_params = {}
    request.body = None
    return request


@pytest.fixture
def mock_response():
    response = Mock()
    response.status = 200
    response.headers = {}
    return response


# ============================================================================
# SERVER BUILDER TESTS
# ============================================================================

def test_server_builder_basic():
    server = ServerBuilder("test", "http://localhost:5000").build()
    assert server.name == "test"
    assert server.url == "http://localhost:5000"
    assert server.weight == 1
    assert server.max_connections == 100
    assert server.healthy == True
    assert server.active_connections == 0


def test_server_builder_with_weight():
    server = ServerBuilder("test", "http://localhost:5000").with_weight(5).build()
    assert server.weight == 5


def test_server_builder_with_max_connections():
    server = ServerBuilder("test", "http://localhost:5000").with_max_connections(200).build()
    assert server.max_connections == 200


def test_server_builder_chaining():
    server = (ServerBuilder("test", "http://localhost:5000")
              .with_weight(3)
              .with_max_connections(50)
              .build())
    assert server.weight == 3
    assert server.max_connections == 50


def test_server_builder_metadata_initialization():
    server = ServerBuilder("test", "http://localhost:5000").build()
    assert server.metadata.total_requests == 0
    assert server.metadata.sucess == 0
    assert server.metadata.failure == 0
    assert server.metadata.sucess_rate == 1.0
    assert server.metadata.speeds == []


def test_server_builder_with_empty_name():
    server = ServerBuilder("", "http://localhost:5000").build()
    assert server.name == ""


def test_server_builder_with_empty_url():
    server = ServerBuilder("test", "").build()
    assert server.url == ""


def test_server_builder_with_negative_weight():
    server = ServerBuilder("test", "http://localhost:5000").with_weight(-5).build()
    assert server.weight == -5


def test_server_builder_with_zero_weight():
    server = ServerBuilder("test", "http://localhost:5000").with_weight(0).build()
    assert server.weight == 0


def test_server_builder_with_zero_max_connections():
    server = ServerBuilder("test", "http://localhost:5000").with_max_connections(0).build()
    assert server.max_connections == 0


def test_server_builder_with_large_weight():
    server = ServerBuilder("test", "http://localhost:5000").with_weight(1000000).build()
    assert server.weight == 1000000


# ============================================================================
# LOAD BALANCER INITIALIZATION TESTS
# ============================================================================

def test_load_balancer_init_least_connections(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    assert lb.strategy == "least_connections"
    assert lb.timeout == 30
    assert lb.healthy == 0.5
    assert len(lb.servers) == 3


def test_load_balancer_init_round_robin(server_dict):
    lb = LoadBalancer("round_robin", 30, server_dict, 0.5)
    assert lb.strategy == "round_robin"
    assert isinstance(lb.load_balancer, RoundRobin)


def test_load_balancer_init_weighted_round_robin(weighted_server_dict):
    lb = LoadBalancer("weighted_round_robin", 30, weighted_server_dict, 0.5)
    assert lb.strategy == "weighted_round_robin"
    assert isinstance(lb.load_balancer, WeightedRoundRobin)


def test_load_balancer_init_least_time(server_dict):
    lb = LoadBalancer("least_time", 30, server_dict, 0.5)
    assert lb.strategy == "least_time"
    assert isinstance(lb.load_balancer, LeastTime)


def test_load_balancer_init_invalid_strategy(server_dict):
    with pytest.raises(ValueError, match="Error initializing load balancer"):
        LoadBalancer("invalid_strategy", 30, server_dict, 0.5)


def test_load_balancer_init_empty_servers():
    with pytest.raises(AssertionError, match="must have at least 1 server"):
        LoadBalancer("least_connections", 30, {}, 0.5)


def test_load_balancer_init_servers_not_dict(basic_server):
    with pytest.raises(AssertionError, match="servers must be a dict"):
        LoadBalancer("least_connections", 30, [basic_server], 0.5)


def test_load_balancer_init_servers_wrong_type():
    with pytest.raises(AssertionError, match="all items in servers must be Server objects"):
        LoadBalancer("least_connections", 30, {"s1": "not_a_server"}, 0.5)


def test_load_balancer_init_with_none_servers():
    with pytest.raises((AssertionError, TypeError)):
        LoadBalancer("least_connections", 30, None, 0.5)


def test_load_balancer_init_with_single_server(basic_server):
    servers = {"server-1": basic_server}
    lb = LoadBalancer("least_connections", 30, servers, 0.5)
    assert len(lb.servers) == 1


def test_load_balancer_init_with_many_servers():
    servers = {}
    for i in range(100):
        servers[f"server-{i}"] = ServerBuilder(f"server-{i}", f"http://host{i}:5000").build()
    lb = LoadBalancer("least_connections", 30, servers, 0.5)
    assert len(lb.servers) == 100


def test_load_balancer_init_sets_start_time(server_dict):
    before = time.time()
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    after = time.time()
    assert before <= lb.start_time <= after


def test_load_balancer_init_requests_counter_zero(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    assert lb.requests == 0


# ============================================================================
# LOAD BALANCER FROM_CONFIG TESTS
# ============================================================================

def test_load_balancer_from_config_basic():
    config = {
        'strategy': 'least_connections',
        'timeout': 30,
        'healthy_threshold': 0.7,
        'servers': [
            {'name': 'api-1', 'url': 'http://localhost:5001', 'weight': 2},
            {'name': 'api-2', 'url': 'http://localhost:5002', 'weight': 1},
        ]
    }
    lb = LoadBalancer.from_config(config)
    assert lb.strategy == 'least_connections'
    assert lb.timeout == 30
    assert lb.healthy == 0.7
    assert len(lb.servers) == 2


def test_load_balancer_from_config_default_values():
    config = {
        'strategy': 'round_robin',
        'servers': [
            {'name': 'api-1', 'url': 'http://localhost:5001'},
        ]
    }
    lb = LoadBalancer.from_config(config)
    assert lb.timeout == 30
    assert lb.healthy == 0.5
    assert lb.servers['api-1'].weight == 1


def test_load_balancer_from_config_missing_strategy():
    config = {
        'servers': [
            {'name': 'api-1', 'url': 'http://localhost:5001'},
        ]
    }
    with pytest.raises(KeyError):
        LoadBalancer.from_config(config)


def test_load_balancer_from_config_missing_servers():
    config = {
        'strategy': 'least_connections',
    }
    with pytest.raises(KeyError):
        LoadBalancer.from_config(config)


def test_load_balancer_from_config_empty_servers_list():
    config = {
        'strategy': 'least_connections',
        'servers': []
    }
    with pytest.raises(AssertionError):
        LoadBalancer.from_config(config)


def test_load_balancer_from_config_invalid_server_format():
    config = {
        'strategy': 'least_connections',
        'servers': [
            {'name': 'api-1'},  # Missing url
        ]
    }
    with pytest.raises(KeyError):
        LoadBalancer.from_config(config)


# ============================================================================
# LOAD BALANCER MAGIC METHODS TESTS
# ============================================================================

def test_load_balancer_len(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    assert len(lb) == 3


def test_load_balancer_repr(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    repr_str = repr(lb)
    assert "LoadBalancer" in repr_str
    assert "least_connections" in repr_str
    assert "timeout=30" in repr_str
    assert "healthy=0.5" in repr_str


def test_load_balancer_str(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    str_output = str(lb)
    assert isinstance(str_output, str)
    assert "Request Rate" in str_output


# ============================================================================
# LOAD BALANCER ADD_SERVER TESTS
# ============================================================================

def test_load_balancer_add_server(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    new_server = ServerBuilder("server-new", "http://localhost:6000").build()
    lb.add_server("server-new", new_server)
    assert "server-new" in lb.servers
    assert lb.servers["server-new"].url == "http://localhost:6000"


def test_load_balancer_add_server_not_server_type(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    with pytest.raises(AssertionError, match="failed to add server"):
        lb.add_server("bad", "not_a_server")


def test_load_balancer_add_server_key_not_string(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    new_server = ServerBuilder("server-new", "http://localhost:6000").build()
    with pytest.raises(AssertionError, match="server_key must be a string"):
        lb.add_server(123, new_server)


def test_load_balancer_add_server_duplicate_key(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    new_server = ServerBuilder("server-0", "http://localhost:6000").build()
    lb.add_server("server-0", new_server)
    assert lb.servers["server-0"].url == "http://localhost:6000"


def test_load_balancer_add_server_none(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    with pytest.raises(AssertionError):
        lb.add_server("none", None)


# ============================================================================
# LOAD BALANCER METRICS TESTS
# ============================================================================

def test_load_balancer_get_healthy_servers(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    healthy = lb.get_healthy_servers()
    assert len(healthy) == 3
    assert all(isinstance(name, str) for name in healthy)


def test_load_balancer_get_healthy_servers_with_unhealthy(server_dict):
    server_dict["server-1"].healthy = False
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    healthy = lb.get_healthy_servers()
    assert len(healthy) == 2
    assert "server-1" not in healthy


def test_load_balancer_get_healthy_servers_all_unhealthy(server_dict):
    for server in server_dict.values():
        server.healthy = False
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    healthy = lb.get_healthy_servers()
    assert len(healthy) == 0


def test_load_balancer_traffic_metrics(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    metrics = lb.traffic_metrics()
    assert "Request Rate" in metrics
    assert "Total Active Connections" in metrics


def test_load_balancer_traffic_metrics_with_active_connections(server_dict):
    server_dict["server-0"].active_connections = 5
    server_dict["server-1"].active_connections = 3
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    metrics = lb.traffic_metrics()
    assert "8" in metrics


def test_load_balancer_performance_metrics(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    metrics = lb.performance_metrics()
    assert "server-0" in metrics
    assert "Total Requests" in metrics
    assert "Average Response Time" in metrics


def test_load_balancer_performance_metrics_with_data(server_dict):
    server_dict["server-0"].metadata.total_requests = 100
    server_dict["server-0"].metadata.total_time = 50.0
    server_dict["server-0"].metadata.latency = 0.5
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    metrics = lb.performance_metrics()
    assert "100" in metrics
    assert "50.00" in metrics


def test_load_balancer_health_metrics(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    metrics = lb.health_metrics()
    assert "HEALTHY" in metrics


def test_load_balancer_health_metrics_with_unhealthy(server_dict):
    server_dict["server-1"].healthy = False
    server_dict["server-1"].metadata.failure = 10
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    metrics = lb.health_metrics()
    assert "UNHEALTHY" in metrics
    assert "10" in metrics


def test_load_balancer_health_metrics_all_healthy(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    metrics = lb.health_metrics()
    assert metrics.count("HEALTHY") >= 3


def test_load_balancer_health_metrics_all_unhealthy(server_dict):
    for server in server_dict.values():
        server.healthy = False
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    metrics = lb.health_metrics()
    assert metrics.count("UNHEALTHY") >= 3


# ============================================================================
# LEAST CONNECTIONS STRATEGY TESTS
# ============================================================================

def test_least_connections_init(server_dict):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    assert lc.servers == server_dict
    assert lc.timeout == 30
    assert lc.healthy == 0.5


def test_least_connections_next_selects_min_connections(server_dict):
    server_dict["server-0"].active_connections = 5
    server_dict["server-1"].active_connections = 2
    server_dict["server-2"].active_connections = 8
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    selected = next(lc)
    assert selected == "server-1"


def test_least_connections_next_all_equal_connections(server_dict):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    selected = next(lc)
    assert selected in server_dict.keys()


def test_least_connections_next_single_server(basic_server):
    servers = {"server-1": basic_server}
    lc = LeastConnectionsLoadBalancing(servers, 30, 0.5)
    selected = next(lc)
    assert selected == "server-1"


def test_least_connections_next_all_max_connections(server_dict):
    for server in server_dict.values():
        server.active_connections = 100
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    selected = next(lc)
    assert selected in server_dict.keys()


def test_least_connections_next_consistent_selection_until_change(server_dict):
    server_dict["server-1"].active_connections = 1
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    selections = [next(lc) for _ in range(5)]
    assert all(s == "server-1" for s in selections)


def test_least_connections_next_switches_when_connections_change(server_dict):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    server_dict["server-0"].active_connections = 10
    selected1 = next(lc)
    server_dict[selected1].active_connections = 20
    server_dict["server-2"].active_connections = 0
    selected2 = next(lc)
    assert selected2 == "server-2"


@pytest.mark.asyncio
async def test_least_connections_handle_request_success(server_dict, mock_request, mock_response):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    
    with patch.object(lc.session, 'request', new_callable=AsyncMock) as mock_req:
        mock_req.return_value = mock_response
        
        result = await lc.handle_request(mock_request)
        
        assert result == mock_response
        mock_req.assert_called_once()


@pytest.mark.asyncio
async def test_least_connections_handle_request_increments_connections(server_dict, mock_request, mock_response):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    server_key = next(lc)
    initial_connections = server_dict[server_key].active_connections
    
    with patch.object(lc.session, 'request', new_callable=AsyncMock) as mock_req:
        async def delayed_response(*args, **kwargs):
            assert server_dict[server_key].active_connections == initial_connections + 1
            return mock_response
        
        mock_req.side_effect = delayed_response
        await lc.handle_request(mock_request)


@pytest.mark.asyncio
async def test_least_connections_handle_request_decrements_connections_on_success(server_dict, mock_request, mock_response):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    server_key = next(lc)
    initial_connections = server_dict[server_key].active_connections
    
    with patch.object(lc.session, 'request', new_callable=AsyncMock) as mock_req:
        mock_req.return_value = mock_response
        await lc.handle_request(mock_request)
        
        assert server_dict[server_key].active_connections == initial_connections


@pytest.mark.asyncio
async def test_least_connections_handle_request_decrements_connections_on_error(server_dict, mock_request):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    server_key = next(lc)
    initial_connections = server_dict[server_key].active_connections
    
    with patch.object(lc.session, 'request', new_callable=AsyncMock) as mock_req:
        mock_req.side_effect = Exception("Network error")
        
        with pytest.raises(Exception):
            await lc.handle_request(mock_request)
        
        assert server_dict[server_key].active_connections == initial_connections


@pytest.mark.asyncio
async def test_least_connections_handle_request_updates_success_metadata(server_dict, mock_request, mock_response):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    server_key = next(lc)
    
    with patch.object(lc.session, 'request', new_callable=AsyncMock) as mock_req:
        mock_req.return_value = mock_response
        await lc.handle_request(mock_request)
        
        assert server_dict[server_key].metadata.sucess == 1
        assert server_dict[server_key].metadata.total_requests == 1


@pytest.mark.asyncio
async def test_least_connections_handle_request_updates_failure_metadata(server_dict, mock_request):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    server_key = next(lc)
    
    with patch.object(lc.session, 'request', new_callable=AsyncMock) as mock_req:
        mock_req.side_effect = Exception("Error")
        
        with pytest.raises(Exception):
            await lc.handle_request(mock_request)
        
        assert server_dict[server_key].metadata.failure == 1
        assert server_dict[server_key].metadata.total_requests == 1


@pytest.mark.asyncio
async def test_least_connections_handle_request_calculates_latency(server_dict, mock_request, mock_response):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    server_key = next(lc)
    
    with patch.object(lc.session, 'request', new_callable=AsyncMock) as mock_req:
        async def delayed_response(*args, **kwargs):
            await asyncio.sleep(0.1)
            return mock_response
        
        mock_req.side_effect = delayed_response
        await lc.handle_request(mock_request)
        
        assert server_dict[server_key].metadata.latency > 0
        assert server_dict[server_key].metadata.total_time > 0
        assert len(server_dict[server_key].metadata.speeds) == 1


@pytest.mark.asyncio
async def test_least_connections_handle_request_updates_health_based_on_success_rate(server_dict, mock_request, mock_response):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    
    with patch.object(lc.session, 'request', new_callable=AsyncMock) as mock_req:
        mock_req.return_value = mock_response
        await lc.handle_request(mock_request)
        
        server_key = list(server_dict.keys())[0]
        assert server_dict[server_key].healthy == True


@pytest.mark.asyncio
async def test_least_connections_handle_request_marks_unhealthy_on_failures(server_dict, mock_request):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    server_key = next(lc)
    
    with patch.object(lc.session, 'request', new_callable=AsyncMock) as mock_req:
        mock_req.side_effect = Exception("Error")
        
        for _ in range(3):
            try:
                await lc.handle_request(mock_request)
            except:
                pass
        
        assert server_dict[server_key].healthy == False


@pytest.mark.asyncio
async def test_least_connections_handle_request_at_max_connections(server_dict, mock_request):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    server_key = next(lc)
    server_dict[server_key].active_connections = 100
    
    with pytest.raises(AssertionError, match="no servers available"):
        await lc.handle_request(mock_request)


# ============================================================================
# ROUND ROBIN STRATEGY TESTS
# ============================================================================

def test_round_robin_init(server_dict):
    rr = RoundRobin(server_dict, 30, 0.5)
    assert rr.servers == server_dict
    assert len(rr.ordered_servers) == 3
    assert rr.n == 3


def test_round_robin_init_creates_ordered_servers(server_dict):
    rr = RoundRobin(server_dict, 30, 0.5)
    assert all(key in server_dict for key in rr.ordered_servers)


def test_round_robin_next_rotates_through_servers(server_dict):
    rr = RoundRobin(server_dict, 30, 0.5)
    selections = [next(rr) for _ in range(10)]
    assert len(set(selections)) >= 2


def test_round_robin_next_single_server(basic_server):
    servers = {"server-1": basic_server}
    rr = RoundRobin(servers, 30, 0.5)
    selections = [next(rr) for _ in range(5)]
    assert all(s == "server-1" for s in selections)


def test_round_robin_next_skips_unhealthy_servers(server_dict):
    server_dict["server-1"].healthy = False
    rr = RoundRobin(server_dict, 30, 0.5)
    selections = [next(rr) for _ in range(10)]
    assert "server-1" not in selections


def test_round_robin_next_skips_at_capacity_servers(server_dict):
    server_dict["server-1"].active_connections = 100
    rr = RoundRobin(server_dict, 30, 0.5)
    selections = [next(rr) for _ in range(10)]
    assert "server-1" not in selections


def test_round_robin_next_raises_when_no_healthy_servers(server_dict):
    for server in server_dict.values():
        server.healthy = False
    rr = RoundRobin(server_dict, 30, 0.5)
    with pytest.raises(RuntimeError, match="No healthy servers available"):
        next(rr)


def test_round_robin_next_raises_when_all_at_capacity(server_dict):
    for server in server_dict.values():
        server.active_connections = 100
    rr = RoundRobin(server_dict, 30, 0.5)
    with pytest.raises(RuntimeError, match="No healthy servers available"):
        next(rr)


def test_round_robin_next_cycles_correctly(server_dict):
    rr = RoundRobin(server_dict, 30, 0.5)
    first_round = [next(rr) for _ in range(3)]
    second_round = [next(rr) for _ in range(3)]
    assert len(set(first_round)) == 3
    assert len(set(second_round)) == 3


def test_round_robin_add_server(server_dict):
    rr = RoundRobin(server_dict, 30, 0.5)
    new_server = ServerBuilder("server-new", "http://localhost:6000").build()
    rr.add_server(new_server)
    assert rr.n == 4


def test_round_robin_add_server_invalid_type(server_dict):
    rr = RoundRobin(server_dict, 30, 0.5)
    with pytest.raises(AssertionError):
        rr.add_server("not_a_server")


# ============================================================================
# WEIGHTED ROUND ROBIN STRATEGY TESTS
# ============================================================================

def test_weighted_round_robin_init(weighted_server_dict):
    wrr = WeightedRoundRobin(weighted_server_dict, 30, 0.5)
    assert wrr.servers == weighted_server_dict
    assert len(wrr.ordered_servers) == 3


def test_weighted_round_robin_builds_request_data(weighted_server_dict):
    wrr = WeightedRoundRobin(weighted_server_dict, 30, 0.5)
    requests_per_round = [req.requests for _, req in wrr.ordered_servers]
    assert 2 in requests_per_round or 5 in requests_per_round


def test_weighted_round_robin_next_respects_weights(weighted_server_dict):
    wrr = WeightedRoundRobin(weighted_server_dict, 30, 0.5)
    selections = [next(wrr) for _ in range(20)]
    
    counts = {}
    for selection in selections:
        counts[selection] = counts.get(selection, 0) + 1
    
    assert counts["server-0"] > counts["server-2"]


def test_weighted_round_robin_next_single_server(basic_server):
    servers = {"server-1": basic_server}
    wrr = WeightedRoundRobin(servers, 30, 0.5)
    selected = next(wrr)
    assert selected == "server-1"


def test_weighted_round_robin_next_skips_unhealthy(weighted_server_dict):
    weighted_server_dict["server-1"].healthy = False
    wrr = WeightedRoundRobin(weighted_server_dict, 30, 0.5)
    selections = [next(wrr) for _ in range(10)]
    assert "server-1" not in selections


def test_weighted_round_robin_next_skips_at_capacity(weighted_server_dict):
    weighted_server_dict["server-1"].active_connections = 100
    wrr = WeightedRoundRobin(weighted_server_dict, 30, 0.5)
    selections = [next(wrr) for _ in range(10)]
    assert "server-1" not in selections


def test_weighted_round_robin_next_raises_when_no_healthy_servers(weighted_server_dict):
    for server in weighted_server_dict.values():
        server.healthy = False
    wrr = WeightedRoundRobin(weighted_server_dict, 30, 0.5)
    with pytest.raises(RuntimeError, match="No healthy servers available"):
        next(wrr)


def test_weighted_round_robin_resets_request_counter(weighted_server_dict):
    wrr = WeightedRoundRobin(weighted_server_dict, 30, 0.5)
    for _ in range(30):
        next(wrr)
    
    request_numbers = [req.request_number for _, req in wrr.ordered_servers]
    assert any(rn == 0 for rn in request_numbers)


def test_weighted_round_robin_add_server(weighted_server_dict):
    wrr = WeightedRoundRobin(weighted_server_dict, 30, 0.5)
    new_server = ServerBuilder("server-new", "http://localhost:6000").with_weight(4).build()
    wrr.add_server("server-new", new_server)
    assert "server-new" in wrr.servers
    assert wrr.n == 4


def test_weighted_round_robin_add_server_rebuilds_structure(weighted_server_dict):
    wrr = WeightedRoundRobin(weighted_server_dict, 30, 0.5)
    initial_n = wrr.n
    new_server = ServerBuilder("server-new", "http://localhost:6000").with_weight(4).build()
    wrr.add_server("server-new", new_server)
    assert wrr.n == initial_n + 1
    assert len(wrr.ordered_servers) == initial_n + 1


def test_weighted_round_robin_with_equal_weights(server_dict):
    wrr = WeightedRoundRobin(server_dict, 30, 0.5)
    selections = [next(wrr) for _ in range(12)]
    counts = {}
    for s in selections:
        counts[s] = counts.get(s, 0) + 1
    
    values = list(counts.values())
    assert max(values) - min(values) <= 2


def test_weighted_round_robin_with_zero_weight():
    servers = {
        "server-0": ServerBuilder("server-0", "http://localhost:5000").with_weight(0).build(),
        "server-1": ServerBuilder("server-1", "http://localhost:5001").with_weight(1).build(),
    }
    with pytest.raises((ValueError, ZeroDivisionError)):
        wrr = WeightedRoundRobin(servers, 30, 0.5)
        next(wrr)


# ============================================================================
# LEAST TIME STRATEGY TESTS
# ============================================================================

def test_least_time_init(server_dict):
    lt = LeastTime(server_dict, 30, 0.5)
    assert lt.servers == server_dict
    assert lt.min_response_time == float("inf")
    assert lt.min_response_time_key is None


def test_least_time_next_initial_selection_random(server_dict):
    lt = LeastTime(server_dict, 30, 0.5)
    selected = next(lt)
    assert selected in server_dict.keys()


def test_least_time_next_prefers_healthy_servers(server_dict):
    server_dict["server-2"].healthy = False
    lt = LeastTime(server_dict, 30, 0.5)
    selections = [next(lt) for _ in range(10)]
    assert "server-2" not in selections or lt.min_response_time_key is None


def test_least_time_next_falls_back_when_no_healthy(server_dict):
    for server in server_dict.values():
        server.healthy = False
    lt = LeastTime(server_dict, 30, 0.5)
    selected = next(lt)
    assert selected in server_dict.keys()


def test_least_time_next_returns_fastest_after_tracking(server_dict):
    lt = LeastTime(server_dict, 30, 0.5)
    
    lt.min_response_time = 0.05
    lt.min_response_time_key = "server-1"
    
    selected = next(lt)
    assert selected == "server-1"


def test_least_time_finally_after_request_updates_min_time(server_dict):
    lt = LeastTime(server_dict, 30, 0.5)
    server = server_dict["server-0"]
    server.metadata.latency = 0.03
    server.healthy = True
    
    lt.finally_after_request(server, 0.03)
    
    assert lt.min_response_time == 0.03
    assert lt.min_response_time_key == "server-0"


def test_least_time_finally_after_request_doesnt_update_for_unhealthy(server_dict):
    lt = LeastTime(server_dict, 30, 0.5)
    lt.min_response_time = 0.1
    lt.min_response_time_key = "server-0"
    
    server = server_dict["server-1"]
    server.metadata.latency = 0.05
    server.healthy = False
    
    lt.finally_after_request(server, 0.05)
    
    assert lt.min_response_time == 0.1
    assert lt.min_response_time_key == "server-0"


def test_least_time_finally_after_request_doesnt_update_for_slower(server_dict):
    lt = LeastTime(server_dict, 30, 0.5)
    lt.min_response_time = 0.03
    lt.min_response_time_key = "server-0"
    
    server = server_dict["server-1"]
    server.metadata.latency = 0.08
    server.healthy = True
    
    lt.finally_after_request(server, 0.08)
    
    assert lt.min_response_time == 0.03
    assert lt.min_response_time_key == "server-0"


def test_least_time_tracks_fastest_over_time(server_dict):
    lt = LeastTime(server_dict, 30, 0.5)
    
    for i, (key, server) in enumerate(server_dict.items()):
        server.metadata.latency = 0.1 - (i * 0.01)
        server.healthy = True
        lt.finally_after_request(server, server.metadata.latency)
    
    assert lt.min_response_time_key == "server-2"


def test_least_time_single_server(basic_server):
    servers = {"server-1": basic_server}
    lt = LeastTime(servers, 30, 0.5)
    selected = next(lt)
    assert selected == "server-1"


# ============================================================================
# CONCURRENT REQUEST TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_least_connections_concurrent_requests(server_dict, mock_request, mock_response):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    
    with patch.object(lc.session, 'request', new_callable=AsyncMock) as mock_req:
        async def delayed_response(*args, **kwargs):
            await asyncio.sleep(0.01)
            return mock_response
        
        mock_req.side_effect = delayed_response
        
        tasks = [lc.handle_request(mock_request) for _ in range(10)]
        results = await asyncio.gather(*tasks)
        
        assert len(results) == 10
        assert all(r == mock_response for r in results)


@pytest.mark.asyncio
async def test_round_robin_concurrent_requests(server_dict, mock_request, mock_response):
    rr = RoundRobin(server_dict, 30, 0.5)
    
    with patch.object(rr.session, 'request', new_callable=AsyncMock) as mock_req:
        async def delayed_response(*args, **kwargs):
            await asyncio.sleep(0.01)
            return mock_response
        
        mock_req.side_effect = delayed_response
        
        tasks = [rr.handle_request(mock_request) for _ in range(10)]
        results = await asyncio.gather(*tasks)
        
        assert len(results) == 10


@pytest.mark.asyncio
async def test_load_balancer_concurrent_handle_incoming_requests(server_dict, mock_request, mock_response):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    
    with patch.object(lb.load_balancer.session, 'request', new_callable=AsyncMock) as mock_req:
        async def delayed_response(*args, **kwargs):
            await asyncio.sleep(0.01)
            return mock_response
        
        mock_req.side_effect = delayed_response
        
        tasks = [lb.handle_incoming_request(mock_request) for _ in range(20)]
        results = await asyncio.gather(*tasks)
        
        assert len(results) == 20
        assert lb.requests == 20


# ============================================================================
# STRESS TESTS
# ============================================================================

def test_load_balancer_with_many_servers():
    servers = {}
    for i in range(1000):
        servers[f"server-{i}"] = ServerBuilder(f"server-{i}", f"http://host{i}:5000").build()
    
    lb = LoadBalancer("least_connections", 30, servers, 0.5)
    assert len(lb.servers) == 1000


def test_round_robin_with_many_servers():
    servers = {}
    for i in range(1000):
        servers[f"server-{i}"] = ServerBuilder(f"server-{i}", f"http://host{i}:5000").build()
    
    rr = RoundRobin(servers, 30, 0.5)
    selections = [next(rr) for _ in range(100)]
    assert len(selections) == 100


def test_weighted_round_robin_with_large_weights():
    servers = {}
    for i in range(10):
        servers[f"server-{i}"] = ServerBuilder(f"server-{i}", f"http://host{i}:5000").with_weight(1000000).build()
    
    wrr = WeightedRoundRobin(servers, 30, 0.5)
    selected = next(wrr)
    assert selected in servers.keys()


@pytest.mark.asyncio
async def test_load_balancer_many_sequential_requests(server_dict, mock_request, mock_response):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    
    with patch.object(lb.load_balancer.session, 'request', new_callable=AsyncMock) as mock_req:
        mock_req.return_value = mock_response
        
        for _ in range(100):
            await lb.handle_incoming_request(mock_request)
        
        assert lb.requests == 100


# ============================================================================
# EDGE CASE TESTS
# ============================================================================

def test_server_with_special_characters_in_name():
    server = ServerBuilder("server@#$%^&*()", "http://localhost:5000").build()
    assert server.name == "server@#$%^&*()"


def test_server_with_very_long_name():
    long_name = "s" * 10000
    server = ServerBuilder(long_name, "http://localhost:5000").build()
    assert server.name == long_name


def test_server_with_very_long_url():
    long_url = "http://localhost:5000/" + "a" * 10000
    server = ServerBuilder("test", long_url).build()
    assert server.url == long_url


def test_load_balancer_with_unicode_server_names():
    servers = {}
    servers["服务器-1"] = ServerBuilder("服务器-1", "http://localhost:5000").build()
    servers["सर्वर-2"] = ServerBuilder("सर्वर-2", "http://localhost:5001").build()
    
    lb = LoadBalancer("least_connections", 30, servers, 0.5)
    assert len(lb.servers) == 2


def test_load_balancer_from_config_with_unicode():
    config = {
        'strategy': 'least_connections',
        'servers': [
            {'name': '服务器-1', 'url': 'http://localhost:5001'},
        ]
    }
    lb = LoadBalancer.from_config(config)
    assert '服务器-1' in lb.servers


def test_load_balancer_metrics_with_zero_elapsed_time(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    lb.start_time = time.time()
    metrics = lb.traffic_metrics()
    assert "0.00" in metrics or "Request Rate" in metrics


def test_metadata_success_rate_with_zero_requests():
    metadata = Metadata(
        total_requests=0,
        speeds=[],
        total_time=0.0,
        latency=0.0,
        sucess=0,
        failure=0,
        sucess_rate=1.0
    )
    assert metadata.sucess_rate == 1.0


def test_round_robin_with_fluctuating_health(server_dict):
    rr = RoundRobin(server_dict, 30, 0.5)
    
    selections = []
    for i in range(20):
        if i % 5 == 0:
            server_dict["server-1"].healthy = not server_dict["server-1"].healthy
        selections.append(next(rr))
    
    assert len(selections) == 20


@pytest.mark.asyncio
async def test_handle_request_with_timeout_error(server_dict, mock_request):
    lc = LeastConnectionsLoadBalancing(server_dict, 1, 0.5)
    
    with patch.object(lc.session, 'request', new_callable=AsyncMock) as mock_req:
        mock_req.side_effect = asyncio.TimeoutError("Request timeout")
        
        with pytest.raises(asyncio.TimeoutError):
            await lc.handle_request(mock_request)


@pytest.mark.asyncio  
async def test_handle_request_with_connection_error(server_dict, mock_request):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    
    with patch.object(lc.session, 'request', new_callable=AsyncMock) as mock_req:
        mock_req.side_effect = ConnectionError("Connection refused")
        
        with pytest.raises(ConnectionError):
            await lc.handle_request(mock_request)


# ============================================================================
# STATE AND IDEMPOTENCY TESTS
# ============================================================================

def test_least_connections_selection_is_consistent_with_same_state(server_dict):
    lc = LeastConnectionsLoadBalancing(server_dict, 30, 0.5)
    server_dict["server-1"].active_connections = 5
    
    selection1 = next(lc)
    selection2 = next(lc)
    
    assert selection1 == selection2


def test_metrics_collector_increments_properly(server_dict):
    lb = LoadBalancer("least_connections", 30, server_dict, 0.5)
    initial_requests = lb.requests
    
    lb.requests += 1
    lb.requests += 1
    lb.requests += 1
    
    assert lb.requests == initial_requests + 3


def test_server_metadata_accumulates_over_requests(basic_server):
    server = basic_server
    
    for i in range(10):
        server.metadata.sucess += 1
        server.metadata.total_requests += 1
        server.metadata.total_time += 0.1
        server.metadata.speeds.append(0.1)
    
    assert server.metadata.sucess == 10
    assert server.metadata.total_requests == 10
    assert len(server.metadata.speeds) == 10
    assert server.metadata.total_time == pytest.approx(1.0)


def test_weighted_round_robin_request_counter_state(weighted_server_dict):
    wrr = WeightedRoundRobin(weighted_server_dict, 30, 0.5)
    
    for _ in range(5):
        next(wrr)
    
    server_key, round_request = wrr.ordered_servers[wrr.current_idx]
    assert round_request.request_number >= 0


# ============================================================================
# INVALID TYPE TESTS
# ============================================================================

def test_load_balancer_init_with_string_timeout(server_dict):
    with pytest.raises((TypeError, AssertionError)):
        LoadBalancer("least_connections", "30", server_dict, 0.5)


def test_load_balancer_init_with_negative_timeout(server_dict):
    with pytest.raises(AssertionError):
        LoadBalancer("least_connections", -10, server_dict, 0.5)


def test_load_balancer_init_with_invalid_healthy_threshold(server_dict):
    with pytest.raises(AssertionError):
        LoadBalancer("least_connections", 30, server_dict, 1.5)


def test_load_balancer_init_with_zero_healthy_threshold(server_dict):
    with pytest.raises(AssertionError):
        LoadBalancer("least_connections", 30, server_dict, 0.0)


def test_load_balancer_init_with_negative_healthy_threshold(server_dict):
    with pytest.raises(AssertionError):
        LoadBalancer("least_connections", 30, server_dict, -0.5)


def test_server_builder_with_none_name():
    with pytest.raises(TypeError):
        ServerBuilder(None, "http://localhost:5000").build()


def test_server_builder_with_none_url():
    with pytest.raises(TypeError):
        ServerBuilder("test", None).build()


def test_weighted_round_robin_requests_init_with_negative():
    wrr_req = WeightedRoundRobinRequests(requests=-5, request_number=0)
    assert wrr_req.requests == -5

