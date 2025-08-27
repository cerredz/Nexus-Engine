import sys
sys.path.append("engine")
from ConsistentHashing.ConsistentHashing import ConsistentHashing
import random

class TestableConsistentHashing(ConsistentHashing):
    """Subclass creating a deterministic ring without calling buggy parent __init__."""
    def __init__(self, servers=2, virtual_nodes=2, db=False):
        # Do not call super().__init__
        if servers != 2 or virtual_nodes != 2:
            # keep simple deterministic fixture for tests below
            pass
        self.servers = servers
        self.nodes = virtual_nodes
        self.ring_nodes = [100, 200, 300, 400]
        self.db = {} if db else None
        # Mapping: server 0 owns indices {0,2}; server 1 owns {1,3}
        self.server_node_mapping = {
            0: [set([0, 2]), 2],
            1: [set([1, 3]), 3],
        }

def test_init_invalid_values():
    # servers == 0
    try:
        ConsistentHashing(0, 1)
        assert False
    except ValueError:
        pass

    # virtual_nodes == 0
    try:
        ConsistentHashing(1, 0)
        assert False
    except ValueError:
        pass


def test_ring_structure_from_override():
    ch = TestableConsistentHashing(servers=2, virtual_nodes=2)
    assert len(ch.ring_nodes) == 4
    assert ch.ring_nodes == [100, 200, 300, 400]
    # Ensure mapping covers all ring indices and is disjoint
    all_indices = set()
    for server, info in ch.server_node_mapping.items():
        owned, max_idx = info
        assert isinstance(owned, set)
        assert max_idx in owned
        all_indices |= set(owned)
        for idx in owned:
            assert 0 <= idx < len(ch.ring_nodes)
    assert all_indices == {0, 1, 2, 3}


def test_get_server_from_index_mapping():
    ch = TestableConsistentHashing(servers=2, virtual_nodes=2)
    # Private method: should map index 0 -> server 0, index 3 -> server 1
    get_server_from_idx = ch._ConsistentHashing__get_server_from_ring_index
    assert get_server_from_idx(0) == 0
    assert get_server_from_idx(3) == 1  # exposes bug if it returns 3 instead of server id


def test_hash_accepts_str_and_bytes():
    ch = TestableConsistentHashing(servers=2, virtual_nodes=2)
    # Should accept str transparently (current impl likely raises TypeError)
    try:
        ch.insert_data("hello")
    except Exception as e:
        assert False, f"insert_data should accept str, but raised: {e}"

    # Also accept bytes
    try:
        ch.insert_data(b"hello")
    except Exception as e:
        assert False, f"insert_data should accept bytes, but raised: {e}"


def test_wrap_around_lookup():
    ch = TestableConsistentHashing(servers=2, virtual_nodes=2)
    # Force hash to a value greater than all ring nodes to test wrap-around to index 0
    ch._ConsistentHashing__hash_object = lambda obj: 10 ** 12  # much larger than 400
    server = ch.insert_data(b"x")
    # Correct behavior: wrap-around to index 0 -> server 0
    assert server == 0


def test_consistency_same_key_same_server():
    ch = TestableConsistentHashing(servers=2, virtual_nodes=2)
    key = b"consistent-key"
    s1 = ch.insert_data(key)
    s2 = ch.insert_data(key)
    s3 = ch.insert_data(key)
    assert s1 == s2 == s3


def test_distribution_reasonable_balance_over_many_keys():
    # With 2 servers and 2 vnodes each, distribution over random keys should be roughly balanced
    ch = TestableConsistentHashing(servers=2, virtual_nodes=2)
    random.seed(42)
    counts = {0: 0, 1: 0}
    for i in range(2000):
        k = f"key-{i}".encode()
        s = ch.insert_data(k)
        counts[s] += 1
    total = counts[0] + counts[1]
    p0 = counts[0] / total
    # Allow wide margin due to small number of vnodes; should not be extremely skewed
    assert 0.3 < p0 < 0.7, f"Unbalanced distribution: {counts}"


def test_insert_data_db_mode_appends_and_returns_server():
    ch = TestableConsistentHashing(servers=2, virtual_nodes=2, db=True)
    # Insert some keys
    keys = [b"a", b"b", b"c", b"d"]
    servers = []
    for k in keys:
        s = ch.insert_data(k)
        servers.append(s)
    # Ensure data is recorded in backing store by server
    for k, s in zip(keys, servers):
        assert s in ch.db
        assert k in ch.db[s]


def test_find_biggest_gap_handles_wrap_around():
    ch = TestableConsistentHashing(servers=2, virtual_nodes=2)
    # Build a ring with a largest gap between 400 -> 100 (wrap-around)
    ch.ring_nodes = [100, 150, 160, 400]
    # largest gap is (400 -> 100) modulo ring space, so index 3 is the start
    idx = ch._ConsistentHashing__find_biggest_node_gap()
    assert idx == 3


def test_insert_virtual_node_increments_ring_and_updates_mapping():
    ch = TestableConsistentHashing(servers=2, virtual_nodes=2)
    before_len = len(ch.ring_nodes)
    before_owned = len(ch.server_node_mapping[0][0])
    # Insert for server 0
    ch.insert_virtual_node(0)
    assert len(ch.ring_nodes) == before_len + 1
    # Server 0 should own one more virtual index
    assert len(ch.server_node_mapping[0][0]) == before_owned + 1


def test_delete_server_moves_nodes_and_db():
    ch = TestableConsistentHashing(servers=2, virtual_nodes=2, db=True)
    # Seed DB with items for server 0 and 1
    items = [b"x", b"y", b"z", b"w"]
    for it in items:
        ch.insert_data(it)
    # Delete server 0 and ensure its nodes/data are reassigned
    ch.delete_server(0)
    assert 0 not in ch.server_node_mapping
    # All data should now belong to some remaining server
    assert ch.servers == 1
    assert len(ch.server_node_mapping) == 1
    remaining_server = next(iter(ch.server_node_mapping.keys()))
    assert remaining_server in ch.db
    # No orphaned data
    total_db_items = sum(len(v) for v in ch.db.values())
    assert total_db_items == len(items)


def test_insert_server_adds_expected_virtual_nodes():
    ch = TestableConsistentHashing(servers=2, virtual_nodes=2)
    before_servers = ch.servers
    before_len = len(ch.ring_nodes)
    ch.insert_server()
    assert ch.servers == before_servers + 1
    # Expect +self.nodes new virtual nodes
    assert len(ch.ring_nodes) == before_len + ch.nodes


if __name__ == "__main__":
    # Allow running this file directly
    for name, obj in list(globals().items()):
        if name.startswith("test_") and callable(obj):
            print(f"Running {name}...")
            obj()
    print("All ConsistentHashing tests attempted")
