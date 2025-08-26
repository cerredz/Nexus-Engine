import random
import hashlib
from typing import Any, Dict

class ConsistentHashing():
    UPPER_BOUND = (2 ** 32) - 1
    # bool represents whether we want the consistent hashing implementation to actually server as the db or not
    def __init__(self, servers: int, virtual_nodes: int, db: bool = False): 
        if servers == 0 or virtual_nodes == 0:
            raise ValueError("error, consistent hashing must have greater than 0 servers and greater than 0 virtual nodes")
        
        self.servers = servers
        self.nodes = virtual_nodes
        self.ring_nodes = [] # array of virtual node indexes
        self.db = {} if db else None

        self.server_node_mapping: Dict[int, list] = {} # mapping a server to its virtual nodes + max index of virtual node (used for delete method)

        self.__build_ring()

    def get_server_data(self, server: int):
        if not self.db:
            return None
        
        return self.db.get(server, None)

    def __build_ring(self):
        pairs = []
        for server in range(self.servers):
            for _ in range(self.nodes):
                random_ring_index = random.randint(0, ConsistentHashing.UPPER_BOUND)
                pairs.append((server, random_ring_index))

        pairs.sort(key=lambda x: x[1])
        self.ring_nodes = [val for _, val in pairs]

        for i, (server, _) in enumerate(pairs):
            if server not in self.server_node_mapping:
                self.server_node_mapping[server] = [set(), -1]
            self.server_node_mapping[server][0].add(i)
            self.server_node_mapping[server][1] = max(self.server_node_mapping[server][1], i)

    def __get_server_from_ring_index(self, index: int) -> int:
        for server, info in self.server_node_mapping.items():
            if index in info[0]:  # info[0] is the set of virtual node indices
                return server
        return 0
    
    def __get_ring_index_from_hash(self, hash_val: int) -> int:
        if not self.ring_nodes:
            raise ValueError("Ring has no virtual nodes")
        return hash_val % len(self.ring_nodes)
    
    def __hash_object(self, obj: Any) -> int:
        b = obj if isinstance(obj, bytes) else str(obj).encode()
        return int(hashlib.sha256(b).hexdigest(), 16)

    def get_server(self, data: Any) -> int:
        hash_val = self.__hash_object(data) % (ConsistentHashing.UPPER_BOUND + 1)
        virtual_node_idx = self.__get_ring_index_from_hash(hash_val)
        return self.__get_server_from_ring_index(virtual_node_idx)
    
    def __find_biggest_node_gap(self):
        n = len(self.ring_nodes)
        max_gap, res = float("-inf"), 0
        for i in range(n):
            j = (i + 1) % n
            gap = (self.ring_nodes[j] - self.ring_nodes[i]) % (ConsistentHashing.UPPER_BOUND + 1)
            if gap > max_gap:
                max_gap, res = gap, i
        return res

    def insert_data(self, data: Any):
        server = self.get_server(data)
        if self.db is not None:
            self.db.setdefault(server, []).append(data)
        return server

    def insert_server(self):
        new_server = self.servers
        self.server_node_mapping[new_server] = [set(), -1]
        for _ in range(self.nodes):
            self.insert_virtual_node(new_server)
        self.servers += 1

    def insert_virtual_node(self, server: int):
        i = self.__find_biggest_node_gap()
        j = (i + 1) % len(self.ring_nodes)
        lower, upper = self.ring_nodes[i], self.ring_nodes[j]
        span = (upper - lower) % (ConsistentHashing.UPPER_BOUND + 1)
        new_val = (lower + span // 2) % (ConsistentHashing.UPPER_BOUND + 1)
        insert_at = i + 1
        self.ring_nodes.insert(insert_at, new_val)

        # shift stored indices at/after insert
        for _, info in self.server_node_mapping.items():
            shifted = set()
            for idx in info[0]:
                shifted.add(idx + 1 if idx >= insert_at else idx)
            info[0] = shifted
            if info[1] >= insert_at:
                info[1] += 1

        if server not in self.server_node_mapping:
            self.server_node_mapping[server] = [set(), -1]
        self.server_node_mapping[server][0].add(insert_at)
        self.server_node_mapping[server][1] = max(self.server_node_mapping[server][1], insert_at)

    # Delete a server in the ring, have to move all virtual nodes / data of that server to the next one on the ring
    def delete_server(self, server_int: int):
        virtual_nodes, max_idx = self.server_node_mapping[server_int][0], self.server_node_mapping[server_int][1]
        next_idx = (max_idx + 1) % len(self.ring_nodes)
        new_server = self.__get_server_from_ring_index(next_idx)

        # remove old server then reassign nodes to new_server
        del self.server_node_mapping[server_int]
        if new_server not in self.server_node_mapping:
            self.server_node_mapping[new_server] = [set(), -1]
        self.server_node_mapping[new_server][0].update(virtual_nodes)
        self.server_node_mapping[new_server][1] = max(self.server_node_mapping[new_server][1], max(virtual_nodes) if virtual_nodes else -1)

        if self.db is not None:
            self.db.setdefault(new_server, [])
            self.db.setdefault(server_int, [])
            self.db[new_server].extend(self.db[server_int])
            del self.db[server_int]

        self.servers -= 1

    def get_node_capacity(self, node: int):
        if node < 0 or node >= len(self.ring_nodes):
            return -1
        
        next_node_idx = (node + 1) % len(self.ring_nodes)
        capacity = (self.ring_nodes[next_node_idx] - self.ring_nodes[node]) % (ConsistentHashing.UPPER_BOUND + 1)
        return capacity

    def get_server_capacity(self, server: int):
        if server not in self.server_node_mapping.keys():
            return -1
        
        capacity = sum(self.get_node_capacity(virtual_node) for virtual_node in self.server_node_mapping[server][0])
        return capacity

        
