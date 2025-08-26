# Consistent Hashing implementation with virtual nodes (vnodes) over a fixed hash space.
# - Goal: provide stable key-to-server mapping where adding/removing servers moves only a small
#   fraction of keys. Vnodes improve balance by spreading each server across the ring.
# - Hash space: [0, UPPER_BOUND], where UPPER_BOUND = 2^32 - 1.
# - Ring model:
#   - ring_nodes: sorted list[int] of vnode positions on the ring (in the hash space).
#   - server_node_mapping: dict[int, [set[int], int]] mapping server_id -> [owned_ring_indices, max_owned_index].
#     The max index is used as a convenient pointer for certain operations (e.g., delete).
# - Optional backing store:
#   - If db=True, inserted keys are appended to a per-server list in self.db for demonstration/testing.
#
# Key operations:
# - Build ring with S servers and N vnodes/server (randomized positions; stored sorted).
# - Map an object to a server by hashing it and mapping that hash to a ring index, then to the owning server.
# - Insert/remove servers (vnodes redistributed) and query capacity contributions (arc sizes).
# - Insert data (optionally recorded in self.db).
#
# Notes:
# - This implementation uses modulo ring length to map a hash to a vnode index. Given a fixed number of
#   vnodes, this makes indices uniformly distributed independently of the absolute ring positions.
# - For gap-based vnode insertion, wrap-around is handled with modulo arithmetic.
# - All methods avoid changing external behavior; comments only document current logic and tradeoffs.

import random
import hashlib
from typing import Any, Dict

# ConsistentHashing:
# - Provides a ring of virtual nodes for S logical servers.
# - Supports hashing keys to servers, inserting/removing servers/vnodes, and simple capacity queries.
# - Optionally stores inserted data by server when db=True to aid testing/demonstration.
class ConsistentHashing():
    UPPER_BOUND = (2 ** 32) - 1

    # Initialize the ring with 'servers' logical servers and 'virtual_nodes' vnodes per server.
    # - servers, virtual_nodes must be > 0; raises ValueError otherwise.
    # - ring_nodes: computed as a sorted list of random positions in [0, UPPER_BOUND].
    # - server_node_mapping: tracks, for each server, the set of owned ring indices and the max index.
    # - db: if True, creates a dict[int, list[Any]]; otherwise None.
    def __init__(self, servers: int, virtual_nodes: int, db: bool = False): 
        if servers == 0 or virtual_nodes == 0:
            raise ValueError("error, consistent hashing must have greater than 0 servers and greater than 0 virtual nodes")
        
        self.servers = servers
        self.nodes = virtual_nodes
        self.ring_nodes = [] # array of virtual node indexes (positions on the ring)
        self.db = {} if db else None

        self.server_node_mapping: Dict[int, list] = {} # mapping: server -> [set(vnode_ring_indices), max_index]

        self.__build_ring()

    # Return stored data for a server when db=True; otherwise returns None.
    # - server: target server id.
    # - When db is a dict, returns db.get(server, None). If db is None (db=False), returns None.
    def get_server_data(self, server: int):
        if not self.db:
            return None
        
        return self.db.get(server, None)

    # Build the initial ring.
    # - For each server, create 'self.nodes' random positions and pair each with its server id.
    # - Sort by position to form ring order; extract positions to self.ring_nodes.
    # - Populate server_node_mapping with the ring indices owned by each server and track max index.
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

    # Internal: map a ring index (0..len(ring_nodes)-1) to its owning server id.
    # - Iterates server_node_mapping to find which server's owned set contains the index.
    # - Returns 0 if not found (should not happen when mapping is consistent).
    def __get_server_from_ring_index(self, index: int) -> int:
        for server, info in self.server_node_mapping.items():
            if index in info[0]:  # info[0] is the set of virtual node indices
                return server
        return 0
    
    # Internal: map a hash value to a ring index.
    # - Uses modulo the number of vnodes to select an index uniformly across ring indices.
    # - Raises ValueError if the ring has no vnodes.
    def __get_ring_index_from_hash(self, hash_val: int) -> int:
        if not self.ring_nodes:
            raise ValueError("Ring has no virtual nodes")
        return hash_val % len(self.ring_nodes)
    
    # Internal: hash arbitrary data to an int using SHA-256.
    # - Accepts bytes or str; str is UTF-8 encoded via str(obj).encode().
    # - Returns the integer value of the hex digest (base 16).
    def __hash_object(self, obj: Any) -> int:
        b = obj if isinstance(obj, bytes) else str(obj).encode()
        return int(hashlib.sha256(b).hexdigest(), 16)

    # Public: map 'data' to a server id.
    # - Hash data to 0..UPPER_BOUND; map to ring index; then find owning server.
    # - Returns the logical server id for this data.
    def get_server(self, data: Any) -> int:
        hash_val = self.__hash_object(data) % (ConsistentHashing.UPPER_BOUND + 1)
        virtual_node_idx = self.__get_ring_index_from_hash(hash_val)
        return self.__get_server_from_ring_index(virtual_node_idx)
    
    # Internal: find the index i whose arc to i+1 (mod N) is the largest on the ring.
    # - Computes gaps with wrap-around using modulo UPPER_BOUND+1 arithmetic.
    # - Returns the index i (start of the largest gap segment).
    def __find_biggest_node_gap(self):
        n = len(self.ring_nodes)
        max_gap, res = float("-inf"), 0
        for i in range(n):
            j = (i + 1) % n
            gap = (self.ring_nodes[j] - self.ring_nodes[i]) % (ConsistentHashing.UPPER_BOUND + 1)
            if gap > max_gap:
                max_gap, res = gap, i
        return res

    # Insert a data item and return the assigned server.
    # - Uses get_server(data) to pick a server.
    # - If db=True, appends the item to self.db[server] (initializing the list if necessary).
    def insert_data(self, data: Any):
        server = self.get_server(data)
        if self.db is not None:
            self.db.setdefault(server, []).append(data)
        return server

    # Insert a new logical server with self.nodes new vnodes.
    # - Creates a fresh entry in server_node_mapping and then inserts self.nodes virtual nodes
    #   by repeatedly calling insert_virtual_node(new_server).
    # - Increments self.servers.
    def insert_server(self):
        new_server = self.servers
        self.server_node_mapping[new_server] = [set(), -1]
        for _ in range(self.nodes):
            self.insert_virtual_node(new_server)
        self.servers += 1

    # Insert a single vnode for 'server' into the ring.
    # - Finds the largest gap (i -> j) on the ring; places a new vnode at the midpoint.
    # - Inserts into self.ring_nodes and shifts all stored indices at/after the insertion point.
    # - Updates server_node_mapping so 'server' owns the new ring index.
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

    # Delete a server from the ring and reassign its vnodes (and data, if db=True) to the next server.
    # - Finds the next ring index after the deleted server's max index and resolves its server owner.
    # - Removes the server from the mapping and merges its vnode indices into the new owner's set.
    # - If db=True, moves all data items into the new owner's list.
    # - Decrements self.servers.
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

    # Capacity (arc length) contributed by a single vnode at index 'node'.
    # - Returns -1 for invalid indices.
    # - Otherwise, returns the modulo distance from ring_nodes[node] to ring_nodes[(node+1) % N].
    def get_node_capacity(self, node: int):
        if node < 0 or node >= len(self.ring_nodes):
            return -1
        
        next_node_idx = (node + 1) % len(self.ring_nodes)
        capacity = (self.ring_nodes[next_node_idx] - self.ring_nodes[node]) % (ConsistentHashing.UPPER_BOUND + 1)
        return capacity

    # Total capacity (sum of arc lengths) owned by 'server'.
    # - Returns -1 if the server is not present in server_node_mapping.
    # - Otherwise, sums get_node_capacity over all vnode indices owned by the server.
    def get_server_capacity(self, server: int):
        if server not in self.server_node_mapping.keys():
            return -1
        
        capacity = sum(self.get_node_capacity(virtual_node) for virtual_node in self.server_node_mapping[server][0])
        return capacity