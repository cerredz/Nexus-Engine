from collections import defaultdict, deque

class AdjacencyList():
    def __init__(self):
        self.map = defaultdict(set)

    def add_vertex(self, v):
        if not self.map[v]:
            self.map[v] = set()

    def remove_vertex(self, v):
        if self.map[v]:
            self.map[v].clear()

    def add_edge(self, v, e):
        self.map[v].add(e)
        self.map[e].add(v)

    def vertices(self):
        return self.map.keys()

    def edge_exists(self, v, e):
        return e in self.map[v]
    
    def remove_edge(self, v, e):
        self.map[v].discard(e)
        self.map[e].discard(v)

    def get_neighbors(self, v):
        return list(self.map[v])
    
    def clear(self):
        self.map.clear()

    def shortest_path(self, v, e):
        if v == e:
            return 0

        if v not in self.map or e not in self.map:
            return -1
        
        visited = {v}
        q = deque([(v, 0)])  # (node, distance)
        
        while q:
            u, d = q.popleft()
            for w in self.map[u]:
                if w == e:
                    return d + 1
                
                if w not in visited:
                    visited.add(w)
                    q.append((w, d + 1))

        return -1

    def has_path (self, v, e):
        return self.shortest_path(v, e) >= 0
    
    @classmethod
    def from_list(cls, edges, vertices=None):
        adj = cls()
        if vertices is not None:
            for v in vertices:
                adj.add_vertex(v)

        for item in edges:
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                raise ValueError("Each item must be a 2-element (u, v) pair")
            u, v = item
            adj.add_edge(u, v)
            
        return adj

