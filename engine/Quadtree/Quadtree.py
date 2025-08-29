from typing import Tuple, Any, List, Optional
import random
import heapq

# Quadtree implementation for 2D point indexing with payloads.
#
# Model
# - Space is a rectangle with origin at (0,0) and dimensions (width x height).
# - Each node covers an axis-aligned rectangular region: [x, x+width) x [y, y+height).
# - Leaves store a list of points (x, y, data) up to max_points.
# - When a leaf is full and a new point is inserted, it subdivides into 4 quadrants
#   (BL, BR, TL, TR) and redistributes existing points.
# - Internal nodes do not store points themselves; only their children do.
#
# Coordinates & Boundaries
# - The right and top edges are exclusive: a point at (width, y) or (x, height) is out of bounds.
# - Insertion or query outside the root bounds is rejected.
#
# Operations
# - insert(x, y, data): O(log N) average, with N points, causing splits as needed.
# - delete(x, y): removes all points exactly at (x, y) in the leaf covering the location.
#   After deletion, it attempts to condense ancestors: if all children are leaves and the
#   total number of points across them is <= max_points, merge them into the parent leaf.
# - query(x, y): returns the point list of the leaf covering (x, y) or None if out of bounds.
#
# Notes
# - This quadtree focuses on point-bucket storage by location, not nearest-neighbor search.
# - The public API uses float inputs for insert; query/delete accept ints (compatible with float).

class Node:
    def __init__(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        points: Optional[List[Tuple[float, float, Any]]] = None,
        is_leaf: bool = True,
        children: Optional[List['Node']] = None
    ):
        # Region covered by this node
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        # Points stored at this node (only for leaves)
        self.points: List[Tuple[float, float, Any]] = points or []
        self.is_leaf = is_leaf
        # Children order: [BL, BR, TL, TR]
        self.children: List[Optional['Node']] = children or [None, None, None, None]  # BL, BR, TL, TR

    def contains(self, px: float, py: float) -> bool:
        # Point is within the node's region (right/top edges are exclusive)
        return (self.x <= px < self.x + self.width) and (self.y <= py < self.y + self.height)

    def quadrant(self, px: float, py: float) -> int:
        # Determine which child quadrant a point belongs to relative to this node
        mx = self.x + self.width / 2.0
        my = self.y + self.height / 2.0
        if py >= my:
            return 2 if px < mx else 3  # TL, TR
        else:
            return 0 if px < mx else 1  # BL, BR

    def subdivide(self) -> None:
        # Create four children covering equal subregions and mark this node as internal
        hw = self.width / 2.0
        hh = self.height / 2.0
        bl = Node(self.x,           self.y,           hw, hh)
        br = Node(self.x + hw,      self.y,           hw, hh)
        tl = Node(self.x,           self.y + hh,      hw, hh)
        tr = Node(self.x + hw,      self.y + hh,      hw, hh)
        self.children = [bl, br, tl, tr]
        self.is_leaf = False

class Quadtree():
    def __init__(self, width: float, height: float, max_points: int):
        # Validate and initialize the quadtree root covering [0,width) x [0,height)
        self.validate_inputs(width, height, max_points)
        self.width = float(width)
        self.height = float(height)
        self.max_points = max_points
        self.root = Node(x=0.0, y=0.0, width=self.width, height=self.height)

    def validate_inputs(self, width, height, max_points):
        # Enforce positive dimensions and a positive integer capacity per leaf
        if not isinstance(max_points, int) or max_points <= 0:
            raise ValueError("Max points must be a positive integer")
        if not (isinstance(width, (int, float)) and width > 0):
            raise ValueError("Width must be a positive number")
        if not (isinstance(height, (int, float)) and height > 0):
            raise ValueError("Height must be a positive number")

    def insert(self, x: float, y: float, data: Any = None) -> bool:
        # Insert a point into the quadtree; returns False if out of bounds
        if not self.root.contains(x, y):
            return False

        def dfs(node: Node, px: float, py: float, payload: Any) -> None:
            if node.is_leaf:
                if len(node.points) < self.max_points:
                    node.points.append((px, py, payload))
                    return
                # Split and redistribute existing points
                node.subdivide()
                old_points = node.points
                node.points = []
                for ox, oy, od in old_points:
                    idx = node.quadrant(ox, oy)
                    dfs(node.children[idx], ox, oy, od)
            # Descend to the appropriate child for the new point
            idx = node.quadrant(px, py)
            dfs(node.children[idx], px, py, payload)

        dfs(self.root, x, y, data)
        return True

    def delete(self, x: int, y: int):
        # Remove all points at exactly (x, y) within the leaf covering the coordinate.
        # Returns True if any point was removed, False otherwise. Performs upward
        # condensation: if a parent's children are all leaves and their total points fit
        # in max_points, merges them into the parent leaf.
        if not self.root.contains(x, y):
            return False

        path = []
        node = self.root

        while not node.is_leaf:
            idx = node.quadrant(x, y)
            child = node.children[idx] if idx < len(node.children) else None
            if not child:
                break
            path.append((node, idx))
            node = child

        before = len(node.points)
        node.points = [(px, py, d) for (px, py, d) in node.points if not (px == x and py == y)]
        if len(node.points) == before:
            return False

        while path:
            parent, _ = path.pop()
            aggregated = []
            all_leaves = True
            for ch in parent.children:
                if ch is None:
                    continue
                if not ch.is_leaf:
                    all_leaves = False
                    break
                aggregated.extend(ch.points)
            if all_leaves and len(aggregated) <= self.max_points:
                parent.points = aggregated
                parent.is_leaf = True
                parent.children = [None, None, None, None]
            else:
                break

        return True    

    def query(self, x: int, y: int):
        # Return the points stored in the leaf covering (x, y), or None if out of bounds.
        if not self.root.contains(x, y):
            return None
    
        node = self.root
        while not node.is_leaf:
            idx = node.quadrant(x, y)
            if len(node.children) <= idx or not node.children[idx]:
                break
            node = node.children[idx]
    
        return node.points
        
    @staticmethod
    def build(self, x_origin: float, y_origin: float, width: float, height: float, data_points: List[Tuple[int, int, int]]):
        # Build a quadtree from a list of (x, y, value) triples and return it.
        # Note: signature suggests origin usage but current implementation assumes root at (0,0).
        quadtree = Quadtree(x=x_origin, y=y_origin, width=width, height=height)
        for x, y, val in data_points:
            quadtree.insert(x,y, val)

        return quadtree

