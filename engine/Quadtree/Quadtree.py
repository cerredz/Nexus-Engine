from typing import Tuple, Any, List, Optional
import random
import heapq

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
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.points: List[Tuple[float, float, Any]] = points or []
        self.is_leaf = is_leaf
        self.children: List[Optional['Node']] = children or [None, None, None, None]  # BL, BR, TL, TR

    def contains(self, px: float, py: float) -> bool:
        # if point lies within the node's "box"
        return (self.x <= px < self.x + self.width) and (self.y <= py < self.y + self.height)

    def quadrant(self, px: float, py: float) -> int:
        mx = self.x + self.width / 2.0
        my = self.y + self.height / 2.0
        if py >= my:
            return 2 if px < mx else 3  # TL, TR
        else:
            return 0 if px < mx else 1  # BL, BR

    def subdivide(self) -> None:
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
        self.validate_inputs(width, height, max_points)
        self.width = float(width)
        self.height = float(height)
        self.max_points = max_points
        self.root = Node(x=0.0, y=0.0, width=self.width, height=self.height)

    def validate_inputs(self, width, height, max_points):
        if not isinstance(max_points, int) or max_points <= 0:
            raise ValueError("Max points must be a positive integer")
        if not (isinstance(width, (int, float)) and width > 0):
            raise ValueError("Width must be a positive number")
        if not (isinstance(height, (int, float)) and height > 0):
            raise ValueError("Height must be a positive number")

    def insert(self, x: float, y: float, data: Any = None) -> bool:
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
        # build quad tree from a list of data points
        quadtree = Quadtree(x=x_origin, y=y_origin, width=width, height=height)
        for x, y, val in data_points:
            quadtree.insert(x,y, val)

        return quadtree

