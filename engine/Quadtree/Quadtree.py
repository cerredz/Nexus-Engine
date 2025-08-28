from typing import Tuple, Any, List
import heapq

class Node():
    def __init__(self, x: int, y: int, width: float | int, height: float | int, points: List, is_leaf: bool, children: List['Node']):
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.points = heapq.heapify(points)
        self.is_leaf = is_leaf
        self.children = children

class Quadtree():
    def __init__(self, width: int, height: int, max_points: int):
        self.validate_inputs(width, height, max_points)
        self.width = width
        self.height = height
        self.max_points = max_points
        
        self.root = Node(x=0, y=0, width=width, height=height, points=[], is_leaf=True, children=[None, None, None, None])

    def validate_inputs(self, width, height, max_points):
        if not isinstance(width, int) or width <= 0:
            raise ValueError("Width must be a positive integer")
        if not isinstance(height, int) or height <= 0:
            raise ValueError("Height must be a positive integer")
        if not isinstance(max_points, int) or max_points <= 0:
            raise ValueError("Max points must be a positive integer")



    def insert(self, val: int):
        node = self.root
        
        def dfs(node: 'Node', val: int, max_points: int):
            if node.is_leaf and len(node.points) < max_points:
                heapq.heappush(node.points, val)
                return 
            
            # Node has too many points, must split into four children
            if node.is_leaf:

                new_width = round(float(node.width / 2), 2)
                new_height = round(float(node.height / 2), 2)
                n = len(node.points)
                
                top_left_node = Node(x=node.x, y=node.y+new_height, width=new_width, height=new_height, points=node.points[:n//4], is_leaf=True, children=[None, None, None, None])
                top_right_node = Node(x=node.x+new_width, y=node.y+new_height,width=new_width, height=new_height, points=node.points[n//4:n//2], is_leaf=True, children=[None, None, None, None])
                bottom_left_node = Node()
                bottom_right_node = Node()

                node.children = [top_left_node, top_right_node, bottom_left_node, bottom_right_node]
                node.is_leaf = False
                
                return dfs(node.children[0], val, max_points)
            
            if not node.is_leaf:
                

            
        dfs(node, val, self.max_points)

    def delete(self, data: Any):
        pass

    def query(self, x_min, x_max, y_min, y_max):
        # returns all data points within a region
        pass
        
    @staticmethod
    def build(self, data_points: List[tuple]):
        # build quad tree from a list of data points
        pass

