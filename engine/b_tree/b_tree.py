from typing import Optional
import math

class Node():
    def __init__(self, m: int, leaf: bool = True):
        self.m = m
        self.keys: list = []
        self.children: list['Node'] = []
        self.leaf = leaf

class BTree():
    def __init__(self, m: int):
        if m < 3:
            raise ValueError("B-Tree order m must be greater than or equal to 3")
        self.tree = Node(m, True)
        self.m_ary = m

    def insert(self, key):
        
        root = self.tree
        MAX_KEYS = self.m_ary - 1
        
        if len(root.keys) == MAX_KEYS:
            new_root = Node(self.m_ary, False)
            new_root.children = [root]
            self.tree = new_root
            self._split_child(new_root, 0, root)
            self._insert_non_full(new_root, key)

        else:
            self._insert_non_full(root, key)

        
    def _split_child(self, parent: Node, index: int, child: Node):
        z = Node(self.m_ary, leaf=child.leaf)

        mid = len(child.keys) //2
        seperator = child.keys[mid]

        z.keys = child.keys[mid + 1:]
        child.keys = child.keys[:mid]

        if not child.leaf:
            pass

        


            

        

        
            














    def exists(self, key):
        return True if self.search(key) is not None else False
    
    def search(self, key, node=None):
        node = self.tree if node is None else node
        
        if node is None:
            return None

        if not node.keys and (node.leaf or not node.children):
            return None

        left, right = 0, len(node.keys) - 1
        while left <= right:
            mid = (left + right) // 2
            if node.keys[mid] == key:
                return node.keys[mid]
            elif node.keys[mid] > key:
                right = mid - 1
            else:
                left = mid + 1

        if node.leaf:
            return None

        child_index = left
        if child_index >= len(node.children):
            return None
        
        return self.search(key, node.children[child_index])

    def delete(self, data):
        pass