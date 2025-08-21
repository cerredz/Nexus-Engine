# Merkle Tree implementation specialized for list[str] inputs.
# Leaves are string contents hashed with SHA-256 (UTF-8), and internal nodes hash the
# concatenation of child digests. The tree duplicates the last child when a level has
# an odd number of nodes, ensuring every internal node has two children.
# For usability and performance, it stores both parent pointers (fast O(log N) updates)
# and an array of levels with root-first ordering (O(1) node access by (level, index)).

from collections import deque
from typing import List, Self, Tuple
from .DiffResult import DiffResult
import hashlib

# Node structure representing a Merkle node (leaf or internal).
# - data: hex digest string for this node (SHA-256 of leaf content or of child digests).
# - left/right: child links (None for leaves).
# - parent: parent link (None at root). Enables efficient upward recomputation.
# - content: human-readable content (leaf string for leaves; "L+R" for internal nodes).
class Node():
    def __init__(self, data, left, right, parent, content):
        self.data = data
        self.left: Node = left
        self.right: Node = right
        self.parent: Node = parent
        self.content: str = content

# MerkleTree built from a list of strings. Provides:
# - Building and (re)building the tree (full rebuild on append ops in this version).
# - Equality and structural diff utilities.
# - Point updates to a leaf with O(log N) recomputation using parent pointers.
# - O(1) node addressing via precomputed root-first levels.
class MerkleTree():
    # Initialize from a list[str] of leaf contents. Builds the tree and the levels index.
    # size mirrors the number of leaves; levels is root-first; tree is the root node (or None).
    def __init__(self, data: List[str]):
        self.data = data
        self.size = len(data)
        self.levels = []
        self.tree = self.init_merkle_tree()

    # Return the root node (or None if the tree is empty). Most callers want root.data.
    def get_root(self):
        return self.tree
    
    # Return the current number of leaves. This mirrors len(self.data) and is kept in sync.
    def get_leaves(self):
        return self.size
    
    # Return the raw string content at a leaf index (0-based). Does not return the hash.
    def get_leaf(self, idx: int):
        return self.data[idx]
    
    # Return the height (number of levels), counting the root as level 0.
    def height(self):
        return len(self.levels)
    
    # Return the number of nodes at a given level (0-based, root-first).
    # Returns -1 if level is out of range.
    def level_width(self, level: int):
        if level < 0 or level >= len(self.level):
            return -1
        
        return len(self.level[level])
    
    # O(1) access to a node by (level, index), where level is root-first and index is left-to-right.
    # Returns None if out of bounds or if the tree is empty.
    def get_node(self, level:int, idx: int) -> Node:
        if not self.tree or level < 0 or idx < 0 or level >= len(self.levels) or idx >= len(self.levels[level]):
            return None
        
        return self.levels[level][idx]
    
    # Generate a SHA-256 hex digest from a string (UTF-8). Used for both leaves and internals.
    # For internal nodes, child digests (hex) are concatenated and hashed.
    def generate_hash(self, data):
        return hashlib.sha256(data.encode('utf-8')).hexdigest()
    
    # Build the Merkle tree from self.data and populate self.levels (root-first).
    # - Leaves: hash each string; store as nodes with no children and parent=None.
    # - Build upward: pair children left/right; if odd, duplicate the last child.
    # - Set parent pointers for fast upward recomputation; return the root node (or None).
    def init_merkle_tree(self):
        if self.size == 0:
            self.levels = []
            self.tree = None
            return None
        # Build the leaf hashess
        leaf_hashes = [Node(self.generate_hash(string), left=None, right=None, parent=None, content=string) for string in self.data]
        levels = [leaf_hashes]
        q = deque(leaf_hashes) 

        # build the rest of the tree
        while len(q) > 1:
            next_q = deque()
            curr_level = []
            
            while q:
                left_child = q.popleft()
                right_child = q.popleft() if q else left_child
                content = f"{left_child.content}+{right_child.content}"
                parent = Node(self.generate_hash(left_child.data + right_child.data), left=left_child, right=right_child, parent=None, content=content)
                left_child.parent = parent
                right_child.parent = parent
                next_q.append(parent)
                curr_level.append(parent)

            levels.append(curr_level)
            q = next_q

        self.levels = list(reversed(levels))
        return q[0] if q else None
    
    # Breadth-first traversal that returns a list of either node hashes or content strings.
    # - If both flags are False, returns [].
    # - Be cautious: content strings on internal nodes grow as "L+R" and can become large.
    def print_tree(self, hashes: bool = False, content: bool = False) -> List[str]:
        if self.size == 0 or (not hashes and not content):
            return []
        res = []
        q = deque()
        q.append(self.tree)
        while q:
            n = len(q)

            for i in range(n):
                curr = q.popleft()
                res.append(curr.data if hashes else curr.content)

                if curr.left:
                    q.append(curr.left)
                
                if curr.right:
                    q.append(curr.right)

        return res  

    # Root equality check: returns True if both trees are non-empty and the root digests match.
    # This is an O(1) identity/equality check over the committed content and order.
    def equals(self, merkle_tree: Self) -> bool:
        return (self.tree is not None and merkle_tree.tree is not None and self.tree.data == merkle_tree.tree.data)

    # Structural diff: walks top-down comparing subtree hashes. Returns:
    # - first_difference: (level, index) of the first differing subtree, or (-1, -1) if none.
    # - differing_subtrees: list[(level, index)] for all differing subtrees discovered.
    # - leaf_differing_indices: list[int] of leaf indices whose leaf hashes differ (bounded by max_diffs).
    # If early_exit is True, stops after the first differing subtree. Assumes same policy/build rules.
    def diff(self, merkle_tree: Self, max_diffs: int, early_exit: bool = False) -> DiffResult:
        if self.equals(merkle_tree):
            return DiffResult(first_difference=(-1, -1), leaf_differing_indices=[], differing_subtrees=[])
        
        differing_subtrees: List[Tuple[int, int]] = []
        first_difference = None

        # compute diff
        curr_level = 0
        curr_level_pairs = [(self.tree, merkle_tree.tree)]        
        while curr_level_pairs:
            next_level_pairs = []
            for idx, (a, b) in enumerate(curr_level_pairs):
                a_hash = a.data if a else None
                b_hash = b.data if b else None

                if a_hash != b_hash:
                    if first_difference is None:
                        first_difference = (curr_level, idx)
                        if early_exit:
                            return DiffResult(
                                first_difference=first_difference,
                                leaf_differing_indices=[],
                                differing_subtrees=[first_difference]
                            )
                    differing_subtrees.append((curr_level, idx))
                    next_level_pairs.append((a.left if a else None, b.left if b else None))
                    next_level_pairs.append((a.right if a else None, b.right if b else None))

            curr_level_pairs = next_level_pairs
            curr_level += 1

        # leaf differences
        leaf_differing_indices: List[int] = []
        a_leaves, b_leaves = self.data, merkle_tree.data
        L = min(len(a_leaves), len(b_leaves))
        for i in range(L):
            if self.generate_hash(a_leaves[i]) != self.generate_hash(b_leaves[i]):
                leaf_differing_indices.append(i)
                if len(leaf_differing_indices) >= max_diffs:
                    break

        if len(leaf_differing_indices) < max_diffs and len(a_leaves) != len(b_leaves):
            i = L
            while i < max(len(a_leaves), len(b_leaves)) and len(leaf_differing_indices) < max_diffs:
                leaf_differing_indices.append(i)
                i += 1

        if first_difference is None:
            first_difference = (-1, -1)

        return DiffResult(
            first_difference=first_difference,
            leaf_differing_indices=leaf_differing_indices,
            differing_subtrees=differing_subtrees,
        )
    
    # Point update: replace the string at leaf index with new_str and recompute upward to the root.
    # - Skips recomputation if the new digest equals the current leaf digest.
    # - Updates self.data, leaf.content, leaf.data, and then all ancestor node digests/content.
    # - Returns True on success; False for out-of-range index.
    def set_leaf(self, index: int, new_str: str):
        if index < 0 or index >= len(self.data):
            return False
        
        # get leaf and update
        leaf = self.levels[-1][index]
        new_digest = self.generate_hash(new_str)
        if new_digest == leaf.data:
            self.data[index] = new_str 
            leaf.content = new_str
            return True
        
        self.data[index] = new_str
        leaf.content = new_str
        leaf.data = self.generate_hash(new_str)

        # traverse back up tree and update hashes
        node = leaf.parent
        while node:
            node.content = f"{node.left.content}+{node.right.content}"
            node.data = self.generate_hash(node.left.data + node.right.data)
            node = node.parent

        return True

    # Append a single leaf (full rebuild version): appends to self.data, rebuilds the tree/levels,
    # updates size, and returns the new leaf Node (last element of the leaf level).
    # This is simple and correct; an O(log N) incremental version can replace it later.
    # TODO: implement in log n time (i got lazy, there has to be a way)
    def append_leaf(self, new_str: str) -> Node:
        self.data.append(new_str)
        self.tree = self.init_merkle_tree()
        self.size = len(self.data)
        return self.levels[-1][-1]

    # Append multiple leaves (full rebuild version): extends self.data, rebuilds the tree/levels,
    # updates size, and returns the list of newly appended leaf Nodes in order.
    # This enables callers to capture handles to the new leaves immediately after append.
    def append_leaves(self, new_strs: list[str]) -> List[Node]:
        start = len(self.data)
        self.data.extend(new_strs)
        self.tree = self.init_merkle_tree()
        self.size = len(self.data)
        return self.levels[-1][start:]
        