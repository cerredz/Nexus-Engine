# Skip List implementation for ordered sets of comparable keys.
# Structure overview:
# - Probabilistic multi-level linked structure with horizontal (left/right) and vertical (top/bottom) pointers.
# - Each level has sentinel endpoints: -inf (left) and +inf (right). All real keys live between them.
# - Search starts at the top-left sentinel and proceeds right/down, yielding expected O(log N) behavior.
# - This implementation stores level endpoints in `self.layers` as [neg_sentinel, pos_sentinel] per level.
#   Real nodes are linked via pointers and are not stored in arrays per level.
# - Note: Some utilities here are intentionally minimal and primarily for internal/testing use.

import random

class Node:
	# Node structure representing a skip-list node (tower element).
	# - data: the comparable key for this node (float('-inf')/float('inf') for sentinels).
	# - left/right: horizontal neighbors on the same level.
	# - bottom/top: vertical links to the same key one level below/above (None at bottom/top).
	left: 'Node | None'
	right: 'Node | None'
	bottom: 'Node | None'
	top: 'Node | None'

	def __init__(self, data, left=None, right=None, bottom=None, top=None):
		self.data = data
		self.left = left
		self.right = right
		self.bottom = bottom
		self.top = top

class SkipList():
	# SkipList storing comparable keys.
	# Fields:
	# - layers: list of [neg_sentinel, pos_sentinel] for each level (0 = base level).
	# - height: number of levels (>= 1 once initialized).
	# - size: count of real keys at the base level.
	# - first/last: optional references intended to track the first/last base-level nodes (not fully maintained).
	def __init__(self):
		self.layers = []
		self.height = 0
		self.size = 0
		self.__create_layer()
		self.first = None
		self.last = None

	# Return the stored first node reference (or None). Intended as the smallest base-level node.
	# Note: This field is not robustly maintained by current insert/delete logic.
	def get_first(self):
		return self.first

	# Return the stored last node reference (or None). Intended as the largest base-level node.
	# Note: This field is not robustly maintained by current insert/delete logic.
	def get_last(self):
		return self.last

	# Return the number of levels currently in the skip list.
	def get_height(self):
		return self.height
	
	# Return the number of elements (keys) stored.
	def get_size(self):
		return self.size
	
	# Return the endpoints [neg_sentinel, pos_sentinel] for a given 1-based level index.
	# Raises ValueError if the requested level does not exist.
	def get_layer(self, layer: int):
		if layer < 1 or layer > self.height:
			raise ValueError(f"Layer does not exist in skip list, must be between 1 and {self.height}")
	
		return self.layers[layer - 1]
	
	# Return a minimal snapshot of the internal structure: [layers, height].
	# Primarily useful for debugging and tests; not a public data export.
	def get_full_list(self):
		return [self.layers, self.height]

	# Random coin flip for tower growth during insert (True ~ heads).
	def __flip_coin(self):
		return random.randint(0, 1) == 1
	
	# Clear the data structure to an empty state and reinitialize with a single level of sentinels.
	# Caution: Current implementation sets self.layers = 0 (int), which will break subsequent list indexing.
	# Intended behavior would be to reset to an empty list before re-creating the base layer.
	def clear(self):
		self.layers = 0
		self.height = 0
		self.__create_layer()
		self.smallest = float('inf')
		self.largest = float("-inf")

	# Return a list representing the "first layer".
	# Current behavior: returns [node or node.data for node in self.layers[0]] which are the two sentinels only.
	# Note: This does not traverse the actual list of values between sentinels; it's mainly a structural peek.
	def to_list(self, nodes: bool = False):
		first_layer = self.layers[0]
		return [node if nodes else node.data for node in first_layer]
	
	# Internal: create a new top layer with -inf and +inf sentinels and link them vertically to the layer below.
	# Called at initialization and during insertion promotion when a new level is needed.
	def __create_layer(self):
		is_first_layer = self.height == 0
		below_left = None if is_first_layer else self.layers[self.height - 1][0]
		below_right = None if is_first_layer else self.layers[self.height - 1][-1]

		neg_node = Node(float('-inf'), left=None, right=None, bottom=below_left, top=None)
		pos_node = Node(float('inf'),   left=neg_node, right=None, bottom=below_right, top=None)
		neg_node.right = pos_node

		if not is_first_layer:
			below_left.top = neg_node
			below_right.top = pos_node

		self.layers.append([neg_node, pos_node])
		self.height += 1

	# Search for a key: proceeds top-down, moving right while right.data < key.
	# If an exact match is seen at the current level (node.right.data == key), returns that node (which may be above base level).
	# Otherwise, descends until reaching the base level and returns the predecessor node at that level (data <= key).
	# Note: Because an exact match can return an upper-level node, callers that need base-level neighbors should adjust.
	def search(self, key):
		node = self.layers[self.height - 1][0]
		while True:
			while node.right and node.right.data < key:
				node = node.right
			
			if node.right and node.right.data == key:
				return node.right
			
			if node.bottom:
				node = node.bottom
			else:
				return node

	# Internal helper: builds a list of predecessor nodes at each level for a target key.
	# preds[i] is the predecessor on level i (0 = base). Used by insert/delete to splice links.
	def __find_predecessors(self, key):
		# builds table of predecessors at each level
		preds = [None] * self.height
		node = self.layers[self.height - 1][0]
		level = self.height - 1
		while True:
			while node.right and node.right.data < key:
				node = node.right
			preds[level] = node
			if node.bottom:
				node = node.bottom
				level -= 1
			else:
				break
		return preds

	# Insert a key:
	# - Uses predecessors to splice into base level.
	# - Promotes the node with geometric probability, adding levels as needed.
	# - Duplicate inserts return the existing node and do not alter size.
	# Complexity: expected O(log N).
	# Note: The `first`/`last` updates at the end compare a Node to float sentinels and will not work as intended.
	def insert(self, key):
		preds = self.__find_predecessors(key)
		pred = preds[0]

		# duplicate check
		if pred.right and pred.right.data == key:
			return pred.right

		# insert at base level
		right = pred.right
		new_node = Node(key, left=pred, right=right, bottom=None, top=None)
		pred.right = new_node
		self.size += 1
		if right:
			right.left = new_node

		# promote with coin flips
		level = 1
		lower = new_node
		while self.__flip_coin():
			if level >= self.height:
				# add new top layer and use its -inf as predecessor
				self.__create_layer()
				preds.append(self.layers[self.height - 1][0])

			pred = preds[level]
			right = pred.right
			upper = Node(key, left=pred, right=right, bottom=lower, top=None)
			pred.right = upper
			if right:
				right.left = upper
			lower.top = upper
			lower = upper
			level += 1
		
		# Intended to update first/last: current comparisons won't succeed because they compare Node to float sentinels.
		if new_node.left == float('-inf'):
			self.first = new_node

		if new_node.right == float('inf'):
			self.last = new_node

		return new_node
	
	# Containment check for a key. True if an exact match is found at any level; False otherwise.
	# Complexity: expected O(log N).
	def contains(self, key):
		node = self.layers[self.height - 1][0]
		while True:
			while node.right and node.right.data < key:
				node = node.right
			
			if node.right and node.right.data == key:
				return True
			
			if node.bottom:
				node = node.bottom
			else:
				return False

	# Delete a key (if present) across all levels, then shrink empty top layers.
	# Complexity: expected O(log N).
	def delete(self, key):
		preds = self.__find_predecessors(key)
		found = False

		for level in range(self.height - 1, -1, -1):
			pred = preds[level]
			curr = pred.right
			if curr and curr.data == key:
				found = True
				forward = curr.right
				pred.right = forward
				if forward:
					forward.left = pred
				if curr.bottom:
					curr.bottom.top = None
				curr.left = curr.right = curr.top = curr.bottom = None

		if not found:
			return

		self.size -= 1
		while self.height > 1 and self.layers[-1][0].right is self.layers[-1][-1]:
			neg, pos = self.layers.pop()
			self.layers[-1][0].top = None
			self.layers[-1][-1].top = None
			self.height -= 1

	# Bulk insert convenience wrapper. No ordering requirement; duplicates are no-ops.
	def insert_many(self, keys: list):
		if len(keys) == 0:
			return
		
		for key in keys:
			self.insert(key)

	# Bulk delete convenience wrapper. Non-existent keys are ignored.
	def delete_many(self, keys: list):
		if len(keys) == 0:
			return
		
		for key in keys:
			self.delete(key)

	# Ceiling utility:
	# - If is_node=True and an exact match occurs during traversal, returns that node (may be upper level).
	# - Otherwise, descends and collects node.data from the last traversal point to the right end at base level.
	# Note: Semantics here are non-standard for "ceiling" (commonly returns a single key or None).
	def ceiling(self, key, is_node: bool = False) -> list:
		# return list of keys >= key or None or starting node where key >= key
		node = self.layers[self.height - 1][0]

		while True:
			while node.right and node.right.data < key:
				node = node.right

			if node.data == key:
				if is_node:
					return node.right

			if node.bottom:
				node = node.bottom
			else:
				break
		
		res = []
		while node:
			res.append(node.data)
			node = node.right

		return res
	
	# Floor utility:
	# - Descends to base level and collects node.data for keys <= given key from the start up to predecessor.
	# Note: Semantics here are non-standard for "floor" (commonly returns a single key or None).
	def floor(self, key) -> list:
		node = self.layers[self.height - 1][0]

		while True:
			while node.right and node.right.data <= key:
				node = node.right   

			if node.bottom:
				node = node.bottom
			else:
				break


		res = []
		curr = self.layers[0][0]
		while curr and curr.right and curr.right != node:
			res.append(curr.right.data)
			curr = curr.right

		return res

	# Successor utility: returns the data for the node after the predecessor/right neighbor.
	# Note: Current logic advances two steps in some cases and can skip the true successor or hit +inf.
	def successor(self, key):
		pred = self.search(key)
		curr = pred.right
		return curr.right.data if curr.right else None

	# Predecessor utility: returns the predecessor's data (or the key itself on exact match).
	# Note: Current logic returns the key itself if an exact match is found (non-standard for predecessor).
	def predecessor(self, key):
		pred = self.search(key)
		return pred.data if pred else None

	# Merge another skip list's contents into this one by iterating other.
	# Note: Current implementation does not advance `node` in the loop, which will not terminate.
	def merge(self, other: 'SkipList'):
		node = other.get_first()
		while node:
			self.insert(node.data)

		return self.get_full_list()