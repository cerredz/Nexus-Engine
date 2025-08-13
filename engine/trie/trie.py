# Trie implementation specialized for string keys.
# Each node stores:
# - char: single character for the edge leading to this node (root has None).
# - children: dict[char, Node] mapping next character to child node.
# - is_end: whether this node terminates a stored key.
#
# Design notes:
# - Keys must be non-empty strings. Empty strings are rejected.
# - Prefix queries treat the empty prefix as invalid (returns False).
# - Key enumeration uses DFS and returns keys in insertion-agnostic order.
# - This structure is optimized for prefix operations and set-like membership.
#
# API overview:
# - insert(key: str) -> bool:
#     Insert a key. Raises ValueError on empty key.
#     Returns a boolean flag; see inline note in method for details.
# - contains(key: str) -> bool:
#     Exact membership test. Returns False for "" or if any character path is missing.
# - starts_with(prefix: str) -> bool:
#     Returns True if at least one key in the trie starts with prefix. False for "".
# - keys_with_prefix(prefix: str, limit: int | None = None) -> list[str]:
#     Enumerate up to 'limit' keys that start with 'prefix'. If limit is None, returns all.
#     Note: does not include 'prefix' itself unless there are further characters (see code).
# - keys() -> list[str]:
#     Return all keys stored in the trie.
# - update(keys: list[str]) -> int:
#     Insert multiple keys; returns a count of how many were considered "new".
# - delete(key: str) -> bool:
#     Remove a key, if present. Returns False for empty key.
#     Assumes the key exists; deleting a non-existent key may raise KeyError when traversing.
# - longest_prefix_of(key: str) -> str:
#     Return the longest stored key that is a prefix of 'key'.
#
# Complexity (N = total characters across all stored keys; L = length of input key/prefix):
# - insert/contains/starts_with/delete/longest_prefix_of: O(L)
# - keys_with_prefix(prefix, limit): O(L + R) where R is number of reported keys (bounded by limit)
# - keys(): O(N)
#
# Implementation caution:
# - This implementation does not normalize case; comparisons are exact.
# - Deletion performs lazy cleanup of orphan nodes on the way back up.
# - Concurrency is not handled; external synchronization is required for multi-threaded use.

class Node():
	def __init__(self, char, is_end:bool = False):
		self.char = char
		self.children: dict = {}
		self.is_end = is_end

class Trie():
	def __init__(self):
		self.root = Node(None)
		self.size = 0
	
	# Return the number of stored keys.
	def length(self):
		return self.size
	
	# Return True if no keys are stored.
	def is_empty(self):
		return self.size == 0
	
	# Remove all keys from the trie.
	def clear(self):
		self.root = Node(None)
		self.size = 0
	
	# Insert a non-empty key into the trie.
	# - Raises ValueError if key == "".
	# - Returns a boolean flag; note that this flag reflects internal state before finalizing
	#   the terminal marker on the last node. Callers use it as an indicator of "newness".
	def insert(self, key: str):
		idx = 0
		curr = self.root

		if key == "":
			raise ValueError("Cannot insert empty string into trie.")

		# Walk/create nodes for each character in the key.
		while idx < len(key):
			curr_char = key[idx]
			if curr_char not in curr.children:
				is_end = True if idx == len(key) - 1 else False
				curr.children[curr_char] = Node(key[idx], is_end)
			
			curr = curr.children[curr_char]
			idx += 1

		# Determine "newness" based on existing terminal state at the last node.
		is_new = curr.is_end
		if is_new:
			self.size += 1

		# Ensure the last node is marked as a key terminus.
		curr.is_end = True
		return is_new # returns if we added new word to trie or if it already existed
	
	# Exact membership check. Returns False for empty string.
	def contains(self, key):
		curr = self.root

		if key == "":
			return False

		for ch in key:
			if not ch in curr.children:
				return False
			
			curr = curr.children[ch]

		return curr.is_end
	
	# Prefix existence check. Returns False if prefix == "".
	def starts_with(self, prefix):
		if prefix == "":
			return False
		
		curr = self.root
		
		for ch in prefix:
			if not ch in curr.children:
				return False
			
			curr = curr.children[ch]
		
		return True

	# Return up to 'limit' keys that start with 'prefix'.
	# - If 'prefix' path is missing, returns [].
	# - If limit is None, returns all matches.
	# - If 'prefix' itself is a stored key, it is included only when 'path' is non-empty during DFS.
	def keys_with_prefix(self, prefix, limit=None):
		res: list[str] = []
		curr = self.root

		if limit is not None and limit <= 0:
			return []

		# Traverse to last char in prefix
		for ch in prefix:
			if not ch in curr.children:
				return []
			
			curr = curr.children[ch]

		if limit is not None and len(res) >= limit:
			return []

		path: list[str] = []
		def dfs(node: Node):
			if limit is not None and len(res) >= limit:
				return
		
			# If node terminates a key and we have collected at least one extra char,
			# report prefix + path.
			if node.is_end and path:
				res.append(prefix + "".join(path))
				if limit is not None and len(res) >= limit:
					return
				
			for ch, child in node.children.items():
				path.append(ch)
				dfs(child)
				path.pop()
				if limit is not None and len(res) >= limit:
					return

		dfs(curr)
		return res

	# Return all keys stored in the trie.
	def keys(self):
		res: list[str] = []
		path: list[str] = []

		def dfs(node: Node):
			if node.is_end and path:
				res.append(''.join(path))
			for ch, child in node.children.items():
				path.append(ch)
				dfs(child)
				path.pop()

		dfs(self.root)
		return res
	
	# Bulk insert a list of keys. Returns a count of keys considered "new" by insert().
	def update(self, keys: list[str]):
		keys_added = 0
		for key in keys:
			new_key = self.insert(key)
			if new_key:
				keys_added += 1

		return keys_added
	
	# Delete a key. Returns False for empty key.
	# Assumes the key exists in the trie; if any character path is missing,
	# traversal will raise a KeyError during access.
	# Performs lazy cleanup: prunes nodes that become non-terminal and childless.
	def delete(self, key):
		if key == "":
			return False
		
		curr = self.root
		stack = [] # parent, char, node
		for ch in key:
			stack.append((curr, ch, curr.children[ch]))
			curr = curr.children[ch]

		# Unmark terminal and update size.
		curr.is_end = False
		self.size -= 1

		# Prune back up until reaching a branching or terminal node.
		while stack:
			parent, ch, curr = stack.pop()
			if curr.children or curr.is_end:
				break

			del parent.children[ch]

		return True

	# Return the longest stored key that is a prefix of 'key'.
	# Example: given keys {"new", "newyork"}, longest_prefix_of("newyorkcity") -> "newyork".
	def longest_prefix_of(self, key):
		# Ex: input: newyorkcity, returns newyork over new
		res = []
		longest = 0

		curr = self.root
		for ch in key:
			if ch not in curr.children:
				break
			
			res.append(ch)
			curr = curr.children[ch]
			if curr.is_end:
				longest = len(res)

		return ''.join(res[:longest])

if __name__ == "__main__":
	# Basic sanity tests
	trie = Trie()
	assert trie.is_empty()
	assert trie.length() == 0

	for w in ["app", "apple", "bat"]:
		trie.insert(w)

	assert not trie.is_empty()
	assert trie.length() == 3
	assert trie.contains("app")
	assert not trie.contains("ap")
	assert trie.starts_with("ap")
	assert set(trie.keys()) == {"app", "apple", "bat"}
	assert set(trie.keys_with_prefix("ap")) == {"app", "apple"}
	assert trie.longest_prefix_of("applepie") == "apple"

	assert trie.delete("app") is True
	assert not trie.contains("app")
	assert trie.contains("apple")
	assert trie.length() == 2

	assert trie.delete("bat") is True
	assert trie.length() == 1

	print("All basic Trie tests passed.")