
class Node():
    def __init__(self, char, is_end:bool = False):
        self.char = char
        self.children: dict = {}
        self.is_end = is_end

class Trie():
    def __init__(self):
        self.root = Node(None)
        self.size = 0
    
    def length(self):
        return self.size
    
    def is_empty(self):
        return self.size == 0
    
    def clear(self):
        self.root = Node(None)
        self.size = 0
    
    def insert(self, key: str):
        idx = 0
        curr = self.root

        if key == "":
            raise ValueError("Cannot insert empty string into trie.")

        while idx < len(key):
            curr_char = key[idx]
            if curr_char not in curr.children:
                is_end = True if idx == len(key) - 1 else False
                curr.children[curr_char] = Node(key[idx], is_end)
            
            curr = curr.children[curr_char]
            idx += 1

        is_new = curr.is_end
        if is_new:
            self.size += 1

        curr.is_end = True
        return is_new # returns if we added new word to trie or if it already existed

    def contains(self, key):
        curr = self.root

        if key == "":
            return False

        for ch in key:
            if not ch in curr.children:
                return False
            
            curr = curr.children[ch]

        return curr.is_end

    def starts_with(self, prefix):
        if prefix == "":
            return False
        
        curr = self.root
        
        for ch in prefix:
            if not ch in curr.children:
                return False
            
            curr = curr.children[ch]
        
        return True

    def keys_with_prefix(self, prefix, limit=None):
        res: list[str] = []
        curr = self.root

        if limit is not None and limit <= 0:
            return []

        # traverse to last char in prefix
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
    
    def update(self, keys: list[str]):
        keys_added = 0
        for key in keys:
            new_key = self.insert(key)
            if new_key:
                keys_added += 1

        return keys_added
    
    def delete(self, key):
        if key == "":
            return False
        
        curr = self.root
        stack = [] # parent, char, node
        for ch in key:
            stack.append((curr, ch, curr.children[ch]))
            curr = curr.children[ch]

        curr.is_end = False
        self.size -= 1

        while stack:
            parent, ch, curr = stack.pop()
            if curr.children or curr.is_end:
                break

            del parent.children[ch]

        return True

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
        print(trie.keys())

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