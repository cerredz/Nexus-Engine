import sys
sys.path.append("engine")
from merkle_tree.merkle_tree import MerkleTree
from merkle_tree.DiffResult import DiffResult

def test_merkle_tree():
    # Empty
    mt = MerkleTree([])
    assert mt.get_root() is None
    assert mt.size == 0

    # Single
    mt = MerkleTree(["a"])
    assert mt.size == 1
    assert mt.get_leaf(0) == "a"
    assert mt.height() == 1

    # Multiple
    data = ["a", "b", "c"]
    mt = MerkleTree(data)
    assert mt.size == 3
    assert mt.height() == 3  # levels: root, 2 internals (with dup c), 4 leaves (a,b,c,c)

    # set_leaf
    mt.set_leaf(1, "d")
    assert mt.get_leaf(1) == "d"

    # append
    new_leaf = mt.append_leaf("e")
    assert mt.size == 4
    assert new_leaf.content == "e"

    # equals
    mt2 = MerkleTree(["a","d","c","e"])
    assert mt.equals(mt2)

    # diff
    mt3 = MerkleTree(["a","d","f","e"])
    diff = mt.diff(mt3, 10)
    assert diff.first_difference == (2, 0)  # adjust based on actual
    assert 2 in diff.leaf_differing_indices

    print("All MerkleTree tests passed")

if __name__ == "__main__":
    test_merkle_tree()
