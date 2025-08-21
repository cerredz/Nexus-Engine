import sys
sys.path.append("engine")
from skip_list.skip_list import SkipList

def test_skip_list():
    sl = SkipList()
    assert sl.get_height() == 1
    assert sl.get_size() == 0

    # Insert
    sl.insert(5)
    assert sl.contains(5)
    assert sl.get_size() == 1

    sl.insert_many([3,7,1,5])  # 5 duplicate
    assert sl.get_size() == 4
    assert sl.contains(1)
    assert sl.contains(3)

    # Search
    node = sl.search(3)
    assert node.data == 3

    # Delete
    sl.delete(3)
    assert not sl.contains(3)
    assert sl.get_size() == 3

    # Ceiling
    ceil = sl.ceiling(2)
    assert 5 in ceil
    assert 7 in ceil

    # Merge
    sl2 = SkipList()
    sl2.insert_many([2,4])
    sl.merge(sl2)
    assert sl.contains(2)
    assert sl.get_size() == 5

    # Clear
    sl.clear()
    assert sl.get_size() == 0

    print("All SkipList tests passed")

if __name__ == "__main__":
    test_skip_list()
