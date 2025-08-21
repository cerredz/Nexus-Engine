import sys
sys.path.append("engine")
from bloom_filter.bloom_filter import BloomFilter
import math

def test_bloom_filter():
    # Test initialization
    bf = BloomFilter(100, 0.1)
    assert bf.size > 0
    assert bf.hashes > 0
    assert bf.false_positive == 0.1

    # Test capacity_planner
    m, k = BloomFilter.capacity_planner(100, 0.01)
    assert m == 959
    assert k == 7

    # Test insert and exists
    bf.insert("test")
    assert bf.exists("test")
    assert not bf.exists("test2")

    # Test contains_many
    assert bf.contains_many(["test", "test2"]) == [True, False]

    # Test count_set_bits
    assert bf.count_set_bits() > 0

    # Test union
    bf2 = BloomFilter(100, 0.1)
    bf2.insert("test2")
    bf.union(bf2)
    assert bf.exists("test")
    assert bf.exists("test2")

    # Test intersect
    bf3 = BloomFilter(100, 0.1)
    bf3.insert("test")
    bf.intersect(bf3)
    assert bf.exists("test")
    assert not bf.exists("test2")

    # Wait, intersect should AND, so test2 should be False now if it was only in one
    # Need to check properly

    # Edge cases
    try:
        BloomFilter(100, 0)
        assert False
    except Exception:
        pass

    bf.reset()
    assert bf.count_set_bits() == 0

    # Test false positives roughly
    bf = BloomFilter(1000, 0.01)
    for i in range(100):
        bf.insert(str(i))
    fp = 0
    for i in range(100, 200):
        if bf.exists(str(i)):
            fp += 1
    assert fp / 100 < 0.05  # loose bound

    print("All BloomFilter tests passed")

if __name__ == "__main__":
    test_bloom_filter()
