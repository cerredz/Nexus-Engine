import sys
sys.path.append("engine")
from HyperLogLog.HyperLogLog import HyperLogLog, run_tests, _assert_within, _rse
import math
import random

def extended_tests():
    run_tests()  # run existing

    # Test empty
    h = HyperLogLog(4)
    assert h.estimate() == 0

    # Test single
    h.insert("test")
    est = h.estimate()
    assert abs(est - 1) < 1

    # Test large N
    N = 100000
    h = HyperLogLog(14)
    for i in range(N):
        h.insert(str(i))
    est = h.estimate()
    tol = 2 * _rse(h.m)
    _assert_within(est, N, tol, "large_n_100k_b14")

    # Test different b
    h = HyperLogLog(1)
    h.insert("a")
    h.insert("b")
    assert h.estimate() > 0

    # Test update_many with duplicates
    h = HyperLogLog(10)
    h.update_many(["a"] * 1000 + ["b"] * 1000)
    est = h.estimate()
    assert abs(est - 2) / 2 < 0.5

    print("All extended HyperLogLog tests passed")

def test_hyperloglog():
    extended_tests()

if __name__ == "__main__":
    test_hyperloglog()
