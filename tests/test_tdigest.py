import sys
import os
import math
from numbers import Number

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engine.TDigest.TDigest import TDigest, Centroid


# Initialization tests
def test_init_with_valid_parameters():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    assert td.alpha == 100
    assert td.neighbors == 10
    assert td.compression_factor == 5
    assert len(td.centroids) == 0
    assert td.total_weight == 0
    assert td.min_value == float("inf")
    assert td.max_value == float("-inf")

def test_init_with_zero_alpha_raises_assertion_error():
    try:
        TDigest(alpha=0, neighbors=10, compression_factor=5)
        assert False, "Should have raised AssertionError"
    except AssertionError:
        pass

def test_init_with_negative_alpha_raises_assertion_error():
    try:
        TDigest(alpha=-1, neighbors=10, compression_factor=5)
        assert False, "Should have raised AssertionError"
    except AssertionError:
        pass

def test_init_with_zero_neighbors_raises_assertion_error():
    try:
        TDigest(alpha=100, neighbors=0, compression_factor=5)
        assert False, "Should have raised AssertionError"
    except AssertionError:
        pass

def test_init_with_float_parameters():
    td = TDigest(alpha=100.5, neighbors=10.7, compression_factor=5.2)
    assert td.alpha == 100.5
    assert td.neighbors == 10.7
    assert td.compression_factor == 5.2

def test_repr():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    assert repr(td) == "TDigest(alpha=100,neighbors=10,compression_factor=5)"

def test_len_empty_digest():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    assert len(td) == 0


# Push tests
def test_push_single_value():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(5.0)
    assert td.total_weight == 1
    assert td.min_value == 5.0
    assert td.max_value == 5.0
    assert len(td.centroids) == 1

def test_push_multiple_values():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(10):
        td.push(i)
    assert td.total_weight == 10
    assert td.min_value == 0
    assert td.max_value == 9

def test_push_with_weight():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(5.0, weight=10)
    assert td.total_weight == 10

def test_push_negative_values():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(-10.0)
    td.push(-5.0)
    assert td.min_value == -10.0
    assert td.max_value == -5.0

def test_push_zero():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(0)
    assert td.total_weight == 1
    assert td.min_value == 0
    assert td.max_value == 0

def test_push_very_large_numbers():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(1e100)
    td.push(1e101)
    assert td.min_value == 1e100
    assert td.max_value == 1e101

def test_push_very_small_numbers():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(1e-100)
    td.push(1e-101)
    assert td.min_value == 1e-101
    assert td.max_value == 1e-100

def test_push_mixed_positive_negative():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(-100)
    td.push(100)
    td.push(0)
    assert td.min_value == -100
    assert td.max_value == 100

def test_push_duplicate_values():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for _ in range(100):
        td.push(5.0)
    assert td.total_weight == 100

def test_push_with_string_raises_assertion_error():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    try:
        td.push("not a number")
        assert False, "Should have raised AssertionError"
    except AssertionError:
        pass

def test_push_with_none_raises_assertion_error():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    try:
        td.push(None)
        assert False, "Should have raised AssertionError"
    except AssertionError:
        pass

def test_push_with_list_raises_assertion_error():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    try:
        td.push([1, 2, 3])
        assert False, "Should have raised AssertionError"
    except AssertionError:
        pass

def test_push_infinity():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(float("inf"))
    assert td.max_value == float("inf")

def test_push_negative_infinity():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(float("-inf"))
    assert td.min_value == float("-inf")

def test_push_nan():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(float("nan"))
    assert math.isnan(td.max_value) or math.isnan(td.min_value)

def test_push_triggers_compression():
    td = TDigest(alpha=10, neighbors=5, compression_factor=2)
    for i in range(1000):
        td.push(i)
    assert len(td.centroids) < 1000

def test_push_with_zero_weight():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(5.0, weight=0)
    assert td.total_weight == 0

def test_push_with_large_weight():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(5.0, weight=1000000)
    assert td.total_weight == 1000000


# Quantile tests
def test_quantile_single_value():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(5.0)
    assert td.quantile(0.5) == 5.0

def test_quantile_min():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    assert td.quantile(0.0) == 0

def test_quantile_max():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    assert td.quantile(1.0) == 99

def test_quantile_median():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(101):
        td.push(i)
    median = td.quantile(0.5)
    assert 45 <= median <= 55

def test_quantile_empty_digest_raises_assertion_error():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    try:
        td.quantile(0.5)
        assert False, "Should have raised AssertionError"
    except AssertionError:
        pass

def test_quantile_negative_q_raises_assertion_error():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(5.0)
    try:
        td.quantile(-0.1)
        assert False, "Should have raised AssertionError"
    except AssertionError:
        pass

def test_quantile_greater_than_one_raises_assertion_error():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(5.0)
    try:
        td.quantile(1.1)
        assert False, "Should have raised AssertionError"
    except AssertionError:
        pass

def test_quantile_multiple_percentiles():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(1000):
        td.push(i)
    q25 = td.quantile(0.25)
    q50 = td.quantile(0.50)
    q75 = td.quantile(0.75)
    assert q25 < q50 < q75

def test_quantile_with_duplicate_values():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for _ in range(100):
        td.push(42)
    assert td.quantile(0.5) == 42

def test_quantile_edge_case_very_small_q():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    result = td.quantile(0.001)
    assert result >= 0

def test_quantile_edge_case_very_large_q():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    result = td.quantile(0.999)
    assert result <= 99


# CDF tests
def test_cdf_empty_digest():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    assert td.cdf(5.0) == 0.0

def test_cdf_single_value():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(5.0)
    assert td.cdf(5.0) == 1.0
    assert td.cdf(4.0) == 0.0
    assert td.cdf(6.0) == 1.0

def test_cdf_below_min():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(10):
        td.push(i)
    assert td.cdf(-1) == 0.0

def test_cdf_above_max():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(10):
        td.push(i)
    assert td.cdf(100) == 1.0

def test_cdf_middle_values():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    cdf_25 = td.cdf(25)
    cdf_50 = td.cdf(50)
    cdf_75 = td.cdf(75)
    assert 0 < cdf_25 < cdf_50 < cdf_75 < 1

def test_cdf_monotonicity():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    prev = 0.0
    for x in range(0, 100, 10):
        current = td.cdf(x)
        assert current >= prev
        prev = current

def test_cdf_with_negative_values():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(-50, 50):
        td.push(i)
    assert 0 <= td.cdf(0) <= 1


# Merge tests
def test_merge_two_empty_digests():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td2 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td1.merge(td2)
    assert td1.total_weight == 0
    assert len(td1.centroids) == 0

def test_merge_empty_into_populated():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td2 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(10):
        td1.push(i)
    initial_weight = td1.total_weight
    td1.merge(td2)
    assert td1.total_weight == initial_weight

def test_merge_populated_into_empty():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td2 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(10):
        td2.push(i)
    td1.merge(td2)
    assert td1.total_weight == 10

def test_merge_two_populated_digests():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td2 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(10):
        td1.push(i)
    for i in range(10, 20):
        td2.push(i)
    td1.merge(td2)
    assert td1.total_weight == 20
    assert td1.min_value == 0
    assert td1.max_value == 19

def test_merge_updates_min_max():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td2 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td1.push(5)
    td1.push(10)
    td2.push(1)
    td2.push(20)
    td1.merge(td2)
    assert td1.min_value == 1
    assert td1.max_value == 20

def test_merge_triggers_compression():
    td1 = TDigest(alpha=10, neighbors=5, compression_factor=2)
    td2 = TDigest(alpha=10, neighbors=5, compression_factor=2)
    for i in range(500):
        td1.push(i)
    for i in range(500, 1000):
        td2.push(i)
    td1.merge(td2)
    assert len(td1.centroids) < 1000

def test_merge_preserves_centroids_in_source():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td2 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(10):
        td2.push(i)
    original_count = len(td2.centroids)
    td1.merge(td2)
    assert len(td2.centroids) == original_count


# Compress tests
def test_compress_empty_digest():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.compress()
    assert len(td.centroids) == 0

def test_compress_single_centroid():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(5.0)
    td.compress()
    assert len(td.centroids) == 1

def test_compress_reduces_centroid_count():
    td = TDigest(alpha=10, neighbors=5, compression_factor=1)
    for i in range(1000):
        td.push(i)
    initial_count = len(td.centroids)
    td.compress()
    assert len(td.centroids) <= initial_count

def test_compress_preserves_total_weight():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    initial_weight = td.total_weight
    td.compress()
    assert td.total_weight == initial_weight

def test_compress_multiple_times_idempotent():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    td.compress()
    count_after_first = len(td.centroids)
    td.compress()
    count_after_second = len(td.centroids)
    assert count_after_first == count_after_second


# Convenience methods tests
def test_median():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(101):
        td.push(i)
    median = td.median()
    assert 45 <= median <= 55

def test_p95():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    p95 = td.p95()
    assert 90 <= p95 <= 99

def test_p99():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    p99 = td.p99()
    assert 95 <= p99 <= 99

def test_min_method():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(10):
        td.push(i)
    assert td.min() == 0

def test_max_method():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(10):
        td.push(i)
    assert td.max() == 9

def test_rank():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    rank = td.rank(50)
    assert 0 <= rank <= 100


# Summary tests
def test_summary_empty_digest():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    summary = td.summary()
    assert summary["count"] == 0
    assert summary["min"] is None
    assert summary["max"] is None
    assert summary["mean"] is None

def test_summary_populated_digest():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    summary = td.summary()
    assert summary["count"] == 100
    assert summary["min"] == 0
    assert summary["max"] == 99
    assert 40 <= summary["mean"] <= 60

def test_summary_single_value():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(42)
    summary = td.summary()
    assert summary["count"] == 1
    assert summary["min"] == 42
    assert summary["max"] == 42
    assert summary["mean"] == 42

def test_summary_percentiles_ordered():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(1000):
        td.push(i)
    summary = td.summary()
    assert summary["p50"] < summary["p90"] < summary["p95"] < summary["p99"]


# Large scale tests
def test_push_large_dataset():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100000):
        td.push(i)
    assert td.total_weight == 100000
    assert len(td.centroids) < 10000

def test_quantile_accuracy_large_dataset():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100000):
        td.push(i)
    median = td.quantile(0.5)
    assert 45000 <= median <= 55000

def test_merge_large_digests():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td2 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(50000):
        td1.push(i)
    for i in range(50000, 100000):
        td2.push(i)
    td1.merge(td2)
    assert td1.total_weight == 100000

def test_large_weights():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(1, weight=1000000)
    td.push(2, weight=1000000)
    assert td.total_weight == 2000000

def test_many_duplicate_values():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for _ in range(10000):
        td.push(42)
    assert td.quantile(0.5) == 42
    assert td.total_weight == 10000


# Edge cases and boundaries tests
def test_alternating_min_max_values():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        if i % 2 == 0:
            td.push(0)
        else:
            td.push(100)
    assert td.min_value == 0
    assert td.max_value == 100

def test_monotonically_increasing_values():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(1000):
        td.push(i)
    assert td.min_value == 0
    assert td.max_value == 999

def test_monotonically_decreasing_values():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(1000, 0, -1):
        td.push(i)
    assert td.min_value == 1
    assert td.max_value == 1000

def test_random_order_values():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    import random
    values = list(range(1000))
    random.shuffle(values)
    for v in values:
        td.push(v)
    assert td.total_weight == 1000

def test_bimodal_distribution():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for _ in range(500):
        td.push(10)
    for _ in range(500):
        td.push(90)
    assert td.total_weight == 1000
    assert td.min_value == 10
    assert td.max_value == 90

def test_uniform_distribution():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(1000):
        td.push(i)
    median = td.quantile(0.5)
    assert 400 <= median <= 600

def test_gaussian_like_distribution():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    import random
    for _ in range(10000):
        td.push(random.gauss(50, 10))
    median = td.quantile(0.5)
    assert 40 <= median <= 60

def test_extreme_outliers():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    td.push(1e10)
    assert td.max_value == 1e10

def test_very_small_alpha():
    td = TDigest(alpha=1, neighbors=1, compression_factor=1)
    for i in range(100):
        td.push(i)
    assert td.total_weight == 100

def test_very_large_alpha():
    td = TDigest(alpha=10000, neighbors=100, compression_factor=100)
    for i in range(100):
        td.push(i)
    assert td.total_weight == 100


# State consistency tests
def test_quantile_cdf_inverse_relationship():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    q = 0.5
    value = td.quantile(q)
    cdf_value = td.cdf(value)
    assert abs(cdf_value - q) < 0.2

def test_multiple_compress_idempotent():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    td.compress()
    median1 = td.quantile(0.5)
    td.compress()
    median2 = td.quantile(0.5)
    assert abs(median1 - median2) < 1.0

def test_push_after_compress_maintains_consistency():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    td.compress()
    initial_weight = td.total_weight
    td.push(200)
    assert td.total_weight == initial_weight + 1

def test_merge_does_not_modify_source():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td2 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(10):
        td2.push(i)
    original_weight = td2.total_weight
    original_centroids = len(td2.centroids)
    td1.merge(td2)
    assert td2.total_weight == original_weight
    assert len(td2.centroids) == original_centroids

def test_sequential_pushes_vs_weighted_push():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td2 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    
    for _ in range(10):
        td1.push(5.0)
    
    td2.push(5.0, weight=10)
    
    assert td1.total_weight == td2.total_weight


# Type validation tests
def test_cdf_with_string_raises_error():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td.push(5)
    try:
        td.cdf("invalid")
        assert False, "Should have raised error"
    except (TypeError, AttributeError):
        pass

def test_merge_with_non_tdigest_raises_error():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    try:
        td.merge("not a digest")
        assert False, "Should have raised error"
    except AttributeError:
        pass


# Corner cases tests
def test_quantile_after_merge():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td2 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(50):
        td1.push(i)
    for i in range(50, 100):
        td2.push(i)
    td1.merge(td2)
    median = td1.quantile(0.5)
    assert 40 <= median <= 60

def test_cdf_after_merge():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td2 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(50):
        td1.push(i)
    for i in range(50, 100):
        td2.push(i)
    td1.merge(td2)
    cdf_50 = td1.cdf(50)
    assert 0.4 <= cdf_50 <= 0.6

def test_multiple_merges():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(10):
        td_temp = TDigest(alpha=100, neighbors=10, compression_factor=5)
        for j in range(10):
            td_temp.push(i * 10 + j)
        td1.merge(td_temp)
    assert td1.total_weight == 100

def test_centroid_dataclass_attributes():
    c = Centroid(mean=5.0, weight=10)
    assert c.mean == 5.0
    assert c.weight == 10

def test_centroid_modification():
    c = Centroid(mean=5.0, weight=10)
    c.mean = 10.0
    c.weight = 20
    assert c.mean == 10.0
    assert c.weight == 20

def test_rank_consistency():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(100):
        td.push(i)
    rank_50 = td.rank(50)
    cdf_50 = td.cdf(50)
    assert abs(rank_50 - cdf_50 * 100) < 10

def test_float_vs_int_values():
    td1 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    td2 = TDigest(alpha=100, neighbors=10, compression_factor=5)
    
    for i in range(100):
        td1.push(float(i))
        td2.push(i)
    
    assert abs(td1.quantile(0.5) - td2.quantile(0.5)) < 1.0

def test_summary_with_negative_values():
    td = TDigest(alpha=100, neighbors=10, compression_factor=5)
    for i in range(-50, 50):
        td.push(i)
    summary = td.summary()
    assert summary["min"] == -50
    assert summary["max"] == 49
    assert -5 <= summary["mean"] <= 5
