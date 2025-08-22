import sys
sys.path.append("engine")  # Adjust based on actual structure
from rate_limiting.FixedWindowCounter import FixedWindowCounter
import pytest  # If available, but since others don't use it, maybe not

def _stub_time(limiter, now_box):
    """Stub the private __get_time method for deterministic testing."""
    limiter.__get_time = lambda: now_box[0]

def test_initialization_valid():
    fwc = FixedWindowCounter('second', 5)
    assert fwc.unit == 'second'
    assert fwc.limit == 5
    assert fwc.allowed == 0
    assert fwc.denied == 0
    assert fwc.map == {}
    assert fwc.blacklist == set()
    assert fwc.user_metrics == {}

def test_initialization_invalid_unit():
    try:
        FixedWindowCounter('invalid', 5)
        assert False, "Should raise ValueError for invalid unit"
    except ValueError as e:
        assert "Invalid rate limiter unit" in str(e)

def test_initialization_invalid_limit():
    try:
        FixedWindowCounter('second', 0)
        assert False, "Should raise ValueError for non-positive limit"
    except ValueError as e:
        assert "Invalid rate limit value" in str(e)

def test_allow_under_limit():
    fwc = FixedWindowCounter('second', 3)
    now = [0]
    _stub_time(fwc, now)

    assert fwc.allow('key1')
    assert fwc.allow('key1')
    assert fwc.allow('key1')
    assert fwc.map['key1'] == 3
    assert fwc.allowed == 3
    assert fwc.denied == 0
    assert fwc.user_metrics['key1'] == [3, 0]

def test_allow_over_limit():
    fwc = FixedWindowCounter('second', 2)
    now = [0]
    _stub_time(fwc, now)

    assert fwc.allow('key1')
    assert fwc.allow('key1')
    assert not fwc.allow('key1')
    assert fwc.map['key1'] == 2
    assert fwc.allowed == 2
    assert fwc.denied == 1
    assert fwc.user_metrics['key1'] == [2, 1]

def test_window_reset():
    fwc = FixedWindowCounter('second', 2)
    now = [0]
    _stub_time(fwc, now)

    assert fwc.allow('key1')
    assert fwc.allow('key1')
    assert not fwc.allow('key1')  # over limit

    now[0] = 1001  # advance past window
    assert fwc.allow('key1')  # should reset
    assert fwc.map['key1'] == 1  # new window count
    assert fwc.allowed == 3
    assert fwc.denied == 1

def test_multiple_keys():
    fwc = FixedWindowCounter('second', 2)
    now = [0]
    _stub_time(fwc, now)

    assert fwc.allow('key1')
    assert fwc.allow('key1')
    assert not fwc.allow('key1')  # key1 over

    assert fwc.allow('key2')
    assert fwc.allow('key2')
    assert not fwc.allow('key2')  # key2 over

    assert fwc.map['key1'] == 2
    assert fwc.map['key2'] == 2
    assert fwc.allowed == 4
    assert fwc.denied == 2

def test_blacklist():
    fwc = FixedWindowCounter('second', 2)
    now = [0]
    _stub_time(fwc, now)

    fwc.add_to_blacklist('badkey')
    assert not fwc.allow('badkey')
    assert fwc.denied == 1
    assert fwc.allowed == 0
    assert 'badkey' in fwc.user_metrics and fwc.user_metrics['badkey'] == [0, 1]

    fwc.remove_from_blacklist('badkey')
    assert fwc.allow('badkey')
    assert fwc.allowed == 1
    assert fwc.map['badkey'] == 1

def test_remaining():
    fwc = FixedWindowCounter('second', 3)
    now = [0]
    _stub_time(fwc, now)

    assert fwc.remaining('key1') == 3  # new key
    assert fwc.allow('key1')
    assert fwc.remaining('key1') == 2
    assert fwc.allow('key1')
    assert fwc.remaining('key1') == 1

    # After window reset
    now[0] = 1001
    assert fwc.remaining('key1') == 3

def test_bad_actors():
    fwc = FixedWindowCounter('second', 1)
    now = [0]
    _stub_time(fwc, now)

    assert fwc.allow('good')

    actors = fwc.bad_actors()
    assert 'good' not in actors

def test_unit_conversions():
    # Test minute
    fwc_min = FixedWindowCounter('minute', 1)
    now = [0]
    _stub_time(fwc_min, now)
    assert fwc_min.allow('k')
    now[0] = 59999
    assert not fwc_min.allow('k')  # still within minute
    now[0] = 60001
    assert fwc_min.allow('k')  # new window

    # Similar for hour and day (abbreviated)
    fwc_hour = FixedWindowCounter('hour', 1)
    now[0] = 0
    _stub_time(fwc_hour, now)
    assert fwc_hour.allow('k')
    now[0] = 3600001
    assert fwc_hour.allow('k')

    fwc_day = FixedWindowCounter('day', 1)
    now[0] = 0
    _stub_time(fwc_day, now)
    assert fwc_day.allow('k')
    now[0] = 86400001
    assert fwc_day.allow('k')

def test_edge_cases():
    fwc = FixedWindowCounter('second', 1)
    now = [0]
    _stub_time(fwc, now)


    # Blacklist remove non-existent
    fwc.remove_from_blacklist('nonexistent')  # no error

    # allow after remove from blacklist
    fwc.add_to_blacklist('temp')
    assert not fwc.allow('temp')
    fwc.remove_from_blacklist('temp')
    assert fwc.allow('temp')

    # Metrics for unseen key
    assert fwc.remaining('unseen') == 1
    assert 'unseen' not in fwc.user_metrics

def run_all_tests():
    test_initialization_valid()
    test_initialization_invalid_unit()
    test_initialization_invalid_limit()
    test_allow_under_limit()
    test_allow_over_limit()
    test_window_reset()
    test_multiple_keys()
    test_blacklist()
    test_remaining()
    test_bad_actors()
    test_unit_conversions()
    test_edge_cases()
    print("All FixedWindowCounter tests passed!")

if __name__ == "__main__":
    run_all_tests()
