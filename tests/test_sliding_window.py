import sys
sys.path.append("engine")
from rate_limiting.SlidingWindow import SlidingWindow

def _stub_time(limiter: SlidingWindow, now_box):
    # Replace the private time getter with a deterministic stub.
    limiter._SlidingWindow__get_time = lambda: now_box[0]

def test_allow_under_limit_and_denies_at_limit():
    sw = SlidingWindow('second', 1, 3)
    now = [0]
    _stub_time(sw, now)

    assert sw.allow('k')
    assert sw.allow('k')
    assert sw.allow('k')
    assert sw.get_allowed() == 3
    assert sw.get_denied() == 0
    assert not sw.allow('k')  # 4th within same window
    assert sw.get_allowed() == 3
    assert sw.get_denied() == 1
    assert sw.user_metrics['k'] == [3, 1]


def test_prune_after_window_and_allow():
    sw = SlidingWindow('second', 1, 3)
    now = [0]
    _stub_time(sw, now)

    assert sw.allow('k2')
    assert sw.allow('k2')
    assert sw.allow('k2')
    assert not sw.allow('k2')  # at capacity

    # Advance beyond 1 second window (inclusive boundary at start)
    now[0] = 1001
    assert sw.allow('k2')  # old timestamps pruned, room available
    # After pruning and append, only the new timestamp should remain within window
    assert len(sw.map['k2']) == 1


def test_inclusive_boundary_at_start():
    # Exactly-at-boundary timestamp should still count (inclusive start)
    sw = SlidingWindow('second', 1, 1)
    now = [1000]
    _stub_time(sw, now)

    assert sw.allow('kb')  # record at t=1000
    now[0] = 2000  # start_window = 1000
    assert not sw.allow('kb')  # timestamp at 1000 is included → still at capacity


def test_blacklist_denies_without_metric_increment():
    sw = SlidingWindow('second', 1, 1)
    now = [0]
    _stub_time(sw, now)

    allowed_before = sw.get_allowed()
    denied_before = sw.get_denied()
    sw.add_to_blacklist('blk')
    assert not sw.allow('blk')
    # Implementation denies early without touching metrics or per-key maps
    assert sw.get_allowed() == allowed_before
    assert sw.get_denied() == denied_before
    assert 'blk' not in sw.user_metrics


def test_remaining_readonly_and_counts():
    sw = SlidingWindow('second', 1, 3)
    now = [0]
    _stub_time(sw, now)

    # New key returns full limit
    assert sw.remaining('r') == 3

    # Consume 2 within window
    assert sw.allow('r')
    assert sw.allow('r')
    before_len = len(sw.map['r'])
    # remaining should not mutate state
    assert sw.remaining('r') == 1
    assert len(sw.map['r']) == before_len

    # Blacklisted key not in map still returns limit (function doesn't consider blacklist)
    sw.add_to_blacklist('x')
    assert sw.remaining('x') == 3


def test_user_metrics_tracking():
    sw = SlidingWindow('second', 1, 2)
    now = [0]
    _stub_time(sw, now)

    assert sw.allow('m')
    assert sw.allow('m')
    assert not sw.allow('m')  # over limit within same window
    assert sw.user_metrics['m'] == [2, 1]
    assert sw.get_allowed() == 2
    assert sw.get_denied() == 1


def test_get_bad_actors():
    sw = SlidingWindow('second', 1, 1)
    now = [0]
    _stub_time(sw, now)

    # good: 1 allow, 0 deny
    assert sw.allow('good')

    # bad: 1 allow, 2 denies
    assert sw.allow('bad')
    assert not sw.allow('bad')
    assert not sw.allow('bad')

    bads = set(sw.bad_actors())
    assert 'bad' in bads
    assert 'good' not in bads


def test_unit_conversions_minute_hour_day():
    # minute
    sw_min = SlidingWindow('minute', 1, 1)
    now = [0]
    _stub_time(sw_min, now)
    assert sw_min.allow('k')
    now[0] = 60001  # 1 minute + 1 ms later
    assert sw_min.allow('k')

    # hour
    sw_hr = SlidingWindow('hour', 1, 1)
    now2 = [0]
    _stub_time(sw_hr, now2)
    assert sw_hr.allow('k')
    now2[0] = 3600001  # 1 hour + 1 ms
    assert sw_hr.allow('k')

    # day
    sw_day = SlidingWindow('day', 1, 1)
    now3 = [0]
    _stub_time(sw_day, now3)
    assert sw_day.allow('k')
    now3[0] = 86400001  # 1 day + 1 ms
    assert sw_day.allow('k')


