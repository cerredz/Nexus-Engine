import datetime
import math
import pytest

from engine.CircuitBreaker.CircuitBreaker import CircuitBreaker


def success_func(value=42):
    return value


def failing_func(exc=ValueError("boom")):
    def _raise(*args, **kwargs):
        raise exc
    return _raise


def now_ts():
    return int(datetime.datetime.now().timestamp())


def test_init_validation_failure_rate_bounds():
    with pytest.raises(ValueError):
        CircuitBreaker("key", 0, 10, 1)
    with pytest.raises(ValueError):
        CircuitBreaker("key", 1.01, 10, 1)


def test_init_validation_duration_and_half_calls():
    with pytest.raises(ValueError):
        CircuitBreaker("key", 0.5, 0, 1)
    with pytest.raises(ValueError):
        CircuitBreaker("key", 0.5, -1, 1)
    with pytest.raises(ValueError):
        CircuitBreaker("key", 0.5, 5, 0)


def test_closed_success_increments_success_and_returns_result():
    cb = CircuitBreaker("key", failure_rate=0.5, duration_until_half_opened=5, open_half_calls=2)
    ok, result, err = cb.run(success_func, 123)
    assert ok is True
    assert result == 123
    assert err is None
    assert cb.is_closed
    assert cb.success == 1 and cb.error == 0


def test_closed_failure_increments_error_and_returns_exception():
    cb = CircuitBreaker("key", failure_rate=0.5, duration_until_half_opened=5, open_half_calls=2)
    ok, result, err = cb.run(failing_func())
    assert ok is False
    assert result is None
    assert isinstance(err, Exception)
    assert cb.is_closed
    assert cb.success == 0 and cb.error == 1


def test_closed_opens_when_failure_rate_reaches_threshold():
    cb = CircuitBreaker("key", failure_rate=0.5, duration_until_half_opened=5, open_half_calls=2)
    # 1 success, 1 failure -> failure rate = 0.5 (>= threshold)
    cb.run(success_func)
    cb.run(failing_func())

    ok, result, err = cb.run(success_func)
    assert ok is False
    assert result is None
    assert isinstance(err, RuntimeError)
    assert cb.is_open
    assert cb.time_opened is not None
    # Counters unchanged by the rejection
    assert cb.success == 1 and cb.error == 1


def test_open_rejects_until_cooldown_then_allows_half_open_probe_success():
    cb = CircuitBreaker("key", failure_rate=0.5, duration_until_half_opened=3, open_half_calls=2)
    # Force open now
    cb.state = CircuitBreaker.OPEN_STATE
    cb.time_opened = now_ts()

    # Before cooldown -> reject
    ok, result, err = cb.run(success_func)
    assert ok is False and result is None and isinstance(err, RuntimeError)
    assert cb.is_open

    # After cooldown -> transition to half-open and execute once
    cb.time_opened = now_ts() - cb.duration_until_half_opened - 1
    ok, result, err = cb.run(success_func, 7)
    assert ok is True and result == 7 and err is None
    assert cb.is_half_open
    assert cb.half_open_success == 1 and cb.half_open_error == 0


def test_half_open_transitions_back_to_open_after_exceeding_probe_limit_with_high_failure_rate():
    cb = CircuitBreaker("key", failure_rate=0.25, duration_until_half_opened=1, open_half_calls=2)
    # Enter half-open by opening long enough ago
    cb.state = CircuitBreaker.OPEN_STATE
    cb.time_opened = now_ts() - cb.duration_until_half_opened - 1

    # First half-open attempt fails
    ok, result, err = cb.run(failing_func(RuntimeError("fail1")))
    assert ok is False and isinstance(err, RuntimeError)
    assert cb.is_half_open
    assert cb.half_open_success == 0 and cb.half_open_error == 1

    # Second half-open attempt fails
    ok, result, err = cb.run(failing_func(RuntimeError("fail2")))
    assert ok is False and isinstance(err, RuntimeError)
    assert cb.half_open_success == 0 and cb.half_open_error == 2
    assert cb.is_half_open

    # Third call triggers evaluation branch (>= open_half_calls) and re-opens without executing target
    ok, result, err = cb.run(success_func)
    assert ok is False and isinstance(err, RuntimeError)
    assert cb.is_open


def test_half_open_transitions_to_closed_on_good_rate_and_executes_closed_call():
    cb = CircuitBreaker("key", failure_rate=0.6, duration_until_half_opened=1, open_half_calls=2)
    cb.state = CircuitBreaker.OPEN_STATE
    cb.time_opened = now_ts() - cb.duration_until_half_opened - 1

    # Two successful half-open probes
    ok, result, err = cb.run(success_func, 1)
    assert ok is True and result == 1
    ok, result, err = cb.run(success_func, 2)
    assert ok is True and result == 2
    assert cb.half_open_success == 2 and cb.half_open_error == 0

    # Next call evaluates probes: since failure_rate=0 <= threshold 0.6, breaker closes and executes in closed state
    ok, result, err = cb.run(success_func, 3)
    assert ok is True and result == 3 and err is None
    assert cb.is_closed
    assert cb.success == 1 and cb.error == 0


def test_time_until_half_open():
    cb = CircuitBreaker("key", failure_rate=0.5, duration_until_half_opened=10, open_half_calls=1)
    assert cb.time_until_half_open() == 0  # closed
    cb.state = CircuitBreaker.OPEN_STATE
    cb.time_opened = now_ts() - 8
    remaining = cb.time_until_half_open()
    assert remaining in (2, 1, 0)  # allow for runtime jitter within a second or two


def test_metrics_property_reflects_counts_and_rate():
    cb = CircuitBreaker("key", failure_rate=0.5, duration_until_half_opened=1, open_half_calls=1)
    cb.run(success_func)
    cb.run(success_func)
    cb.run(failing_func())
    m = cb.metrics
    assert m["state"] == CircuitBreaker.CLOSED_STATE
    assert m["success"] == 2 and m["error"] == 1
    assert math.isclose(m["failure_rate"], 1/3, rel_tol=0, abs_tol=1e-4)


def test_reset_clears_state_and_counters():
    cb = CircuitBreaker("key", failure_rate=0.5, duration_until_half_opened=1, open_half_calls=2)
    cb.run(success_func)
    cb.run(failing_func())
    # Open it
    cb.state = CircuitBreaker.OPEN_STATE
    cb.time_opened = now_ts()
    # Add some half-open counters too
    cb.half_open_success, cb.half_open_error = 3, 4

    cb.reset()
    assert cb.is_closed
    assert cb.time_opened is None
    assert cb.success == 0 and cb.error == 0
    assert cb.half_open_success == 0 and cb.half_open_error == 0


def test_state_properties_flags():
    cb = CircuitBreaker("key", failure_rate=0.5, duration_until_half_opened=1, open_half_calls=1)
    assert cb.is_closed and not cb.is_open and not cb.is_half_open
    cb.state = CircuitBreaker.OPEN_STATE
    assert cb.is_open and not cb.is_closed and not cb.is_half_open
    cb.state = CircuitBreaker.HALF_OPEN_STATE
    assert cb.is_half_open and not cb.is_open and not cb.is_closed


def test_wrap_decorator_success_and_fallback_on_error_and_open():
    cb = CircuitBreaker("key", failure_rate=0.5, duration_until_half_opened=100, open_half_calls=1)

    @cb.wrap()
    def plus_one(x):
        return x + 1

    assert plus_one(3) == 4

    # Wrap with fallback to handle function errors
    def fallback(err, *args, **kwargs):
        return "fallback"

    cb2 = CircuitBreaker("key2", failure_rate=0.5, duration_until_half_opened=100, open_half_calls=1)

    @cb2.wrap(fallback=fallback)
    def will_fail():
        raise RuntimeError("nope")

    assert will_fail() == "fallback"

    # Fallback on open-state rejection
    cb3 = CircuitBreaker("key3", failure_rate=0.5, duration_until_half_opened=1000, open_half_calls=1)
    cb3.state = CircuitBreaker.OPEN_STATE
    cb3.time_opened = now_ts()

    @cb3.wrap(fallback=fallback)
    def any_func():
        return 99

    assert any_func() == "fallback"


