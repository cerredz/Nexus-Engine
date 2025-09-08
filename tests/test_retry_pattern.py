import time
import pytest
from unittest.mock import patch

from engine.retry.retry import retry


def test_retry_success_no_retries():
    call_counter = {"count": 0}

    @retry(max_retries=3, delay=0)
    def echo(value):
        call_counter["count"] += 1
        return value

    result = echo("ok")
    assert result == "ok"
    assert call_counter["count"] == 1


def test_retry_eventual_success_after_failures():
    sleep_calls = []

    def fake_sleep(d):
        sleep_calls.append(d)

    state = {"n": 0}

    @retry(max_retries=5, delay=0.0)
    def flaky():
        state["n"] += 1
        if state["n"] <= 2:
            raise ValueError("fail")
        return "ok"

    with patch("time.sleep", side_effect=fake_sleep):
        assert flaky() == "ok"
    assert state["n"] == 3
    assert len(sleep_calls) == 2


def test_retry_exhaustion_raises_and_attempts_count():
    sleep_calls = []

    def fake_sleep(d):
        sleep_calls.append(d)

    call_counter = {"count": 0}

    @retry(max_retries=2, delay=0.0)
    def always_fail():
        call_counter["count"] += 1
        raise ValueError("nope")

    with patch("time.sleep", side_effect=fake_sleep):
        with pytest.raises(ValueError):
            always_fail()

    # attempts = max_retries + 1
    assert call_counter["count"] == 3
    assert len(sleep_calls) == 2


def test_exceptions_filter_only_retries_on_specified_exceptions():
    sleep_calls = []

    def fake_sleep(d):
        sleep_calls.append(d)

    call_counter = {"count": 0}

    @retry(max_retries=3, delay=0.0, exceptions=(ValueError,))
    def raises_key_error():
        call_counter["count"] += 1
        raise KeyError("boom")

    with patch("time.sleep", side_effect=fake_sleep):
        with pytest.raises(KeyError):
            raises_key_error()

    # Should not retry because KeyError not in exceptions
    assert call_counter["count"] == 1
    assert len(sleep_calls) == 0


def test_exceptions_filter_multiple_and_subclass():
    sleep_calls = []

    def fake_sleep(d):
        sleep_calls.append(d)

    call_counter = {"count": 0}

    @retry(max_retries=3, delay=0.0, exceptions=(ValueError, KeyError))
    def raises_key_then_ok():
        call_counter["count"] += 1
        if call_counter["count"] <= 2:
            raise KeyError("retry me")
        return "ok"

    with patch("time.sleep", side_effect=fake_sleep):
        assert raises_key_then_ok() == "ok"
    assert call_counter["count"] == 3
    assert len(sleep_calls) == 2

    # Subclass retry: IndexError is a LookupError
    sleep_calls.clear()
    subclass_calls = {"count": 0}

    @retry(max_retries=1, delay=0.0, exceptions=(LookupError,))
    def raises_index_then_ok():
        subclass_calls["count"] += 1
        if subclass_calls["count"] == 1:
            raise IndexError("retry subclass")
        return "ok"

    with patch("time.sleep", side_effect=fake_sleep):
        assert raises_index_then_ok() == "ok"
    assert subclass_calls["count"] == 2
    assert len(sleep_calls) == 1


def test_decorator_without_parentheses_supported():
    sleep_calls = []

    def fake_sleep(d):
        sleep_calls.append(d)

    call_counter = {"count": 0}

    @retry
    def always_fail():
        call_counter["count"] += 1
        raise RuntimeError("boom")

    with patch("time.sleep", side_effect=fake_sleep):
        with pytest.raises(RuntimeError):
            always_fail()

    # default max_retries=3 -> attempts=4 -> 3 sleeps
    assert call_counter["count"] == 4
    assert len(sleep_calls) == 3


def test_exponential_backoff_sequence_doubles_delays():
    sleep_calls = []

    def fake_sleep(d):
        sleep_calls.append(d)

    state = {"n": 0}

    @retry(max_retries=3, delay=0.01, exponential_backoff=True)
    def fail_three_then_ok():
        state["n"] += 1
        if state["n"] <= 3:
            raise ValueError("fail")
        return "ok"

    with patch("time.sleep", side_effect=fake_sleep):
        assert fail_three_then_ok() == "ok"
    assert state["n"] == 4
    assert sleep_calls == [0.01, 0.02, 0.04]


def test_wraps_preserves_name_and_doc():
    def original():
        """This is the original docstring."""
        return 1

    wrapped = retry(max_retries=0)(original)
    assert wrapped.__name__ == original.__name__
    assert wrapped.__doc__ == original.__doc__
    assert wrapped() == 1


def test_zero_max_retries_behavior():
    calls = {"count": 0}

    @retry(max_retries=0, delay=0.0)
    def fail_once():
        calls["count"] += 1
        raise ValueError("fail once")

    with pytest.raises(ValueError):
        fail_once()
    assert calls["count"] == 1

    @retry(max_retries=0)
    def succeed():
        return 42

    assert succeed() == 42


def test_invalid_max_retries_type_raises_typeerror_on_call():
    # Type error occurs when attempting to compute range(max_retries + 1)
    @retry(max_retries="3")
    def f():
        return "x"

    with pytest.raises(TypeError):
        f()


def test_invalid_delay_type_causes_typeerror_when_sleeping():
    # time.sleep expects a real number; passing a string should raise TypeError
    calls = {"count": 0}

    @retry(max_retries=1, delay="0.1")
    def fail_once_then_ok():
        calls["count"] += 1
        if calls["count"] == 1:
            raise ValueError("boom")
        return "ok"

    with pytest.raises(TypeError):
        fail_once_then_ok()


def test_negative_delay_raises_valueerror_from_sleep():
    calls = {"count": 0}

    @retry(max_retries=1, delay=-0.01)
    def fail_once_then_ok():
        calls["count"] += 1
        if calls["count"] == 1:
            raise ValueError("boom")
        return "ok"

    with pytest.raises(ValueError):
        fail_once_then_ok()


def test_large_number_of_retries_performance():
    calls = {"count": 0}

    @retry(max_retries=300, delay=0.0)
    def always_fail():
        calls["count"] += 1
        raise RuntimeError("still failing")

    with patch("time.sleep", lambda d: None):
        with pytest.raises(RuntimeError):
            always_fail()

    assert calls["count"] == 301


def test_args_kwargs_passthrough():
    @retry(max_retries=0)
    def add(a, b=0):
        return a + b

    assert add(2, b=3) == 5


def test_none_return_value_no_retry():
    calls = {"count": 0}

    @retry(max_retries=5, delay=0.0)
    def return_none():
        calls["count"] += 1
        return None

    with patch("time.sleep", lambda d: None):
        assert return_none() is None
    assert calls["count"] == 1


def test_state_not_shared_between_calls():
    sleep_calls = []

    def fake_sleep(d):
        sleep_calls.append(d)

    state = {"n": 0}

    @retry(max_retries=5, delay=0.0)
    def sometimes_fail():
        state["n"] += 1
        if state["n"] <= 2:
            raise ValueError("fail")
        return state["n"]

    # First call requires retries (2 sleeps)
    with patch("time.sleep", side_effect=fake_sleep):
        assert sometimes_fail() == 3
    assert len(sleep_calls) == 2

    # Second call should not be affected by any internal decorator state
    # (function succeeds immediately because external state already advanced)
    with patch("time.sleep", side_effect=fake_sleep):
        assert sometimes_fail() == 4
    assert len(sleep_calls) == 2


