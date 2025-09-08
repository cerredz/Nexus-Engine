import asyncio
import pytest

from engine.Timeout.Timeout import Timeout, timeout


# --------------------
# Helper primitives
# --------------------
async def _sleep_and_return(value, delay):
    await asyncio.sleep(delay)
    return value


# --------------------
# Class-based decorator: core functionality
# --------------------
def test_timeout_class_allows_completion_before_deadline():
    decorator = Timeout(seconds=0.5)

    async def work(x):
        await asyncio.sleep(0.01)
        return x * 2

    wrapped = decorator(work)
    result = asyncio.run(wrapped(3))
    assert result == 6


def test_timeout_class_raises_timeout_without_fallback():
    decorator = Timeout(seconds=0.01)

    async def slow():
        await asyncio.sleep(0.1)
        return "done"

    wrapped = decorator(slow)
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(wrapped())


def test_timeout_class_calls_sync_fallback_on_timeout():
    sentinel = object()

    def fallback(*args, **kwargs):
        return sentinel

    decorator = Timeout(seconds=0.01, fallback=fallback)

    async def slow(x):
        await asyncio.sleep(0.1)
        return x

    wrapped = decorator(slow)
    result = asyncio.run(wrapped(123))
    assert result is sentinel


def test_timeout_class_calls_async_fallback_on_timeout():
    async def fallback(*args, **kwargs):
        return "async-fallback"

    decorator = Timeout(seconds=0.01, fallback=fallback)

    async def slow():
        await asyncio.sleep(0.1)
        return "ok"

    wrapped = decorator(slow)
    result = asyncio.run(wrapped())
    assert result == "async-fallback"


def test_timeout_class_propagates_non_timeout_exceptions():
    decorator = Timeout(seconds=0.5)

    async def explode():
        raise ValueError("boom")

    wrapped = decorator(explode)
    with pytest.raises(ValueError):
        asyncio.run(wrapped())


def test_timeout_class_rejects_sync_functions_with_typeerror():
    decorator = Timeout(seconds=0.1)

    def not_async():
        return 1

    with pytest.raises(TypeError):
        decorator(not_async)


def test_timeout_class_fallback_receives_args_and_kwargs():
    captured = {}

    def fallback(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = dict(kwargs)
        return "fb"

    decorator = Timeout(seconds=0.01, fallback=fallback)

    async def slow(a, b=2, *, key="x"):
        await asyncio.sleep(0.1)
        return a + b

    wrapped = decorator(slow)
    result = asyncio.run(wrapped(5, b=7, key="alpha"))
    assert result == "fb"
    assert captured["args"] == (5,)
    assert captured["kwargs"] == {"b": 7, "key": "alpha"}


def test_timeout_class_supports_methods_and_fallback_receives_self():
    events = []

    def fallback(self, x):
        events.append(("fallback", id(self), x))
        return "method-fb"

    decorator = Timeout(seconds=0.01, fallback=fallback)

    class Thing:
        def __init__(self):
            self.ident = id(self)

        @decorator
        async def do(self, x):
            await asyncio.sleep(0.05)
            return x * 3

    obj = Thing()
    result = asyncio.run(obj.do(9))
    assert result == "method-fb"
    assert events and events[0][0] == "fallback"
    assert events[0][1] == id(obj)
    assert events[0][2] == 9


def test_timeout_class_seconds_none_disables_timeout():
    decorator = Timeout(seconds=None)

    async def slow():
        await asyncio.sleep(0.02)
        return "ok"

    wrapped = decorator(slow)
    result = asyncio.run(wrapped())
    assert result == "ok"


def test_timeout_class_negative_seconds_immediate_timeout():
    decorator = Timeout(seconds=-1)

    async def slow():
        await asyncio.sleep(0.01)
        return "never"

    wrapped = decorator(slow)
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(wrapped())


def test_timeout_class_fallback_exception_bubbles_up():
    def fallback(*args, **kwargs):
        raise RuntimeError("fallback-error")

    decorator = Timeout(seconds=0.01, fallback=fallback)

    async def slow():
        await asyncio.sleep(0.1)
        return 1

    wrapped = decorator(slow)
    with pytest.raises(RuntimeError, match="fallback-error"):
        asyncio.run(wrapped())


def test_timeout_class_preserves_function_metadata():
    decorator = Timeout(seconds=0.5)

    async def original_name():
        return 1

    wrapped = decorator(original_name)
    assert wrapped.__name__ == "original_name"


# --------------------
# Function decorator: core functionality
# --------------------
def test_timeout_function_decorator_with_parentheses_success():
    @timeout(seconds=0.5)
    async def work(x):
        await asyncio.sleep(0.01)
        return x + 10

    result = asyncio.run(work(5))
    assert result == 15


def test_timeout_function_decorator_times_out_and_calls_fallback():
    @timeout(seconds=0.01, fallback=lambda *a, **k: "f")
    async def slow():
        await asyncio.sleep(0.1)
        return "ok"

    result = asyncio.run(slow())
    assert result == "f"


def test_timeout_function_decorator_without_parentheses_uses_defaults():
    @timeout
    async def quick():
        await asyncio.sleep(0.01)
        return "done"

    result = asyncio.run(quick())
    assert result == "done"


def test_timeout_function_decorator_rejects_sync_functions_both_forms():
    def sync_fn():
        return 1

    with pytest.raises(TypeError):
        timeout(seconds=0.1)(sync_fn)

    with pytest.raises(TypeError):
        timeout(sync_fn)


def test_timeout_function_decorator_seconds_none_disables_timeout():
    @timeout(seconds=None)
    async def slow():
        await asyncio.sleep(0.02)
        return 42

    result = asyncio.run(slow())
    assert result == 42


def test_timeout_function_decorator_negative_seconds_immediate_timeout():
    @timeout(seconds=-0.5)
    async def slow():
        await asyncio.sleep(0.01)
        return 1

    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(slow())


def test_timeout_function_decorator_preserves_function_metadata():
    @timeout(seconds=0.5)
    async def named_function():
        return 1

    assert named_function.__name__ == "named_function"


# --------------------
# Idempotency and state
# --------------------
def test_class_wrapper_idempotent_results_across_multiple_calls():
    decorator = Timeout(seconds=0.5)

    async def calc(x):
        await asyncio.sleep(0.01)
        return x * x

    wrapped = decorator(calc)
    r1 = asyncio.run(wrapped(7))
    r2 = asyncio.run(wrapped(7))
    r3 = asyncio.run(wrapped(7))
    assert r1 == r2 == r3 == 49


def test_function_wrapper_idempotent_results_across_multiple_calls():
    @timeout(seconds=0.5)
    async def calc(x):
        await asyncio.sleep(0.01)
        return x + 1

    r1 = asyncio.run(calc(4))
    r2 = asyncio.run(calc(4))
    r3 = asyncio.run(calc(4))
    assert r1 == r2 == r3 == 5


# --------------------
# Light concurrency / performance sanity
# --------------------
def test_many_concurrent_calls_finish_within_timeout():
    decorator = Timeout(seconds=0.5)

    async def quick(i):
        await asyncio.sleep(0.01)
        return i

    wrapped = decorator(quick)

    async def run_many(n):
        results = await asyncio.gather(*(wrapped(i) for i in range(n)))
        return results

    out = asyncio.run(run_many(50))
    assert out == list(range(50))


# --------------------
# Ensure exceptions bubble through function decorator too
# --------------------
def test_function_decorator_propagates_non_timeout_exceptions():
    @timeout(seconds=0.5)
    async def bad():
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        asyncio.run(bad())


