# Timeout utilities for asyncio coroutines.
# Provides:
# - Class-based decorator `Timeout` for per-instance configuration.
# - Function decorator `timeout` for ergonomic usage (works with or without parentheses).
# Behavior:
# - Bounds execution time using asyncio.wait_for with the configured seconds.
# - On timeout: runs optional `fallback(*args, **kwargs)` (sync or async) and returns its result;
#   otherwise re-raises asyncio.TimeoutError.
# Constraints:
# - Only supports async callables; raises TypeError for sync functions.
# Notes:
# - wait_for cancels the wrapped coroutine task on timeout; ensure your code tolerates CancellationError.
# - Fallback executes on the event loop; avoid blocking operations.
#
# Example (class-based):
#   @Timeout(seconds=1.5, fallback=lambda *a, **k: 'timed out')
#   async def fetch(): ...
#
# Example (function decorator):
#   @timeout(seconds=2)
#   async def compute(): ...

from typing import Callable, Optional
from functools import wraps
import asyncio

# Class-based decorator enforcing a timeout on async functions.
# - Configure per-instance `seconds` and optional `fallback`.
# - Use as `@Timeout(seconds=..., fallback=...)`.
class Timeout:
    def __init__(self, seconds: float = 3.0, fallback: Optional[Callable] = None):
        # Timeout threshold in seconds and optional fallback callable.
        self.seconds = seconds
        self.fallback = fallback

    def __call__(self, func: Callable):
        # Only allow wrapping `async def` functions.
        if not asyncio.iscoroutinefunction(func):
            raise TypeError("Timeout can only be used on async functions")

        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Execute the coroutine with a deadline; on timeout, invoke fallback if provided.
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=self.seconds)
            except asyncio.TimeoutError:
                if self.fallback:
                    res = self.fallback(*args, **kwargs)
                    return await res if asyncio.iscoroutine(res) else res
                raise

        return wrapper


# Function decorator variant supporting both `@timeout` and `@timeout(seconds=...)` forms.
# Parameters mirror `Timeout`:
# - seconds: timeout in seconds (default 3.0)
# - fallback: optional sync/async callable invoked on timeout, receiving the original args
def timeout(_func: Optional[Callable] = None, *, seconds: float = 3.0, fallback: Optional[Callable] = None):
    def decorator(func: Callable):
        # Enforce async-only usage.
        if not asyncio.iscoroutinefunction(func):
            raise TypeError("@timeout can only be used on async functions")

        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Run with deadline; on timeout, invoke fallback if provided.
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=seconds)
            except asyncio.TimeoutError:
                if fallback:
                    res = fallback(*args, **kwargs)
                    return await res if asyncio.iscoroutine(res) else res
                raise

        return wrapper

    return decorator if _func is None else decorator(_func)