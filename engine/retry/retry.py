# Retry decorator for synchronous callables.
# Provides configurable retry attempts with fixed or exponential backoff.
#
# Usage examples
# 1) Default settings (retry 3 times after the initial attempt, 0.1s delay):
#    @retry
#    def do_work():
#        ...
#
# 2) Customized settings:
#    @retry(max_retries=5, delay=0.25, exponential_backoff=True, exceptions=(ValueError,))
#    def sometimes_fails():
#        ...
#
# Parameters
# - _func: Internal parameter enabling both bare @retry and @retry(...)-style usage.
# - max_retries: Number of retries after the initial attempt. Total attempts = max_retries + 1.
# - delay: Initial sleep (in seconds) between failed attempts.
# - exponential_backoff: When True, doubles the delay after each failed attempt.
# - exceptions: Tuple of exception types that should trigger a retry.
#
# Behavior
# - Calls the wrapped function; on success, returns immediately.
# - On failure, sleeps for the current delay and optionally doubles it if exponential_backoff is True.
# - If the last attempt fails (i.e., after max_retries), re-raises the last exception.
# - Preserves the wrapped function's metadata via functools.wraps.
#
# Notes & limitations
# - This implementation is synchronous and uses time.sleep, which blocks the current thread.
# - There is no jitter or maximum backoff cap.
# - The exceptions parameter restricts which exceptions trigger retries; others are
#   re-raised immediately without retrying.
from functools import wraps
from typing import Callable, Optional, Tuple, Type
import time 

def retry(_func: Optional[Callable] = None, *, max_retries: int = 3, delay: float = .1, exponential_backoff: bool = False, exceptions: Tuple[Type[BaseException], ...] = (Exception,),):
    # Decorator factory: supports both @retry and @retry(...). Returns a decorator that
    # installs retry logic around the target function.
    def decorator(func: Callable):
        # Preserve function identity (name, docstring, etc.).
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Start with the configured delay and increase it on each failure if exponential_backoff.
            current_delay = delay
            # Attempt indices: 0..max_retries inclusive -> total attempts = max_retries + 1.
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    # If we've exhausted all attempts, surface the last error to the caller.
                    if attempt == max_retries:
                        raise

                    # Wait before the next retry attempt.
                    time.sleep(current_delay)
                    if exponential_backoff:
                        # Double the delay for the next attempt when backoff is enabled.
                        current_delay *= 2
                except Exception:
                    # Exception not in the retry allowlist; re-raise immediately.
                    raise
        
        return wrapper
    return decorator if _func is None else decorator(_func)

    