# Circuit Breaker implementation.
#
# Purpose: protect callers from repeatedly invoking an unstable dependency by
# failing fast once a failure threshold is reached, then probing for recovery
# after a cool-down period.
#
# States and behavior:
# - Closed: normal operation. Calls are allowed. Track success/error counts.
#   When observed failure rate >= failure_rate, transition to Open and record time_opened.
# - Open: short-circuit. No calls are allowed. Calls fail immediately until the
#   cool-down elapses (duration_until_half_opened seconds).
# - Half-Open: probe mode after cool-down. Allow up to open_half_calls trial calls.
#   If the half-open failure rate <= failure_rate, transition to Closed (reset counters).
#   Otherwise transition back to Open and start another cool-down.
#
# API highlights:
# - run(fn, *args, **kwargs) -> (ok, result, err): executes respecting the state machine.
# - wrap(fallback=None): decorator that wraps a function; optionally handles errors
#   with a fallback(err, *args, **kwargs) if provided.
# - metrics: property exposing current counters, failure rate, state, and timestamps.
# - reset(): return to Closed and clear counters.
#
# Notes:
# - Failure rates are computed on integer counters; division-by-zero is guarded.
# - This implementation is stateful and not thread-safe by itself.
# - The class is transport-agnostic; it works with any callable.

import datetime
from typing import Callable, Tuple, Any, Optional

class CircuitBreaker():
    CLOSED_STATE, HALF_OPEN_STATE, OPEN_STATE = "Closed", "Half-Open", "Open"

    def __init__(self, key: str, failure_rate: float, duration_until_half_opened: int, open_half_calls: int):
        # Constructor inputs:
        # - key: identifier for this breaker instance (informational; not used in logic)
        # - failure_rate: threshold in (0, 1]; when observed failure rate >= this value in Closed
        #   state, the breaker trips to Open.
        # - duration_until_half_opened: cool-down period (seconds) before moving from Open to Half-Open.
        # - open_half_calls: number of probe calls permitted in Half-Open before deciding to Close or Open.
        self.__validate_inputs(failure_rate, duration_until_half_opened, open_half_calls)

        self.state = CircuitBreaker.CLOSED_STATE # either 'Closed', 'Half-Open', or 'Open'
        self.time_opened = None

        self.key = key
        self.failure_rate = failure_rate
        self.duration_until_half_opened = duration_until_half_opened
        self.open_half_calls = open_half_calls

        self.success, self.error = 0, 0
        self.half_open_success, self.half_open_error = 0,0

    def get_state(self):
        # Return the current state string: one of "Closed", "Open", or "Half-Open".
        return self.state
    
    @property
    def is_closed(self):
        # Convenience predicate: True if breaker is in Closed state.
        return self.state == CircuitBreaker.CLOSED_STATE
    
    @property
    def is_open(self):
        # Convenience predicate: True if breaker is in Open state.
        return self.state == CircuitBreaker.OPEN_STATE
    
    @property
    def is_half_open(self):
        # Convenience predicate: True if breaker is in Half-Open state.
        return self.state == CircuitBreaker.HALF_OPEN_STATE
    
    @property
    def metrics(self):
        # Return a snapshot of breaker metrics and state.
        # Keys:
        # - state: current state string
        # - success / error: counters for Closed state attempts
        # - half_open_success / half_open_error: counters for Half-Open probe attempts
        # - failure_rate: computed as error / (success + error) for Closed counters (rounded to 4 decimals)
        # - time_opened: unix timestamp when entering Open (or None)
        total = self.success + self.error
        failure_rate = (self.error / total) if total else 0.0
        return {
            "state": self.state,
            "success": self.success,
            "error": self.error,
            "half_open_success": self.half_open_success,
            "half_open_error": self.half_open_error,
            "failure_rate": round(failure_rate, 4),
            "time_opened": self.time_opened,
        }
    
    def reset(self):
        # Reset the breaker to Closed state and clear all counters and time_opened.
        self.state = CircuitBreaker.CLOSED_STATE
        self.time_opened = None
        self.success, self.error = 0,0 
        self.half_open_success, self.half_open_error = 0,0

    def time_until_half_open(self) -> int:
        # If currently Open, return the number of seconds remaining until Half-Open.
        # Returns 0 if not Open or if time_opened is unknown.
        if self.state != CircuitBreaker.OPEN_STATE or self.time_opened is None:
            return 0
        now = int(datetime.datetime.now().timestamp())
        return max(0, self.duration_until_half_opened - (now - self.time_opened))

    def __get_failure_rate(self) -> float:
        # Private helper: compute current failure rate for Closed counters.
        total = self.success + self.error
        if total == 0:
            return 0.0
        return round(self.error / total, 4)

    def __validate_inputs(self, failure_rate, duration_until_half_opened, open_half_calls):
        # Validate constructor inputs; raise ValueError for any invalid setting.
        if failure_rate <= 0 or failure_rate > 1:
            raise ValueError("Error constructing Circuit Breaker, failure rate x must be 0 < x <= 1")
        
        if duration_until_half_opened <= 0:
            raise ValueError("Error constructing Circuit Breaker, duration_until_half_opened must be greater than 0")

        if open_half_calls <= 0:
            raise ValueError("Error constructing Circuit Breaker, open_half_calls must be greater than 0")

    def __handle_closed(self, function: Callable, *args, **kwargs) -> Tuple[bool, Any, Exception]:
        # Execute callable while in Closed state.
        # - If failure rate threshold is met/exceeded, trip to Open and fail fast.
        # - Otherwise attempt the call; update success/error counters and return (ok, result, err).
        should_be_open = self.__get_failure_rate() >= self.failure_rate
        
        if should_be_open:
            self.state = CircuitBreaker.OPEN_STATE
            self.time_opened = int(datetime.datetime.now().timestamp())
            return False, None, RuntimeError("Circuit Breaker is in the Open State, no calls allowed.")

        try:
            result = function(*args, **kwargs)
        except Exception as e:
            self.error += 1
            return False, None, e
        
        self.success += 1
        return True, result, None
    
    def __handle_open(self, function: Callable, *args, **kwargs) -> Tuple[bool, Any, Exception]:
        # Execute callable while in Open state.
        # - If cool-down elapsed, switch to Half-Open and delegate there.
        # - Otherwise fail fast with an Open-state error.
        now = int(datetime.datetime.now().timestamp())
        should_be_half_open = (now - self.time_opened) >= self.duration_until_half_opened
        
        if should_be_half_open:
            self.state = CircuitBreaker.HALF_OPEN_STATE
            self.half_open_success, self.half_open_error = 0, 0
            return self.__handle_half_open(function, *args, **kwargs)
        
        return False, None, RuntimeError("Circuit Breaker in open state, no requests allowed.")

    def __handle_half_open(self, function: Callable, *args, **kwargs) -> Tuple[bool, Any, Exception]:
        # Execute callable while in Half-Open (probe) state.
        # - Allow up to open_half_calls attempts.
        # - After the probe window, close if failure rate <= threshold else reopen.
        # - While within the probe window, attempt the call and update half-open counters.
        half_open_calls = self.half_open_success + self.half_open_error
        
        if half_open_calls >= self.open_half_calls:
            total = self.half_open_success + self.half_open_error
            failure_rate = (self.half_open_error / total) if total else 1.0
            should_be_closed = failure_rate <= self.failure_rate
            if should_be_closed:
                self.success, self.error = 0, 0
                self.time_opened = None
                self.state = CircuitBreaker.CLOSED_STATE
                return self.__handle_closed(function, *args, **kwargs)
            else:
                self.state = CircuitBreaker.OPEN_STATE
                self.time_opened = int(datetime.datetime.now().timestamp())
                return False, None, RuntimeError("Circuit Breaker in open state, no requests allowed.")

        try:
            result = function(*args, **kwargs)
        except Exception as e:
            self.half_open_error += 1
            return False, None, e
        
        self.half_open_success += 1
        return True, result, None

    def run(self, function: Callable, *args, **kwargs):
        # Public entry point to execute a callable respecting the breaker state machine.
        # Returns a tuple (ok: bool, result: Any, err: Exception|None).
        # - When ok is True: result contains the callable's return value.
        # - When ok is False: err contains the raised exception (or an Open-state error).
        # case 1: Circuit breaker is Closed:
        if self.state == CircuitBreaker.CLOSED_STATE:
            return self.__handle_closed(function, *args, **kwargs)
        
        # case 2: Circuit breaker is Open
        if self.state == CircuitBreaker.OPEN_STATE:
            return self.__handle_open(function, *args, **kwargs)
        
        # case 3: Circuit breaker is Half-Open
        if self.state == CircuitBreaker.HALF_OPEN_STATE:
            return self.__handle_half_open(function, *args, **kwargs)
        
    def wrap(self, fallback=None):
        # Decorator factory: wraps a function so that calls are guarded by this CircuitBreaker.
        # - If the call succeeds, return the function's result.
        # - If it fails and a fallback is provided, call fallback(err, *args, **kwargs).
        # - Otherwise, re-raise the error.
        #
        # Example:
        #   breaker = CircuitBreaker(key="payments", failure_rate=0.5, duration_until_half_opened=10, open_half_calls=3)
        #   @breaker.wrap(fallback=lambda err, *a, **kw: "fallback-value")
        #   def call_service(x):
        #       ...
        #   result = call_service(42)
        def decorator(func):
            def wrapper(*args, **kwargs):
                ok, result, err = self.run(func, *args, **kwargs)
                if ok:
                    return result
                if fallback is not None:
                    return fallback(err, *args, **kwargs)
                raise err
            return wrapper
        return decorator
