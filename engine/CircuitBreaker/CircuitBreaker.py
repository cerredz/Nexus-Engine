import datetime
from typing import Callable, Tuple, Any, Optional

class CircuitBreaker():
    CLOSED_STATE, HALF_OPEN_STATE, OPEN_STATE = "Closed", "Half-Open", "Open"

    def __init__(self, key: str, failure_rate: float, duration_until_half_opened: int, open_half_calls: int):
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
        return self.state
    
    @property
    def is_closed(self):
        return self.state == CircuitBreaker.CLOSED_STATE
    
    @property
    def is_open(self):
        return self.state == CircuitBreaker.OPEN_STATE
    
    @property
    def is_half_open(self):
        return self.state == CircuitBreaker.HALF_OPEN_STATE
    
    @property
    def metrics(self):
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
        self.state = CircuitBreaker.CLOSED_STATE
        self.time_opened = None
        self.success, self.error = 0,0 
        self.half_open_success, self.half_open_error = 0,0

    def time_until_half_open(self) -> int:
        if self.state != CircuitBreaker.OPEN_STATE or self.time_opened is None:
            return 0
        now = int(datetime.datetime.now().timestamp())
        return max(0, self.duration_until_half_opened - (now - self.time_opened))

    def __get_failure_rate(self) -> float:
        total = self.success + self.error
        if total == 0:
            return 0.0
        return round(self.error / total, 4)

    def __validate_inputs(self, failure_rate, duration_until_half_opened, open_half_calls):
        if failure_rate <= 0 or failure_rate > 1:
            raise ValueError("Error constructing Circuit Breaker, failure rate x must be 0 < x <= 1")
        
        if duration_until_half_opened <= 0:
            raise ValueError("Error constructing Circuit Breaker, duration_until_half_opened must be greater than 0")

        if open_half_calls <= 0:
            raise ValueError("Error constructing Circuit Breaker, open_half_calls must be greater than 0")

    def __handle_closed(self, function: Callable, *args, **kwargs) -> Tuple[bool, Any, Exception]:
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
        now = int(datetime.datetime.now().timestamp())
        should_be_half_open = (now - self.time_opened) >= self.duration_until_half_opened
        
        if should_be_half_open:
            self.state = CircuitBreaker.HALF_OPEN_STATE
            self.half_open_success, self.half_open_error = 0, 0
            return self.__handle_half_open(function, *args, **kwargs)
        
        return False, None, RuntimeError("Circuit Breaker in open state, no requests allowed.")

    def __handle_half_open(self, function: Callable, *args, **kwargs) -> Tuple[bool, Any, Exception]:
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
        # case 1: Circuit breaker is closed:
        if self.state == CircuitBreaker.CLOSED_STATE:
            return self.__handle_closed(function, *args, **kwargs)
        
        # case 2: Circuit breaker is open
        if self.state == CircuitBreaker.OPEN_STATE:
            return self.__handle_open(function, *args, **kwargs)
        
        # case 3: Circuit braker is half-open
        if self.state == CircuitBreaker.HALF_OPEN_STATE:
            return self.__handle_half_open(function, *args, **kwargs)
        
    def wrap(self, fallback=None):
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
