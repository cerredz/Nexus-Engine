# Fixed-window rate limiter implementation that tracks request counts on a per-key basis.
# This limiter divides time into fixed windows (e.g., 60 seconds) and allows a specified
# number of requests per key within each window. When a window expires, all counts reset.
# It supports blacklisting, tracks per-key and global metrics, and provides utilities
# for querying remaining requests and time until the next window reset.
# This implementation is not thread-safe.
from typing import List, Dict
from datetime import datetime

# FixedWindowCounter enforces a request limit per key over a discrete time window.
# - Tracks requests for unique keys using a dictionary.
# - Resets all counters when the time window elapses.
# - Supports dynamic blacklisting of keys.
# - Provides metrics for both global and per-key allowed/denied requests.
# - Offers utilities to check remaining request quotas and time until reset.
class FixedWindowCounter():
    valid_units = ['second', 'minute', 'hour', 'day']

    # Initializes the rate limiter with a specified time unit and request limit.
    # - unit: 'second', 'minute', 'hour', or 'day'.
    # - limit: Maximum number of requests allowed per key within the window.
    # Sets up internal state for tracking requests, windows, and metrics.
    def __init__(self, unit: str, limit: int):
        self.unit = self.__validate_unit(unit)
        self.limit = self.__validate_limit(limit)
        self.map: Dict[str, int] = {}
        self.start_window = self.__get_time()
        self.blacklist: set = set()

        self.allowed = 0
        self.denied = 0
        self.user_metrics = {}

    # Returns the total number of allowed requests across all keys since initialization.
    def get_allowed(self):
        return self.allowed
    
    # Returns the total number of denied requests across всех keys since initialization.
    def get_denied(self):
        return self.denied
    
    # Adds a key to the blacklist. Any subsequent requests from this key will be denied
    # until it is removed.
    def add_to_blacklist(self, key):
        self.blacklist.add(key)

    # Removes a key from the blacklist, allowing it to make requests again.
    # Does not raise an error if the key is not in the blacklist.
    def remove_from_blacklist(self, key):
        self.blacklist.discard(key)
    
    # Validate unit against allowed values; raises ValueError for invalid input.
    def __validate_unit(self, unit):
        if unit not in FixedWindowCounter.valid_units:
            raise ValueError("Invalid rate limiter unit, must be of type 'second', 'minute', 'hour', or 'day'. ")
        
        return unit
    
    # Validate limit (> 0); raises ValueError otherwise.
    def __validate_limit(self, limit):
        if limit <= 0:
            raise ValueError("Invalid rate limit value, must be greater than 0.")
        
        return limit
    
    # Current time in epoch milliseconds (wall clock).
    # NOTE: Tests may stub an instance attribute named "__get_time" (without name mangling).
    # To support that, internal callers should use _now_ms(), which prefers the instance stub.
    def __get_time(self):
        return int(datetime.now().timestamp() * 1000)

    # Returns current time in ms, preferring a test-provided instance stub named "__get_time".
    def _now_ms(self):
        override = getattr(self, "__get_time", None)
        if callable(override):
            try:
                return int(override())
            except Exception:
                # Fallback to real time if the override misbehaves
                pass
        return int(datetime.now().timestamp() * 1000)

    # Ensures the current window is aligned with the current time.
    # Resets the window if time moved forward past the window end or backwards before start.
    def _ensure_window_current(self):
        unit_ms = {'second': 1000, 'minute': 60000, 'hour': 3600000, 'day': 86400000}
        now_ms = self._now_ms()
        window_len = unit_ms[self.unit]
        if now_ms < self.start_window or now_ms > self.start_window + window_len:
            self.start_window = now_ms
            self.map.clear()
    
    # Checks if the current time is past the end of the previous window.
    def __is_past_prev_window_end(self):
        unit_ms = {'second': 1000, 'minute': 60000, 'hour': 3600000, 'day': 86400000}
        return self.start_window + unit_ms[self.unit] < self._now_ms()
    
    # Returns the time in milliseconds until the current window expires and all counts reset.
    # Returns 0 if the window has already expired.
    def get_time_until_reset(self):
        unit_ms = {'second': 1000, 'minute': 60000, 'hour': 3600000, 'day': 86400000}
        window_end = self.start_window + unit_ms[self.unit]
        return max(0, window_end - self._now_ms())

    # Checks if a request from a given key is allowed.
    # Returns True if the key is within its request limit for the current window.
    # Returns False if the key is blacklisted or has exceeded its limit.
    # This method updates both global and per-key metrics.
    def allow(self, key):
        self.user_metrics.setdefault(key, [0, 0]) # [allowed, denied]

        if key in self.blacklist:
            self.denied += 1
            self.user_metrics[key][1] += 1
            return False
        
        self._ensure_window_current()

        count = self.map.get(key, 0)
        if count >= self.limit:
            self.denied += 1
            self.user_metrics[key][1] += 1
            return False
        
        self.map[key] = count + 1
        self.allowed += 1
        self.user_metrics[key][0] += 1
        return True

    # Returns the number of remaining requests a key can make in the current window.
    # Returns the full limit for keys that have not yet made a request.
    def remaining(self, key):
        self._ensure_window_current()
        return self.limit - self.map.get(key, 0)

    # Returns a list of keys whose denied request count is greater than their allowed count.
    # This can be used to identify clients that are persistently exceeding their limits.
    def bad_actors(self):
        res = []
        for key, value in self.user_metrics.items():
            allowed, denied = value
            if denied > allowed:
                res.append(key)
        
        return res