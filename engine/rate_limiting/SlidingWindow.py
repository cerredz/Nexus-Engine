# Sliding-window rate limiter (per-key) with millisecond resolution.
# Stores a sorted list of request timestamps (epoch ms) per key and decides allow/deny
# based on how many fall within the last rolling time window.
#
# Supported units:
# - 'second', 'minute', 'hour', 'day' (each converted to milliseconds internally).
#
# Semantics:
# - A request is allowed if the count of timestamps within the last window is < limit.
# - Window boundary is inclusive at the start: timestamps >= start_of_window are counted
#   (because pruning uses bisect_left on sorted timestamps).
#
# State:
# - map: Dict[str, List[int]] → per-key ascending timestamps (epoch ms).
# - blacklist: set[str] → keys that are always denied by allow().
# - user_metrics: Dict[str, List[int]] → per-key [allowed, denied] counters.
# - allowed / denied: global counters across all keys since instance creation.
#
# Performance:
# - allow(key): O(log n) to find boundary plus O(k) to slice when pruning (k pruned items),
#   and O(1) append when allowed.
# - remaining(key): O(log n) (no mutation).
#
# Notes:
# - Old timestamps are pruned only on the over-limit path; under light load, stale entries
#   may remain until a later over-limit check triggers pruning.
# - This implementation is single-process and not thread-safe; add locks for concurrency.
# - Clock is wall time (not monotonic); large clock jumps can affect behavior.
from datetime import datetime
from typing import Dict, List
from bisect import bisect_left

class SlidingWindow():
    valid_units = ['second', 'minute', 'hour', 'day']
    MIN_WINDOW = 1
    MAX_WINDOW = 100000

    # Initialize a sliding-window limiter.
    # - unit: one of valid_units ('second'|'minute'|'hour'|'day')
    # - window: size of the window in the given unit (inclusive at start boundary)
    # - limit: max number of requests allowed within the rolling window (> 0)
    # Side effects: initializes per-key map, blacklist, metrics, and global counters.
    def __init__(self, unit: str, window: int, limit: int):
        self.unit = self.__validate_unit(unit)
        self.window = self.__validate_window(window)
        self.limit = self.__validate_limit(limit)
        self.map: Dict[str, List[int]] = {}
        self.blacklist: set = set()
        self.user_metrics = {}
        self.allowed = 0
        self.denied = 0

    # Global number of allowed requests since construction (all keys).
    def get_allowed(self):
        return self.allowed
    
    # Global number of denied requests since construction (all keys).
    def get_denied(self):
        return self.denied
    
    # Per-key metrics accessor.
    # Returns a tuple (allowed, denied) for the given key.
    # If the key has no recorded metrics, returns (0, 0).
    def get_user_metrics(self, key):
        if key not in self.user_metrics:
            return (0,0)
        
        return self.user_metrics[0], self.user_metrics[1]

    # Validate unit against allowed values; raises ValueError for invalid input.
    def __validate_unit(self, unit):
        if unit not in SlidingWindow.valid_units:
            raise ValueError("Invalid rate limiter unit, must be of type 'second', 'minute', 'hour', or 'day'. ")
        
        return unit
        
    # Validate window bounds [MIN_WINDOW, MAX_WINDOW]; raises ValueError otherwise.
    def __validate_window(self, window):
        if window < SlidingWindow.MIN_WINDOW or window > SlidingWindow.MAX_WINDOW:
            raise ValueError("Invalid window, keep between 1 and 100,000")
        
        return window
        
    # Validate limit (> 0); raises ValueError otherwise.
    def __validate_limit(self, limit):
        if limit <= 0:
            raise ValueError("Invalid rate limit value, must be greater than 0.")
        
        return limit
    
    # Current time in epoch milliseconds (wall clock).
    def __get_time(self):
        return int(datetime.now().timestamp() * 1000)

    # Compute the inclusive start boundary (epoch ms) for the current rolling window.
    # Uses a per-call conversion from unit to milliseconds; entries >= this value are counted.
    def __get_starting_time(self):
        current_time = self.__get_time()
        unit_ms = {'second': 1000, 'minute': 60000, 'hour': 3600000, 'day': 86400000}
        return current_time - self.window * unit_ms[self.unit]

    # Add a key to the blacklist. Any subsequent allow(key) returns False.
    def add_to_blacklist(self, key):
        self.blacklist.add(key)

    # Main decision API. Returns True if the request is allowed, False otherwise.
    # Behavior:
    # 1) Immediately deny if key is blacklisted.
    # 2) Initialize per-key storage and per-key metrics on first use.
    # 3) If current in-window count < limit, record timestamp and allow.
    # 4) Otherwise, prune out-of-window timestamps (using bisect_left on sorted list),
    #    then re-check capacity; if below limit, append and allow; else deny.
    # Side effects:
    # - On allow: records timestamp and increments per-key/global allowed counters.
    # - On deny: increments per-key/global denied counters.
    # Complexity: O(log n + k) when pruning (k pruned), O(1) when under limit and no prune.
    def allow(self, key):
        if key in self.blacklist:
            return False
        
        if not key in self.map:
            self.map[key] = []
            self.user_metrics[key] = [0,0]

        # under limit, allow
        if len(self.map[key]) < self.limit:
            self.map[key].append(self.__get_time())
            self.user_metrics[key][0] += 1
            self.allowed += 1
            return True

        # over limit, binary search for first timestamp within window and then update list
        start_window = self.__get_starting_time()
        arr = self.map[key]

        # First index with value >= start_window (inclusive boundary).
        i = bisect_left(arr, start_window)
        if i > 0:
            arr = arr[i:]
            self.map[key] = arr

        if len(arr) < self.limit:
            arr.append(self.__get_time())
            self.user_metrics[key][0] += 1
            self.allowed += 1
            return True
        
        self.user_metrics[key][1] += 1
        self.denied += 1
        return False
    
    # Read-only capacity query for a key (does not mutate state).
    # Returns how many additional requests are currently allowed for this key,
    # computed as limit - count_in_window, clamped to [0, limit].
    # Notes:
    # - Does not consider blacklist; a blacklisted key may return a positive number here.
    # - Does not prune internal state; counts are derived logically via bisect_left.
    def remaining(self, key):
        if key not in self.map:
            return self.limit
        start_window = self.__get_starting_time()
        arr = self.map[key]
        i = bisect_left(arr, start_window)
        current = len(arr) - i
        return max(0, self.limit - current)
    
    # Returns a list of keys considered "bad actors": those with denied > allowed
    # according to per-key user_metrics. O(number_of_keys).
    def get_bad_actors(self):
        res = []
        for key, value in self.user_metrics.items():
            allowed, denied = value
            if denied > allowed:
                res.append(key)
        
        return res