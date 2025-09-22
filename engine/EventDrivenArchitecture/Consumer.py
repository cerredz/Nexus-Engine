from typing import Callable
from functools import wraps

class Consumer():
    @staticmethod
    @wraps
    def run_consumer(self, action: Callable, *args, **kwargs):
        return action(*args, **kwargs)