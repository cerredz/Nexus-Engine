from typing import Callable
from functools import wraps

class Publisher():
    def __init__(self, name: str, action: Callable):
        self.name = name
        self.action = action
        