from dataclasses import dataclass
from typing import Any, Callable
from collections import deque
import datetime

@dataclass
class TaskResult():
    completed_at: datetime.datetime
    result: Any

class TaskQueue():
    def __init__(self, priorities: int = 1):
        assert priorities > 0, "priorities must be greater than 0, (number denotes how many priority groups their are)"
        self.q = [deque([]) for i in range(priorities)]
        self.failed = deque([])
        self.priorities = priorities

    def __len__(self):
        total = sum(len(queue) for queue in self.q)
        return total

    def enqueue(self, priority: int, task: Callable, *args, **kwargs):
        assert priority < self.priorities and priority >= 0, f"priority must be greater than -1 and less than {self.priorities}"
        self.q[priority].append((task, args, kwargs))

    def enqueue_failure(self, task: Callable, *args, **kwargs):
        self.failed.append((task, args, kwargs))

    def dequeue(self):
        for i in range(self.priorities):
            if len(self.q[i]) != 0:
                return self.q[i].popleft()
        
        return None