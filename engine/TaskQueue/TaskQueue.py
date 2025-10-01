# Priority-based task queue implementation for managing callable tasks.
# Supports multiple priority levels (0 = highest priority) where tasks are dequeued
# in priority order. Each priority level is backed by a deque for O(1) enqueue/dequeue.
# Tasks are stored as (callable, args, kwargs) tuples, enabling flexible task definitions.
# A separate failed queue tracks tasks that need reprocessing (currently unused in core logic).

from dataclasses import dataclass
from typing import Any, Callable
from collections import deque
import datetime

# TaskResult encapsulates the output of a completed task.
# - completed_at: timestamp when the task finished execution
# - result: the return value from the task callable (can be any type)
@dataclass
class TaskResult():
    completed_at: datetime.datetime
    result: Any

# TaskQueue manages a priority-based queue of tasks with O(1) enqueue and O(P) dequeue
# where P is the number of priority levels. Provides:
# - Multiple priority levels (0 = highest priority, configurable at init)
# - FIFO ordering within each priority level
# - O(1) enqueue by priority
# - O(P) dequeue that scans priorities from highest to lowest
# - Separate failure queue for tracking failed tasks (optional use)
class TaskQueue():
    # Initialize a task queue with the specified number of priority levels.
    # - priorities: number of priority levels (must be > 0). Priority 0 is highest.
    # Creates a list of deques, one per priority level, plus a separate failed queue.
    def __init__(self, priorities: int = 1):
        assert priorities > 0, "priorities must be greater than 0, (number denotes how many priority groups their are)"
        self.q = [deque([]) for i in range(priorities)]
        self.failed = deque([])
        self.priorities = priorities

    # Return the total number of tasks across all priority levels (excluding failed queue).
    # This is O(P) where P is the number of priority levels.
    def __len__(self):
        total = sum(len(queue) for queue in self.q)
        return total

    # Enqueue a task at the specified priority level.
    # - priority: priority level (0 = highest, must be < self.priorities)
    # - task: callable to execute
    # - *args, **kwargs: arguments to pass to the callable when executed
    # Appends (task, args, kwargs) tuple to the appropriate priority queue.
    def enqueue(self, priority: int, task: Callable, *args, **kwargs):
        assert priority < self.priorities and priority >= 0, f"priority must be greater than -1 and less than {self.priorities}"
        self.q[priority].append((task, args, kwargs))

    # Enqueue a failed task to the separate failure queue.
    # This is a utility for tracking tasks that failed execution and may need reprocessing.
    # - task: callable that failed
    # - *args, **kwargs: original arguments for the failed task
    def enqueue_failure(self, task: Callable, *args, **kwargs):
        self.failed.append((task, args, kwargs))

    # Dequeue the next task from the highest-priority non-empty queue.
    # Scans priority levels from 0 (highest) to priorities-1 (lowest) and returns
    # the first available task as a (callable, args, kwargs) tuple.
    # Returns None if all queues are empty.
    def dequeue(self):
        for i in range(self.priorities):
            if len(self.q[i]) != 0:
                return self.q[i].popleft()
        
        return None