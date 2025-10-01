# Asynchronous worker implementation for executing tasks from a TaskQueue.
# The worker continuously polls the queue, executes tasks in separate threads
# (to avoid blocking the event loop), and stores results with timestamps.
# Features automatic retry with exponential backoff on failures, task timeout handling,
# and a bounded results deque that auto-evicts oldest results when full.
# Designed for long-running background task processing with graceful shutdown support.

from collections import deque
from .TaskQueue import TaskQueue, TaskResult
import datetime
import asyncio

# Worker executes tasks asynchronously from a TaskQueue with retry and timeout handling.
# Provides:
# - Continuous async task execution loop (execute_tasks)
# - Task timeout enforcement with configurable duration
# - Automatic retry with exponential backoff (1s, 2s, 4s, 8s, ...)
# - Bounded result storage (oldest results auto-evicted when limit reached)
# - Graceful shutdown via stop() method
# - Thread-based task execution to prevent blocking the async event loop
class Worker():
    # Initialize a worker with task queue, result limits, retry policy, and timeout.
    # - task_queue: TaskQueue instance to pull tasks from (must not be None)
    # - max_results: maximum number of TaskResult objects to keep in memory (oldest evicted first)
    # - max_retries: number of retry attempts for failed tasks (with exponential backoff)
    # - timeout: seconds to wait for each task execution before timing out (default: 30)
    # Results are stored in a deque; retries use backoff = 1 * (2 ** retry_count) seconds.
    def __init__(self, task_queue: 'TaskQueue', max_results: int, max_retries: int, timeout: int = 30):
        assert task_queue is not None, "task_queue cannot be None"
        assert max_results > 0 and max_retries > 0 and timeout > 0, "max_results and max_retries and timeout must be greater than 0"
        self.task_queue = task_queue
        self.max_results = max_results
        self.max_retries = max_retries
        self.results = deque([])
        self.timeout = timeout
        self.is_running = False
    
    # Return the most recent TaskResult (leftmost in deque), or None if no results exist.
    # Note: "last" here means most recent chronologically, not positionally last.
    def get_last_result(self):
        if self.results: return self.results[0]

    # Return a copy of all stored results as a list (oldest to newest).
    # Returns a new list to prevent external mutation of the internal deque.
    def get_results(self):
        return list(self.results)

    # Signal the worker to stop processing tasks.
    # Sets is_running to False, which will cause execute_tasks() to exit its main loop.
    # This is a graceful shutdown: the current task will complete before stopping.
    def stop(self):
        self.is_running = False 
    
    # Main async loop: continuously dequeue and execute tasks until stop() is called.
    # - Polls task_queue.dequeue() in a loop
    # - Executes tasks in a separate thread via asyncio.to_thread to avoid blocking
    # - Enforces timeout using asyncio.wait_for
    # - Stores successful results in self.results (auto-evicting oldest if at max_results)
    # - On timeout or exception, invokes __handle_task_failure for retry logic
    # - Sleeps briefly (0.01s) when queue is empty to avoid busy-waiting
    async def execute_tasks(self):
        self.is_running = True

        while self.is_running:
            curr_task = None
            args = ()
            kwargs = {}
            
            try:
                task_data = self.task_queue.dequeue()
                if task_data == None: 
                    await asyncio.sleep(0.01)  # Brief sleep to prevent busy-waiting on empty queue
                    continue
            
                curr_task, args, kwargs = task_data
                # Execute in separate thread to avoid blocking the event loop
                res = await asyncio.wait_for(asyncio.to_thread(curr_task, *args, **kwargs), timeout=self.timeout)
                if len(self.results) >= self.max_results: self.results.popleft()
                self.results.append(TaskResult(completed_at=datetime.datetime.now(), result=res))
            
            except asyncio.TimeoutError:
                print(f"Task timed out after {self.timeout} seconds. If you want to increase timeout pass timeout prop into the constructor")
                if curr_task is not None:
                    await self.__handle_task_failure(curr_task, args, kwargs)
            except Exception as e:
                if curr_task is not None:
                    await self.__handle_task_failure(curr_task, args, kwargs)
                
    # Private retry handler with exponential backoff.
    # Attempts to re-execute a failed task up to max_retries times with increasing delays.
    # - Backoff formula: 1 * (2 ** retries) seconds (1s, 2s, 4s, 8s, 16s, ...)
    # - On successful retry, stores the result and breaks out of retry loop
    # - On continued failure, silently exhausts all retries (no final error stored)
    # - task: the callable that failed
    # - args, kwargs: original arguments for the task
    async def __handle_task_failure(self, task, args, kwargs):
        if task is None:
            return

        retries = 0
        while retries < self.max_retries:
            backoff = 1 * (2 ** retries)
            await asyncio.sleep(backoff)
            try:
                res = await asyncio.wait_for(asyncio.to_thread(task, *args, **kwargs), timeout=self.timeout)
                if len(self.results) >= self.max_results: self.results.popleft()
                self.results.append(TaskResult(completed_at=datetime.datetime.now(), result=res))
                break  # Success - exit retry loop
            except Exception as e:
                retries += 1  # Increment on failure and continue to next retry

            
                



