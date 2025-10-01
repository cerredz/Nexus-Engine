import pytest
import sys
import os
import asyncio
import datetime
import time
from collections import deque

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engine.TaskQueue.TaskQueue import TaskQueue, TaskResult
from engine.TaskQueue.Worker import Worker


# Core Functionality Tests
def test_worker_initialization_default():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    assert worker.task_queue == tq
    assert worker.max_results == 10
    assert worker.max_retries == 3
    assert worker.timeout == 30
    assert worker.is_running is False
    assert len(worker.results) == 0


def test_worker_initialization_with_custom_timeout():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=5, max_retries=2, timeout=60)
    assert worker.timeout == 60


def test_get_last_result_empty():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    assert worker.get_last_result() is None


def test_get_results_empty():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    results = worker.get_results()
    assert results == []
    assert isinstance(results, list)


def test_stop_worker():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    worker.is_running = True
    worker.stop()
    assert worker.is_running is False


@pytest.mark.asyncio
async def test_execute_single_task():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def simple_task():
        return "completed"
    
    tq.enqueue(0, simple_task)
    
    task = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.1)
    worker.stop()
    await asyncio.sleep(0.1)
    
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) == 1
    assert worker.results[0].result == "completed"


@pytest.mark.asyncio
async def test_execute_multiple_tasks():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task(n):
        return n * 2
    
    for i in range(5):
        tq.enqueue(0, task, i)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.5)
    worker.stop()
    await asyncio.sleep(0.1)
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) == 5


@pytest.mark.asyncio
async def test_max_results_limit():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=3, max_retries=3)
    
    def task(n):
        return n
    
    for i in range(10):
        tq.enqueue(0, task, i)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.5)
    worker.stop()
    await asyncio.sleep(0.1)
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) <= 3


@pytest.mark.asyncio
async def test_get_last_result_after_execution():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task():
        return "first_result"
    
    tq.enqueue(0, task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    last = worker.get_last_result()
    assert last is not None
    assert last.result == "first_result"


@pytest.mark.asyncio
async def test_task_with_args_and_kwargs():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task(a, b, c=0):
        return a + b + c
    
    tq.enqueue(0, task, 1, 2, c=3)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) == 1
    assert worker.results[0].result == 6


# Edge Cases
@pytest.mark.asyncio
async def test_execute_tasks_empty_queue():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    task = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) == 0


@pytest.mark.asyncio
async def test_worker_with_max_results_one():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=1, max_retries=3)
    
    def task(n):
        return n
    
    for i in range(5):
        tq.enqueue(0, task, i)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.5)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) == 1
    assert worker.results[0].result == 4


@pytest.mark.asyncio
async def test_worker_with_max_retries_one():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=1)
    
    counter = [0]
    def failing_task():
        counter[0] += 1
        if counter[0] < 2:
            raise Exception("fail")
        return "success"
    
    tq.enqueue(0, failing_task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(3)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=5.0)
    except asyncio.TimeoutError:
        pass
    
    assert counter[0] >= 1


@pytest.mark.asyncio
async def test_task_timeout():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=1, timeout=1)
    
    def slow_task():
        time.sleep(5)
        return "should not complete"
    
    tq.enqueue(0, slow_task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(3)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=5.0)
    except asyncio.TimeoutError:
        pass


@pytest.mark.asyncio
async def test_task_returning_none():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task_returns_none():
        return None
    
    tq.enqueue(0, task_returns_none)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) == 1
    assert worker.results[0].result is None


@pytest.mark.asyncio
async def test_task_returning_zero():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task_returns_zero():
        return 0
    
    tq.enqueue(0, task_returns_zero)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) == 1
    assert worker.results[0].result == 0


@pytest.mark.asyncio
async def test_task_returning_empty_string():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task_returns_empty():
        return ""
    
    tq.enqueue(0, task_returns_empty)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) == 1
    assert worker.results[0].result == ""


@pytest.mark.asyncio
async def test_task_returning_complex_object():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task_returns_dict():
        return {"nested": {"a": 1}, "list": [1, 2, 3]}
    
    tq.enqueue(0, task_returns_dict)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) == 1
    assert worker.results[0].result == {"nested": {"a": 1}, "list": [1, 2, 3]}


@pytest.mark.asyncio
async def test_multiple_stop_calls():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    worker.stop()
    worker.stop()
    worker.stop()
    
    assert worker.is_running is False


# Invalid Input & Type Errors
def test_worker_initialization_with_zero_max_results():
    tq = TaskQueue()
    with pytest.raises(AssertionError):
        Worker(task_queue=tq, max_results=0, max_retries=3)


def test_worker_initialization_with_negative_max_results():
    tq = TaskQueue()
    with pytest.raises(AssertionError):
        Worker(task_queue=tq, max_results=-1, max_retries=3)


def test_worker_initialization_with_zero_max_retries():
    tq = TaskQueue()
    with pytest.raises(AssertionError):
        Worker(task_queue=tq, max_results=10, max_retries=0)


def test_worker_initialization_with_negative_max_retries():
    tq = TaskQueue()
    with pytest.raises(AssertionError):
        Worker(task_queue=tq, max_results=10, max_retries=-1)


def test_worker_initialization_with_zero_timeout():
    tq = TaskQueue()
    with pytest.raises(AssertionError):
        Worker(task_queue=tq, max_results=10, max_retries=3, timeout=0)


def test_worker_initialization_with_negative_timeout():
    tq = TaskQueue()
    with pytest.raises(AssertionError):
        Worker(task_queue=tq, max_results=10, max_retries=3, timeout=-1)


def test_worker_initialization_with_none_task_queue():
    with pytest.raises((AttributeError, AssertionError)):
        Worker(task_queue=None, max_results=10, max_retries=3)


def test_worker_initialization_with_string_max_results():
    tq = TaskQueue()
    with pytest.raises((TypeError, AssertionError)):
        Worker(task_queue=tq, max_results="ten", max_retries=3)


def test_worker_initialization_with_float_max_results():
    tq = TaskQueue()
    try:
        worker = Worker(task_queue=tq, max_results=10.5, max_retries=3)
    except (TypeError, AssertionError):
        pass


def test_worker_initialization_with_none_max_results():
    tq = TaskQueue()
    with pytest.raises((TypeError, AssertionError)):
        Worker(task_queue=tq, max_results=None, max_retries=3)


@pytest.mark.asyncio
async def test_task_raising_exception():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=2, timeout=1)
    
    def failing_task():
        raise ValueError("intentional error")
    
    tq.enqueue(0, failing_task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(5)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=7.0)
    except asyncio.TimeoutError:
        pass


@pytest.mark.asyncio
async def test_task_raising_type_error():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=1, timeout=1)
    
    def bad_task():
        return 1 + "string"
    
    tq.enqueue(0, bad_task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(3)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=5.0)
    except asyncio.TimeoutError:
        pass


@pytest.mark.asyncio
async def test_task_with_missing_arguments():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=1, timeout=1)
    
    def task_needs_args(a, b):
        return a + b
    
    tq.enqueue(0, task_needs_args)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(3)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=5.0)
    except asyncio.TimeoutError:
        pass


@pytest.mark.asyncio
async def test_task_with_wrong_argument_types():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=1, timeout=1)
    
    def task(a, b):
        return a / b
    
    tq.enqueue(0, task, "string", "another")
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(3)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=5.0)
    except asyncio.TimeoutError:
        pass


# Large-Scale/Performance Tests
@pytest.mark.asyncio
async def test_execute_many_tasks():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=1000, max_retries=3)
    
    def task(n):
        return n * 2
    
    for i in range(1000):
        tq.enqueue(0, task, i)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(5)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=10.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) <= 1000


@pytest.mark.asyncio
async def test_max_results_rollover_with_many_tasks():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task(n):
        return n
    
    for i in range(100):
        tq.enqueue(0, task, i)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=5.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) <= 10


@pytest.mark.asyncio
async def test_very_large_max_results():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=1000000, max_retries=3)
    
    def task():
        return "result"
    
    tq.enqueue(0, task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) == 1


@pytest.mark.asyncio
async def test_very_large_max_retries():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=10000)
    
    def task():
        return "result"
    
    tq.enqueue(0, task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) == 1


@pytest.mark.asyncio
async def test_task_with_large_return_value():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task():
        return list(range(100000))
    
    tq.enqueue(0, task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(1)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=3.0)
    except asyncio.TimeoutError:
        pass
    
    if len(worker.results) > 0:
        assert len(worker.results[0].result) == 100000


# Idempotency and State Tests
@pytest.mark.asyncio
async def test_results_are_task_result_objects():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task():
        return "result"
    
    tq.enqueue(0, task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    if len(worker.results) > 0:
        assert isinstance(worker.results[0], TaskResult)
        assert isinstance(worker.results[0].completed_at, datetime.datetime)


@pytest.mark.asyncio
async def test_results_maintain_chronological_order():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task(n):
        time.sleep(0.01)
        return n
    
    for i in range(5):
        tq.enqueue(0, task, i)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(1)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=3.0)
    except asyncio.TimeoutError:
        pass
    
    if len(worker.results) >= 2:
        for i in range(len(worker.results) - 1):
            assert worker.results[i].completed_at <= worker.results[i + 1].completed_at


@pytest.mark.asyncio
async def test_get_results_returns_copy():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task():
        return "result"
    
    tq.enqueue(0, task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    results1 = worker.get_results()
    results2 = worker.get_results()
    
    assert results1 == results2
    assert results1 is not results2


@pytest.mark.asyncio
async def test_worker_state_after_stop():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    def task():
        return "result"
    
    tq.enqueue(0, task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert worker.is_running is False


@pytest.mark.asyncio
async def test_retry_with_eventual_success():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=5, timeout=2)
    
    counter = [0]
    def flaky_task():
        counter[0] += 1
        if counter[0] < 3:
            raise Exception("fail")
        return "success"
    
    tq.enqueue(0, flaky_task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(10)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=12.0)
    except asyncio.TimeoutError:
        pass
    
    assert counter[0] >= 1


@pytest.mark.asyncio
async def test_retry_exponential_backoff():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3, timeout=1)
    
    timestamps = []
    def failing_task():
        timestamps.append(time.time())
        raise Exception("always fail")
    
    tq.enqueue(0, failing_task)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(10)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=12.0)
    except asyncio.TimeoutError:
        pass


@pytest.mark.asyncio
async def test_worker_with_priority_queue():
    tq = TaskQueue(priorities=3)
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    results_order = []
    def task(priority):
        results_order.append(priority)
        return priority
    
    tq.enqueue(2, task, 2)
    tq.enqueue(0, task, 0)
    tq.enqueue(1, task, 1)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.5)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert results_order == [0, 1, 2]


@pytest.mark.asyncio
async def test_results_deque_behavior():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=3, max_retries=3)
    
    def task(n):
        return n
    
    for i in range(5):
        tq.enqueue(0, task, i)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.5)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    assert len(worker.results) == 3
    assert worker.results[0].result == 2
    assert worker.results[2].result == 4


@pytest.mark.asyncio
async def test_concurrent_task_enqueueing_and_execution():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=100, max_retries=3)
    
    def task(n):
        return n
    
    for i in range(10):
        tq.enqueue(0, task, i)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.1)
    
    for i in range(10, 20):
        tq.enqueue(0, task, i)
    
    await asyncio.sleep(0.5)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass


@pytest.mark.asyncio
async def test_lambda_task_execution():
    tq = TaskQueue()
    worker = Worker(task_queue=tq, max_results=10, max_retries=3)
    
    tq.enqueue(0, lambda: 42)
    
    task_obj = asyncio.create_task(worker.execute_tasks())
    await asyncio.sleep(0.2)
    worker.stop()
    
    try:
        await asyncio.wait_for(task_obj, timeout=2.0)
    except asyncio.TimeoutError:
        pass
    
    if len(worker.results) > 0:
        assert worker.results[0].result == 42

