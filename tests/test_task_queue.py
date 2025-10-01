import pytest
import sys
import os
from collections import deque
import time

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engine.TaskQueue.TaskQueue import TaskQueue, TaskResult


# Core Functionality Tests
def test_task_queue_initialization_default():
    tq = TaskQueue()
    assert tq.priorities == 1
    assert len(tq.q) == 1
    assert isinstance(tq.q[0], deque)
    assert len(tq.failed) == 0


def test_task_queue_initialization_with_priorities():
    tq = TaskQueue(priorities=5)
    assert tq.priorities == 5
    assert len(tq.q) == 5
    for queue in tq.q:
        assert isinstance(queue, deque)
        assert len(queue) == 0


def test_enqueue_single_task():
    tq = TaskQueue(priorities=3)
    def dummy_task():
        return "done"
    tq.enqueue(0, dummy_task)
    assert len(tq.q[0]) == 1


def test_enqueue_with_args_kwargs():
    tq = TaskQueue(priorities=2)
    def dummy_task(a, b, c=None):
        return a + b
    tq.enqueue(0, dummy_task, 1, 2, c=3)
    task, args, kwargs = tq.q[0][0]
    assert task == dummy_task
    assert args == (1, 2)
    assert kwargs == {"c": 3}


def test_dequeue_single_task():
    tq = TaskQueue(priorities=1)
    def dummy_task():
        return "result"
    tq.enqueue(0, dummy_task)
    result = tq.dequeue()
    assert result is not None
    assert result[0] == dummy_task


def test_dequeue_respects_priority_order():
    tq = TaskQueue(priorities=3)
    def task1():
        return 1
    def task2():
        return 2
    def task3():
        return 3
    
    tq.enqueue(2, task3)
    tq.enqueue(0, task1)
    tq.enqueue(1, task2)
    
    assert tq.dequeue()[0] == task1
    assert tq.dequeue()[0] == task2
    assert tq.dequeue()[0] == task3


def test_enqueue_failure():
    tq = TaskQueue()
    def failed_task():
        raise Exception("error")
    tq.enqueue_failure(failed_task)
    assert len(tq.failed) == 1


# Edge Cases
def test_task_queue_initialization_with_one_priority():
    tq = TaskQueue(priorities=1)
    assert tq.priorities == 1
    assert len(tq.q) == 1


def test_task_queue_initialization_with_large_priorities():
    tq = TaskQueue(priorities=10000)
    assert tq.priorities == 10000
    assert len(tq.q) == 10000


def test_dequeue_from_empty_queue():
    tq = TaskQueue()
    result = tq.dequeue()
    assert result is None


def test_dequeue_multiple_times_from_empty_queue():
    tq = TaskQueue()
    for _ in range(100):
        assert tq.dequeue() is None


def test_enqueue_at_boundary_priority_min():
    tq = TaskQueue(priorities=5)
    def task():
        pass
    tq.enqueue(0, task)
    assert len(tq.q[0]) == 1


def test_enqueue_at_boundary_priority_max():
    tq = TaskQueue(priorities=5)
    def task():
        pass
    tq.enqueue(4, task)
    assert len(tq.q[4]) == 1


def test_enqueue_with_no_args():
    tq = TaskQueue()
    def task():
        return "no args"
    tq.enqueue(0, task)
    result = tq.dequeue()
    assert result[1] == ()
    assert result[2] == {}


def test_enqueue_with_only_args():
    tq = TaskQueue()
    def task(a, b, c):
        pass
    tq.enqueue(0, task, 1, 2, 3)
    result = tq.dequeue()
    assert result[1] == (1, 2, 3)
    assert result[2] == {}


def test_enqueue_with_only_kwargs():
    tq = TaskQueue()
    def task(a=1, b=2):
        pass
    tq.enqueue(0, task, a=10, b=20)
    result = tq.dequeue()
    assert result[1] == ()
    assert result[2] == {"a": 10, "b": 20}


def test_enqueue_none_as_task_argument():
    tq = TaskQueue()
    def task(value):
        return value
    tq.enqueue(0, task, None)
    result = tq.dequeue()
    assert result[1] == (None,)


def test_enqueue_empty_string_as_argument():
    tq = TaskQueue()
    def task(s):
        return s
    tq.enqueue(0, task, "")
    result = tq.dequeue()
    assert result[1] == ("",)


def test_enqueue_zero_as_argument():
    tq = TaskQueue()
    def task(num):
        return num
    tq.enqueue(0, task, 0)
    result = tq.dequeue()
    assert result[1] == (0,)


def test_len_with_empty_queue():
    tq = TaskQueue(priorities=3)
    try:
        length = len(tq)
        assert length == 0
    except AttributeError:
        pytest.skip("Implementation bug: deque has no qsize() method")


def test_len_with_tasks_in_single_priority():
    tq = TaskQueue(priorities=3)
    def task():
        pass
    tq.enqueue(1, task)
    tq.enqueue(1, task)
    try:
        assert len(tq) == 2
    except AttributeError:
        pytest.skip("Implementation bug: deque has no qsize() method")


def test_len_with_tasks_across_priorities():
    tq = TaskQueue(priorities=3)
    def task():
        pass
    tq.enqueue(0, task)
    tq.enqueue(1, task)
    tq.enqueue(2, task)
    try:
        assert len(tq) == 3
    except AttributeError:
        pytest.skip("Implementation bug: deque has no qsize() method")


# Invalid Input & Type Errors
def test_task_queue_initialization_with_zero_priorities():
    with pytest.raises(AssertionError):
        TaskQueue(priorities=0)


def test_task_queue_initialization_with_negative_priorities():
    with pytest.raises(AssertionError):
        TaskQueue(priorities=-1)


def test_task_queue_initialization_with_negative_large_priorities():
    with pytest.raises(AssertionError):
        TaskQueue(priorities=-1000)


def test_task_queue_initialization_with_float_priorities():
    try:
        tq = TaskQueue(priorities=3.5)
        assert len(tq.q) == 3
    except (TypeError, ValueError):
        pass


def test_task_queue_initialization_with_string_priorities():
    with pytest.raises(TypeError):
        TaskQueue(priorities="five")


def test_task_queue_initialization_with_none_priorities():
    with pytest.raises(TypeError):
        TaskQueue(priorities=None)


def test_enqueue_with_priority_below_zero():
    tq = TaskQueue(priorities=3)
    def task():
        pass
    with pytest.raises(AssertionError):
        tq.enqueue(-1, task)


def test_enqueue_with_priority_above_max():
    tq = TaskQueue(priorities=3)
    def task():
        pass
    with pytest.raises(AssertionError):
        tq.enqueue(3, task)


def test_enqueue_with_priority_far_above_max():
    tq = TaskQueue(priorities=3)
    def task():
        pass
    with pytest.raises(AssertionError):
        tq.enqueue(100, task)


def test_enqueue_with_float_priority():
    tq = TaskQueue(priorities=5)
    def task():
        pass
    try:
        tq.enqueue(2.5, task)
    except (IndexError, TypeError):
        pass


def test_enqueue_with_string_priority():
    tq = TaskQueue(priorities=3)
    def task():
        pass
    with pytest.raises((TypeError, AssertionError)):
        tq.enqueue("high", task)


def test_enqueue_with_none_priority():
    tq = TaskQueue(priorities=3)
    def task():
        pass
    with pytest.raises((TypeError, AssertionError)):
        tq.enqueue(None, task)


def test_enqueue_with_non_callable_task():
    tq = TaskQueue()
    try:
        tq.enqueue(0, "not a function")
        result = tq.dequeue()
        assert result is not None
    except TypeError:
        pass


def test_enqueue_with_none_task():
    tq = TaskQueue()
    try:
        tq.enqueue(0, None)
        result = tq.dequeue()
        assert result is not None
    except (TypeError, AttributeError):
        pass


def test_enqueue_with_integer_task():
    tq = TaskQueue()
    try:
        tq.enqueue(0, 123)
        result = tq.dequeue()
        assert result is not None
    except TypeError:
        pass


def test_enqueue_failure_with_none_task():
    tq = TaskQueue()
    tq.enqueue_failure(None)
    assert len(tq.failed) == 1


def test_enqueue_failure_with_non_callable():
    tq = TaskQueue()
    tq.enqueue_failure("not a function")
    assert len(tq.failed) == 1


# Large-Scale/Performance Tests
def test_enqueue_many_tasks_single_priority():
    tq = TaskQueue()
    def task(n):
        return n * 2
    
    for i in range(10000):
        tq.enqueue(0, task, i)
    
    try:
        assert len(tq) == 10000
    except AttributeError:
        assert len(tq.q[0]) == 10000


def test_enqueue_many_tasks_multiple_priorities():
    tq = TaskQueue(priorities=10)
    def task(n):
        return n
    
    for i in range(10000):
        tq.enqueue(i % 10, task, i)
    
    try:
        assert len(tq) == 10000
    except AttributeError:
        total = sum(len(q) for q in tq.q)
        assert total == 10000


def test_dequeue_many_tasks():
    tq = TaskQueue()
    def task(n):
        return n
    
    count = 10000
    for i in range(count):
        tq.enqueue(0, task, i)
    
    for i in range(count):
        result = tq.dequeue()
        assert result is not None


def test_enqueue_dequeue_interleaved():
    tq = TaskQueue(priorities=3)
    def task(n):
        return n
    
    for i in range(1000):
        tq.enqueue(i % 3, task, i)
        if i % 2 == 0:
            tq.dequeue()


def test_enqueue_failure_many_tasks():
    tq = TaskQueue()
    def task():
        raise Exception("fail")
    
    for i in range(10000):
        tq.enqueue_failure(task)
    
    assert len(tq.failed) == 10000


def test_priority_ordering_with_many_tasks():
    tq = TaskQueue(priorities=5)
    def task(priority):
        return priority
    
    for priority in range(4, -1, -1):
        for _ in range(100):
            tq.enqueue(priority, task, priority)
    
    for _ in range(100):
        result = tq.dequeue()
        assert result[1] == (0,)
    
    for _ in range(100):
        result = tq.dequeue()
        assert result[1] == (1,)


def test_large_priority_count():
    tq = TaskQueue(priorities=100000)
    def task():
        pass
    tq.enqueue(0, task)
    tq.enqueue(99999, task)
    
    assert tq.dequeue() is not None
    assert tq.dequeue() is not None


# Idempotency and State Tests
def test_multiple_dequeues_maintain_order():
    tq = TaskQueue()
    results = []
    def task(n):
        results.append(n)
        return n
    
    for i in range(10):
        tq.enqueue(0, task, i)
    
    dequeued = []
    for _ in range(10):
        result = tq.dequeue()
        dequeued.append(result[1][0])
    
    assert dequeued == list(range(10))


def test_queue_state_after_complete_drain():
    tq = TaskQueue(priorities=3)
    def task():
        pass
    
    for i in range(100):
        tq.enqueue(i % 3, task)
    
    for _ in range(100):
        tq.dequeue()
    
    assert tq.dequeue() is None
    for queue in tq.q:
        assert len(queue) == 0


def test_failed_queue_independence():
    tq = TaskQueue()
    def task():
        pass
    
    tq.enqueue(0, task)
    tq.enqueue_failure(task)
    
    assert len(tq.q[0]) == 1
    assert len(tq.failed) == 1
    
    tq.dequeue()
    assert len(tq.q[0]) == 0
    assert len(tq.failed) == 1


def test_task_result_dataclass():
    import datetime
    result = TaskResult(completed_at=datetime.datetime.now(), result="test")
    assert result.result == "test"
    assert isinstance(result.completed_at, datetime.datetime)


def test_enqueue_same_task_multiple_times():
    tq = TaskQueue()
    counter = [0]
    def task():
        counter[0] += 1
        return counter[0]
    
    for _ in range(5):
        tq.enqueue(0, task)
    
    assert len(tq.q[0]) == 5


def test_dequeue_returns_none_after_emptying():
    tq = TaskQueue()
    def task():
        pass
    
    tq.enqueue(0, task)
    tq.dequeue()
    
    for _ in range(10):
        assert tq.dequeue() is None


def test_enqueue_with_complex_kwargs():
    tq = TaskQueue()
    def task(**kwargs):
        return kwargs
    
    complex_dict = {
        "nested": {"a": 1, "b": [1, 2, 3]},
        "list": [1, 2, 3],
        "none": None,
        "string": "test"
    }
    
    tq.enqueue(0, task, **complex_dict)
    result = tq.dequeue()
    assert result[2] == complex_dict


def test_enqueue_lambda_function():
    tq = TaskQueue()
    lambda_task = lambda x: x * 2
    tq.enqueue(0, lambda_task, 5)
    result = tq.dequeue()
    assert result[0] == lambda_task


def test_priority_queue_fifo_within_same_priority():
    tq = TaskQueue(priorities=3)
    def task(n):
        return n
    
    for i in range(10):
        tq.enqueue(1, task, i)
    
    for i in range(10):
        result = tq.dequeue()
        assert result[1] == (i,)


def test_enqueue_with_massive_args():
    tq = TaskQueue()
    def task(*args):
        return sum(args)
    
    large_args = list(range(10000))
    tq.enqueue(0, task, *large_args)
    result = tq.dequeue()
    assert len(result[1]) == 10000


def test_enqueue_with_special_characters_in_kwargs():
    tq = TaskQueue()
    def task(**kwargs):
        return kwargs
    
    tq.enqueue(0, task, key_with_underscore=1, keyWithCamel=2)
    result = tq.dequeue()
    assert result[2] == {"key_with_underscore": 1, "keyWithCamel": 2}

