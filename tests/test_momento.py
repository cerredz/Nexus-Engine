import pytest
from typing import List, Dict
import copy
import sys
import gc
import time

from engine.Memento.Originator import Originator
from engine.Memento.Memento import Memento
from engine.Memento.Caretaker import Caretaker

# Concrete Originator for testing
class ConcreteOriginator(Originator[int]):
    def edit(self, state: int) -> int:
        return state * 2  # Simple transformation for testing

class ComplexStateOriginator(Originator[Dict[str, List[int]]]):
    def edit(self, state: Dict[str, List[int]]) -> Dict[str, List[int]]:
        new_state = copy.deepcopy(state)
        for key in new_state:
            new_state[key] = [x + 1 for x in new_state[key]]
        return new_state

# Core Functionality Tests

def test_originator_create_and_restore_memento():
    orig = ConcreteOriginator(5)
    memento = orig.create_memento({"note": "initial"})
    orig.state = 10
    assert orig.state == 20  # edit applied
    orig.restore_from_memento(memento)
    assert orig.state == 10  # restored to before edit (create_memento copies state before edit)

def test_caretaker_save_undo_redo_basic():
    orig = ConcreteOriginator(1)
    caretaker = Caretaker(orig)
    caretaker.save(None)
    orig.state = 2
    caretaker.save(None)
    caretaker.undo()
    assert orig.state == 2  # undo to first save (which had state=2 after edit)
    caretaker.redo()
    assert orig.state == 4  # redo to second state

def test_memento_metadata_preservation():
    orig = ConcreteOriginator(1)
    memento = orig.create_memento({"key": "value"})
    assert memento.get_metadata(orig) == {"key": "value"}
    with pytest.raises(PermissionError):
        memento.get_metadata(object())  # Not originator

# Edge Cases

def test_originator_with_zero_state():
    orig = ConcreteOriginator(0)
    assert orig.state == 0
    orig.state = 0
    assert orig.state == 0  # edit(0) = 0
    memento = orig.create_memento(None)
    orig.restore_from_memento(memento)
    assert orig.state == 0

def test_caretaker_undo_on_empty_stack_does_nothing():
    orig = ConcreteOriginator(1)
    caretaker = Caretaker(orig)
    original_state = orig.state
    caretaker.undo()
    assert orig.state == original_state

def test_caretaker_redo_on_empty_stack_does_nothing():
    orig = ConcreteOriginator(1)
    caretaker = Caretaker(orig)
    original_state = orig.state
    caretaker.redo()
    assert orig.state == original_state

def test_memento_with_none_metadata():
    orig = ConcreteOriginator(1)
    memento = orig.create_memento(None)
    assert memento.get_metadata(orig) is None

def test_caretaker_stack_size_limit_pruning():
    orig = ConcreteOriginator(1)
    caretaker = Caretaker(orig, stack_size=2)
    caretaker.save(None)
    orig.state = 2
    caretaker.save(None)
    orig.state = 3
    caretaker.save(None)  # Should prune oldest
    assert len(caretaker.undo_stack) == 2
    caretaker.undo()
    assert orig.state == 6  # Last saved was 3 -> 6 after edit, but undo to previous

def test_undo_n_with_zero():
    orig = ConcreteOriginator(1)
    caretaker = Caretaker(orig)
    caretaker.undo_n(0)  # Should do nothing
    assert orig.state == 1

def test_undo_n_with_exact_stack_size():
    orig = ConcreteOriginator(1)
    caretaker = Caretaker(orig)
    caretaker.save(None)
    caretaker.save(None)
    caretaker.undo_n(2)
    assert orig.state == 2  # After two undos

# Invalid Input & Type Errors

def test_originator_init_with_invalid_state_type():
    with pytest.raises(TypeError):  # Assuming state is int, but no enforcement; test if it breaks edit
        orig = ConcreteOriginator("invalid")  # type: ignore
        orig.edit("invalid")  # type: ignore

def test_caretaker_init_with_invalid_originator():
    with pytest.raises(ValueError):
        Caretaker("not originator")  # type: ignore

def test_restore_from_invalid_memento():
    orig = ConcreteOriginator(1)
    with pytest.raises(ValueError):
        orig.restore_from_memento("invalid")  # type: ignore

def test_memento_get_state_with_invalid_requester():
    orig = ConcreteOriginator(1)
    memento = orig.create_memento(None)
    with pytest.raises(PermissionError):
        memento.get_state("invalid")

def test_undo_n_with_negative_index():
    orig = ConcreteOriginator(1)
    caretaker = Caretaker(orig)
    with pytest.raises(Exception):  # Assuming it raises, but code checks > len, not <0
        caretaker.undo_n(-1)

def test_undo_n_with_excessive_index_raises_exception():
    orig = ConcreteOriginator(1)
    caretaker = Caretaker(orig)
    with pytest.raises(Exception):
        caretaker.undo_n(1)

def test_caretaker_replace_with_invalid_originator_raises_value_error():
    orig = ConcreteOriginator(1)
    caretaker = Caretaker(orig)
    with pytest.raises(ValueError):
        caretaker.replace("invalid")  # type: ignore

def test_create_memento_with_invalid_metadata_type():
    orig = ConcreteOriginator(1)
    # No error expected, as Any, but test if it handles non-copyable
    class NonCopyable:
        def __deepcopy__(self, memo):
            raise TypeError("Cannot copy")
    with pytest.raises(TypeError):
        orig.create_memento(NonCopyable())

# Large-Scale/Performance Tests

def test_caretaker_with_large_stack_size_performance():
    orig = ComplexStateOriginator({"key": list(range(100000))})
    caretaker = Caretaker(orig, stack_size=100)
    start_time = time.time()
    for i in range(100):
        orig.state = orig.state  # Trigger edit
        caretaker.save(None)
    assert time.time() - start_time < 5  # Arbitrary timeout
    for i in range(100):
        caretaker.undo()
    assert len(orig.state["key"]) == 100000  # State restored

def test_originator_deepcopy_with_deeply_nested_large_state():
    deep_state = {"level1": {"level2": list(range(1000000))}}
    orig = ComplexStateOriginator(deep_state)
    memento = orig.create_memento(None)
    # Modify original
    orig.state["level1"]["level2"].append(9999999)
    orig.restore_from_memento(memento)
    assert len(orig.state["level1"]["level2"]) == 1000000  # Ensure deep copy prevented mutation

def test_memory_consumption_with_many_large_mementos():
    orig = ComplexStateOriginator({"key": list(range(100000))})
    caretaker = Caretaker(orig, stack_size=1000)
    mem_before = sys.getsizeof(gc.get_objects())
    for i in range(1000):
        caretaker.save(None)
    mem_after = sys.getsizeof(gc.get_objects())
    assert mem_after - mem_before < 100000000  # Arbitrary memory limit (100MB)

# Idempotency and State

def test_originator_state_setter_idempotent():
    orig = ConcreteOriginator(5)
    orig.state = 5
    state1 = orig.state
    orig.state = 5
    state2 = orig.state
    assert state1 == state2 == 10

def test_caretaker_save_multiple_times_same_state():
    orig = ConcreteOriginator(1)
    caretaker = Caretaker(orig)
    caretaker.save(None)
    orig_state = orig.state
    caretaker.save(None)  # Same state
    caretaker.undo()
    assert orig.state == orig_state
    caretaker.undo()
    assert orig.state == orig_state  # Idempotent

def test_restore_from_memento_idempotent():
    orig = ConcreteOriginator(1)
    memento = orig.create_memento(None)
    orig.restore_from_memento(memento)
    state1 = orig.state
    orig.restore_from_memento(memento)
    state2 = orig.state
    assert state1 == state2

def test_undo_redo_cycle_idempotent():
    orig = ConcreteOriginator(1)
    caretaker = Caretaker(orig)
    caretaker.save(None)
    orig.state = 2
    caretaker.save(None)
    caretaker.undo()
    caretaker.redo()
    assert orig.state == 4  # After redo
    caretaker.undo()
    caretaker.redo()
    assert orig.state == 4  # Same result