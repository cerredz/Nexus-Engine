# Originator abstract base class for the Memento pattern.
# It holds mutable state of type T, provides a property for state access with an edit hook,
# and methods to create deep-copied Mementos and restore from them.
# Subclasses must implement the edit method to customize state modifications.

from collections import deque
from curses import meta
from typing import Any, Generic, T, Optional
from abc import ABC, abstractmethod
from .Memento import Memento
import copy

class Originator(ABC, Generic[T]):
    # Initialize with the initial state.
    def __init__(self, state: T):
        self._state = state

    # Property getter for the current state.
    @property
    def state(self) -> T:
        return self._state

    # Property setter that applies the abstract edit method before storing the new state.
    @state.setter
    def state(self, value: T):
        self._state = self.edit(value) 

    # Abstract method for subclasses to implement custom editing logic on state changes.
    # This allows validation, transformation, or other processing before setting the state.
    @abstractmethod
    def edit(self, state: T) -> T:
        pass

    # Create a Memento with a deep copy of the current state and optional metadata.
    # Metadata is also deep-copied if provided.
    def create_memento(self, metadata: Optional[Any]) -> 'Memento[T]':
        from .Memento import Memento  
        state_copy = self._deepcopy_state()
        if metadata:
            metadata = copy.deepcopy(metadata)

        return Memento(state_copy, metadata)

    # Restore the internal state from a given Memento, validating it's a valid Memento.
    # Accesses the state via the Memento's getter, which checks permissions.
    def restore_from_memento(self, memento: 'Memento[T]'):
        if not type(memento, Memento):
            raise ValueError("Invalid memento")
        self._state = memento.get_state(self)  

    # Internal helper to deep-copy the current state for Memento creation.
    def _deepcopy_state(self) -> T:
        return copy.deepcopy(self._state)
