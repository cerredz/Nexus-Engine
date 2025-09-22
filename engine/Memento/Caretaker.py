# Caretaker class for managing undo/redo history in the Memento pattern.
# It maintains bounded stacks (deques) of Mementos for an Originator, allowing save,
# undo, redo, and bulk undo operations. The stack size limits memory usage by evicting
# oldest entries when full.

from typing import Generic, TypeVar, List, Optional, Any, Deque
from .Originator import Originator
from .Memento import Memento  
from collections import deque

T = TypeVar('T')

class Caretaker(Generic[T]):

    # Initialize with an Originator and optional max stack size (default 128).
    # Validates the originator and initializes empty undo/redo deques.
    def __init__(self, originator: Originator[T], stack_size: int = 128):
        if not type(originator, Originator):
            raise ValueError('Must pass an Originator instance')

        self.originator = originator
        self.undo_stack: Deque[Memento[T]] = deque([])
        self.redo_stack: Deque[Memento[T]] = deque([])
        self.stack_size = stack_size

    # Save the current Originator state as a Memento with optional metadata.
    # Evicts oldest if undo stack is full, appends the new Memento, and clears redo stack.
    def save(self, metadata: Optional[Any]):
        if len(self.undo_stack) >= self.stack_size:
            self.undo_stack.popleft()
        """Save current state to undo stack and clear redo stack."""
        self.undo_stack.append(self.originator.create_memento(metadata))
        self.redo_stack.clear()

    # Undo the last change by popping from undo stack, saving current to redo, and restoring.
    # Does nothing if undo stack is empty.
    def undo(self):
        """Revert to previous state if available."""
        if not self.undo_stack:
            return  
        memento = self.undo_stack.pop()

        if len(self.redo_stack) >= self.stack_size:
            self.redo_stack.popleft()

        self.redo_stack.append(self.originator.create_memento())  
        self.originator.restore_from_memento(memento)

    # Perform multiple undos by calling undo repeatedly.
    # Raises Exception if requested index exceeds available undo states.
    def undo_n(self, index: int):
        if index > len(self.undo_stack):
            raise Exception(f"Cannot undo_n {index} times, not enough states saved.")
        
        for i in range(index):
            self.undo()

    # Redo the next change by popping from redo stack, saving current to undo, and restoring.
    # Does nothing if redo stack is empty.
    def redo(self):
        """Reapply next state if available."""
        if not self.redo_stack:
            return

        if len(self.undo_stack) >= self.stack_size:
            self.undo_stack.popleft()

        memento = self.redo_stack.pop()
        self.undo_stack.append(self.originator.create_memento())
        self.originator.restore_from_memento(memento)

    # Replace the managed Originator with a new one, clearing both stacks.
    # Validates the new originator.
    def replace(self, originator: Originator[T]):
        if not type(originator, Originator):
            raise ValueError('Must pass an Originator instance')

        self.originator = originator
        self.undo_stack: Deque[Memento[T]] = deque([])
        self.redo_stack: Deque[Memento[T]] = deque([])
