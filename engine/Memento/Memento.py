# Memento class for the Memento design pattern, encapsulating a snapshot of state.
# It stores generic state T and optional metadata, enforcing access only by the Originator
# to maintain encapsulation. State and metadata are retrieved via getter methods that
# check the requester's type.

from typing import Generic, T, Optional, Any

class Memento(Generic[T]):
    # Initialize with the state to snapshot and optional metadata.
    # The state is stored privately, and metadata can be any object for additional context.
    def __init__(self, state: T, metadata: Optional[Any] = None):
        self._state = state
        self._metadata = metadata  # Store metadata here for the snapshot

    # Retrieve the stored state, but only if the requester is an Originator instance.
    # Raises PermissionError otherwise to prevent unauthorized access.
    def get_state(self, requester: Any) -> T:
        from .Originator import Originator
        if not isinstance(requester, Originator):  # Fixed type check
            raise PermissionError("Only Originator can access state")
        return self._state

    # Retrieve the stored metadata, accessible only by Originator for encapsulation.
    # Raises PermissionError if the requester is not an Originator.
    def get_metadata(self, requester: Any) -> Optional[Any]:
        """Retrieve metadata; accessible only by Originator for encapsulation."""
        from .Originator import Originator
        if not isinstance(requester, Originator):
            raise PermissionError("Only Originator can access metadata")
        return self._metadata

