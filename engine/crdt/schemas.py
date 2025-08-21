from dataclasses import dataclass
from typing import Optional

@dataclass
class Atom:
	id: str
	value: Optional[str] = None
	predecessor_id: Optional[str] = None
	tombstone: bool = False
	timestamp: int = 0
	sequence: int = 0
	site_id: str = "ROOT"