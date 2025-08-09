from pydantic import BaseModel, Field
from typing import Optional

class Atom:
    id: str
    tombstone: False
    value: Optional[str] = None
    predecessor_id: Optional[str] = None
    tombstone: int = Field(default=0)
    site_id: str = "ROOT"
