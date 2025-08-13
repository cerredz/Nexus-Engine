from pydantic import BaseModel
from typing import Tuple, List

class DiffResult(BaseModel):
    first_difference: Tuple[int, int]
    leaf_differing_indices: List[int]
    differing_subtrees: List[Tuple[int, int]]
