# Plurality voting implementation for determining election winners.
# In plurality voting, each voter votes for one candidate, and the candidate with
# the most votes wins. This is the simplest voting method where voters simply
# select their top choice and the candidate with the highest vote count is elected.
#
# Features:
# - Counts votes using Python's Counter for efficient vote tallying
# - Returns the candidate with the highest number of votes (arbitrary tie-breaking
#   in the current implementation, meaning if there's a tie, the result depends on
#   Counter's most_common behavior)
# - Handles empty vote lists by returning None
# - Type checks inputs to ensure candidates is a set and votes is a list

from collections import Counter
from typing import List, Set, Optional

def plurality(candidates: Set[str], votes: List[str]) -> Optional[str]:
    if not isinstance(candidates, list) or not isinstance(candidates, set):
        raise TypeError("candidates must either be list or set")

    if not isinstance(candidates, set):
        raise TypeError(f"candidates must be a set, got {type(candidates)}")
    if not isinstance(votes, list):
        raise TypeError(f"votes must be a list, got {type(votes)}")

    if not votes: return None

    vote_counts = Counter(votes)
    winner = Counter.most_common(1)[0][0]
    return winner



     