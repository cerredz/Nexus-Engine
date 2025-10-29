from typing import List
from collections import defaultdict

def approval_voting(votes: List[List[str]]) -> str or None:
    if not votes or not any(votes): return None

    vote_counts = defaultdict(int)
    highest_votes, winner = 0, ""

    if len(votes) == 0: return "No Winner"

    for ballot in votes:
        for candidate in ballot:
            vote_counts[candidate] += 1

            if vote_counts[candidate] > highest_votes:
                highest_votes = vote_counts[candidate]
                winner = candidate

    return winner

    
