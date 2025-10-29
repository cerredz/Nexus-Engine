"""
Cordorcet voting method implementation.
Implements the Cordorcet method, which selects a candidate who would beat every other 
candidate in a head-to-head election. Returns None if no such candidate exists (a tie or circular preference issue).
Uses pairwise comparisons to determine if each candidate is a Cordorcet winner.
"""

from typing import List, Set

def wins(candidate1: str, candidate2: str, votes: List[List[str]]) -> bool:
    # evalutes winner from candidate1 perspectrive, 
    # returns true if candidate1 wins and false otherwise
    cand1_votes, cand2_votes = 0, 0
    for ballot in votes:
        for candidate in ballot:
            if candidate == candidate1:
                cand1_votes += 1
                break
            
            if candidate == candidate2:
                cand2_votes += 1
                break

    return cand1_votes >= cand2_votes # tie goes to candidate 1

def cordorcet(candidates: List[str], votes: List[List[str]]) -> str or None:
    # make sure we have votes
    if not candidates or not votes or not any(votes): return None
    assert len(candidates) == len(set(candidates)), "algorithm works, but have duplicate candidates in candidates list. Will result in no winners"

    n = len(candidates)

    # apply condorcet
    for candidate in candidates:
        is_condorcet_winner = True
        for opponent in candidates:
            if candidate != opponent and not wins(candidate, opponent, votes):
                is_condorcet_winner = False
                break
            
        if is_condorcet_winner:
            return candidate


    return None





