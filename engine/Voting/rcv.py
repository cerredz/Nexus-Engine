# Ranked Choice Voting (RCV) Implementation
# 
# This module implements the Ranked Choice Voting (also known as Instant Runoff Voting)
# electoral system. In RCV, voters rank candidates in order of preference. If no candidate
# receives a majority (>50%) of first-choice votes, the candidate with the fewest votes
# is eliminated, and their votes are redistributed to the next-ranked candidates on those
# ballots. This process repeats until a candidate achieves a majority or only one remains.
#
# The algorithm:
# 1. Count first-choice votes for all active candidates
# 2. If any candidate has >50% of votes, they win immediately
# 3. Otherwise, eliminate the candidate with the fewest votes
# 4. Repeat from step 1 with remaining candidates
#
# This implementation handles ballots as ranked lists where earlier positions indicate
# higher preference. Only the highest-ranked active candidate on each ballot receives
# that ballot's vote in each round.

import enum
from typing import Set, List
from collections import Counter, defaultdict
import heapq

MAJORITY=.5

# Determine the winner of an election using Ranked Choice Voting.
#
# Args:
#     candidates: Set of all valid candidate identifiers (strings).
#     votes: List of ballots, where each ballot is a list of candidate strings ranked
#            from most preferred (index 0) to least preferred. Ballots may be incomplete
#            (not ranking all candidates) or contain only a subset of candidates.
#
# Returns:
#     str: The identifier of the winning candidate. Returns the last remaining candidate
#          if no candidate achieves a majority (>50%) through the elimination process.
#
# Algorithm:
#     - Maintains a set of active candidates, eliminating one per round
#     - Each round counts votes by finding the first active candidate on each ballot
#     - Checks if any candidate has majority (>50% of votes cast for active candidates)
#     - If no majority, eliminates the candidate with fewest votes
#     - Continues until one candidate remains or a majority winner emerges
#
# Note:
#     - Ballots ranking eliminated candidates effectively skip to their next preference
#     - Ties in elimination (multiple candidates with minimum votes) are broken arbitrarily
#       by whichever candidate is encountered last during iteration
def rcv(candidates: Set[str], votes: List[List[str]]):
    active_candidates = set(candidates)
    total_votes = len(votes)

    while len(active_candidates) > 1:
        vote_counts = defaultdict(int)
        for ballot in votes:
            for candidate in ballot:
                if candidate in active_candidates:
                    vote_counts[candidate] += 1
                    break

        current_total_votes = sum(vote_counts.values())
        for candidate, count in vote_counts.items():
            if float(count) / current_total_votes > 0.5:
                return candidate 
        
        min_votes, winner = float("inf"), ""
        for candidate in active_candidates:
            count = vote_counts.get(candidate, 0)
            if count < min_votes:
                min_votes = count
                loser = candidate

        active_candidates.remove(loser)
        
    return list(active_candidates)[0]