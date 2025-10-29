# Borda Count voting system implementation.
# Votes are ranked preference lists, where candidates are assigned points based on
# their position. The default system assigns (n-1, n-2, ..., 1, 0) points for first
# through last place respectively in an n-candidate ballot, but custom point allocations
# can be provided. The winner is determined by summing points across all ballots.

from typing import Set, List
from collections import defaultdict

# Compute the Borda Count winner from a set of ranked preference ballots.
# - candidates: Set of all candidate names appearing in ballots.
# - votes: List of ranked preference lists, where each list represents one voter's preferences.
# - points: Optional custom point allocation list. If not provided, defaults to [n-1, n-2, ..., 0] where
#   n is the number of candidates ranked by each voter. Must match the length of each voter's preference list.
# Returns: The candidate name with the highest total points, or "No Winner" if no votes were cast.
def borda_count(candidates: Set[str], votes: List[List[str]], points: List[int] = None):
    # initialize counts and points
    vote_counts = defaultdict(int)
    if not points: points = [len(votes) - i - 1 for i in range(0, len(votes[0]))]
    
    # count the points of the votes
    for ballot in votes:
        assert len(ballot) == len(points), "points list must be same length of voter's preference list"
        for i, candidate in enumerate(ballot):
            vote_points = points[i]
            vote_counts[candidate] += vote_points

    # determinte winner with whoeveer has the most points
    highest_points, winner = float("-inf"), ""
    for candidate, points in vote_counts.items():
        if points > highest_points:
            highest_points = points
            winner = candidate

    if highest_points == float("-inf"): return "No Winner"

    return winner


