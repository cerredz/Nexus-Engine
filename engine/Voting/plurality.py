from collections import defaultdict
from typing import List, Set

def plurality(candidates: List or Set, participants: Set):
    
    if not isinstance(candidates, list) or not isinstance(candidates, set):
        raise TypeError("candidates must either be list or set")

    if not isinstance(participants, set):
        raise TypeError("participants must be set of strs")

    votes = {key: 0 for key in set(candidates)}
    max_votes, winner = 0, ""
    
    for participant_vote in participants:
        if not isinstance(participant_vote, str):
            raise TypeError("participants must be set of strs")
        
        if not participant_vote in votes:
            raise ValueError(f"participant {participant_vote} is not in the list of candidates")

        votes[participant_vote] += 1
        if votes[participant_vote] > :


        



     