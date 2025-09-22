from typing import List
import numpy as np

def flip_vertical(points: List[List[int]]):
    arr = np.array(points)
    return np.flip(arr, axis=0)

def flip_horizontal(points: List[List[int]]):
    arr = np.array(points)
    return np.flip(arr, axis=1)

def flip_degrees(points: List[List[int]], degree:int, clockwise: bool=True):
    pass
