# Nearest-Neighbor (NN) image upscaling for 2D integer grids.
# - Input: `points` is an HxW list-of-lists of ints (e.g., grayscale pixels).
# - Output: NumPy `ndarray` of shape (H * scale, W * scale) with dtype=int.
# - Method: each source value is replicated into a `scale x scale` block.
# - Constraints: `scale` must be an integer > 1. Non-integer scaling is not supported.
# - Complexity: time O(H * W * scale^2), memory O(H * W * scale^2).
# - Notes: favors simplicity and determinism; no smoothing/anti-aliasing is performed.
#
# Example
# -------
# points = [
#     [1, 2],
#     [3, 4],
# ]
# up = nearest_neighbor_interpolation(points, scale=2)
# # up ->
# # array([[1, 1, 2, 2],
# #        [1, 1, 2, 2],
# #        [3, 3, 4, 4],
# #        [3, 3, 4, 4]])
 
from typing import List
import numpy as np
 
# Helper: tile a single integer into a `scale x scale` block.
# Returns a Python list-of-lists (not a NumPy array).
def grow_point(point: int, scale: int):
    return [[point] * scale for i in range(scale)]
 
# Nearest-neighbor upscaling of a 2D integer grid.
# Parameters:
# - points: HxW list-of-lists of ints. All rows must be the same length.
# - scale: integer > 1 (defaults to 2). Values <= 1 are rejected.
# Behavior:
# - Raises ValueError if `scale <= 1`.
# - Allocates the destination array and fills it with `scale x scale` tiles per source.
# Returns:
# - NumPy array of shape (H * scale, W * scale), dtype=int.
# Caveats:
# - The function expects `scale` to be an integer; fractional factors are not supported.
# - Input validation is minimal; malformed (ragged) inputs may raise an IndexError.
def nearest_neighbor_interpolation(points: List[List[int]], scale: int = 2.0):
    if scale <= 1: raise ValueError("Error, nn interpolation only supports ints of greater than 1")
    n, m = len(points) ,len(points[0])
 
    # initialize new points array
    new_points = np.zeros((n * scale, m * scale), dtype=int)
 
    # interpolate all points and add to new array
    for i in range(n):
        for j in range(m):
            new_point = grow_point(points[i][j], scale)
            new_points[i * scale:i * scale + scale, j * scale:j * scale + scale]= new_point
 
    return new_points
 
 
 
 
 
 