from typing import List
import numpy as np

def grow_point(point: int, scale: int):
    return [[point] * scale for i in range(scale)]

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

if __name__ == "__main__":
    points = [[1,2,3], [1,2,3]]
    print(points)
    points = nearest_neighbor_interpolation(points, 2)
    print(points)







