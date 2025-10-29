import math

def polsby_popper(area: float, perimeter: float):
    assert area > 0 and perimeter > 0, "area and perimeter must be greater than 0 for polsby popper"
    return (4 * math.pi * area) / math.pow(perimeter, 2)

def reock(area: float, smallest_bounding_circle_area: float):
    if area < 0 or smallest_bounding_circle_area < 0:
        raise ValueError("area and smallest bounding circle area must be greater than 0")
    
    return area / smallest_bounding_circle_area

def convex_hull(population: int, population_convex_hull: int):
    if population < 0 or population_convex_hull < 0:
        raise ValueError("inputs must be positive")
    return population / population_convex_hull

