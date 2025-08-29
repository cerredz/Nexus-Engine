import sys
import pytest

sys.path.append("engine")
from Quadtree.Quadtree import Quadtree


def test_init_validation():
    with pytest.raises(ValueError):
        Quadtree(width=0, height=10, max_points=1)
    with pytest.raises(ValueError):
        Quadtree(width=10, height=0, max_points=1)
    with pytest.raises(ValueError):
        Quadtree(width=10, height=10, max_points=0)
    # Valid
    qt = Quadtree(width=10, height=10, max_points=2)
    assert qt.width == 10.0 and qt.height == 10.0 and qt.max_points == 2


def test_insert_and_query_basic():
    qt = Quadtree(width=100, height=100, max_points=4)
    assert qt.insert(10, 20, "A") is True
    pts = qt.query(10, 20)
    assert isinstance(pts, list)
    assert (10, 20, "A") in pts


def test_insert_out_of_bounds_and_query_outside():
    qt = Quadtree(width=50, height=50, max_points=2)
    # Right/top edge are exclusive
    assert qt.insert(50, 25, "X") is False
    assert qt.insert(25, 50, "Y") is False
    # Negative is out of bounds
    assert qt.insert(-1, 0, "Z") is False
    # Query outside returns None
    assert qt.query(100, 100) is None


def test_subdivision_and_quadrant_assignment():
    qt = Quadtree(width=8, height=8, max_points=1)
    # First goes to BL
    assert qt.insert(1, 1, "bl") is True
    # Second forces split and goes to TR
    assert qt.insert(6, 6, "tr") is True
    # Root should have subdivided
    assert qt.root.is_leaf is False
    # Query returns leaf contents for each coordinate
    bl_pts = qt.query(1, 1)
    tr_pts = qt.query(6, 6)
    assert (1, 1, "bl") in bl_pts
    assert (6, 6, "tr") in tr_pts


def test_delete_single_point_and_idempotency():
    qt = Quadtree(width=20, height=20, max_points=4)
    qt.insert(10, 10, "A")
    assert qt.delete(10, 10) is True
    # Now leaf contains no points at that location
    pts = qt.query(10, 10)
    assert isinstance(pts, list)
    assert all(not (x == 10 and y == 10) for (x, y, _) in pts)
    # Deleting again should report False
    assert qt.delete(10, 10) is False


def test_delete_removes_all_duplicates():
    qt = Quadtree(width=20, height=20, max_points=4)
    qt.insert(5, 5, "A")
    qt.insert(5, 5, "B")
    qt.insert(5, 5, "C")
    assert qt.delete(5, 5) is True
    pts = qt.query(5, 5)
    # All entries at (5,5) should be gone
    assert all(not (x == 5 and y == 5) for (x, y, _) in pts)
    # Subsequent delete returns False
    assert qt.delete(5, 5) is False


def test_condense_after_deletion():
    # Use max_points=2 so aggregated children <= 2 triggers condense
    qt = Quadtree(width=8, height=8, max_points=2)
    # Insert three points in three different quadrants to force subdivision
    qt.insert(1, 1, "p1")   # BL
    qt.insert(6, 1, "p2")   # BR -> triggers split at second insert
    qt.insert(1, 6, "p3")   # TL
    assert qt.root.is_leaf is False
    # Delete one to reduce aggregate to 2 -> should condense root
    assert qt.delete(6, 1) is True
    assert qt.root.is_leaf is True
    assert qt.root.children == [None, None, None, None]
    assert sorted(qt.root.points) == sorted([(1, 1, "p1"), (1, 6, "p3")])


def test_boundary_conditions_exclusive_upper_edges():
    qt = Quadtree(width=10, height=10, max_points=2)
    assert qt.insert(0, 0, "ok") is True
    assert qt.insert(9, 9, "ok2") is True
    # Exclusive at right/top edges
    assert qt.insert(10, 0, "bad") is False
    assert qt.insert(0, 10, "bad") is False


def test_float_insert_coordinates_allowed():
    qt = Quadtree(width=5.0, height=5.0, max_points=3)
    assert qt.insert(2.5, 1.5, "f1") is True
    # Query by an int coordinate inside same leaf returns the leaf contents
    pts = qt.query(2, 1)
    assert isinstance(pts, list)
    assert (2.5, 1.5, "f1") in pts
