import sys
sys.path.append("engine")
from Bitmap.Bitmap import Bitmap

def test_bitmap():
    # Test initialization
    bm = Bitmap(b=3)
    assert bm.b == 3
    assert bm.capacity == 8
    assert bm.size == 0
    assert len(bm.map) == 1
    assert bm.map[0] == 0

    # Test add single
    bm.add([1])
    assert bm.size == 1
    assert bm.contains([1])
    assert not bm.contains([2])

    # Test add multiple
    bm.add([2, 3, 1])  # 1 is duplicate
    assert bm.size == 3
    assert bm.contains([1,2,3])

    # Test delete
    bm.delete([2])
    assert bm.size == 2
    assert not bm.contains([2])
    assert bm.contains([1,3])

    # Test clear
    bm.clear()
    assert bm.size == 0
    assert not bm.contains([1])

    # Test range set
    bm.set_range(0, 4)
    assert bm.size == 4
    assert bm.contains([0,1,2,3])

    # Test range clear
    bm.clear_range(1,3)
    assert bm.size == 2
    assert bm.contains([0,3])
    assert not bm.contains([1])

    # Test union
    bm2 = Bitmap(b=3, data=[4,5])
    union = Bitmap.union(bm, bm2)
    assert union.size == 4
    assert union.contains([0,3,4,5])

    # Test intersect
    bm3 = Bitmap(b=3, data=[3,4])
    inter = Bitmap.intersect(bm, bm3)
    assert inter.size == 1
    assert inter.contains([3])

    # Test xor
    xor = Bitmap.xor(bm, bm3)
    assert xor.size == 2
    assert xor.contains([0,4])

    # Test complement
    comp = Bitmap.complement(bm)
    assert comp.size == 6
    assert comp.contains([1,2,4,5,6,7])

    # Test from_bytes
    bm_bytes = bm.get_map()
    bm4 = Bitmap.from_bytes(3, bm_bytes)
    assert bm4 == bm

    # Test from_set
    bm5 = Bitmap.from_set(3, {0,3})
    assert bm5 == bm

    # Edge cases
    try:
        Bitmap(b=-1)
        assert False
    except ValueError:
        pass

    bm.add([7])
    try:
        bm.add([8])
        assert False
    except IndexError:
        pass

    try:
        Bitmap.union(bm, Bitmap(b=4))
        assert False
    except ValueError:
        pass

    print("All Bitmap tests passed")

if __name__ == "__main__":
    test_bitmap()
