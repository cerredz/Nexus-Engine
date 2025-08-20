# Bitmap implementation specialized for non-negative integer indices in a fixed universe.
# - Storage: packed bit array using a bytearray (1 bit per possible index).
# - Capacity: 2^b possible indices in [0, 2^b).
# - Operations: set/clear/test membership, union/intersect/xor, range set/clear, and basic constructors.
# - Cardinality: tracked incrementally via `size` (number of bits set to 1).
# - Complexity: O(1) membership set/clear/test; O(n_bytes) for set algebra and serialization.
# - Notes:
#   - Methods that accept collections (e.g., add/delete/contains) interpret inputs as iterables of indices.
#   - Unused tail bits in the last byte are masked by constructors and complement.
#   - This class focuses on correctness and clarity; word-wise optimizations are left for later.

class Bitmap():
    # Initialize the bitmap with b bits of address space (capacity 2^b).
    # - b: bit width; capacity = 1 << b.
    # - data: optional iterable of indices to set on construction (validated/added by add()).
    # Fields:
    # - b: the configured bit width.
    # - capacity: number of representable indices (2^b).
    # - _nbytes: number of bytes needed to store capacity bits.
    # - map: bytearray of packed bits (LSB-first within each byte).
    # - size: number of set bits (cardinality).
    def __init__(self, b: int = 16, data: list[int] = None):
        if b < 0:
            raise ValueError("Error, cannot initialize bitmap with less than 0 bits")
        
        self.b = b
        self.capacity = 1 << b
        self._nbytes = (self.capacity + 7) // 8
        self.map = bytearray(self._nbytes)  
        self.size = 0

        if data:
            self.add(data)

    # Return the number of set bits (cardinality). Mirrors len(bitmap).
    def __len__(self):
        return self.size
    
    # Equality by shape and contents:
    # - Returns NotImplemented for non-Bitmap.
    # - Returns False if bit widths differ.
    # - Otherwise compares packed byte arrays for exact equality.
    def __eq__(self, other: object):
        if not isinstance(other, Bitmap):
            return NotImplemented
        
        if self.b != other.b:
            return False
        
        return self.map == other.map

    # Placeholder for addition; not standard for set algebra.
    # - Currently requires same b; returns NotImplemented on mismatch or non-Bitmap.
    # - Prefer bitwise operators (|, &, ^) or the static methods (union/intersect/xor).
    def __add__(self, other: object):
        if not isinstance(other, Bitmap):
            return NotImplemented
        
        if self.b != other.b:
            return NotImplemented
        
        self.map = self.map + other.map

    # Placeholder for subtraction; not standard for set algebra.
    # - Currently requires same b; returns NotImplemented on mismatch or non-Bitmap.
    # - Prefer defining difference or using range/clear operations as needed.
    def __sub__(self, other):
        if not isinstance(other, Bitmap):
            return NotImplemented
        
        if self.b != other.b:
            return NotImplemented
        
        self.map = self.map - other.map

    # Truthiness: True if any bit is set; False if empty. O(1).
    def __bool__(self):
        return self.size > 0
    
    # Return configured bit width b (so capacity == 1 << b).
    def get_bits(self):
        return self.b
    
    # Return an immutable bytes copy of the packed bit array.
    # - External mutation cannot affect internal state.
    def get_map(self):
        # return a copy to avoid external mutation; use .map if you truly want direct access
        return bytes(self.map)

    # Bounds check helper for single index d; raises IndexError if out of [0, capacity).
    def _check_index(self, d: int):
        if d < 0 or d >= self.capacity:
            raise IndexError("Error, index out of range for Bitmap capacity")

    # Byte/bit location helper for index d.
    # - Returns (byte_index, bit_mask) where bit_mask has exactly one bit set.
    def _loc(self, d: int):
        # Returns (byte_index, bit_mask)
        return (d >> 3), (1 << (d & 7))

    # Set membership for all indices in the iterable `data`.
    # - Coerces inputs to int (rejects non-coercible types); rejects out-of-range indices.
    # - Idempotent: setting an already-set bit is a no-op for `size`.
    # - Complexity: O(k), where k is number of indices provided (amortized O(1) per bit).
    def add(self, data: list):
        for d in data:
            if type(d) is not int:  # avoid bool being accepted implicitly
                try:
                    d = int(d)
                except Exception:
                    raise TypeError("Error, Bitmap accepts only integers")
            self._check_index(d)
            bi, mask = self._loc(d)
            if (self.map[bi] & mask) == 0:
                self.size += 1
                self.map[bi] |= mask

    # Clear all bits and reset cardinality to zero. O(n_bytes).
    def clear(self):
        self.map = bytearray(self._nbytes)
        self.size = 0
        
    # Clear membership for all indices in the iterable `data`.
    # - Coerces and validates indices like add().
    # - Idempotent: clearing an already-clear bit is a no-op for `size`.
    # - Complexity: O(k), where k is number of indices provided.
    def delete(self, data):
        for d in data:
            if type(d) is not int:
                try:
                    d = int(d)
                except Exception:
                    raise TypeError("Error, Bitmap accepts only integers")
            self._check_index(d)
            bi, mask = self._loc(d)
            if (self.map[bi] & mask) != 0:
                self.size -= 1
                self.map[bi] &= (~mask) & 0xFF

    # Return True iff all provided indices are currently set.
    # - Returns False if any index is out of range, non-coercible, or currently clear.
    # - Complexity: O(k).
    def contains(self, data: list):
        for d in data:
            if type(d) is not int:
                try:
                    d = int(d)
                except Exception:
                    return False
            if d < 0 or d >= self.capacity:
                return False
            bi, mask = self._loc(d)
            if (self.map[bi] & mask) == 0:
                return False
        return True
    
    # Set algebra: union of two bitmaps with identical bit width.
    # - Returns a new Bitmap whose bits are the bitwise OR of the inputs.
    # - Complexity: O(n_bytes).
    @staticmethod
    def union(bitmap1: 'Bitmap', bitmap2: 'Bitmap'):
        if bitmap1.get_bits() != bitmap2.get_bits():
            raise ValueError("Error creating union for bitmaps, bitmaps must have same length for set operations.")
        bits = bitmap1.get_bits()
        out = Bitmap(bits)
        for i in range(out._nbytes):
            out.map[i] = bitmap1.map[i] | bitmap2.map[i]
        out.size = sum(int(b).bit_count() for b in out.map)
        return out

    # Set algebra: intersection of two bitmaps with identical bit width.
    # - Returns a new Bitmap whose bits are the bitwise AND of the inputs.
    # - Complexity: O(n_bytes).
    @staticmethod
    def intersect(bitmap1: 'Bitmap', bitmap2: 'Bitmap'):
        if bitmap1.get_bits() != bitmap2.get_bits():
            raise ValueError("Error creating intersect for bitmaps, bitmaps must have same length for set operations.")
        bits = bitmap1.get_bits()
        out = Bitmap(bits)
        for i in range(out._nbytes):
            out.map[i] = bitmap1.map[i] & bitmap2.map[i]
        out.size = sum(int(b).bit_count() for b in out.map)
        return out

    # Set algebra: symmetric difference (XOR) of two bitmaps with identical bit width.
    # - Returns a new Bitmap whose bits are the bitwise XOR of the inputs.
    # - Complexity: O(n_bytes).
    @staticmethod
    def xor(bitmap1: 'Bitmap', bitmap2: 'Bitmap'):
        if bitmap1.get_bits() != bitmap2.get_bits():
            raise ValueError("Error creating xor for bitmaps, bitmaps must have same length for set operations.")
        bits = bitmap1.get_bits()
        out = Bitmap(bits)
        for i in range(out._nbytes):
            out.map[i] = bitmap1.map[i] ^ bitmap2.map[i]
        out.size = sum(int(b).bit_count() for b in out.map)
        return out
    
    # Complement (within capacity) of a bitmap: flips every valid bit.
    # - Masks unused tail bits in the last byte so they remain 0.
    # - Complexity: O(n_bytes).
    @staticmethod
    def complement(bitmap: 'Bitmap'):
        bits = bitmap.get_bits()
        out = Bitmap(bits)
        full = bitmap.capacity // 8
        rem = bitmap.capacity & 7
        for i in range(full):
            out.map[i] = (~bitmap.map[i]) & 0xFF
        if rem:
            mask = (1 << rem) - 1
            out.map[full] = (~bitmap.map[full]) & mask
        out.size = sum(int(b).bit_count() for b in out.map)
        return out
    
    # Construct a bitmap from a packed bytes object.
    # - b: bit width for capacity.
    # - byts: exact-length buffer of _nbytes (mismatched length raises ValueError).
    # - Masks unused tail bits in the last byte and recomputes size.
    @staticmethod
    def from_bytes(b: int, byts: bytes):
        bm = Bitmap(b=b, data=None)
        if len(byts) != bm._nbytes:
            raise ValueError("Error, byte length does not match capacity for given b")
        bm.map[:] = byts
        rem = bm.capacity & 7
        if rem:
            bm.map[-1] &= (1 << rem) - 1
        bm.size = sum(int(x).bit_count() for x in bm.map)
        return bm
    
    # Construct a bitmap from a set of indices.
    # - Uses add() for validation and cardinality maintenance.
    @staticmethod
    def from_set(b: int, st: set):
        bitmap = Bitmap(b=b, data=list(st))
        return bitmap
        
    # Set all bits in the half-open interval [low, high).
    # - Validates bounds and updates cardinality only when flipping 0→1.
    # - Complexity: O(high - low).
    def set_range(self, low: int, high: int):
        # sets [low, high) as before; simple per-bit loop keeps size correct
        if low < 0 or low > high or high > self.capacity:
            raise ValueError("Error setting range, make sure low < high and in range of capacity.")
        for i in range(low, high):
            bi, mask = self._loc(i)
            if (self.map[bi] & mask) == 0:
                self.size += 1
                self.map[bi] |= mask

    # Clear all bits in the half-open interval [low, high).
    # - Validates bounds and updates cardinality only when flipping 1→0.
    # - Complexity: O(high - low).
    def clear_range(self, low: int, high: int):
        if low < 0 or low > high or high > self.capacity:
            raise ValueError("Error clearing range, make sure low < high and in range of capacity.")
        for i in range(low, high):
            bi, mask = self._loc(i)
            if (self.map[bi] & mask) != 0:
                self.size -= 1
                self.map[bi] &= (~mask) & 0xFF