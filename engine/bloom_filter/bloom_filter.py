# Bloom Filter implementation specialized for string keys.
# This structure supports fast, space-efficient membership tests with a tunable
# false-positive rate. It guarantees:
# - No false negatives (keys that were inserted will never return "absent").
# - Possible false positives (keys not inserted may return "maybe present").
#
# Design:
# - Uses a bit array (`bitarray`) with `size` bits (computed from capacity `n` and
#   target false-positive rate `p`) and `hashes` hash functions (k-optimal).
# - Derives k indices with "double hashing": h_i = (h1 + i * h2) mod size,
#   where h1 is SHA-256 and h2 is MD5 of the key (both truncated to 64 bits).
# - Interprets membership as:
#     exists(key) == True  -> "maybe present" (with probability ~ p of being absent)
#     exists(key) == False -> "definitely not present"
#
# API includes helpers for batch checks, union/intersection (with compatibility checks),
# and a capacity planner to compute (size bits, k) for given (n, p).

from typing import List
from bitarray import bitarray
from typing import Iterable, Tuple
import hashlib
import math

class BloomFilter():
    # Initialize a Bloom filter for an expected capacity `n` items and target false-positive
    # probability `p` (0 < p < 1). Computes:
    #   size (bits) = m_bits = -n * ln(p) / (ln 2)^2
    #   hashes (k)  = round((m_bits / n) * ln 2), at least 1
    # Allocates and zero-initializes the bit array.
    # Raises:
    #   Exception: if p <= 0 or p >= 1.
    def __init__(self, n: int, p: float = .1):
        if p <= 0 or p >= 1:
            raise Exception("Error rate for bloom filter must be between 0 and 1.")

        n = max(1, int(n))
        p = float(p)
        # m_bits = -n * ln(p) / (ln 2)^2
        m_bits = max(1, int(math.ceil(-n * math.log(p) / (math.log(2) ** 2))))
        # k_opt = (m_bits / n) * ln 2
        k_opt = max(1, int(round((m_bits / n) * math.log(2))))

        self.array = bitarray(m_bits)
        self.array.setall(0)
        self.size = m_bits
        self.hashes = k_opt
        self.false_positive = p
    
    # Print the raw bit values of the filter (diagnostics/educational use).
    def print_bf(self):
        for i in range(self.size):
            print(f"{self.array[i]}, ")
    
    # Return the underlying bit array. Useful for low-level operations or serialization.
    def get_bf(self):
        return self.array

    # Clear the filter (set all bits to 0). Keeps the same size and number of hashes.
    def reset(self):
        self.array.setall(0)
        
    # Compute k indices in [0, size) for a given key using double hashing.
    # - h1: SHA-256(key) truncated to 64 bits
    # - h2: MD5(key) truncated to 64 bits (fallback to 1 if zero)
    # Returns:
    #   List[int]: exactly `hashes` indices to probe/set.
    def hashed(self, key: str) -> List[int]:
        data = key.encode("utf-8")
        h1 = int.from_bytes(hashlib.sha256(data).digest()[:8], "big")
        h2 = int.from_bytes(hashlib.md5(data).digest()[:8], 'big') or 1
        m = self.size
        k = self.hashes
        return [(h1 + i * h2) % m for i in range(k)]

    # Pythonic membership: enables `key in bloom_filter`.
    # Equivalent to `exists(key)`.
    def __contains__(self, key):
        return self.exists(key)

    # Membership query:
    #   True  -> "maybe present" (with false-positive probability ~ p)
    #   False -> "definitely not present"
    def exists(self, key: str) -> bool:
        idxs = self.hashed(key)
        return all(self.array[i] == 1 for i in idxs)
        
    # Insert a key: sets the k corresponding bits to 1.
    def insert(self, key: str):
        idxs = self.hashed(key)
        for i in idxs:
            self.array[i] = 1

    # Batch membership checks. Returns a list of booleans in the same order as input keys.
    def contains_many(self, keys: Iterable[str]) -> List[bool]:
        return [self.exists(key) for key in keys]
        
    # Return the number of bits currently set to 1 (population count).
    # Useful to estimate saturation or current false-positive rate.
    def count_set_bits(self) -> int:
        return self.array.count(1)
    
    # In-place union with another Bloom filter (bitwise OR).
    # Requires both filters to have identical `size` and `hashes`; otherwise raises.
    # Effect: after union, the filter reports "maybe present" for keys present in either input.
    def union(self, bf: 'BloomFilter'):
        bf_array = bf.get_bf()
        if len(bf_array) != self.size or bf.hashes != self.hashes:
            raise Exception("Union failed: bloom filters must have same size and number of hashes.")
        for i in range(self.size):
            self.array[i] = self.array[i] or bf_array[i]

    # In-place intersection with another Bloom filter (bitwise AND).
    # Requires identical `size` and `hashes`; otherwise raises.
    # Effect: after intersection, "maybe present" only if both inputs were "maybe present".
    def intersect(self, bf: 'BloomFilter'):
        bf_array = bf.get_bf()
        if len(bf_array) != self.size or bf.hashes != self.hashes:
            raise Exception("Intersection failed: bloom filters must have same size and number of hashes.")
        for i in range(self.size):
            self.array[i] = self.array[i] and bf_array[i]
   
    # Capacity planner utility:
    # Given an expected number of items `n` and target false-positive rate `p`, return the
    # computed (m_bits, k) parameters that this implementation uses.
    @staticmethod
    def capacity_planner(n: int, p: float) -> Tuple[int, int]:
        if p <= 0 or p >= 1:
            raise Exception("Error rate for bloom filter must be between 0 and 1.")
        
        n = max(1, int(n))
        p = float(p)
        # m_bits = -n * ln(p) / (ln 2)^2
        m_bits = max(1, int(math.ceil(-n * math.log(p) / (math.log(2) ** 2))))
        # k_opt = (m_bits / n) * ln 2
        k_opt = max(1, int(round((m_bits / n) * math.log(2))))

        return (m_bits, k_opt)


if __name__ == "__main__":
    # Example usage (disabled): construct and plan parameters.
    # bf = BloomFilter(n=100, p=.1)
    # print(BloomFilter.capacity_planner(n=100, p=.001))
    pass