import hashlib
import random
import pickle
import math

class HyperLogLog():
    def __init__(self, b):
        if b < 1:
            raise ValueError("b must be >= 1")
        self.b = b # bits used for bucket
        self.m = 2 ** b # num of buckets
        self.buckets = [0] * self.m # actual value of each bucket
        self.sum = float(self.m)  # Σ 2^{-M[i]} with all M[i]=0 -> m
        self.harmonic_mean = self.m / self.sum
        self.zero_buckets = self.m  # track zero registers for small-range correction
        
        if self.m == 16:
            self.HYPERLOGLOG_CONSTANT = 0.673
        elif self.m == 32:
            self.HYPERLOGLOG_CONSTANT = 0.697
        elif self.m == 64:
            self.HYPERLOGLOG_CONSTANT = 0.709
        else:
            self.HYPERLOGLOG_CONSTANT = 0.7213 / (1 + 1.079 / self.m)   
    def print(self):
        print(f"Bits: {self.b}, Number of Buckets (m): {self.m}, Sum: {self.sum}, Harmonic Mean: {self.harmonic_mean}")

    def get_buckets(self):
        return self.buckets
        
    def convert_into_bits(self, data):
        payload = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        total_bits = max(1, 2 * self.b)
        digest_bytes = (total_bits + 7) // 8
        bits = int.from_bytes(hashlib.shake_128(payload).digest(digest_bytes), 'big')
        return bin(bits)[2:].zfill(digest_bytes * 8)[:total_bits]

    def compute_recip(self, idx):
        if idx < 0 or idx >= len(self.buckets):
            raise ValueError("Error computing reciprocal of 2 ^ value, idx out of range.")
        
        value = self.buckets[idx]
        return 2 ** (-value)
    
    def compute_harmonic_mean(self):
        return (self.m / self.sum) if self.sum else float("inf")

    def update(self, data):
        idx = self.insert(data)
        if idx != -1:
            self.harmonic_mean = self.compute_harmonic_mean()

    def insert(self, data):
        if data is None:
            raise ValueError("Error, cannot insert type None into the HyperLogLog")

        bits = self.convert_into_bits(data)
        i = int(bits[:self.b], 2)          # bucket index
        rem = bits[self.b:]                # remainder (used for rho)
        pos = rem.find('1')
        rho = (pos + 1) if pos != -1 else (len(rem) + 1)  # leading zeros + 1

        old = self.buckets[i]
        if rho > old:
            # Maintain Σ 2^{-M[i]} incrementally
            if old == 0:
                self.zero_buckets -= 1
            self.sum += (2 ** -rho) - (2 ** -old)
            self.buckets[i] = rho
            return i
            
        return -1

    def update_many(self, data):
        for d in data:
            self.update(d)

    def estimate(self):
        # Raw estimate
        E = self.HYPERLOGLOG_CONSTANT * self.m * self.harmonic_mean
        # Small-range correction (linear counting) when appropriate
        if E <= 2.5 * self.m and self.zero_buckets > 0:
            return self.m * math.log(self.m / self.zero_buckets)
        return E
    

def _rse(m):
    return 1.04 / math.sqrt(m)

def _assert_within(estimate, truth, tolerance, name):
    rel_err = abs(estimate - truth) / truth if truth else 0.0
    print(f"{name}: estimate={estimate:.2f}, truth={truth}, rel_err={rel_err:.4f}, tol={tolerance:.4f}")
    assert rel_err <= tolerance, f"{name} failed: rel_err {rel_err:.4f} > tol {tolerance:.4f}"

def run_tests():
    random.seed(42)

    # Test 1: ~5k unique integers with b=12 (m=4096), allow a safe tolerance
    N = 5000
    h = HyperLogLog(12)
    data = list(range(N))
    h.update_many(data)
    est = h.estimate()
    tol = max(4 * _rse(h.m), 0.06)  # cushion for finite-hash width
    _assert_within(est, N, tol, "unique_ints_5k_b12")

    # Test 2: many duplicates from 2k unique values
    K = 2000
    base = list(range(-1000, 1000))
    draws = [random.choice(base) for _ in range(100000)]
    h = HyperLogLog(12)
    h.update_many(draws)
    est = h.estimate()
    tol = max(4 * _rse(h.m), 0.06)
    _assert_within(est, K, tol, "duplicates_100k_from_2k_unique_b12")

    # Test 3: mixed data types (~800 uniques)
    mix = []
    mix.extend(list(range(200)))
    mix.extend([f"s{i}" for i in range(200)])
    mix.extend([i + 0.123 for i in range(200)])
    mix.extend([(i % 2 == 0, i) for i in range(200)])
    true_n = len(set(mix))
    h = HyperLogLog(12)
    h.update_many(mix)
    est = h.estimate()
    tol = max(4 * _rse(h.m), 0.06)
    _assert_within(est, true_n, tol, "mixed_types_~800_b12")

    # Test 4: small N with b=10
    N = 100
    h = HyperLogLog(10)
    data = list(range(N))
    h.update_many(data)
    est = h.estimate()
    tol = max(4 * _rse(h.m), 0.10)  # slightly larger tolerance for very small N
    _assert_within(est, N, tol, "small_n_100_b10")

    # Test 5: negatives and very large ints (~2k uniques)
    negs = list(range(-1000, 0))
    larges = [10**12 + i for i in range(1000)]
    data = negs + larges
    h = HyperLogLog(12)
    h.update_many(data)
    est = h.estimate()
    tol = max(4 * _rse(h.m), 0.06)
    _assert_within(est, len(data), tol, "negatives_and_large_ints_2k_b12")

    print("All tests passed.")

if __name__ == "__main__":
    run_tests()