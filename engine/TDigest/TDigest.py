# TDigest implementation specialized for streaming quantiles and mergeable summaries.
#
# This data structure incrementally summarizes a univariate distribution using a set of
# centroids (mean, weight). It preserves higher resolution near the distribution tails so
# that extreme quantiles (e.g., p99/p99.9) remain accurate, while using a compact memory
# footprint that is sub-linear in the number of observations.
#
# Key properties:
# - Streaming inserts: accepts one value at a time (optionally with integer weight)
# - Adaptive accuracy: smaller centroids near 0th/100th percentiles, larger in the middle
# - Mergeable: two digests can be merged and recompressed with accuracy guarantees
# - Fast queries: quantile and CDF queries are O(#centroids), which is typically small
#
# Provides:
# - push(x, weight): add value(s) to the digest
# - quantile(q): approximate value at quantile q ∈ [0, 1]
# - cdf(x): approximate fraction of observations ≤ x
# - merge(other): combine another digest into this one
# - compress(): reduce centroid count while respecting k-space constraints
# - summary(): convenient dictionary of common statistics and percentiles
from dataclasses import dataclass
from numbers import Number
import math
from typing import List, Tuple
from itertools import accumulate

@dataclass
class Centroid():
    mean: float
    weight: int

class TDigest():
    # Initialize the digest with accuracy/candidate selection parameters.
    # - alpha: compression/accuracy parameter used by the scaling function
    # - neighbors: how many nearby centroids to consider when attempting merges in push()
    # - compression_factor: soft bound for when to call compress() (size threshold)
    def __init__(self, alpha: int, neighbors: int, compression_factor: int):
        assert alpha > 0 and neighbors > 0 and compression_factor > 0, "alpha, neighbors, and compression_factor for t-digest must be greater than 0"
        self.neighbors: int = neighbors
        self.compression_factor: int = compression_factor
        self.alpha: int = alpha
        self.centroids: List[Centroid] = []
        self.total_weight, self.min_value, self.max_value = 0, float("inf"), float("-inf")

    def __repr__(self):
        return f"TDigest(alpha={self.alpha},neighbors={self.neighbors},compression_factor={self.compression_factor})"

    def __len__(self):
        return len(self.centroids)

    def _calculate_max_weight(self, delta_k: float) -> float:
        # Calculate the maximum weight allowed in a k-space region based on alpha and total weight
        return delta_k * (self.total_weight / self.alpha)

    def __scaling_function(self, q: float):
        assert 0 <= q <= 1, "q must be between 0 and 1 (inclusive) in the scaling function"
        return (self.alpha / (2 * math.pi)) * math.asin(2 * q - 1)

    def __get_centroid_merge_condidates(self, x: Number):
        # Get merge centroid candidates using neighbors parameter around the insertion index of x
        sum_weight = 0
        candidates: List[Tuple[Centroid, int]] = []
        insert_idx = self.__insert_idx(x)
        start_idx = max(0, insert_idx - (self.neighbors // 2))
        end_idx = min(len(self.centroids) - 1, insert_idx + (self.neighbors // 2))

        # get weights up until start_idx
        for i in range(start_idx):
            sum_weight += self.centroids[i].weight

        for i in range(start_idx, end_idx, 1):
            candidates.append((self.centroids[i], sum_weight))
            sum_weight += self.centroids[i].weight

        return candidates

    def __get_centroids_quantiles(self, candidates: List[Tuple[Centroid, int]]):
        qs: List[float] = []
        for cent, weight in candidates:
            q = (weight + (cent.weight / 2)) / self.total_weight
            qs.append(q)

        return qs

    def __insert_idx(self, x: Number):
        # Binary search for the position where x should be inserted to keep centroids sorted by mean
        left, right = 0, len(self.centroids) - 1
        while left < right:
            middle =  (left + right) // 2
            if self.centroids[middle].mean < x:
                left = middle + 1
            else:
                right = middle

        return left

    def __get_centroid_quantile(self, idx: int, centroid_weight: int):
        assert idx >= 0 and idx < len(self.centroids), "failed to __get_quantile, idx must be in range of centroids"
        weight = 0
        for i in range(idx):
            weight += self.centroids[i].weight
        
        return (weight + (centroid_weight / 2)) / self.total_weight

    def push(self, x: Number, weight: int = 1):
        # Streaming insert (optionally weighted). Attempts to merge into a nearby centroid
        # if the k-space constraint allows; otherwise inserts a new centroid.
        assert isinstance(x, Number), "push failed, x must be a number"
        # update min/max
        # Handle NaN specially since NaN comparisons always return False
        if math.isnan(x):
            self.min_value = x
            self.max_value = x
        else:
            if x < self.min_value: self.min_value = x
            if x > self.max_value: self.max_value = x

        # edge case: empty centroid list
        if len(self.centroids) == 0: 
            self.centroids.append(Centroid(mean=x, weight=weight))
            self.total_weight += weight
            return
        
        # find merge candiates / quantiles
        candidates = self.__get_centroid_merge_condidates(x)
        qs = self.__get_centroids_quantiles(candidates)
        
        # try to merge with an existing centroid
        for candidate_tuple, q in zip(candidates, qs):
            centroid, _ = candidate_tuple
            k_current = self.__scaling_function(q - centroid.weight / 2 / self.total_weight)
            k_next = self.__scaling_function(q + centroid.weight / 2 / self.total_weight)
            delta_k = k_next - k_current
            max_weight = self._calculate_max_weight(delta_k)

            if centroid.weight + weight <= max_weight:
                new_mean = (centroid.mean * centroid.weight + x * weight) / (centroid.weight + weight)
                centroid.mean = new_mean
                centroid.weight += weight
                self.total_weight += weight
                return
        
        # no merge possible, create new centroid
        insert_idx = self.__insert_idx(x)
        self.centroids.insert(insert_idx, Centroid(mean=x, weight=weight))
        self.total_weight += weight

        if len(self.centroids) > self.alpha * self.compression_factor:
            self.compress()

    def compress(self):
        # Compress the centroid list by merging adjacent centroids while respecting k-space limits.
        if len(self.centroids) <= 1:
            return
        # Ensure centroids are sorted by mean before compressing
        self.centroids.sort(key=lambda c: c.mean)
        new_list: List[Centroid] = []
        cumulative_weight = 0
        for centroid in self.centroids:
            if not new_list:
                # start with the first centroid
                new_list.append(Centroid(mean=centroid.mean, weight=centroid.weight))
                cumulative_weight += centroid.weight
                continue

            last = new_list[-1]
            # quantile at the center of the last centroid in the output
            q_center_last = (cumulative_weight - last.weight / 2) / self.total_weight
            k_current = self.__scaling_function(q_center_last - last.weight / 2 / self.total_weight)
            k_next = self.__scaling_function(q_center_last + last.weight / 2 / self.total_weight)
            delta_k = k_next - k_current
            max_weight = self._calculate_max_weight(delta_k)

            if last.weight + centroid.weight <= max_weight:
                # merge centroid into last
                combined_weight = last.weight + centroid.weight
                new_mean = (last.mean * last.weight + centroid.mean * centroid.weight) / combined_weight
                last.mean = new_mean
                last.weight = combined_weight
                cumulative_weight += centroid.weight
            else:
                # cannot merge; start a new centroid in output
                new_list.append(Centroid(mean=centroid.mean, weight=centroid.weight))
                cumulative_weight += centroid.weight

        # Replace old centroids with compressed list (total_weight remains unchanged)
        self.centroids = new_list

    def quantile(self, q: float):
        # Estimate the value at quantile q using linear interpolation between neighboring centroid centers.
        assert 0 <= q <= 1, "failed finding quantile, q must be in between 0 and 1 (inclusive)"
        assert len(self.centroids) > 0, "failed finding quantile (no centroids / numbers in t-digest)"

        if q == 0:
            return self.min_value
        if q == 1:
            return self.max_value
        if len(self.centroids) == 1:
            return self.centroids[0].mean

        # Ensure centroids are sorted by mean
        self.centroids.sort(key=lambda c: c.mean)

        target = q * self.total_weight
        centers: List[float] = []
        cumulative = 0.0
        for c in self.centroids:
            centers.append(cumulative + c.weight / 2)
            cumulative += c.weight

        # Interpolate using centroid centers
        if target <= centers[0]:
            left_rank = 0.0
            right_rank = centers[0]
            if right_rank == left_rank:
                return self.centroids[0].mean
            t = (target - left_rank) / (right_rank - left_rank)
            return self.min_value + t * (self.centroids[0].mean - self.min_value)

        for i in range(len(centers) - 1):
            if centers[i] < target <= centers[i + 1]:
                left_x = self.centroids[i].mean
                right_x = self.centroids[i + 1].mean
                left_rank = centers[i]
                right_rank = centers[i + 1]
                if right_rank == left_rank:
                    return left_x
                t = (target - left_rank) / (right_rank - left_rank)
                return left_x + t * (right_x - left_x)

        # Beyond last center, interpolate to max_value
        last_idx = len(self.centroids) - 1
        left_rank = centers[-1]
        right_rank = float(self.total_weight)
        if right_rank == left_rank:
            return self.centroids[last_idx].mean
        t = (target - left_rank) / (right_rank - left_rank)
        return self.centroids[last_idx].mean + t * (self.max_value - self.centroids[last_idx].mean)

    def merge(self, t_digest: 'TDigest'):
        # Merge another digest into this one by appending its centroids and compressing if needed.
        # Append copies of centroids to avoid aliasing between digests
        for centroid in t_digest.centroids:
            self.centroids.append(Centroid(mean=centroid.mean, weight=centroid.weight))
            self.total_weight += centroid.weight

        # Update bounds and sort prior to optional compression
        if t_digest.centroids:
            self.min_value = min(self.min_value, t_digest.min_value)
            self.max_value = max(self.max_value, t_digest.max_value)
        self.centroids.sort(key=lambda c: c.mean)

        if len(self.centroids) > self.alpha * self.compression_factor:
            self.compress()

    def cdf(self, x: Number):
        # Estimate the cumulative distribution function at x (fraction of observations ≤ x).
        # Handle empty
        if len(self.centroids) == 0:
            return 0.0
        # Edges
        if x < self.min_value:
            return 0.0
        if x >= self.max_value:
            return 1.0

        # Ensure sorted
        self.centroids.sort(key=lambda c: c.mean)

        # Build center ranks and means
        centers: List[float] = []
        means: List[float] = []
        cumulative = 0.0
        for c in self.centroids:
            centers.append(cumulative + c.weight / 2)
            means.append(c.mean)
            cumulative += c.weight

        total = float(self.total_weight)

        # Before first mean: interpolate from min_value to first mean
        if x <= means[0]:
            left_val = self.min_value
            right_val = means[0]
            left_rank = 0.0
            right_rank = centers[0]
            if right_val == left_val:
                rank = right_rank
            else:
                t = (x - left_val) / (right_val - left_val)
                rank = left_rank + t * (right_rank - left_rank)
            return rank / total

        # Between means: interpolate between adjacent centroid centers
        for i in range(len(means) - 1):
            if means[i] <= x <= means[i + 1]:
                left_val = means[i]
                right_val = means[i + 1]
                left_rank = centers[i]
                right_rank = centers[i + 1]
                if right_val == left_val:
                    # identical means — step at right centroid center
                    return right_rank / total
                t = (x - left_val) / (right_val - left_val)
                rank = left_rank + t * (right_rank - left_rank)
                return rank / total

        # After last mean: interpolate from last mean to max_value
        left_val = means[-1]
        right_val = self.max_value
        left_rank = centers[-1]
        right_rank = total
        if right_val == left_val:
            rank = right_rank
        else:
            t = (x - left_val) / (right_val - left_val)
            rank = left_rank + t * (right_rank - left_rank)
        return rank / total

    def median(self):
        return self.quantile(.5)

    def p95(self):
        return self.quantile(.95)

    def p99(self):
        return self.quantile(.99)

    def rank(self, x: Number):
        # Return the approximate rank (number of observations ≤ x)
        return self.cdf(x) * self.total_weight

    def summary(self):
        # Return a compact summary of the distribution.
        # Includes count, min, max, weighted mean, and common percentiles.
        if self.total_weight == 0:
            return {
                "count": 0,
                "min": None,
                "max": None,
                "mean": None,
                "p50": None,
                "p90": None,
                "p95": None,
                "p99": None,
            }

        weighted_sum = 0.0
        for c in self.centroids:
            weighted_sum += c.mean * c.weight

        return {
            "count": int(self.total_weight),
            "min": self.min_value,
            "max": self.max_value,
            "mean": weighted_sum / self.total_weight,
            "p50": self.quantile(0.50),
            "p90": self.quantile(0.90),
            "p95": self.quantile(0.95),
            "p99": self.quantile(0.99),
        }

    def min(self):
        return self.min_value

    def max(self):
        return self.max_value









        

