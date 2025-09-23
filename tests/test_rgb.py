import pytest
import numpy as np
import time
import sys

from engine.Colors.rgb import rgb_to_hsv

# Core Functionality: Basic conversions with known RGB to HSV mappings
def test_rgb_to_hsv_black():
    rgb = np.array([[[0, 0, 0]]], dtype=np.uint8)
    hsv = rgb_to_hsv(rgb)
    assert hsv.shape == (1, 1, 3)
    assert np.allclose(hsv, [[[0, 0, 0]]])

def test_rgb_to_hsv_white():
    rgb = np.array([[[255, 255, 255]]], dtype=np.uint8)
    hsv = rgb_to_hsv(rgb)
    assert np.allclose(hsv, [[[0, 0, 1]]])

def test_rgb_to_hsv_red():
    rgb = np.array([[[255, 0, 0]]], dtype=np.uint8)
    hsv = rgb_to_hsv(rgb)
    assert np.allclose(hsv, [[[0, 1, 1]]])

def test_rgb_to_hsv_green():
    rgb = np.array([[[0, 255, 0]]], dtype=np.uint8)
    hsv = rgb_to_hsv(rgb)
    assert np.allclose(hsv, [[[120, 1, 1]]])

def test_rgb_to_hsv_blue():
    rgb = np.array([[[0, 0, 255]]], dtype=np.uint8)
    hsv = rgb_to_hsv(rgb)
    assert np.allclose(hsv, [[[240, 1, 1]]])

def test_rgb_to_hsv_gray():
    rgb = np.array([[[128, 128, 128]]], dtype=np.uint8)
    hsv = rgb_to_hsv(rgb)
    assert np.allclose(hsv, [[[0, 0, 128/255]]])

def test_rgb_to_hsv_multiple_pixels():
    rgb = np.array([[[255, 0, 0], [0, 255, 0], [0, 0, 255]]], dtype=np.uint8)
    hsv = rgb_to_hsv(rgb)
    expected = np.array([[[0, 1, 1], [120, 1, 1], [240, 1, 1]]])
    assert np.allclose(hsv, expected)

# Edge Cases: Boundary values, zeros, maxes, floating point inputs
def test_rgb_to_hsv_empty_array_raises_exception():
    with pytest.raises(Exception, match="Input must be  \(_x_x3\). "):
        rgb_to_hsv(np.array([]))

def test_rgb_to_hsv_zero_dimensions():
    rgb = np.zeros((0, 0, 3))
    hsv = rgb_to_hsv(rgb)
    assert hsv.shape == (0, 0, 3)
    assert hsv.dtype == np.float64

def test_rgb_to_hsv_min_rgb_values():
    rgb = np.array([[[0, 0, 0]]], dtype=np.uint8)
    hsv = rgb_to_hsv(rgb)
    assert np.allclose(hsv, [[[0, 0, 0]]])

def test_rgb_to_hsv_max_rgb_values():
    rgb = np.array([[[255, 255, 255]]], dtype=np.uint8)
    hsv = rgb_to_hsv(rgb)
    assert np.allclose(hsv, [[[0, 0, 1]]])

def test_rgb_to_hsv_floating_point_input():
    rgb = np.array([[[0.5, 0.5, 0.5]]], dtype=np.float32) * 255  # Equivalent to [127.5, 127.5, 127.5]
    hsv = rgb_to_hsv(rgb)
    assert np.allclose(hsv, [[[0, 0, 0.5]]], atol=1e-6)

def test_rgb_to_hsv_hue_wrap_around():
    rgb = np.array([[[255, 0, 1]]], dtype=np.uint8)  # Close to red, hue should be near 360/0
    hsv = rgb_to_hsv(rgb)
    assert np.allclose(hsv[0,0,0], 359.7647, atol=1e-4)  # (60 * ((0-1)/(255-0)) + 360) % 360 ≈ 359.7647

def test_rgb_to_hsv_delta_zero_non_zero_cmax():
    rgb = np.array([[[100, 100, 100]]], dtype=np.uint8)
    hsv = rgb_to_hsv(rgb)
    assert np.allclose(hsv, [[[0, 0, 100/255]]])

def test_rgb_to_hsv_invalid_values_out_of_range():
    rgb = np.array([[[-1, -1, -1]]], dtype=np.int16)  # Negative values; function doesn't validate range, but results may be incorrect
    hsv = rgb_to_hsv(rgb)
    assert not np.all(hsv >= 0)  # H/S/V may be negative or invalid, but no raise

# Large-Scale/Performance Tests: Big images, time/memory checks
def test_rgb_to_hsv_large_image_performance():
    large = np.random.randint(0, 256, size=(1000, 1000, 3), dtype=np.uint8)
    start_time = time.time()
    hsv = rgb_to_hsv(large)
    elapsed = time.time() - start_time
    assert hsv.shape == (1000, 1000, 3)
    assert elapsed < 1.0  # Arbitrary threshold for 1M pixels
    # Memory: Ensure no excessive usage (rough check via sys)
    mem_before = large.nbytes
    mem_after = hsv.nbytes
    assert mem_after == mem_before * 8  # HSV is float64 (8x uint8)

def test_rgb_to_hsv_extremely_large_image_does_not_crash():
    very_large = np.random.randint(0, 256, size=(5000, 5000, 3), dtype=np.uint8)  # 75M pixels
    try:
        hsv = rgb_to_hsv(very_large)
        assert hsv.shape == (5000, 5000, 3)
    except MemoryError:
        pass

# Idempotency and State: Multiple calls, same input same output (stateless function)
def test_rgb_to_hsv_idempotent_same_input_same_output():
    rgb = np.array([[[255, 0, 0]]], dtype=np.uint8)
    hsv1 = rgb_to_hsv(rgb)
    hsv2 = rgb_to_hsv(rgb)
    assert np.allclose(hsv1, hsv2)

def test_rgb_to_hsv_multiple_calls_different_inputs():
    rgb1 = np.array([[[255, 0, 0]]])
    rgb2 = np.array([[[0, 255, 0]]])
    hsv1 = rgb_to_hsv(rgb1)
    hsv2 = rgb_to_hsv(rgb2)
    assert not np.allclose(hsv1, hsv2)
    # Call again with rgb1
    hsv3 = rgb_to_hsv(rgb1)
    assert np.allclose(hsv1, hsv3)
