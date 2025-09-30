import sys
import pytest
import numpy as np
from PIL import Image

from engine.Image.Thresholding import simple_thresholding, truncated_thresholding, zero_threshold


# ============================================================================
# Core Functionality Tests
# ============================================================================

def test_simple_thresholding_basic_non_inverted():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    result = simple_thresholding(img, threshold=128)
    expected = np.array([[[0, 255, 255]]], dtype=np.uint8)
    assert np.array_equal(result, expected)
    assert result.dtype == np.uint8


def test_simple_thresholding_basic_inverted():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    result = simple_thresholding(img, threshold=128, inverted=True)
    # inverted: >= threshold -> 0, < threshold -> 255
    # 100 < 128 -> 255, 150 >= 128 -> 0, 200 >= 128 -> 0
    expected = np.array([[[255, 0, 0]]], dtype=np.uint8)
    assert np.array_equal(result, expected)
    assert result.dtype == np.uint8


def test_truncated_thresholding_basic():
    img = np.array([[[50, 150, 250]]], dtype=np.uint8)
    result = truncated_thresholding(img, threshold=200)
    expected = np.array([[[50, 150, 200]]], dtype=np.uint8)
    assert np.array_equal(result, expected)


def test_zero_threshold_basic():
    img = np.array([[[50, 150, 250]]], dtype=np.uint8)
    result = zero_threshold(img, threshold=100)
    expected = np.array([[[0, 150, 250]]], dtype=np.uint8)
    assert np.array_equal(result, expected)


# ============================================================================
# Edge Cases: Boundary Values
# ============================================================================

def test_simple_thresholding_threshold_zero():
    img = np.array([[[0, 128, 255]]], dtype=np.uint8)
    result = simple_thresholding(img, threshold=0)
    # All values >= 0, so all should be 255
    expected = np.array([[[255, 255, 255]]], dtype=np.uint8)
    assert np.array_equal(result, expected)


def test_simple_thresholding_threshold_255():
    img = np.array([[[0, 128, 255]]], dtype=np.uint8)
    result = simple_thresholding(img, threshold=255)
    # All values < 255 except 255, so mostly 0
    expected = np.array([[[0, 0, 255]]], dtype=np.uint8)
    assert np.array_equal(result, expected)


def test_simple_thresholding_all_zeros():
    img = np.zeros((2, 2, 3), dtype=np.uint8)
    result = simple_thresholding(img, threshold=1)
    expected = np.zeros((2, 2, 3), dtype=np.uint8)
    assert np.array_equal(result, expected)


def test_simple_thresholding_all_255():
    img = np.full((2, 2, 3), 255, dtype=np.uint8)
    result = simple_thresholding(img, threshold=128)
    expected = np.full((2, 2, 3), 255, dtype=np.uint8)
    assert np.array_equal(result, expected)


def test_truncated_thresholding_threshold_zero():
    img = np.array([[[50, 100, 150]]], dtype=np.uint8)
    result = truncated_thresholding(img, threshold=0)
    # All values capped at 0
    expected = np.array([[[0, 0, 0]]], dtype=np.uint8)
    assert np.array_equal(result, expected)


def test_truncated_thresholding_threshold_255():
    img = np.array([[[50, 100, 255]]], dtype=np.uint8)
    result = truncated_thresholding(img, threshold=255)
    # No values should be capped
    expected = img.copy()
    assert np.array_equal(result, expected)


def test_zero_threshold_threshold_zero():
    img = np.array([[[0, 100, 255]]], dtype=np.uint8)
    result = zero_threshold(img, threshold=0)
    # Only 0 becomes 0
    expected = np.array([[[0, 100, 255]]], dtype=np.uint8)
    assert np.array_equal(result, expected)


def test_zero_threshold_threshold_255():
    img = np.array([[[0, 100, 255]]], dtype=np.uint8)
    result = zero_threshold(img, threshold=255)
    # All values <= 255, so all become 0
    expected = np.zeros((1, 1, 3), dtype=np.uint8)
    assert np.array_equal(result, expected)


def test_single_pixel_image():
    img = np.array([[[128, 64, 192]]], dtype=np.uint8)
    result = simple_thresholding(img, threshold=100)
    expected = np.array([[[255, 0, 255]]], dtype=np.uint8)
    assert np.array_equal(result, expected)


def test_empty_image_dimensions():
    img = np.zeros((0, 0, 3), dtype=np.uint8)
    result = simple_thresholding(img, threshold=128)
    assert result.shape == (0, 0, 3)


# ============================================================================
# PIL Image Input Tests
# ============================================================================

def test_simple_thresholding_accepts_pil_image():
    pil_img = Image.new('RGB', (2, 2), color=(100, 150, 200))
    result = simple_thresholding(pil_img, threshold=128)
    assert isinstance(result, np.ndarray)
    assert result.dtype == np.uint8
    assert result.shape == (2, 2, 3)
    # 100 < 128 -> 0, 150 >= 128 -> 255, 200 >= 128 -> 255
    assert np.array_equal(result[0, 0], [0, 255, 255])


def test_truncated_thresholding_accepts_pil_image():
    pil_img = Image.new('RGB', (1, 1), color=(50, 150, 250))
    result = truncated_thresholding(pil_img, threshold=200)
    assert isinstance(result, np.ndarray)
    assert np.array_equal(result[0, 0], [50, 150, 200])


def test_zero_threshold_accepts_pil_image():
    pil_img = Image.new('RGB', (1, 1), color=(50, 150, 250))
    result = zero_threshold(pil_img, threshold=100)
    assert isinstance(result, np.ndarray)
    assert np.array_equal(result[0, 0], [0, 150, 250])


# ============================================================================
# Invalid Input & Type Errors
# ============================================================================

def test_simple_thresholding_invalid_threshold_negative():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    with pytest.raises(AssertionError, match="Threshold must be between 0 and 255"):
        simple_thresholding(img, threshold=-1)


def test_simple_thresholding_invalid_threshold_too_high():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    with pytest.raises(AssertionError, match="Threshold must be between 0 and 255"):
        simple_thresholding(img, threshold=256)


def test_simple_thresholding_invalid_threshold_float():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    with pytest.raises(AssertionError, match="Threshold must be an integer"):
        simple_thresholding(img, threshold=128.5)


def test_simple_thresholding_invalid_threshold_string():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    with pytest.raises(AssertionError, match="Threshold must be an integer"):
        simple_thresholding(img, threshold="128")


def test_simple_thresholding_invalid_threshold_none():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    with pytest.raises(AssertionError, match="Threshold must be an integer"):
        simple_thresholding(img, threshold=None)


def test_simple_thresholding_wrong_shape_2d():
    img = np.array([[100, 150, 200]], dtype=np.uint8)
    with pytest.raises(AssertionError, match="Points must be of shape"):
        simple_thresholding(img, threshold=128)


def test_simple_thresholding_wrong_shape_4d():
    img = np.array([[[[100, 150, 200]]]], dtype=np.uint8)
    with pytest.raises(AssertionError, match="Points must be of shape"):
        simple_thresholding(img, threshold=128)


def test_simple_thresholding_wrong_channels_1():
    img = np.array([[[100]]], dtype=np.uint8)
    with pytest.raises(AssertionError, match="Points must be of shape"):
        simple_thresholding(img, threshold=128)


def test_simple_thresholding_wrong_channels_4():
    img = np.array([[[100, 150, 200, 255]]], dtype=np.uint8)
    with pytest.raises(AssertionError, match="Points must be of shape"):
        simple_thresholding(img, threshold=128)


def test_simple_thresholding_wrong_dtype_float32():
    img = np.array([[[100.0, 150.0, 200.0]]], dtype=np.float32)
    with pytest.raises(AssertionError, match="Input array must be of dtype uint8"):
        simple_thresholding(img, threshold=128)


def test_simple_thresholding_wrong_dtype_int32():
    img = np.array([[[100, 150, 200]]], dtype=np.int32)
    with pytest.raises(AssertionError, match="Input array must be of dtype uint8"):
        simple_thresholding(img, threshold=128)


def test_simple_thresholding_none_input():
    with pytest.raises((AssertionError, AttributeError, TypeError)):
        simple_thresholding(None, threshold=128)


def test_truncated_thresholding_invalid_inputs():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    
    with pytest.raises(AssertionError):
        truncated_thresholding(img, threshold=-10)
    
    with pytest.raises(AssertionError):
        truncated_thresholding(img, threshold=300)


def test_zero_threshold_invalid_inputs():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    
    with pytest.raises(AssertionError):
        zero_threshold(img, threshold=-5)
    
    with pytest.raises(AssertionError):
        zero_threshold(img, threshold=500)


# ============================================================================
# Large-Scale / Performance Tests
# ============================================================================

def test_simple_thresholding_large_image():
    large_img = np.random.randint(0, 256, size=(1000, 1000, 3), dtype=np.uint8)
    result = simple_thresholding(large_img, threshold=128)
    assert result.shape == (1000, 1000, 3)
    assert result.dtype == np.uint8
    # Verify all values are either 0 or 255
    assert np.all((result == 0) | (result == 255))


def test_simple_thresholding_very_large_image():
    very_large = np.random.randint(0, 256, size=(2000, 2000, 3), dtype=np.uint8)
    result = simple_thresholding(very_large, threshold=100)
    assert result.shape == (2000, 2000, 3)
    assert result.dtype == np.uint8


def test_truncated_thresholding_large_image():
    large_img = np.random.randint(0, 256, size=(1000, 1000, 3), dtype=np.uint8)
    threshold = 150
    result = truncated_thresholding(large_img, threshold=threshold)
    assert result.shape == (1000, 1000, 3)
    # Verify no values exceed threshold
    assert np.all(result <= threshold)


def test_zero_threshold_large_image():
    large_img = np.random.randint(0, 256, size=(1000, 1000, 3), dtype=np.uint8)
    threshold = 100
    result = zero_threshold(large_img, threshold=threshold)
    assert result.shape == (1000, 1000, 3)
    # Verify values <= threshold are 0, others unchanged
    mask = large_img <= threshold
    assert np.all(result[mask] == 0)


def test_memory_efficiency_no_excessive_copies():
    img = np.random.randint(0, 256, size=(500, 500, 3), dtype=np.uint8)
    original_size = img.nbytes
    result = simple_thresholding(img, threshold=128)
    result_size = result.nbytes
    # Result should be same size as input
    assert result_size == original_size


# ============================================================================
# Idempotency and State Tests
# ============================================================================

def test_simple_thresholding_idempotent():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    result1 = simple_thresholding(img, threshold=128)
    result2 = simple_thresholding(img, threshold=128)
    result3 = simple_thresholding(img, threshold=128)
    assert np.array_equal(result1, result2)
    assert np.array_equal(result2, result3)


def test_simple_thresholding_does_not_modify_input():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    original = img.copy()
    result = simple_thresholding(img, threshold=128)
    assert np.array_equal(img, original)


def test_truncated_thresholding_idempotent():
    img = np.array([[[50, 150, 250]]], dtype=np.uint8)
    result1 = truncated_thresholding(img, threshold=200)
    result2 = truncated_thresholding(img, threshold=200)
    assert np.array_equal(result1, result2)


def test_zero_threshold_idempotent():
    img = np.array([[[50, 150, 250]]], dtype=np.uint8)
    result1 = zero_threshold(img, threshold=100)
    result2 = zero_threshold(img, threshold=100)
    assert np.array_equal(result1, result2)


def test_double_thresholding_idempotent():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    result1 = simple_thresholding(img, threshold=128)
    # Applying same threshold again should give same result
    result2 = simple_thresholding(result1, threshold=128)
    assert np.array_equal(result1, result2)


# ============================================================================
# Complex Scenarios
# ============================================================================

def test_simple_thresholding_mixed_values_per_channel():
    # Each channel has different distribution
    img = np.array([[[10, 100, 250], [50, 150, 200], [128, 128, 128]]], dtype=np.uint8)
    result = simple_thresholding(img, threshold=128)
    expected = np.array([[[0, 0, 255], [0, 255, 255], [255, 255, 255]]], dtype=np.uint8)
    assert np.array_equal(result, expected)


def test_truncated_threshold_no_change_when_all_below():
    img = np.array([[[10, 50, 100]]], dtype=np.uint8)
    result = truncated_thresholding(img, threshold=200)
    assert np.array_equal(result, img)


def test_zero_threshold_no_change_when_all_above():
    img = np.array([[[100, 150, 200]]], dtype=np.uint8)
    result = zero_threshold(img, threshold=50)
    assert np.array_equal(result, img)


def test_chain_operations():
    # Test chaining different thresholding operations
    img = np.random.randint(0, 256, size=(10, 10, 3), dtype=np.uint8)
    
    # Truncate first
    result1 = truncated_thresholding(img, threshold=200)
    assert np.all(result1 <= 200)
    
    # Then zero out low values
    result2 = zero_threshold(result1, threshold=50)
    assert np.all((result2 == 0) | (result2 > 50))
    
    # Finally binary threshold
    result3 = simple_thresholding(result2, threshold=100)
    assert np.all((result3 == 0) | (result3 == 255))


def test_gradient_image():
    # Create gradient from 0 to 255
    gradient = np.tile(np.arange(256, dtype=np.uint8).reshape(1, 256, 1), (1, 1, 3))
    result = simple_thresholding(gradient, threshold=128)
    
    # First 128 should be 0, rest 255
    assert np.all(result[0, :128, :] == 0)
    assert np.all(result[0, 128:, :] == 255)


def test_all_functions_with_same_input_produce_different_outputs():
    img = np.array([[[50, 150, 250]]], dtype=np.uint8)
    threshold = 100
    
    r1 = simple_thresholding(img, threshold=threshold)
    r2 = truncated_thresholding(img, threshold=threshold)
    r3 = zero_threshold(img, threshold=threshold)
    
    # All three should produce different results
    assert not np.array_equal(r1, r2)
    assert not np.array_equal(r2, r3)
    assert not np.array_equal(r1, r3)


def test_extreme_aspect_ratios():
    # Very wide image
    wide_img = np.random.randint(0, 256, size=(1, 1000, 3), dtype=np.uint8)
    result = simple_thresholding(wide_img, threshold=128)
    assert result.shape == (1, 1000, 3)
    
    # Very tall image
    tall_img = np.random.randint(0, 256, size=(1000, 1, 3), dtype=np.uint8)
    result = simple_thresholding(tall_img, threshold=128)
    assert result.shape == (1000, 1, 3)


def test_inverted_parameter_produces_opposite_results():
    img = np.array([[[50, 150, 200]]], dtype=np.uint8)
    threshold = 128
    
    normal = simple_thresholding(img, threshold=threshold, inverted=False)
    inverted = simple_thresholding(img, threshold=threshold, inverted=True)
    
    # They should not be equal
    assert not np.array_equal(normal, inverted)
    
    # Check specific logic:
    # normal: <128 -> 0, >=128 -> 255
    # inverted: >=128 -> 0, <128 -> 255
    assert np.array_equal(normal[0, 0], [0, 255, 255])
    assert np.array_equal(inverted[0, 0], [255, 0, 0])


def test_boundary_value_at_exact_threshold():
    # Test behavior when pixel value exactly equals threshold
    img = np.array([[[127, 128, 129]]], dtype=np.uint8)
    threshold = 128
    
    result = simple_thresholding(img, threshold=threshold, inverted=False)
    # 127 < 128 -> 0, 128 >= 128 -> 255, 129 >= 128 -> 255
    assert np.array_equal(result[0, 0], [0, 255, 255])
    
    result_inv = simple_thresholding(img, threshold=threshold, inverted=True)
    # 127 < 128 -> 255, 128 >= 128 -> 0, 129 >= 128 -> 0
    assert np.array_equal(result_inv[0, 0], [255, 0, 0])
