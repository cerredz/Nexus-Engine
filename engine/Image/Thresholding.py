import numpy as np
from PIL import Image
from .util import image_validator

@image_validator
def simple_thresholding(points: np.ndarray or Image.Image, threshold: int, inverted: bool = False):
    """
    Applies simple binary thresholding to an RGB image array.
    
    Args:
        points: Input image as numpy array of shape (height, width, 3) with dtype uint8,
                or PIL Image that will be converted to RGB numpy array.
        threshold: Threshold value (0-255). Pixels are compared against this value.
        inverted: If False (default), pixels >= threshold become 255, others become 0.
                  If True, pixels >= threshold become 0, others become 255.
    
    Returns:
        numpy.ndarray: Thresholded image of same shape as input with dtype uint8.
                       Each channel is thresholded independently.
    
    Example:
        >>> img = np.array([[[100, 150, 200]]], dtype=np.uint8)
        >>> result = simple_thresholding(img, threshold=128)
        >>> # result: [[[0, 255, 255]]]
    """
    return np.where(points < threshold, 0, 255).astype(np.uint8) if not inverted else np.where(points >= threshold, 0, 255).astype(np.uint8)

@image_validator
def truncated_thresholding(points: np.ndarray or Image.Image, threshold: int):
    """
    Applies truncated thresholding to clip pixel values at a maximum threshold.
    
    Args:
        points: Input image as numpy array of shape (height, width, 3) with dtype uint8,
                or PIL Image that will be converted to RGB numpy array.
        threshold: Maximum allowed value (0-255). Pixels above this are clipped to threshold.
    
    Returns:
        numpy.ndarray: Image with values capped at threshold. Shape matches input.
                       Pixels > threshold become threshold, others remain unchanged.
    
    Example:
        >>> img = np.array([[[50, 150, 250]]], dtype=np.uint8)
        >>> result = truncated_thresholding(img, threshold=200)
        >>> # result: [[[50, 150, 200]]]
    """
    return np.where(points > threshold, threshold, points)

@image_validator
def zero_threshold(points: np.ndarray or Image.Image, threshold: int):
    """
    Applies zero thresholding by setting pixels at or below threshold to zero.
    
    Args:
        points: Input image as numpy array of shape (height, width, 3) with dtype uint8,
                or PIL Image that will be converted to RGB numpy array.
        threshold: Threshold value (0-255). Pixels at or below this become 0.
    
    Returns:
        numpy.ndarray: Image with low values zeroed out. Shape matches input.
                       Pixels <= threshold become 0, others remain unchanged.
    
    Example:
        >>> img = np.array([[[50, 150, 250]]], dtype=np.uint8)
        >>> result = zero_threshold(img, threshold=100)
        >>> # result: [[[0, 150, 250]]]
    """
    return np.where(points <= threshold, 0, points)



    

