import numpy as np
from PIL import Image

def simple_thresholding(points: np.ndarray or Image.Image, threshold: int, inverted: bool = False):
    """
    Applies simple binary thresholding to an RGB image array.
    Pixels below the threshold become 0; above become 255.
    Assumes 8-bit uint8 values.
    """
    if isinstance(points, Image.Image):
        points = np.array(points.convert('RGB'))

    assert points.ndim == 3 and points.shape[2] == 3, "Points must be of shape (x,x,3)."
    assert points.dtype == np.uint8, "Input array must be of dtype uint8"
    assert threshold > 0, "Threshold must be greater than 0."
    assert threshold < 255, "Lower threshold must be lower than 255."



    return np.where(points < threshold, 0, 255).astype(np.unit8) if not inverted else np.where(points > threshold, 0, 255).astype(np.unit8)


    

