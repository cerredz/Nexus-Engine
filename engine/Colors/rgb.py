import numpy as np

def rgb_to_hsv(points: np.ndarray):
    """
    Converts an RGB image (3D NumPy array) to HSV color space.

    Parameters:
    points (np.ndarray): A 3D array of shape (height, width, 3) where each pixel is an RGB triplet
                         with values in [0, 255]. The array should be of dtype float or int.

    Returns:
    np.ndarray: A 3D array of the same shape with HSV values. Hue is in [0, 360), Saturation and
                Value in [0, 1].

    Raises:
    Exception: If input is not a NumPy array or does not have the expected shape (NxMx3).

    Notes:
    - This is a vectorized implementation using NumPy for efficiency.
    - Handles achromatic cases (delta == 0) by setting H=0.
    - Saturation is 0 when cmax == 0 to avoid division by zero.
    """
    if not isinstance(points, np.ndarray):
        raise Exception("Input must be numpy array.")

    if not points.ndim == 3 or not points.shape[-1] == 3:
        raise Exception("Input must be  (_x_x3). ")

    if points.size == 0:
        return np.zeros(points.shape, dtype=float)

    points = points / 255.0
    r, g, b = points[..., 0], points[..., 1], points[..., 2]
    cmax = np.maximum.reduce([r, g, b])
    cmin = np.minimum.reduce([r,g,b])
    delta = cmax - cmin

    h = np.zeros_like(cmax)
    mask_r = (cmax == r) & (delta > 0)
    h[mask_r] = (60 * ((g - b)[mask_r] / delta[mask_r]) + 360) % 360
    mask_g = (cmax == g) & (delta > 0)
    h[mask_g] = (60 * ((b - r)[mask_g] / delta[mask_g]) + 120) % 360
    mask_b = (cmax == b) & (delta > 0)
    h[mask_b] = (60 * ((r - g)[mask_b] / delta[mask_b]) + 240) % 360

    s = np.zeros_like(cmax)
    mask_s = cmax != 0
    s[mask_s] = delta[mask_s] / cmax[mask_s]

    v = cmax

    return np.stack([h, s, v], axis=-1)

