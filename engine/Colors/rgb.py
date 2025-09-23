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


def rgb_to_greyscale(points: np.ndarray, key: str = None):
    """
        Converts an RGB image (3D NumPy array) to greyscale using various methods.

        Parameters:
        points (np.ndarray): A 3D array of shape (height, width, 3) where each pixel is an RGB triplet
                             with values in [0, 255]. The array should be of dtype float or int.
        key (str, optional): The method to use for greyscale conversion. Options are:
                             - None (default): Simple average of R, G, B.
                             - "Luminosity": Weighted average (0.299R + 0.587G + 0.114B).
                             - "Lightness": Average of the maximum and minimum components.
                             - "Maximum Component": The maximum of R, G, B.

        Returns:
        np.ndarray: A 2D array of shape (height, width) with greyscale values in [0, 255].

        Raises:
        Exception: If input is not a NumPy array, does not have the expected shape (NxMx3),
                   or if an invalid key is provided.

        Notes:
        - This is a vectorized implementation using NumPy for efficiency.
        - Assumes input values are in [0, 255]; output is also in [0, 255].
        - For empty input arrays, returns an empty 2D array.
    """

    if not isinstance(points, np.ndarray):
        raise Exception("Input must be numpy array.")

    if not points.ndim == 3 or not points.shape[-1] == 3:
        raise Exception("Input must be  (_x_x3). ")

    if points.size == 0:
        return np.zeros(points.shape, dtype=float)

    r, g, b = points[..., 0], points[..., 1], points[..., 2]

    if not key:
        greyscale = (r + g + b) / 3
        return greyscale
    
    match key:
        case "Luminosity":
            greyscale = (.299 * r + .587 * g + .114 * b)
        case "Lightness":
            greyscale = (np.maximum(r, g, b) + np.minimum(r,g, b)) / 2
        case "Maximum Component":
            greyscale = np.maximum(r,g, b)
        case _:
            raise Exception("Invalid greyscale algorithm key, must either be 'Luminosity', 'Lightness', or 'Maximum Component'")

    return greyscale

