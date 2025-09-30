import numpy as np
from PIL import Image
from typing import Callable
from functools import wraps

def validate_points(points: np.ndarray or Image.Image):
    if isinstance(points, Image.Image):
            points = np.array(points.convert('RGB'))

    assert points.ndim == 3 and points.shape[2] == 3, "Points must be of shape (x,x,3)."
    assert points.dtype == np.uint8, "Input array must be of dtype uint8"

def validate_threshold(threshold: int):
    assert isinstance(threshold, int), "Threshold must be an integer."
    assert 0 <= threshold <= 255, "Threshold must be between 0 and 255 (inclusive)."

def image_validator(func: Callable):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Convert args to list for modification
        args = list(args)
        
        # Validate and convert points (first argument or 'points' kwarg)
        if args:
            if isinstance(args[0], Image.Image):
                args[0] = np.array(args[0].convert('RGB'))
            validate_points(args[0])
        elif 'points' in kwargs:
            if isinstance(kwargs['points'], Image.Image):
                kwargs['points'] = np.array(kwargs['points'].convert('RGB'))
            validate_points(kwargs['points'])
        
        # Validate threshold (second argument or 'threshold' kwarg)
        if len(args) > 1:
            if isinstance(args[1], int):
                validate_threshold(args[1])
        elif 'threshold' in kwargs:
            validate_threshold(kwargs['threshold'])
        
        return func(*args, **kwargs)
    return wrapper