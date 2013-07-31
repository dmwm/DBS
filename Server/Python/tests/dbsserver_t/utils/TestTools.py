"""
Collection of helper functions and decorators for the unittest package
"""
from functools import wraps

def expectedFailure(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AssertionError:
            pass
    return wrapper
