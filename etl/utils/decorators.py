import functools
from datetime import datetime
from etl.utils import pprint


def time_it(func):
    """ Decorator for timing functions """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        end = datetime.now()
        pprint.time(f'Function {func.__name__} took {end - start}')
        print()
        return result
    return wrapper

