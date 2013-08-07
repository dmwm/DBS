"""
Decorator to change the type of an url parameter. url parameters containing a
list are usually encoded as strings. This decorator uses cjson.decode to
re-transform them to a list
"""
import ast
from collections import namedtuple
from functools import wraps

from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

def transformInputType(*convert):
    def wrap_f(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            for parameter in convert:
                try:
                    kwargs[parameter] = ast.literal_eval(kwargs[parameter])
                except ValueError:
                    pass
                except SyntaxError:
                    pass
                except KeyError:
                    pass
            return f(*args, **kwargs)
        return wrapper
    return wrap_f

run_tuple = namedtuple('run_tuple', ['min_run', 'max_run'])

def parseRunRange(run_range):
    if isinstance(run_range, list):
        for item in run_range:
            for new_item in parseRunRange(item):
                yield new_item
    elif isinstance(run_range, str):
        try:
            min_run, max_run = run_range.split('-', 1)
        except ValueError:
            yield run_range
        else:
            if not min_run: #run_range="-n"
                yield run_range
            else:
                yield run_tuple(min_run, max_run)
    elif isinstance(run_range, (int, long)):
        yield run_range
    else:
        dbsExceptionHandler('dbsException-invalid-input2', "Invalid input: run/run_range.",
        serverError="invalid run/run_range: %s" %run_range)
