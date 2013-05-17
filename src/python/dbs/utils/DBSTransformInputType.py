"""
Decorator to change the type of an url parameter. url parameters containing a
list are usually encoded as strings. This decorator uses cjson.decode to
re-transform them to a list
"""
import ast
from itertools import izip
from collections import namedtuple
from cherrypy import HTTPError

def transformInputType(*convert):
    def wrap_f(f):
        def wrapper(*args, **kwargs):
            params = dict(izip(f.func_code.co_varnames, args))
            for parameter in convert:
                try:
                    params['param'].kwargs[parameter] = ast.literal_eval(params['param'].kwargs[parameter])
                except (ValueError, SyntaxError, KeyError):
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
            yield run_tuple(min_run, max_run)
    else:
        raise HTTPError("500 Server Error", "Run_rage error")

def addDefaults(param, **kwargs):
    for key, default_value in kwargs.iteritems():
        if not param.kwargs.has_key(key):
            param.kwargs[key] = default_value
