"""
Decorator to change the type of an url parameter. url parameters containing a
list are usually encoded as strings. This decorator uses cjson.decode to
re-transform them to a list
"""
import ast
from itertools import izip

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
