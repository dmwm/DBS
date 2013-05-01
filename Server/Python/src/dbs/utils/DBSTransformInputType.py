"""
Decorator to change the type of an url parameter. url parameters containing a
list are usually encoded as strings. This decorator uses cjson.decode to
re-transform them to a list
"""
import ast

def transformInputType(*convert):
    def wrap_f(f):
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
