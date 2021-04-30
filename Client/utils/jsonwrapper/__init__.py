#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-


"""
JSON wrapper around different JSON python implementations.
We use simplejson (json), cjson and yajl JSON implementation.

NOTE: different JSON implementation handle floats in different way
Here are few examples


..doctest::

    r1={"ts":time.time()}
    print r1
    {'ts': 1374255843.891289}

Python json:

..doctest::

    print json.dumps(r1), json.loads(json.dumps(r1))
    {"ts": 1374255843.891289} {u'ts': 1374255843.891289}

CJSON:

..doctest::
    print cjson.encode(r1), cjson.decode(cjson.encode(r1))
    {"ts": 1374255843.89} {'ts': 1374255843.89}

YAJL:

..doctest::

    print yajl.dumps(r1), yajl.loads(yajl.dumps(r1))
    {"ts":1.37426e+09} {u'ts': 1374260000.0}

Therefore when records contains timestamp it is ADVISED to round it to integer.
Then json/cjson implementations will agreee on input/output, while yajl will
still differ (for that reason we can't use yajl).
"""

__author__ = "Valentin Kuznetsov <vkuznet@gmail.com>"

from future import standard_library
standard_library.install_aliases()

try:
    from exceptions import Exception
except ImportError:
    # Assuming we are running under python3 environment:
    pass

MODULE = None

try:
    import yajl
    MODULE = "yajl"
except ImportError:
    try:
        import cjson
        MODULE = "cjson"
    except ImportError:
        try:
            import ujson
            MODULE = "ujson"
        except ImportError:
            # use default JSON module
            import json
            MODULE = "json"


class EncodeError(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.name = str(self.__class__.__name__)
        self.message = message

    def __str__(self):
        return repr(self.name': '+self.message)


class DecodeError(Exception):
    def __init__(self):
        Exception.__init__(self, message)
        self.name = str(self.__class__.__name__)
        self.message = message

    def __str__(self):
        return repr(self.name': '+self.message)


def encode(source):
    try:
        return dumps(source)
    except Exception as ex:
        raise EncodeError(str(ex))


def decode(source):
    try:
        return load(source)
    except Exception as ex:
        raise DecodeError(str(ex))


def loads(idict, **kwargs):
    """
    Based on default MODULE invoke appropriate JSON decoding API call
    """
    if  MODULE == 'json':
        return json.loads(idict, **kwargs)
    elif MODULE == 'cjson':
        return cjson.decode(idict)
    elif MODULE == 'ujson':
        return ujson.decode(idict)
    elif MODULE == 'yajl':
        try: # yajl.loads("123") will fail
            res = yajl.loads(idict)
        except: # fall back into default python JSON
            res = json.loads(idict, **kwargs)
        return res
    else:
        raise Exception("Not support JSON module: %s" % MODULE)


def load(source):
    """
    Use json.load for back-ward compatibility, since cjson doesn't
    provide this method. The load method works on file-descriptor
    objects.
    """
    if  MODULE == 'json':
        return json.load(source)
    elif MODULE == 'cjson':
        data = source.read()
        return cjson.decode(data)
    elif MODULE == 'ujson':
        return ujson.load(source)
    elif MODULE == 'yajl':
        return yajl.load(source)
    else:
        raise Exception("Not support JSON module: %s" % MODULE)


def dumps(idict, **kwargs):
    """
    Based on default MODULE invoke appropriate JSON encoding API call
    """
    if  MODULE == 'json':
        return json.dumps(idict, **kwargs)
    elif MODULE == 'cjson':
        return cjson.encode(idict)
    elif MODULE == 'ujson':
        return ujson.dumps(idict)
    elif MODULE == 'yajl':
        return yajl.dumps(idict)
    else:
        raise Exception("JSON module %s is not supported" % MODULE)


def dump(doc, source):
    """
    Use json.dump for back-ward compatibility, since cjson doesn't
    provide this method. The dump method works on file-descriptor
    objects.
    """
    if  MODULE == 'json':
        return json.dump(doc, source)
    elif MODULE == 'cjson':
        stj = cjson.encode(doc)
        return source.write(stj)
    if  MODULE == 'ujson':
        return ujson.dump(doc, source)
    elif MODULE == 'yajl':
        return yajl.dump(doc, source)
    else:
        raise Exception("JSON module %s is not supported" % MODULE)


class JSONEncoder(object):
    """
    JSONEncoder wrapper
    """
    def __init__(self, **kwargs):
        self.encoder = json.JSONEncoder(**kwargs)
        if  kwargs and 'sort_keys' in kwargs:
            self.module = 'default'
        else:
            self.module = MODULE

    def encode(self, idict):
        """Decode JSON method"""
        if  self.module == 'cjson':
            return cjson.encode(idict)
        elif self.module == 'ujson':
            return ujson.encode(idict)
        elif self.module == 'yajl':
            return yajl.Encoder().encode(idict)
        return self.encoder.encode(idict)

    def iterencode(self, idict):
        "Encode input dict"
        return self.encoder.iterencode(idict)


class JSONDecoder(object):
    """
    JSONDecoder wrapper
    """
    def __init__(self, **kwargs):
        self.decoder = json.JSONDecoder(**kwargs)
        if  kwargs:
            self.module = 'default'
        else:
            self.module = MODULE

    def decode(self, istring):
        """Decode JSON method"""
        if  MODULE == 'cjson':
            return cjson.decode(istring)
        elif MODULE == 'ujson':
            return ujson.decode(istring)
        elif MODULE == 'yajl':
            return yajl.Decoder().decode(istring)
        return self.decoder.decode(istring)

    def raw_decode(self, istring):
        "Decode given string"
        return self.decoder.raw_decode(istring)
