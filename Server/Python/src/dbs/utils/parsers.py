#!/usr/bin/env python

# system modules
import os
import sys
import yaml
import json
import cjson
import cStringIO as StringIO
import argparse
# import psutil

from collections import Mapping, Container
from json import JSONEncoder, JSONDecoder


# code taken from:
# https://code.tutsplus.com/tutorials/understand-how-much-memory-your-python-objects-use--cms-25609
def deep_getsizeof(o, ids):
    """Find the memory footprint of a Python object
    This is a recursive function that drills down a Python object graph
    like a dictionary holding nested dictionaries with lists of lists
    and tuples and sets.
    The sys.getsizeof function does a shallow size of only. It counts each
    object inside a container as pointer only regardless of how big it
    really is.
    :param o: the object
    :param ids:
    :return:
    """
    d = deep_getsizeof
    if id(o) in ids:
        return 0

    r = sys.getsizeof(o)
    ids.add(id(o))

    if isinstance(o, str) or isinstance(0, unicode):
        return r

    if isinstance(o, Mapping):
        return r + sum(d(k, ids) + d(v, ids) for k, v in o.iteritems())

    if isinstance(o, Container):
        return r + sum(d(x, ids) for x in o)

    return r

def jsonstreamer(func):
    """JSON streamer decorator"""
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        data = func (self, *args, **kwds)
        if  isinstance(data, dict):
            for chunk in JSONEncoder().iterencode(data):
                yield chunk
        elif  isinstance(data, list) or isinstance(data, types.GeneratorType):
            sep = ''
            for rec in data:
                if  sep:
                    yield sep
                for chunk in JSONEncoder().iterencode(rec):
                    yield chunk
                if  not sep:
                    sep = ', '
        else:
            msg = 'jsonstreamer, improper data type %s' % type(data)
            raise Exception(msg)
    return wrapper

def json_stream_decoder(istring):
    "Decode given json data into json stream"
    decoder = JSONDecoder()
    return decoder.decode(istring)

@jsonstreamer
def json_stream_encoder(data):
    "Encode given json data into json stream"
    return data

def parseFileObject(file_object, method='cjson'):
    """
    Helper function to decode given file object with given method.
    Supported methods are:
    - cjson, use cjson python module
    - json, use json python module
    - yaml, use yaml python module
    - json_stream, use json_stream, see json_stream_(en|de)coder methods
    """
    if method == 'cjson':
        body = file_object.read()
        indata = cjson.decode(body)
    elif method == 'json':
        indata = json.load(file_object)
    elif method == 'yaml':
        indata = yaml.load(file_object)
    elif method == 'json_stream':
        body = file_object.read()
        indata = json_stream_decoder(body)
    else:
        raise NotImplementedError
    return indata

def size_format(uinput):
    """
    Format file size utility, it converts file size into KB, MB, GB, TB, PB units
    """
    try:
        num = float(uinput)
    except Exception as exc:
        print_exc(exc)
        return "N/A"
    base = 1000. # CMS convention to use power of 10
    if  base == 1000.: # power of 10
        xlist = ['', 'KB', 'MB', 'GB', 'TB', 'PB']
    elif base == 1024.: # power of 2
        xlist = ['', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
    for xxx in xlist:
        if  num < base:
            return "%3.1f%s" % (num, xxx)
        num /= base

def convert2json_stream(fobj, oname=None):
    """
    Helper function to convert given file from JSON to json_stream format.
    User may provide optional oname (output file name) attribute to specify
    where output should be written
    """
    jstr = ''
    if isinstance(fobj, dict):
        for chunk in json_stream_encoder(fobj):
            jstr += chunk + '\n'
    elif hasattr(fobj, 'read'):
        for chunk in json_stream_encoder(json.load(open(fobj))):
            jstr += chunk + '\n'
    if oname:
        with open(oname, 'w') as ostream:
            ostream.write(jstr)
    return jstr

def convert2yaml(fobj, oname=None):
    """
    Helper function to convert given file from JSON to yaml format.
    User may provide optional oname (output file name) attribute to specify
    where output should be written. Code is based on python yaml module, see
    https://github.com/yaml/pyyaml
    """
    if isinstance(fobj, dict):
        indata = fobj
    elif hasattr(fobj, 'read'):
        indata = json.load(fobj)
    if oname:
        with open(oname, 'w') as ostream:
            ostream.write(yaml.dump(indata, Dumper=yaml.CDumper))
    return yaml.dump(indata, Dumper=yaml.CDumper)

def test(fname, jformat, times, use_gc=False, dump=False):
    import gc
    import psutil
    process = psutil.Process(os.getpid())
    for idx in range(times):
        print("test {}: {}".format(jformat, idx))
        mem0 = process.memory_info().rss
        with open(fname) as istream:
            indata = parseFileObject(istream, method=jformat)
            print('size: {}'.format(deep_getsizeof(indata, set())))
            if not idx and dump:
                print(json.dumps(indata))
            if use_gc:
                del indata
                gc.collect()
        mem1 = process.memory_info().rss
        rss = mem1-mem0
        print("memory: {} ({} bytes)".format(size_format(rss), rss))
        print("\n")

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", action="store",
            dest="fin", default="", help="Input file")
        self.parser.add_argument("--format", action="store",
            dest="format", default="", help="Output file")
        self.parser.add_argument("--times", action="store",
            dest="times", default="1", help="How many times to run a test, default 1")
        self.parser.add_argument("--use_gc", action="store_true",
            dest="use_gc", default=False, help="Use gc.collect() after each test")
        self.parser.add_argument("--dump", action="store_true",
            dest="dump", default=False, help="dump decoded object on stdout")

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    test(opts.fin, opts.format, int(opts.times), opts.use_gc, opts.dump)

if __name__ == '__main__':
    main()
